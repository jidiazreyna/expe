# -*- coding: utf-8 -*-


#!/usr/bin/env python3


"""
Descarga un expediente del SAC (vÃƒÂ­a Teletrabajo -> Portal de Aplicaciones -> Intranet),
adjuntos incluidos, y arma un ÃƒÂºnico PDF.
"""

import os, sys, tempfile, shutil, datetime, threading, re, logging, contextlib
from pathlib import Path
from tkinter import Tk, StringVar, BooleanVar, filedialog, messagebox, Canvas, Frame, Label, Menu
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from PyPDF2 import PdfReader, PdfWriter, PdfMerger
from reportlab.pdfgen import canvas
from PIL import Image, ImageTk
import requests, mimetypes
from urllib.parse import quote, urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import queue
from tkinter import Toplevel, ttk
from tkinter.scrolledtext import ScrolledText
from tempfile import TemporaryDirectory
import subprocess
import asyncio

try:
    import ttkbootstrap as tb
    _TTKBOOTSTRAP_OK = True
except Exception:
    tb = None
    _TTKBOOTSTRAP_OK = False

UI_THEME = (os.getenv("SAC_UI_THEME") or "flatly").strip() or "flatly"


def _subprocess_hidden_kwargs() -> dict:
    if os.name != "nt":
        return {}
    kwargs = {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0)}
    try:
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
        kwargs["startupinfo"] = startupinfo
    except Exception:
        pass
    return kwargs


def _convert_office_with_word(path: Path) -> Path | None:
    if os.name != "nt" or path.suffix.lower() not in {".doc", ".docx", ".rtf"}:
        return None
    try:
        import win32com.client  # type: ignore
    except Exception:
        return None

    pdf = path.with_suffix(".pdf")
    word = None
    doc = None
    try:
        logging.info(f"[CNV:WORD] {path.name} -> {pdf.name}")
        word = win32com.client.DispatchEx("Word.Application")
        word.Visible = False
        word.DisplayAlerts = 0
        doc = word.Documents.Open(str(path), ReadOnly=True, AddToRecentFiles=False)
        doc.ExportAsFixedFormat(str(pdf), 17)
        if pdf.exists() and _is_real_pdf(pdf):
            logging.info(f"[CNV:WORD:OK] {pdf.name}")
            return pdf
    except Exception as e:
        logging.info(f"[CNV:WORD:ERR] {path.name} · {e}")
    finally:
        try:
            if doc is not None:
                doc.Close(False)
        except Exception:
            pass
        try:
            if word is not None:
                word.Quit()
        except Exception:
            pass
    return None


def _convert_docx_text_to_pdf(path: Path) -> Path | None:
    if path.suffix.lower() != ".docx":
        return None
    try:
        import zipfile
        import xml.etree.ElementTree as ET
    except Exception:
        return None

    try:
        with zipfile.ZipFile(path) as zf:
            xml_bytes = zf.read("word/document.xml")
    except Exception as e:
        logging.info(f"[CNV:DOCXTXT:ERR] {path.name} · {e}")
        return None

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        logging.info(f"[CNV:DOCXTXT:ERR] {path.name} · {e}")
        return None

    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []
    for p in root.findall(".//w:p", ns):
        chunks = []
        for t in p.findall(".//w:t", ns):
            if t.text:
                chunks.append(t.text)
        line = _norm_ws("".join(chunks))
        if line:
            paragraphs.append(line)

    if not paragraphs:
        logging.info(f"[CNV:DOCXTXT] {path.name}: sin texto extraible")
        return None

    pdf = path.with_suffix(".pdf")
    try:
        logging.info(f"[CNV:DOCXTXT] {path.name} -> {pdf.name}")
        c = canvas.Canvas(str(pdf), pagesize=(595.27, 841.89))
        width, height = 595.27, 841.89
        margin = 54
        y = height - margin
        c.setFont("Helvetica-Bold", 11)
        c.drawString(margin, y, Path(path).name.encode("latin-1", "replace").decode("latin-1"))
        y -= 24
        c.setFont("Helvetica", 10)
        max_chars = 95
        for para in paragraphs:
            words = para.split()
            line = ""
            for word in words:
                candidate = f"{line} {word}".strip()
                if len(candidate) > max_chars and line:
                    if y < margin:
                        c.showPage()
                        c.setFont("Helvetica", 10)
                        y = height - margin
                    c.drawString(margin, y, line.encode("latin-1", "replace").decode("latin-1"))
                    y -= 13
                    line = word
                else:
                    line = candidate
            if line:
                if y < margin:
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    y = height - margin
                c.drawString(margin, y, line.encode("latin-1", "replace").decode("latin-1"))
                y -= 13
            y -= 5
        c.save()
        if pdf.exists() and _is_real_pdf(pdf):
            logging.info(f"[CNV:DOCXTXT:OK] {pdf.name}")
            return pdf
    except Exception as e:
        logging.info(f"[CNV:DOCXTXT:ERR] {path.name} · {e}")
    return None

# --- OCR WinRT: compatibilidad winsdk (Py 3.12+) y winrt (Py 3.8Ã¯Â¿Â½?"3.11)
# --- OCR WinRT (Windows) -----------------------------------------------
try:
    from winsdk.windows.media import ocr as winocr
    from winsdk.windows.globalization import Language as WinLanguage
    from winsdk.windows.storage.streams import InMemoryRandomAccessStream, DataWriter
    from winsdk.windows.graphics.imaging import BitmapDecoder
    _WINOCR_OK = True
except Exception:
    _WINOCR_OK = False
import threading
import logging

def _repair_mojibake_text(text) -> str:
    s = "" if text is None else str(text)
    for _ in range(3):
        if not any(mark in s for mark in ("Ã", "Â", "â", "ð")):
            break
        try:
            fixed = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            break
        if not fixed or fixed == s:
            break
        s = fixed
    return s

class TkQueueHandler(logging.Handler):
    """Handler de logging que empuja los mensajes a una queue para la UI."""
    def __init__(self, q):
        super().__init__()
        self.q = q

    def emit(self, record):
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        msg = _repair_mojibake_text(msg)
        try:
            self.q.put_nowait(msg)
        except Exception:
            pass

def _run_ocr_sync(png_bytes: bytes, lang_tag: str):
    """
    Ejecuta la coroutina _winocr_recognize_png en un hilo con su propio event loop,
    evitando el error 'coroutine was never awaited' o 'asyncio.run()...' si ya hay un loop corriendo.
    """
    out = {"val": None, "err": None}
    def _worker():
        try:
            out["val"] = asyncio.run(_winocr_recognize_png(png_bytes, lang_tag))
        except Exception as e:
            out["err"] = e
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join()
    if out["err"]:
        raise out["err"]
    return out["val"]

# --------------------------- RUTAS Y RECURSOS --------------------------
if getattr(sys, "frozen", False):  # ejecutable .exe
    BASE_PATH = Path(sys._MEIPASS)
else:  # .py suelto
    BASE_PATH = Path(__file__).parent

# Playwright buscarÃƒÂ¡ el navegador empaquetado aquÃƒÂ­ (portabiliza el .exe)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BASE_PATH / "ms-playwright")

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry




def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _playwright_browsers_dir() -> Path:
    raw = os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or str(BASE_PATH / "ms-playwright")
    return Path(raw)


def _chromium_revision_sort_key(path: Path) -> int:
    m = re.search(r"chromium-(\d+)", path.as_posix())
    return int(m.group(1)) if m else -1


def _local_chromium_candidates() -> list[Path]:
    root = _playwright_browsers_dir()
    if not root.exists():
        return []

    patterns = (
        "chromium-*/chrome-win/chrome.exe",
        "chromium-*/chrome-win64/chrome.exe",
        "chromium-*/chrome-linux/chrome",
        "chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium",
    )
    seen = set()
    candidates = []
    for pattern in patterns:
        for item in root.glob(pattern):
            resolved = item.resolve()
            if not item.is_file() or resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(item)

    return sorted(candidates, key=_chromium_revision_sort_key, reverse=True)


def _launch_chromium(chromium, **kwargs):
    try:
        return chromium.launch(**kwargs)
    except Exception as exc:
        msg = str(exc)
        if "Executable doesn't exist" not in msg:
            raise

        browser_root = _playwright_browsers_dir()
        current_executable = kwargs.get("executable_path")
        candidate = None
        for item in _local_chromium_candidates():
            if current_executable and Path(current_executable) == item:
                continue
            candidate = item
            break

        if candidate is None:
            raise RuntimeError(
                f"{msg}\n\nNo se encontro un Chromium utilizable en '{browser_root}'. "
                "Reinstala los navegadores de Playwright con 'playwright install chromium' "
                "o vuelve a copiar la carpeta 'ms-playwright' correspondiente a esta version."
            ) from exc

        retry_kwargs = dict(kwargs)
        retry_kwargs["executable_path"] = str(candidate)
        logging.warning("[NAV] Chromium revision faltante; reintentando con %s", candidate)
        try:
            return chromium.launch(**retry_kwargs)
        except Exception as retry_exc:
            raise RuntimeError(
                f"{msg}\n\nTambien fallo el Chromium local encontrado en '{candidate}'. "
                "La version de Playwright y la carpeta 'ms-playwright' no coinciden. "
                "Ejecuta 'playwright install chromium' en el mismo entorno desde el que corres la app."
            ) from retry_exc

# --------- Seguridad/Permisos ---------
PERM_MSG = "El usuario no tiene los permisos suficientes para visualizar este contenido."


def _norm_ws(s: str) -> str:
    # normaliza nbsp, tabs y saltos ? 1 espacio; recorta extremos
    import re

    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _tiene_mensaje_permiso(texto: str) -> bool:
    # detectar el mensaje aunque estÃƒÂ© rodeado de otros textos (p. ej. tÃƒÂ­tulo del modal)
    import unicodedata, re

    t = _norm_ws(texto or "").lower()
    base = _norm_ws(PERM_MSG).lower()

    # match por substring directo
    if base in t:
        return True

    # variantes robustas (sin acentos)
    def deacc(s):
        return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").lower()

    if deacc(base) in deacc(t):
        return True

    # heurÃƒÂ­stica por frases clave
    if ("no tiene los permisos suficientes" in t) and ("visualizar este contenido" in t):
        return True

    return False


def _is_real_pdf(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except Exception:
        return False


def _pdf_es_login_portal(path: Path) -> bool:
    txt = ""
    try:
        import fitz

        doc = fitz.open(str(path))
        for i in range(min(doc.page_count, 2)):
            txt += doc[i].get_text("text") or ""
        doc.close()
    except Exception:
        try:
            for p in PdfReader(str(path)).pages[:2]:
                txt += p.extract_text() or ""
        except Exception:
            return False

    t = (txt or "").lower()
    return ("ingrese nombre de usuario y contraseÃƒÂ±a" in t) or ("portal" in t and "intranet" in t)


def _pdf_contiene_mensaje_permiso(path: Path) -> bool:
    """HeurÃƒÂ­stica: si el PDF trae el cartel de 'no tiene permisos', lo descartamos."""
    txt = ""
    try:
        # PyMuPDF rÃƒÂ¡pido si estÃƒÂ¡
        import fitz

        doc = fitz.open(str(path))
        for i in range(min(doc.page_count, 3)):
            txt += doc[i].get_text("text") or ""
        doc.close()
    except Exception:
        try:
            # Fallback PyPDF2
            for p in PdfReader(str(path)).pages[:3]:
                txt += p.extract_text() or ""
        except Exception:
            return False
    return _tiene_mensaje_permiso(txt)


def _contenido_operacion_valido(texto: str) -> bool:
    """
    Considera vÃƒÂ¡lido todo contenido que NO sea el mensaje de permisos.
    (Hay operaciones muy cortas Ã¯Â¿Â½?"p.ej. 'Se declara confidencial'Ã¯Â¿Â½?" que antes se filtraban por longitud.)
    """
    t = _norm_ws(texto or "")
    if not t:
        return False
    return not _tiene_mensaje_permiso(t)


# --- URLs base ---------------------------------------------------------
TELETRABAJO_URL = "https://teletrabajo.justiciacordoba.gob.ar/remote/login?lang=sp"
URL_BASE = "https://www.tribunales.gov.ar"
URL_LOGIN = f"{URL_BASE}/SacInterior/Login.aspx"
URL_RADIOGRAFIA = f"{URL_BASE}/SacInterior/_Expedientes/Radiografia.aspx"
INTRANET_LOGIN_URL = "https://aplicaciones.tribunales.gov.ar/portalwebnet/#/login"
INTRANET_HOME_URL = "https://aplicaciones.tribunales.gov.ar/portalwebnet/#/"
SAC_MENU_DEFAULT_URL = "https://www.tribunales.gov.ar/SacInterior/Menu/Default.aspx"


def _is_teletrabajo(u: str) -> bool:
    return "teletrabajo.justiciacordoba.gob.ar" in (u or "")


def _is_tribunales(u: str) -> bool:
    import re

    return bool(re.search(r"https?://([a-z0-9-]+\.)*tribunales\.gov\.ar", u or "", re.I))



import subprocess, shutil as _shutil


def _kill_spurious_popups(ctx):
    """Cierra popups que no sean parte del Libro (p. ej. portal Intranet)."""

    def _handler(p):
        try:
            p.wait_for_load_state("domcontentloaded", timeout=3000)
        except Exception:
            pass
        try:
            u = (p.url or "")
            if ("ExpedienteLibro.aspx" not in u) and ("SacInterior" not in u):
                try:
                    p.close()
                except Exception:
                    pass
        except Exception:
            pass

    ctx.on("page", _handler)
    return _handler


def _kill_overlays(page):
    """Oculta/remueve cortinas/overlays que pueden interceptar el click."""
    try:
        page.evaluate(
            """
            () => {
                const sels = [
                    '#divDialogCourtian_0',
                    '.divDialogCourtian',
                    '.divDialogCortina',
                try:
                    lnks = idx_page.get_links()
                    logging.info(f"[INDICE] links_on_page={len(lnks)} last={lnks[-1] if lnks else {}}")
                except Exception:
                    pass
                    '.ui-widget-overlay',
                    '.ui-widget-shadow',
                    '.modal-backdrop',
                    '.modal[role=dialog]'
                ];
                for (const s of sels) {
                    document.querySelectorAll(s).forEach(el => {
                        el.style.pointerEvents = 'none';
                        el.style.display = 'none';
                        el.remove();
                    });
                }
            }
            """
        )
    except Exception:
        pass


def _asegurar_seccion_operaciones_visible(page):
    """Muestra la secciÃƒÂ³n 'OPERACIONES' si estÃƒÂ¡ colapsada y la desplaza a la vista."""
    try:
        # toggles tÃƒÂ­picos
        toggle = page.locator(
            "a[href*=\"Seccion('Operaciones')\"], a[onclick*=\"Seccion('Operaciones')\"], "
            "a:has-text('OPERACIONES')"
        ).first
        cont = page.locator("#cphDetalle_gvOperaciones, table[id*='gvOperaciones']").first

        oculto = False
        if cont.count():
            try:
                oculto = cont.evaluate("el => getComputedStyle(el).display === 'none'")
            except Exception:
                pass

        if (not cont.count() or oculto) and toggle.count():
            toggle.click()
            page.wait_for_timeout(100)

        # desplazar tÃƒÂ­tulo/tabla a la vista
        for sel in ["#cphDetalle_gvOperaciones", "table[id*='gvOperaciones']", "text=/^\\s*OPERACIONES\\s*$/i"]:
            loc = page.locator(sel).first
            if loc.count():
                try:
                    loc.scroll_into_view_if_needed()
                except Exception:
                    pass
                break
    except Exception:
        pass


def etapa(msg: str):
    """Marca una etapa visible en la ventana de progreso y en el debug.log."""
    logging.info(f"[ETAPA] {_repair_mojibake_text(msg)}")


def _esperar_radiografia_listo(page, timeout=120):
    """
    Espera a que RadiografÃƒÂ­a termine de cargar luego de la bÃƒÂºsqueda.
    Considera AJAX: esperamos a ver carÃƒÂ¡tula/fojas y que 'Operaciones' o 'Adjuntos' estÃƒÂ©n
    renderizados (o, al menos, que el encabezado del expediente cambie).
    """
    import time, re

    t0 = time.time()

    # algo de vida en la carÃƒÂ¡tula
    pistas_ok = [
        "text=/\\bEXPEDIENTE NÃ‚Â°\\b/i",
        "text=/\\bCarÃƒÂ¡tula\\b/i",
        "text=/\\bTotal de Fojas\\b/i",
        "#cphDetalle_lblNroExpediente",
    ]

    # timeout viene en ms ? convertimos a segundos
    deadline = t0 + (max(0, int(timeout)) / 1000.0)

    while time.time() < deadline:
        try:
            hay_carat = any(page.locator(s).first.count() for s in pistas_ok)
        except Exception:
            hay_carat = False

        # secciones que suelen llegar por AJAX
        try:
            _asegurar_seccion_operaciones_visible(page)
        except Exception:
            pass

        try:
            hay_ops_grid = (
                page.locator(
                    "[onclick*=\"VerDecretoHtml(\"], [href*=\"VerDecretoHtml(\"], "
                    "#cphDetalle_gvOperaciones tr"
                ).count()
                > 0
            )
        except Exception:
            hay_ops_grid = False

        try:
            hay_adj = page.locator("#cphDetalle_gvAdjuntos tr").count() > 0
        except Exception:
            hay_adj = False

        if hay_carat and (hay_ops_grid or hay_adj):
            # Ã¯Â¿Â½?~or TrueÃ¯Â¿Â½?T ? si carÃƒÂ¡tula ya cargÃƒÂ³, damos unos ms extra y seguimos
            page.wait_for_timeout(300)
            return page.wait_for_timeout(120)

    # timeout: igual seguimos, pero ya dimos tiempo razonable
    return


def _buscar_contenedor_operacion(root, op_id: str):
    sels = [
        f"[id='{op_id}']",
        f"[data-codigo='{op_id}']",
        f"[aria-labelledby*='{op_id}']",
        f"[aria-controls*='{op_id}']",
        f".{op_id}",
        f"[id*='{op_id}']",
    ]
    for sc in _all_scopes(root):
        for sel in sels:
            try:
                loc = sc.locator(sel).first
                if loc.count():
                    return loc  # no exijo is_visible: a veces estÃƒÂ¡ fuera de viewport
            except Exception:
                continue
    return None


def _esperar_contenedor_operacion(libro, op_id: str, timeout_ms: int = 4000):
    """
    Después de disparar `onItemClick`, el contenedor puede aparecer con un pequeño delay
    o en otro scope/frame del Libro. Buscamos siempre sobre `libro` completo, no sobre
    un scope reducido.
    """
    import time as _time

    end_at = _time.time() + max(0.5, timeout_ms / 1000.0)
    last = None
    while _time.time() < end_at:
        try:
            last = _buscar_contenedor_operacion(libro, op_id)
        except Exception:
            last = None
        if last:
            try:
                if last.count():
                    return last
            except Exception:
                return last
        try:
            libro.wait_for_timeout(200)
        except Exception:
            pass
    return last


def _descargar_ops_en_paralelo(
    session, template_url: str, op_ids: list[str], tmp_dir: Path, max_workers=6
) -> dict[str, Path]:
    out = {}

    def _one(op_id):
        url = template_url.replace("{ID}", op_id)
        dst = tmp_dir / f"op_{op_id}.pdf"
        p = _descargar_archivo(session, url, dst)
        if not p or not _is_real_pdf(p) or _pdf_contiene_mensaje_permiso(p):
            try:
                dst.unlink()
            except Exception:
                pass
            return (op_id, None)
        return (op_id, _pdf_sin_blancos(p))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for k, v in ex.map(_one, op_ids):
            if v:
                out[k] = v
    return out


def _ensure_pdf(path: Path) -> Path:
    """
    Si path ya es PDF ? lo devuelve. Si es imagen ? convierte con PIL.
    Si es doc/xls/ppt (y hay LibreOffice) ? convierte con soffice.
    Caso contrario, deja el archivo como estÃƒÂ¡ (no rompe).
    """
    ext = path.suffix.lower()
    if ext == ".pdf":
        return path

    # imÃƒÂ¡genes
    if ext in {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}:
        pdf = path.with_suffix(".pdf")
        Image.open(path).save(pdf, "PDF", resolution=144.0)
        return pdf

    # office (si hay LibreOffice)
    soffice = (
        _shutil.which("soffice")
        or _shutil.which("soffice.exe")
        or r"C:\Program Files\LibreOffice\program\soffice.exe"
    )
    if Path(str(soffice)).exists():
        outdir = path.parent
        try:
            subprocess.run(
                [soffice, "--headless", "--convert-to", "pdf", "--outdir", str(outdir), str(path)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                **_subprocess_hidden_kwargs(),
            )
            pdf = path.with_suffix(".pdf")
            if pdf.exists():
                return pdf
        except Exception:
            pass
    else:
        logging.info(f"[CNV:OFF] LibreOffice no encontrado; no puedo convertir {path.name}")

    word_pdf = _convert_office_with_word(path)
    if word_pdf:
        return word_pdf
    docx_pdf = _convert_docx_text_to_pdf(path)
    if docx_pdf:
        return docx_pdf

    # si no pudimos convertir, devolvemos el original (se omitirÃƒÂ¡ en la fusiÃƒÂ³n si no es PDF)
    return path




# --- MERGE TURBO con fitz y fallback agrupado con PyPDF2 ---
try:
    import fitz  # PyMuPDF

    def fusionar_bloques_inline(bloques, destino: Path):
        """
        Fast path con PyMuPDF:
        - insert_pdf para cada bloque (ultra rÃƒÂ¡pido).
        - Si header_text, dibuja marco+cabecera en las pÃƒÂ¡ginas reciÃƒÂ©n insertadas.
        """
        dst = fitz.open()
        margin = 18
        for pdf_path, header_text in bloques:
            try:
                src = fitz.open(str(pdf_path))
            except Exception as e:
                logging.info(f"[MERGE:SKIP] {Path(pdf_path).name} Ã‚Â· {e}")
                continue

            start = dst.page_count
            dst.insert_pdf(src)
            end = dst.page_count
            src.close()

            if header_text:
                title = str(header_text)[:180]
                for i in range(start, end):
                    page = dst[i]
                    rect = page.rect
                    page.draw_rect(
                        fitz.Rect(margin, margin, rect.width - margin, rect.height - margin),
                        width=1,
                    )
                    try:
                        page.insert_text((margin + 10, rect.height - margin + 2), title, fontname="helv", fontsize=12)
                    except Exception:
                        page.insert_text((margin + 10, rect.height - margin + 2), title, fontsize=12)

            logging.info(
                f"[MERGE:+FITZ] {Path(pdf_path).name} Ã‚Â· pÃƒÂ¡ginas={end-start} Ã‚Â· header={'sÃƒÂ­' if header_text else 'no'}"
            )

        dst.save(str(destino), deflate=True, garbage=3)
        dst.close()
        logging.info(f"[MERGE:DONE/FITZ] {destino.name}")

except Exception:

    from PyPDF2 import PdfMerger

    def fusionar_bloques_inline(bloques, destino: Path):
        """
        Fallback PyPDF2 AGRUPADO:
        - Junta runs seguidos SIN header y los concatena directo con PdfMerger (sin paginar).
        - Para los que requieren header, estampa una COPIA temporal con _estampar_header y la agrega.
        - Al final, una sola escritura con PdfMerger.
        """
        final_parts: list[Path] = []
        temps: list[Path] = []
        i = 0
        N = len(bloques)

        while i < N:
            pdf_path, hdr = bloques[i]
            if hdr is None:
                # run de PDFs sin header
                j = i
                run = []
                while j < N and bloques[j][1] is None:
                    run.append(Path(bloques[j][0]))
                    j += 1

                # agregamos los paths tal cual (concatena rapidÃƒÂ­simo)
                final_parts.extend(run)
                i = j
                continue

            # bloque con header ? estampar a archivo temporal
            stamped = Path(tempfile.mkstemp(suffix=".stamped.pdf")[1])
            try:
                _estampar_header(Path(pdf_path), stamped, texto=str(hdr))
                final_parts.append(stamped)
                temps.append(stamped)
            except Exception as e:
                logging.info(f"[MERGE:HDR-ERR] {Path(pdf_path).name} Ã‚Â· {e}")
            i += 1

        # Concat ÃƒÂºnico
        merger = PdfMerger()
        for part in final_parts:
            merger.append(str(part))
            logging.info(f"[MERGE:+FAST] {part.name}")

        with open(destino, "wb") as f:
            merger.write(f)
        merger.close()
        logging.info(f"[MERGE:DONE/FAST] {destino.name}")

        for t in temps:
            try:
                t.unlink()
            except Exception:
                pass

def _contar_paginas_pdf(path: Path) -> int:
    try:
        return len(PdfReader(str(path)).pages)
    except Exception:
        pass
    try:
        import fitz
        doc = fitz.open(str(path))
        n = int(doc.page_count)
        doc.close()
        return n
    except Exception:
        return 1


def fusionar_bloques_con_indice(
    bloques,
    destino: Path,
    index_title: str = "INDICE",
    keep_sidecar: bool = False,
    front_matter_pages: int = 0,
    skip_first_block_in_index: bool = False,
):
    """
    Fusiona bloques PDF, inserta un ÃƒÂ­ndice clickable detrÃƒÂ¡s de la carÃƒÂ¡tula y devuelve
    (idx_page_count, relink_items) donde:
      - idx_page_count: cantidad de pÃƒÂ¡ginas del ÃƒÂ­ndice insertadas
      - relink_items  : [{'title', 'start', 'target', 'y'}] para re-inyectar links post-OCR
    TambiÃƒÂ©n escribe <destino>.toc.json con ese mapeo.
    El archivo auxiliar se elimina automÃƒÂ¡ticamente salvo que keep_sidecar sea True.
    """
    try:
        import fitz  # PyMuPDF
    except Exception:
        # Sin PyMuPDF: fusiÃƒÂ³n simple sin ÃƒÂ­ndice
        fusionar_bloques_inline(bloques, destino)
        try:
            logging.info(f"[MERGE:DONE/NO_FITZ] {destino.name}")
        except Exception:
            pass
        return 0, []

    def _add_goto_link(pg, rect, target_page_zero_based) -> bool:
        """Crea un link interno robusto, compatible con varias versiones de PyMuPDF."""
        try:
            pg.insert_link({"kind": fitz.LINK_GOTO, "from": rect, "page": int(target_page_zero_based)})
            return True
        except Exception:
            pass
        try:
            pg.insert_link({"kind": fitz.LINK_GOTO, "rect": rect, "page": int(target_page_zero_based)})
            return True
        except Exception:
            pass
        try:
            pg.add_link(rect=rect, page=int(target_page_zero_based))
            return True
        except Exception:
            return False

    dst = fitz.open()
    margin = 18
    items_info = []  # (title_for_toc, start_page_zero_based)

    # --- InserciÃƒÂ³n de bloques ---
    for item in bloques:
        if isinstance(item, (list, tuple)) and len(item) >= 3:
            pdf_path, header_text, toc_title = item[0], item[1], item[2]
        else:
            pdf_path, header_text = item[0], item[1]
            toc_title = None

        try:
            src = fitz.open(str(pdf_path))
        except Exception as e:
            try: logging.info(f"[MERGE:SKIP] {Path(pdf_path).name} - {e}")
            except Exception: pass
            continue

        start = dst.page_count
        dst.insert_pdf(src)
        end = dst.page_count
        src.close()

        title_for_toc = (str(toc_title).strip() if toc_title
                         else (str(header_text).strip() if header_text else Path(pdf_path).name))
        items_info.append((title_for_toc, start))

        # Header opcional
        if header_text:
            title = str(header_text)[:180]
            for i in range(start, end):
                page = dst[i]
                rect = page.rect
                try:
                    page.draw_rect(fitz.Rect(margin, margin, rect.width - margin, rect.height - margin), width=1)
                except Exception:
                    pass
                try:
                    page.insert_text((margin + 10, rect.height - margin + 2), title, fontname="helv", fontsize=12)
                except Exception:
                    page.insert_text((margin + 10, rect.height - margin + 2), title, fontsize=12)

    # --- ÃƒÂndice ---
    idx_page_count = 0
    relink_items = []
    insert_at_page = max(0, min(int(front_matter_pages or 0), dst.page_count))
    entries = list(items_info[1:] if skip_first_block_in_index else items_info)

    if dst.page_count > 0 and entries:
        try:
            first_rect = dst[0].rect
            pw, ph = first_rect.width, first_rect.height
            entries = sorted(entries, key=lambda x: x[1])
            if entries:
                fs = 11
                row_h = 24
                row_gap = 6
                chrome_top = margin + 2
                chrome_h = 52
                subtitle_y = chrome_top + chrome_h + 18
                header_y = subtitle_y + 26
                y_start = header_y + 22
                x_left = margin + 12
                x_right = pw - margin - 12
                page_box_w = 58
                title_col_right = x_right - page_box_w - 16
                title_y = chrome_top + 18

                color_band = (0.15, 0.28, 0.46)
                color_band_text = (1.0, 1.0, 1.0)
                color_subtle = (0.35, 0.41, 0.48)
                color_header_fill = (0.92, 0.95, 0.98)
                color_header_line = (0.79, 0.84, 0.90)
                color_row_even = (0.985, 0.989, 0.995)
                color_row_odd = (0.955, 0.973, 0.992)
                color_row_border = (0.87, 0.90, 0.94)
                color_text = (0.10, 0.15, 0.22)
                color_page_box = (0.22, 0.42, 0.67)

                def _safe_draw_rect(pg: fitz.Page, rect, **kw) -> fitz.Page | None:
                    try:
                        pg.draw_rect(rect, **kw)
                        return pg
                    except AttributeError as e:
                        if "is_pdf" in str(e):
                            try:
                                pg = dst.load_page(pg.number)
                                pg.draw_rect(rect, **kw)
                                return pg
                            except Exception as e2:
                                try: logging.info(f"[INDICE] draw_rect error: {e2}")
                                except Exception: pass
                                return None
                        try: logging.info(f"[INDICE] draw_rect error: {e}")
                        except Exception: pass
                        return None
                    except Exception as e:
                        try: logging.info(f"[INDICE] draw_rect error: {e}")
                        except Exception: pass
                        return None

                def _calc_pages(n_items: int) -> int:
                    if n_items <= 0:
                        return 0
                    y = y_start
                    pages = 1
                    for _ in range(n_items):
                        if y + row_h > ph - margin - 20:
                            pages += 1
                            y = y_start
                        y += row_h + row_gap
                    return pages

                idx_page_count = _calc_pages(len(entries))
                index_pages: list[int] = []
                for i in range(idx_page_count):
                    pno = insert_at_page + i
                    try:
                        dst.new_page(pno=pno, width=pw, height=ph)
                        pg = dst.load_page(pno)
                    except Exception as e:
                        try: logging.warning(f"[INDICE] no se pudo crear/cargar pÃƒÂ¡gina {pno}: {e}")
                        except Exception: pass
                        continue
                    if getattr(pg, "parent", None) is None:
                        try: logging.warning(f"[INDICE] pÃƒÂ¡gina {pno} sin parent; se omite")
                        except Exception: pass
                        continue
                    index_pages.append(pno)

                if not index_pages:
                    try: logging.warning("[INDICE] no se generaron pÃƒÂ¡ginas vÃƒÂ¡lidas de ÃƒÂ­ndice")
                    except Exception: pass
                else:
                    idx_page_count = len(index_pages)
                    page_rows: list[list[dict]] = [[] for _ in range(idx_page_count)]
                    try: logging.info(f"[INDICE] entries={len(entries)} idx_pages={idx_page_count}")
                    except Exception: pass

                    def _foja_for_page(p: int):
                        # p es 0-based del PDF final
                        skip = insert_at_page + idx_page_count
                        if p < skip:
                            return None
                        return 1 + ((p - skip) // 2)

                    page_idx = 0
                    def _load_idx_page() -> fitz.Page | None:
                        pno = index_pages[page_idx]
                        try:
                            pg = dst.load_page(pno)
                        except Exception as e:
                            try: logging.warning(f"[INDICE] no se pudo recargar pÃƒÂ¡gina {pno}: {e}")
                            except Exception: pass
                            return None
                        if getattr(pg, "parent", None) is None:
                            try: logging.warning(f"[INDICE] pÃƒÂ¡gina {pno} sin parent; se omite")
                            except Exception: pass
                            return None
                        return pg

                    def _safe_insert_text(pg: fitz.Page, pos, txt, **kw) -> tuple[fitz.Page, bool]:
                        try:
                            pg.insert_text(pos, txt, **kw)
                            return pg, True
                        except AttributeError as e:
                            if "is_pdf" in str(e):
                                try:
                                    pg = dst.load_page(pg.number)
                                    pg.insert_text(pos, txt, **kw)
                                    return pg, True
                                except Exception as e2:
                                    try: logging.info(f"[INDICE] insert_text error: {e2}")
                                    except Exception: pass
                                    return pg, False
                            try: logging.info(f"[INDICE] insert_text error: {e}")
                            except Exception: pass
                            return pg, False
                        except Exception as e:
                            try: logging.info(f"[INDICE] insert_text error: {e}")
                            except Exception: pass
                            return pg, False

                    def _safe_insert_textbox(pg: fitz.Page, rect, txt, **kw) -> tuple[fitz.Page, bool]:
                        try:
                            pg.insert_textbox(rect, txt, **kw)
                            return pg, True
                        except AttributeError as e:
                            if "is_pdf" in str(e):
                                try:
                                    pg = dst.load_page(pg.number)
                                    pg.insert_textbox(rect, txt, **kw)
                                    return pg, True
                                except Exception as e2:
                                    try: logging.info(f"[INDICE] insert_textbox error: {e2}")
                                    except Exception: pass
                                    return pg, False
                            try: logging.info(f"[INDICE] insert_textbox error: {e}")
                            except Exception: pass
                            return pg, False
                        except Exception as e:
                            try: logging.info(f"[INDICE] insert_textbox error: {e}")
                            except Exception: pass
                            return pg, False

                    def _paint_index_page(pg: fitz.Page, continued: bool = False) -> fitz.Page | None:
                        if pg is None:
                            return None
                        band_rect = fitz.Rect(margin, chrome_top, pw - margin, chrome_top + chrome_h)
                        pg = _safe_draw_rect(
                            pg,
                            band_rect,
                            color=color_band,
                            fill=color_band,
                            width=0,
                        )
                        if pg is None:
                            return None

                        band_title = index_title + (" (continuación)" if continued else "")
                        pg, ok = _safe_insert_text(
                            pg,
                            (x_left, title_y),
                            band_title,
                            fontname="helv",
                            fontsize=19,
                            color=color_band_text,
                        )
                        if not ok:
                            return None

                        subtitle = "Contenido seleccionado y ordenado para el PDF final."
                        pg, ok = _safe_insert_text(
                            pg,
                            (x_left, subtitle_y),
                            subtitle,
                            fontname="helv",
                            fontsize=10,
                            color=color_subtle,
                        )
                        if not ok:
                            return None

                        header_rect = fitz.Rect(x_left, header_y, x_right, header_y + 18)
                        pg = _safe_draw_rect(
                            pg,
                            header_rect,
                            color=color_header_line,
                            fill=color_header_fill,
                            width=0.8,
                        )
                        if pg is None:
                            return None

                        pg, ok = _safe_insert_text(
                            pg,
                            (x_left + 8, header_y + 12),
                            "Documento",
                            fontname="helv",
                            fontsize=9,
                            color=color_text,
                        )
                        if not ok:
                            return None
                        pg, ok = _safe_insert_text(
                            pg,
                            (x_right - page_box_w + 8, header_y + 12),
                            "Página",
                            fontname="helv",
                            fontsize=9,
                            color=color_text,
                        )
                        if not ok:
                            return None
                        return pg

                    idx_page = _load_idx_page()
                    if idx_page is not None:
                        idx_page = _paint_index_page(idx_page, continued=False)
                    y = y_start
                    toc_outline = []

                    for title, start_page in entries:
                        if y + row_h > ph - margin - 20:
                            page_idx += 1
                            if page_idx >= len(index_pages):
                                try:
                                    logging.warning("[INDICE] pÃƒÂ¡ginas de ÃƒÂ­ndice insuficientes; se detiene la generaciÃƒÂ³n")
                                except Exception:
                                    pass
                                break
                            idx_page = _load_idx_page()
                            if idx_page is not None:
                                idx_page = _paint_index_page(idx_page, continued=True)
                            y = y_start

                        t = _norm_ws(str(title))[:140]
                        idx_page = _load_idx_page()
                        if idx_page is None:
                            continue

                        row_rect = fitz.Rect(x_left, y, x_right, y + row_h)
                        row_fill = color_row_even if (len(relink_items) % 2 == 0) else color_row_odd
                        idx_page = _safe_draw_rect(
                            idx_page,
                            row_rect,
                            color=color_row_border,
                            fill=row_fill,
                            width=0.6,
                        )
                        if idx_page is None:
                            continue

                        title_rect = fitz.Rect(x_left + 8, y + 4, title_col_right - 8, y + row_h - 4)
                        idx_page, ok = _safe_insert_textbox(
                            idx_page,
                            title_rect,
                            t,
                            fontname="helv",
                            fontsize=fs,
                            color=color_text,
                            align=0,
                        )
                        if not ok:
                            continue

                        target_page = start_page + idx_page_count  # 0-based
                        toc_outline.append([1, t, target_page + 1])

                        try:
                            logging.info(f"[INDICE] item title={t[:50]} start={start_page} target={target_page} y={y}")
                        except Exception:
                            pass

                        fj_txt = str(target_page + 1)

                        idx_page = _load_idx_page()
                        if idx_page is None:
                            continue
                        page_box_rect = fitz.Rect(x_right - page_box_w, y + 3, x_right - 6, y + row_h - 3)
                        idx_page = _safe_draw_rect(
                            idx_page,
                            page_box_rect,
                            color=color_page_box,
                            fill=color_page_box,
                            width=0.6,
                        )
                        if idx_page is None:
                            continue
                        idx_page, ok = _safe_insert_textbox(
                            idx_page,
                            page_box_rect,
                            fj_txt,
                            fontname="helv",
                            fontsize=10,
                            color=color_band_text,
                            align=1,
                        )
                        if not ok:
                            continue

                        try:
                            page_rows[page_idx].append(
                                {
                                    "title": t,
                                    "page_txt": fj_txt,
                                    "y": float(y),
                                    "stripe_even": bool(len(relink_items) % 2 == 0),
                                }
                            )
                        except Exception:
                            pass

                        # Rect clickable
                        link_rect = fitz.Rect(x_left - 2, y, x_right, y + row_h)
                        idx_page = _load_idx_page()
                        if idx_page is not None:
                            ok_link = _add_goto_link(idx_page, link_rect, target_page)
                        else:
                            ok_link = False
                        try:
                            logging.info(f"[INDICE] link_{'ok' if ok_link else 'fail'} {t[:50]} -> p{target_page}")
                        except Exception:
                            pass

                        relink_items.append({
                            "title": t,
                            "start": (index_pages[page_idx] + 1),   # 1-based
                            "target": (target_page + 1),      # 1-based
                            "y": float(y + row_h - 4)
                        })
                        y += row_h + row_gap

                    try:
                        if toc_outline:
                            dst.set_toc(toc_outline)
                    except Exception:
                        pass

                    try:
                        import io
                        from reportlab.pdfbase import pdfmetrics

                        def _fit_reportlab_line(text: str, font_name: str, font_size: float, max_w: float) -> str:
                            txt = _norm_ws(text or "")
                            if max_w <= 0:
                                return txt
                            if pdfmetrics.stringWidth(txt, font_name, font_size) <= max_w:
                                return txt
                            while len(txt) > 3 and pdfmetrics.stringWidth(txt + "…", font_name, font_size) > max_w:
                                txt = txt[:-1].rstrip()
                            return (txt + "…") if txt else ""

                        overlay_buf = io.BytesIO()
                        c_idx = canvas.Canvas(overlay_buf, pagesize=(pw, ph))
                        for idx_local, _pno in enumerate(index_pages):
                            continued = idx_local > 0
                            rows_here = page_rows[idx_local] if idx_local < len(page_rows) else []
                            band_title = index_title + (" (continuación)" if continued else "")

                            c_idx.setFillColorRGB(*color_band)
                            c_idx.setStrokeColorRGB(*color_band)
                            c_idx.rect(margin, ph - (chrome_top + chrome_h), pw - (margin * 2), chrome_h, fill=1, stroke=0)

                            c_idx.setFillColorRGB(*color_band_text)
                            c_idx.setFont("Helvetica-Bold", 19)
                            c_idx.drawString(x_left, ph - title_y, band_title)

                            c_idx.setFillColorRGB(*color_subtle)
                            c_idx.setFont("Helvetica", 10)
                            c_idx.drawString(x_left, ph - subtitle_y, "Contenido seleccionado y ordenado para el PDF final.")

                            c_idx.setFillColorRGB(*color_header_fill)
                            c_idx.setStrokeColorRGB(*color_header_line)
                            c_idx.rect(x_left, ph - (header_y + 18), x_right - x_left, 18, fill=1, stroke=1)

                            c_idx.setFillColorRGB(*color_text)
                            c_idx.setFont("Helvetica-Bold", 9)
                            c_idx.drawString(x_left + 8, ph - (header_y + 12), "Documento")
                            c_idx.drawString(x_right - page_box_w + 8, ph - (header_y + 12), "Página")

                            for row in rows_here:
                                y_top = float(row.get("y") or 0.0)
                                row_y = ph - (y_top + row_h)
                                fill = color_row_even if row.get("stripe_even") else color_row_odd
                                c_idx.setFillColorRGB(*fill)
                                c_idx.setStrokeColorRGB(*color_row_border)
                                c_idx.rect(x_left, row_y, x_right - x_left, row_h, fill=1, stroke=1)

                                title_text = _fit_reportlab_line(
                                    str(row.get("title") or ""),
                                    "Helvetica",
                                    fs,
                                    max(40, title_col_right - x_left - 20),
                                )
                                c_idx.setFillColorRGB(*color_text)
                                c_idx.setFont("Helvetica", fs)
                                c_idx.drawString(x_left + 8, ph - (y_top + 16), title_text)

                                page_box_x = x_right - page_box_w
                                page_box_y = ph - (y_top + row_h - 3)
                                page_box_h = row_h - 6
                                c_idx.setFillColorRGB(*color_page_box)
                                c_idx.setStrokeColorRGB(*color_page_box)
                                c_idx.rect(page_box_x, page_box_y, page_box_w - 6, page_box_h, fill=1, stroke=1)

                                c_idx.setFillColorRGB(*color_band_text)
                                c_idx.setFont("Helvetica-Bold", 10)
                                c_idx.drawCentredString(
                                    page_box_x + ((page_box_w - 6) / 2.0),
                                    ph - (y_top + 16),
                                    str(row.get("page_txt") or ""),
                                )
                            c_idx.showPage()
                        c_idx.save()

                        src_overlay = fitz.open(stream=overlay_buf.getvalue(), filetype="pdf")
                        for idx_local, pno in enumerate(index_pages):
                            try:
                                pg_overlay = dst.load_page(pno)
                            except Exception:
                                continue
                            try:
                                pg_overlay.show_pdf_page(pg_overlay.rect, src_overlay, idx_local, keep_proportion=True, overlay=True)
                            except Exception as e_overlay:
                                try: logging.info(f"[INDICE] overlay error: {e_overlay}")
                                except Exception: pass
                        src_overlay.close()
                    except Exception as e_overlay_all:
                        try: logging.info(f"[INDICE] overlay reportlab error: {e_overlay_all}")
                        except Exception: pass

                    # Sidecar
                    try:
                        sidecar = destino.with_suffix(".toc.json")
                        import json
                        with open(sidecar, "w", encoding="utf-8") as f:
                            json.dump({"items": relink_items, "idx_pages": idx_page_count},
                                      f, ensure_ascii=False, indent=2)
                        logging.info(f"[INDICE] sidecar guardado: {sidecar.name} (items={len(relink_items)})")
                        if not keep_sidecar:
                            sidecar.unlink(missing_ok=True)
                    except Exception as e:
                        logging.info(f"[INDICE] sidecar error: {e}")

                    # DiagnÃƒÂ³stico: contar links por pÃƒÂ¡gina del ÃƒÂ­ndice
                    try:
                        for pno in index_pages:
                            try:
                                pg = dst.load_page(pno)
                            except Exception:
                                continue
                            if getattr(pg, "parent", None) is None:
                                continue
                            ln, c = pg.first_link, 0
                            while ln:
                                c += 1
                                ln = ln.next
                            logging.info(f"[INDICE] links en pÃƒÂ¡gina {pg.number+1}: {c}")
                    except Exception:
                        pass
            else:
                try: logging.info("[INDICE] sin entradas; no se genera ÃƒÂ­ndice")
                except Exception: pass
        except Exception as e:
            try: logging.info(f"[INDICE] error: {e}")
            except Exception: pass

    # --- Guardado ---
    dst.save(str(destino), deflate=True, garbage=3)  # preserva anotaciones
    dst.close()
    try: logging.info(f"[MERGE:DONE/INDICE] {destino.name}")
    except Exception: pass
    return idx_page_count, relink_items


def _relink_indice_con_fitz(pdf_path: Path, items: list[dict],
                            left=36, right=36, line_h=20, pad_top=3, pad_bottom=3) -> tuple[bool, Path]:
    """
    Reinyecta anotaciones clickeables en las pÃƒÂ¡ginas de ÃƒÂ­ndice tras OCR/fojas.
    items: [{'start':1,'target':7,'y':70,'title':'...'}, ...]
    Devuelve (ok, path_final) donde path_final puede diferir si el archivo de
    destino estaba en uso.
    """
    import fitz, math, os, time, shutil
    if not items:
        return True, pdf_path
    try:
        doc = fitz.open(str(pdf_path))

        # 1) limpiar links existentes en pÃƒÂ¡ginas de ÃƒÂ­ndice
        for it in items:
            p = int(it.get("start", 1)) - 1
            if 0 <= p < doc.page_count:
                pg = doc[p]
                ln = pg.first_link
                while ln:
                    nxt = ln.next
                    pg.delete_link(ln)
                    ln = nxt

        # 2) reinsertar
        for it in items:
            p_from = int(it.get("start", 1)) - 1
            p_to   = int(it.get("target", 1)) - 1
            y      = float(it.get("y", 0.0))
            if not (0 <= p_from < doc.page_count and 0 <= p_to < doc.page_count):
                continue
            pg = doc[p_from]
            W, H = pg.rect.width, pg.rect.height
            rect = fitz.Rect(left, max(0, y - line_h + pad_top),
                             max(left + 50, W - right),
                             min(H, y + pad_bottom))
            pg.insert_link({"kind": fitz.LINK_GOTO, "from": rect, "page": p_to, "zoom": 0})
        try:
            doc.save(str(pdf_path), incremental=True, deflate=True)
        except Exception as err:
            if "code=4" in str(err):
                tmp = pdf_path.with_suffix(".tmp.pdf")
                doc.save(str(tmp), deflate=True)
                doc.close()
                try:
                    os.replace(str(tmp), str(pdf_path))
                except PermissionError:
                    # El archivo destino estÃƒÂ¡ abierto; guardar con un nombre alternativo
                    alt = pdf_path
                    orig = pdf_path
                    i = 1
                    while alt.exists():
                        alt = pdf_path.with_name(f"{pdf_path.stem} ({i}){pdf_path.suffix}")
                        i += 1
                    shutil.move(str(tmp), str(alt))
                    logging.info(f"[INDICE/LINK] destino en uso, guardado como {alt.name}")
                    for _ in range(5):
                        try:
                            orig.unlink(missing_ok=True)
                            break
                        except PermissionError:
                            time.sleep(0.2)
                        except Exception:
                            break
                    pdf_path = alt
                finally:
                    tmp.unlink(missing_ok=True)
            else:
                doc.close()
                raise
        else:
            doc.close()
        return True, pdf_path
    except Exception as e:
        logging.info(f"[INDICE/LINK:ERR] {e}")
        return False, pdf_path


def _log_links_en_pagina(pdf_path: Path, pagina_1b: int, etiqueta: str):
    import fitz
    try:
        doc = fitz.open(str(pdf_path))
        p = pagina_1b - 1
        if 0 <= p < doc.page_count:
            ln = doc[p].first_link
            n = 0
            while ln:
                n += 1
                ln = ln.next
            logging.info(f"[{etiqueta}] links en pÃƒÂ¡gina {pagina_1b}: {n}")
        doc.close()
    except Exception as e:
        logging.info(f"[{etiqueta}] no se pudo contar links: {e}")


def _listar_ops_ids_radiografia(sac, wait_ms: int | None = None, scan_frames: bool = True) -> list[str]:
    """
    Busca ids de operaciones en RadiografÃƒÂ­a de forma rÃƒÂ¡pida.
    - Espera como mÃƒÂ¡x. RADIO_OPS_WAIT_MS (default 1200 ms) en la page principal.
    - Si no encuentra, escanea frames con una espera mÃƒÂ­nima (300 ms c/u).
    - Corta en cuanto encuentra al menos una.
    """
    import time, re

    ids = set()
    sels_js = "[onclick*=\"VerDecretoHtml(\"], [href*=\"VerDecretoHtml(\"]"
    wait_ms = int(os.getenv("RADIO_OPS_WAIT_MS", "300")) if wait_ms is None else int(wait_ms)

    def _cosechar(sc):
        try:
            n = sc.locator(sels_js).count()
        except Exception:
            n = 0
        for i in range(n):
            el = sc.locator(sels_js).nth(i)
            href = el.get_attribute("href") or ""
            oc = el.get_attribute("onclick") or ""
            m = re.search(r"VerDecretoHtml\('([^']+)'", href or oc)  # acepta GUID o numÃƒÂ©rico
            if m:
                ids.add(m.group(1))

    # Asegurar que la secciÃƒÂ³n estÃƒÂ© visible y hacer una pasada rÃƒÂ¡pida
    try:
        _asegurar_seccion_operaciones_visible(sac)
    except Exception:
        pass

    # Salto rÃƒÂ¡pido si se pide saltar el gate completo
    try:
        if _env_true("SKIP_ACCESS_GATE", "0"):
            return []
    except Exception:
        pass

    # Espera corta en la page principal
    deadline = time.time() + max(0, wait_ms) / 1000.0
    while time.time() < deadline:
        _cosechar(sac)
        if ids:
            break
        try:
            sac.wait_for_timeout(120)
        except Exception:
            break

    # Si aÃƒÂºn no hay ids y estÃƒÂ¡ permitido, frames express (300 ms c/u, corta al primer hallazgo)
    if not ids and scan_frames:
        for fr in list(sac.frames):
            end = time.time() + 0.3
            while time.time() < end:
                _cosechar(fr)
                if ids:
                    break
                try:
                    fr.wait_for_timeout(120)
                except Exception:
                    break
            if ids:
                break

    return list(ids)


def _puedo_abrir_alguna_operacion(sac) -> bool:
    # Si se solicita saltar el gate, asumir que podemos abrir.
    try:
        if _env_true("SKIP_ACCESS_GATE", "0"):
            return True
    except Exception:
        pass

    sels_click = [
        "#cphDetalle_gvOperaciones td:nth-child(2) a",
        "table[id*='gvOperaciones'] td:nth-child(2) a",
        "[onclick*=\"VerDecretoHtml(\"], [href*=\"VerDecretoHtml(\"]",
    ]
    scopes = [sac] + list(sac.frames)

    for sc in scopes:
        for sel in sels_click:
            loc = sc.locator(sel).first
            if not loc.count():
                continue

            try:
                _kill_overlays(sc)
            except Exception:
                pass

            try:
                loc.scroll_into_view_if_needed()
            except Exception:
                pass

            try:
                loc.click(force=True)
            except Exception:
                try:
                    loc.evaluate(
                        "el => el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true}))"
                    )
                except Exception:
                    try:
                        loc.evaluate("el => el.click()")
                    except Exception:
                        continue

            dialog = _locator_modal_texto_operacion(sac)
            try:
                dialog.wait_for(state="visible", timeout=180)
                contenido = _texto_modal_operacion(dialog, timeout=300)
            except Exception:
                contenido = ""
            finally:
                try:
                    dialog.locator(".ui-dialog-titlebar-close, .close, button[aria-label='Close']").first.click()
                except Exception:
                    pass

            return _contenido_operacion_valido(contenido)

    return False


def _texto_modal_operacion(dialog, timeout=500) -> str:
    """
    Devuelve texto util del modal de operacion.
    Si el contenido viene en un iframe, intenta leer el body del frame.
    """
    try:
        dialog.wait_for(state="visible", timeout=timeout)
    except Exception:
        pass

    # 1) Intenta leer desde iframe.
    try:
        fr_loc = dialog.locator("iframe").first
        if fr_loc.count():
            try:
                eh = fr_loc.element_handle()
                if eh:
                    fr = eh.content_frame()
                else:
                    fr = None
            except Exception:
                fr = None

            if fr:
                for _ in range(30):  # ~3s
                    try:
                        txt = (fr.locator("body").inner_text() or "").strip()
                    except Exception:
                        txt = ""
                    if txt:
                        return txt
                    try:
                        fr.wait_for_timeout(100)
                    except Exception:
                        break
    except Exception:
        pass

    # 2) Fallback: texto del propio contenedor.
    for _ in range(20):  # ~2s
        try:
            t = (dialog.inner_text() or "").strip()
        except Exception:
            t = ""
        if t:
            return t
        try:
            dialog.wait_for_timeout(100)
        except Exception:
            break

    # 3) Ultimo recurso: HTML -> texto plano.
    try:
        html = dialog.inner_html() or ""
        import re

        return re.sub(r"<[^>]+>", " ", html)
    except Exception:
        return ""


def _locator_modal_texto_operacion(sac):
    import re

    base = sac.locator(
        ".ui-dialog, .modal, [role='dialog'], div[id*='TextoOp'], div[id*='TextoOperacion']"
    )
    try:
        scoped = base.filter(has_text=re.compile(r"texto\s+de\s+la\s+operaci.n", re.I))
        if scoped.count():
            return scoped.last
    except Exception:
        pass
    return base.last


def _op_visible_con_contenido_en_radiografia(sac, op_id: str) -> bool:
    _kill_overlays(sac)

    def _abrir_via_click_o_js(sc):
        link = sc.locator(
            f'[href*="VerDecretoHtml(\'{op_id}\')"], [onclick*="VerDecretoHtml(\'{op_id}\')"]'
        ).first
        if link.count():
            try:
                link.scroll_into_view_if_needed()
                link.click()
                return True
            except Exception:
                try:
                    link.evaluate(
                        "el => el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true}))"
                    )
                    return True
                except Exception:
                    try:
                        link.evaluate("el => el.click()")
                        return True
                    except Exception:
                        pass
        try:
            sc.evaluate(
                "id => { try { if (window.VerDecretoHtml) VerDecretoHtml(id) } catch(e){} }",
                op_id,
            )
            return True
        except Exception:
            return False

    opened = any(_abrir_via_click_o_js(sc) for sc in [sac] + list(sac.frames))
    if not opened:
        return False

    dialog = _locator_modal_texto_operacion(sac)

    try:
        contenido = _texto_modal_operacion(dialog, timeout=500)
    except Exception:
        contenido = ""
    finally:
        try:
            dialog.locator(".ui-dialog-titlebar-close, .close, button[aria-label='Close']").first.click()
        except Exception:
            pass

    return _contenido_operacion_valido(contenido)


def _op_denegada_en_radiografia(sac, op_id: str) -> bool:
    """Devuelve True si el modal de la operacion muestra permisos insuficientes."""
    for sc in [sac] + list(sac.frames):
        link = sc.locator(
            f'[href*="VerDecretoHtml(\'{op_id}\')"], [onclick*="VerDecretoHtml(\'{op_id}\')"]'
        ).first
        if link.count():
            try:
                _kill_overlays(sc)
            except Exception:
                pass
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                link.click(force=True)
            except Exception:
                try:
                    link.evaluate(
                        "el => el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true}))"
                    )
                except Exception:
                    try:
                        sc.evaluate("id => { if (window.VerDecretoHtml) VerDecretoHtml(id) }", op_id)
                    except Exception:
                        continue

            dialog = _locator_modal_texto_operacion(sac)
            try:
                dialog.wait_for(state="visible", timeout=180)
                contenido = _texto_modal_operacion(dialog, timeout=300)
            except Exception:
                contenido = ""
            finally:
                try:
                    dialog.locator(".ui-dialog-titlebar-close, .close, button[aria-label='Close']").first.click()
                except Exception:
                    pass

            return _tiene_mensaje_permiso(contenido)

    return False


# ------------------------- UTILIDADES PDF ------------------------------
def _estampar_header(origen: Path, destino: Path, texto="ADJUNTO"):
    """
    Dibuja un marco en todo el borde y un texto (e.g. 'ADJUNTO Ã¯Â¿Â½?" archivo.pdf')
    en la parte superior de CADA pÃƒÂ¡gina del PDF 'origen', y lo guarda en 'destino'.
    """
    # Camino rÃƒÂ¡pido con PyMuPDF (fitz)
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(origen))
        margin = 18
        try:
            title = str(texto)
        except Exception:
            title = texto
        for page in doc:
            rect = page.rect
            # marco
            try:
                page.draw_rect(
                    fitz.Rect(margin, margin, rect.width - margin, rect.height - margin),
                    color=(0, 0, 0), width=1
                )
            except Exception:
                pass
            # cabecera
            try:
                sz = 12
                x = margin + 10
                y = rect.height - margin + 2
                page.insert_text(fitz.Point(x, y), title[:150], fontsize=sz, fontname="helv", color=(0, 0, 0))
            except Exception:
                pass
        doc.save(str(destino), deflate=True, garbage=3)
        doc.close()
        return
    except Exception:
        pass

    # Fallback: ReportLab + PyPDF2
    r = PdfReader(str(origen))
    w = PdfWriter()

    for i, p in enumerate(r.pages):
        pw = float(p.mediabox.width)
        ph = float(p.mediabox.height)

        tmp = origen.with_suffix(f".overlay_{i}.pdf")
        c = canvas.Canvas(str(tmp), pagesize=(pw, ph))
        margin = 18
        c.setLineWidth(1)
        c.rect(margin, margin, pw - 2 * margin, ph - 2 * margin)
        try:
            title = str(texto)
        except Exception:
            title = texto
        c.setFont("Helvetica-Bold", 12)
        c.drawString(margin + 10, ph - margin + 2, title[:150])
        c.save()

        overlay = PdfReader(str(tmp)).pages[0]
        p.merge_page(overlay)
        w.add_page(p)
        tmp.unlink(missing_ok=True)

    with open(destino, "wb") as f:
        w.write(f)


def _libro_scope(libro):
    """
    Devuelve el frame/pÃƒÂ¡gina que realmente contiene el Libro:
    - URL de ExpedienteLibro o
    - Presencia de #indice/.indice y anchors de operaciones.
    """

    def _is_book_scope(sc):
        try:
            u = (getattr(sc, "url", "") or "")
        except Exception:
            u = ""
        has_book_url = ("ExpedienteLibro.aspx" in u) or ("/_Expedientes/ExpedienteLibro" in u)
        try:
            has_index = sc.locator("#indice, .indice").first.count() > 0
        except Exception:
            has_index = False
        try:
            has_ops = sc.locator("a[onclick*='onItemClick'], [data-codigo]").first.count() > 0
        except Exception:
            has_ops = False
        return (has_index and has_ops) or (has_book_url and has_ops)

    # 1) pÃƒÂ¡gina principal
    try:
        if _is_book_scope(libro):
            return libro
    except Exception:
        pass

    # 2) frames hijos
    for fr in getattr(libro, "frames", []):
        try:
            if _is_book_scope(fr):
                return fr
        except Exception:
            continue

    # 3) ÃƒÂºltimo recurso: el primer frame con anchors de operaciones
    for fr in getattr(libro, "frames", []):
        try:
            if fr.locator("a[onclick*='onItemClick'], [data-codigo]").first.count():
                return fr
        except Exception:
            continue

    return libro


def _all_scopes(root):
    """Itera la pÃƒÂ¡gina y todos sus frames (profundidad)."""
    try:
        yield root
        for fr in getattr(root, "frames", []):
            yield from _all_scopes(fr)
    except Exception:
        return


def _listar_operaciones_rapido(libro):
    import re, time

    GUID_RE = re.compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        re.I,
    )

    def _iter_frames(scope):
        yield scope
        for fr in getattr(scope, "frames", []):
            yield from _iter_frames(fr)

    def _expand(scope):
        # SOLO dentro del contenedor del ÃƒÂ­ndice
        idx = None
        for sel in ("#indice", ".indice"):
            try:
                loc = scope.locator(sel).first
                if loc.count():
                    idx = loc
                    break
            except Exception:
                pass
        if not idx:
            return  # no tocar nada fuera del ÃƒÂ­ndice

        sels = [
            ".dropdown-toggle[aria-expanded='false']",
            "a.nav-link.dropdown-toggle[aria-expanded='false']",
            "[data-bs-toggle='collapse'][aria-expanded='false']",
            "[data-bs-toggle='dropdown'][aria-expanded='false']",
        ]
        for s in sels:
            try:
                btns = idx.locator(s)
                for i in range(min(btns.count(), 25)):
                    b = btns.nth(i)
                    try:
                        b.click()
                    except Exception:
                        try:
                            b.evaluate("el=>el.click()")
                        except Exception:
                            pass
            except Exception:
                continue
            try:
                scope.wait_for_timeout(150)
            except Exception:
                pass

    def _scroll(scope):
        # SOLO scrolleo del ÃƒÂ­ndice (nada de wheel global)
        for sel in ("#indice", ".indice"):
            try:
                if scope.locator(sel).first.count():
                    scope.eval_on_selector(sel, "el=>el.scrollBy(0, el.clientHeight||600)")
                    return
            except Exception:
                pass

    def _collect_from(scope):
        anchors = scope.locator(
            # onclick inline u href javascript:onItemClick(...)
            "a[onclick*='onItemClick('], a[href*='onItemClick('], "
            # data-attrs
            "a[data-codigo], [role='button'][data-codigo], li[data-codigo] a, nav a[data-codigo], "
            # tabs/pills que guardan relaciÃƒÂ³n por aria-controls / clases con GUID
            "a[aria-controls], a.nav-link"
        )
        n = anchors.count()
        items, vistos = [], set()
        for i in range(n):
            a = anchors.nth(i)
            oc = a.get_attribute("onclick") or ""
            href = a.get_attribute("href") or ""
            data_id = a.get_attribute("data-codigo")
            data_tipo = a.get_attribute("data-tipo") or ""
            aria_ctl = a.get_attribute("aria-controls") or ""
            clases = a.get_attribute("class") or ""

            m = re.search(r"onItemClick\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]", oc + " " + href)
            if m:
                op_id, tipo = m.group(1), m.group(2)
            elif data_id:
                op_id, tipo = data_id, data_tipo
            elif GUID_RE.search(aria_ctl or ""):
                op_id, tipo = GUID_RE.search(aria_ctl).group(0), data_tipo
            elif GUID_RE.search(clases or ""):
                op_id, tipo = GUID_RE.search(clases).group(0), data_tipo
            else:
                continue

            if op_id in vistos:
                continue

            try:
                t = (a.inner_text() or "").strip() or (a.get_attribute("title") or "").strip()
            except Exception:
                t = ""

            items.append({"id": op_id, "tipo": tipo, "titulo": t})
            vistos.add(op_id)
        return items

    S = _libro_scope(libro)
    try:
        S.wait_for_load_state("domcontentloaded")
        S.wait_for_load_state("networkidle")
    except Exception:
        pass

    # si el ÃƒÂ­ndice estÃƒÂ¡ en pestaÃƒÂ±a "ÃƒÂndice", mostrarla
    for sel in ("[data-bs-target='#indice']", "a[href='#indice']", "[aria-controls='indice']"):
        try:
            loc = S.locator(sel).first
            if loc.count():
                try:
                    loc.click()
                except Exception:
                    loc.evaluate("el=>el.click()")
                break
        except Exception:
            pass

    t0 = time.time()
    while (time.time() - t0) < 20.0:
        for sc in _iter_frames(S):
            try:
                _expand(sc)
                items = _collect_from(sc)
                if items:
                    return items
                _scroll(sc)
            except Exception:
                continue
        try:
            S.wait_for_timeout(250)
        except Exception:
            break

    return []


def _url_from_ver_adjunto(js_call: str, proxy_prefix: str) -> str | None:
    """
    Convierte "javascript:VerAdjuntoFichero('29229802')" o UUID en una URL real,
    preservando el mismo /proxy/.
    """
    m = re.search(r"VerAdjuntoFichero\(\s*['\"]([^'\"]+)['\"]\s*\)", js_call or "", re.I)
    if not m:
        return None

    file_id = quote((m.group(1) or "").strip(), safe="")
    if not file_id:
        return None
    # Ruta real usada por SAC para un adjunto individual:
    real = f"https://aplicaciones.tribunales.gov.ar/SacInterior/_Expedientes/Fichero.aspx?idFichero={file_id}"
    try:
        return _proxify_abs_url(proxy_prefix, real) if proxy_prefix else real
    except Exception:
        return (proxy_prefix + real) if proxy_prefix else real


def _imagen_a_pdf(img: Path) -> Path:
    pdf = img.with_suffix(".pdf")
    Image.open(img).save(pdf, "PDF", resolution=120.0)
    return pdf


def fusionar_pdfs(lista, destino: Path):
    w = PdfWriter()
    for pdf in lista:
        for p in PdfReader(str(pdf)).pages:
            w.add_page(p)
    with open(destino, "wb") as f:
        w.write(f)


def _pdf_char_count(path: Path, paginas: int = 3) -> int:
    """
    Cuenta caracteres de texto en las primeras paginas del PDF.
    Usa pdfminer si estÃƒÂ¡; si no, PyPDF2. Devuelve un entero.
    """
    try:
        from pdfminer.high_level import extract_text
        txt = extract_text(str(path), maxpages=int(paginas)) or ""
        return len((txt or "").strip())
    except Exception:
        try:
            r = PdfReader(str(path))
            n = min(len(r.pages), max(1, int(paginas)))
            total = 0
            for i in range(n):
                t = (r.pages[i].extract_text() or "").strip()
                total += len(t)
            return total
        except Exception:
            return 0


def _has_enough_text(path: Path, paginas: int = 3) -> bool:
    # Umbral por defecto mÃƒÂ¡s alto para ser estrictos al considerar que ya hay texto
    min_chars = int(os.getenv("OCR_MIN_CHARS", "1200"))
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(path))
        total = 0
        n = min(doc.page_count, max(1, int(paginas)))
        for i in range(n):
            pg = doc[i]
            h = pg.rect.height
            top, bottom = 0.12*h, 0.88*h  # ignora ~12% superior e inferior (cabecera/pie)
            for x0,y0,x1,y1,txt,*_ in (pg.get_text("blocks") or []):
                if y1 <= top or y0 >= bottom:
                    continue
                total += len((txt or "").strip())
        doc.close()
        return total >= min_chars
    except Exception:
        # Fallback PyPDF2
        try:
            from PyPDF2 import PdfReader
            r = PdfReader(str(path))
            total = 0
            for p in r.pages[:paginas]:
                total += len((p.extract_text() or "").strip())
            return total >= min_chars
        except Exception:
            return False


def _page_has_text(pg, min_chars: int = 50) -> bool:
    try:
        # Count text only in the page body (ignore header/footer and side margins).
        r = pg.rect
        top = r.height * 0.15
        bottom = r.height * 0.85
        left = r.width * 0.06
        right = r.width * 0.94
        total = 0
        for x0, y0, x1, y1, txt, *_ in (pg.get_text("blocks") or []):
            if (y1 <= top) or (y0 >= bottom) or (x1 <= left) or (x0 >= right):
                continue
            t = (txt or "").strip()
            if len(t) < 8:
                continue
            total += len(t)
        return total >= min_chars
    except Exception:
        try:
            t = (pg.get_text("text") or "").strip()
            return len(t) >= (min_chars * 2)
        except Exception:
            return False


async def _winocr_recognize_png(png_bytes: bytes, lang_tag: str):
    stream = InMemoryRandomAccessStream()
    writer = DataWriter(stream)
    writer.write_bytes(png_bytes)
    await writer.store_async()
    stream.seek(0)

    decoder = await BitmapDecoder.create_async(stream)
    sbmp = await decoder.get_software_bitmap_async()

    engine = winocr.OcrEngine.try_create_from_language(WinLanguage(lang_tag))
    if engine is None:
        engine = winocr.OcrEngine.try_create_from_user_profile_languages()
    if engine is None:
        raise RuntimeError("Motor WinOCR no disponible (falta paquete de idioma en Windows).")

    result = await engine.recognize_async(sbmp)
    return result


def convertir_pdf_a_imagenes(
    pdf_path: str | Path, out_dir: str | Path, formato: str = "png", dpi: int = 300
) -> list[str]:
    """Convierte cada pÃƒÂ¡gina de un PDF en un archivo de imagen independiente.

    Se intentarÃƒÂ¡ usar `pdfimages (Poppler) si estÃƒÂ¡ disponible en el sistema.
    Si no se encuentra, se probarÃƒÂ¡ `pdftoppm. Como ÃƒÂºltimo recurso, se
    utilizarÃƒÂ¡ PyMuPDF <https://pymupdf.readthedocs.io/>_ (`fitz).

    Los archivos resultantes se nombran `page_001.png, page_002.png,
    etc. y se guardan en `out_dir.

    Parameters
    ----------
    pdf_path:
        Ruta al archivo PDF de origen.
    out_dir:
        Directorio donde se guardarÃƒÂ¡n las imÃƒÂ¡genes.
    formato:
        Formato de salida: `"png" (por defecto) o "tiff".
    dpi:
        ResoluciÃƒÂ³n para el renderizado cuando se utiliza PyMuPDF o `pdftoppm.

    Returns
    -------
    list[str]
        Lista con las rutas de las imÃƒÂ¡genes generadas.

    Raises
    ------
    FileNotFoundError
        Si `pdf_path no existe.
    ValueError
        Si `formato no es "png" ni "tiff".
    RuntimeError
        Si no hay herramientas disponibles para realizar la conversiÃƒÂ³n.
    """

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"Archivo PDF no encontrado: {pdf_path}")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    formato = formato.lower()
    if formato not in {"png", "tiff"}:
        raise ValueError("formato debe ser 'png' o 'tiff'")

    tmp_base = out_dir / "tmp_page"
    ext = "png" if formato == "png" else "tiff"

    def _renombrar_salida() -> list[str]:
        generados = sorted(out_dir.glob(f"{tmp_base.name}*"))
        imagenes: list[str] = []
        for i, src in enumerate(generados, 1):
            dst = out_dir / f"page_{i:03d}.{ext}"
            src.rename(dst)
            imagenes.append(str(dst))
        return imagenes

    # 1) Intento: pdfimages
    cmd = None
    if shutil.which("pdfimages"):
        cmd = ["pdfimages", f"-{formato}", str(pdf_path), str(tmp_base)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **_subprocess_hidden_kwargs())
            return _renombrar_salida()
        except Exception:
            pass

    # 2) Intento: pdftoppm
    if shutil.which("pdftoppm"):
        cmd = ["pdftoppm", f"-{formato}", "-r", str(dpi), str(pdf_path), str(tmp_base)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **_subprocess_hidden_kwargs())
            return _renombrar_salida()
        except Exception:
            pass

    # 3) Fallback: PyMuPDF
    try:
        import fitz
    except Exception as e:  # pragma: no cover - se ejecuta solo si falta fitz
        raise RuntimeError(
            "No se encontraron 'pdfimages', 'pdftoppm' ni la librerÃƒÂ­a PyMuPDF"
        ) from e

    doc = fitz.open(str(pdf_path))
    imagenes: list[str] = []
    for i, pagina in enumerate(doc, 1):
        pix = pagina.get_pixmap(dpi=dpi)
        dst = out_dir / f"page_{i:03d}.{ext}"
        pix.save(str(dst))
        imagenes.append(str(dst))
    return imagenes

def _apply_winocr_to_pdf(pdf_in: Path, dst: Path, lang_tags: list[str] | None = None, dpi: int = 300) -> bool:
    """
    Aplica OCR WinRT/Windows a un PDF y agrega texto seleccionable.
    Solo realiza OCR sobre Ã¯Â¿Â½?oadjuntosÃ¯Â¿Â½?Ã¯Â¿Â½ (pÃƒÂ¡ginas escaneadas / sin texto ÃƒÂºtil en el cuerpo).
    Probado con PyMuPDF 1.26.4 (MuPDF 1.26.7) en Windows / Python 3.12.

    ENV opcionales:
      OCR_DEBUG=1                -> logs extra
      OCR_INVISIBLE=0/1          -> si 1, texto invisible (no recomendado para selecciÃƒÂ³n)
      OCR_VISIBLE_TEXT=1         -> fuerza texto visible
      OCR_ROTATIONS="0,90,270"   -> rotaciones a probar
      OCR_SCALE=2.0              -> escalado previo para OCR
      PAGE_BODY_MIN_CHARS=50     -> umbral para Ã¯Â¿Â½?opÃƒÂ¡gina ya tiene textoÃ¯Â¿Â½?Ã¯Â¿Â½
      OCR_USE_OCG=0/1            -> si 1, intenta capa OCG
      OCR_FONT="helv"            -> fuente PDF estÃƒÂ¡ndar a usar
      WINOCR_LANGS="es-AR+es-ES+en-US"
    """
    import os, datetime, logging
    try:
        import fitz  # PyMuPDF
    except Exception as e:
        logging.info(f"[WINOCR] PyMuPDF no disponible: {e}")
        return False

    # requisito externo
    if not _WINOCR_OK:
        logging.info("[WINOCR] Paquete winsdk/winrt no disponible.")
        return False

    # idiomas
    if not lang_tags:
        lang_tags = os.getenv("WINOCR_LANGS", "es-AR+es-ES+en-US").split("+")

    # flags
    dbg            = os.getenv("OCR_DEBUG", "1").lower() in ("1", "true", "yes", "on")
    force_visible  = os.getenv("OCR_VISIBLE_TEXT", "1").lower() in ("1", "true", "yes", "on")
    make_invisible = os.getenv("OCR_INVISIBLE", "0").lower() in ("1", "true", "yes", "on") and not force_visible
    min_chars      = int(os.getenv("PAGE_BODY_MIN_CHARS", "50"))
    use_ocg        = os.getenv("OCR_USE_OCG", "0").lower() in ("1", "true", "yes", "on")
    font_name      = os.getenv("OCR_FONT", "helv")  # fuente base PDF, no requiere incrustar

    # --- helpers -----------------------------------------------------------------
    def _shrink_font_to_fit(text: str, rect: "fitz.Rect", base_size: float) -> float:
        """Baja font-size si el ancho del texto supera el rect. Mantiene altura."""
        try:
            w = fitz.get_text_length(text, fontname=font_name, fontsize=base_size)
            if w > 0 and rect.width > 0 and w > rect.width:
                base_size *= (rect.width / w)
        except Exception:
            pass
        return max(3.5, base_size)

    def _draw_word(page: "fitz.Page", rect: "fitz.Rect", text: str):
        """Dibuja una palabra visible y seleccionable (insert_text, no textbox)."""
        if not text:
            return
        h = rect.height
        size = _shrink_font_to_fit(text, rect, base_size=max(4.0, h * 0.86))
        baseline_y = rect.y1 - max(0.6, h * 0.08)
        if make_invisible:
            # Nota: algunos visores no permiten selecciÃƒÂ³n con render_mode=3
            page.insert_text(
                fitz.Point(rect.x0, baseline_y),
                text, fontsize=size, fontname=font_name, render_mode=3, color=(0, 0, 0)
            )
        else:
            page.insert_text(
                fitz.Point(rect.x0, baseline_y),
                text, fontsize=size, fontname=font_name, render_mode=0, color=(0, 0, 0)
            )

    def _is_attachment_page(pg: "fitz.Page") -> bool:
        """HeurÃƒÂ­stica: sin texto de cuerpo + presencia/ÃƒÂ¡rea de imagen relevante."""
        try:
            if _page_has_text(pg, min_chars=min_chars):
                return False
        except Exception:
            pass
        try:
            page_area = float(pg.rect.width * pg.rect.height)
            d = pg.get_text("dict")
            img_area = 0.0
            for b in d.get("blocks", []):
                if b.get("type") == 1 and "bbox" in b:  # imagen
                    rect = fitz.Rect(b["bbox"])
                    img_area += float(rect.width * rect.height)
            # adjunto si la/s imagen/es cubren una parte importante de la pÃƒÂ¡gina
            if page_area > 0 and (img_area / page_area) > 0.35:
                return True
        except Exception:
            pass
        # ÃƒÂºltimo recurso: si no hay texto y hay al menos una imagen embebida
        try:
            return len(pg.get_images(full=True)) > 0
        except Exception:
            return True
    # ---------------------------------------------------------------------------

    # abrir
    try:
        src = fitz.open(str(pdf_in))
    except Exception as e:
        logging.info(f"[WINOCR] No pude abrir PDF origen: {e}")
        return False

    out = fitz.open()
    try:
        # metadatos
        out.set_metadata({
            "keywords": "OCR,Searchable",
            "creator": "SACDownloader",
            "producer": "SACDownloader",
            "title": f"Expediente con OCR - {pdf_in.name}",
            "creationDate": datetime.datetime.now().strftime("D:%Y%m%d%H%M%S"),
        })

        # OCG opcional (no recomendado para compatibilidad de selecciÃƒÂ³n)
        ocr_layer = None
        if use_ocg:
            try:
                ocr_layer = out.add_ocg("OCR Layer", on=True, intent="View")
            except Exception as e:
                logging.info(f"[WINOCR] add_ocg fallÃƒÂ³, sigo sin OCG: {e}")
                ocr_layer = None

        # Recorrer pÃƒÂ¡ginas y hacer OCR SOLO en adjuntos
        for i in range(src.page_count):
            pg = src[i]

            # Si NO es adjunto -> copiar tal cual, sin OCR
            if not _is_attachment_page(pg):
                out.insert_pdf(src, from_page=i, to_page=i)
                if dbg:
                    logging.info(f"[WINOCR:DBG] page={i+1} sin OCR (no es adjunto)")
                continue

            # Renderizar imagen de esa pÃƒÂ¡gina (solo para este adjunto)
            try:
                zoom = dpi / 72.0
                mat = fitz.Matrix(zoom, zoom)
                pix = pg.get_pixmap(matrix=mat, alpha=False)
                png_bytes = pix.tobytes("png")
                img_w, img_h = pix.width, pix.height
            except Exception as e:
                logging.info(f"[WINOCR] No pude rasterizar pÃƒÂ¡gina {i+1}: {e}")
                out.insert_pdf(src, from_page=i, to_page=i)
                continue

            page_w, page_h = float(pg.rect.width), float(pg.rect.height)

            # OCR (rotaciones + preproc)
            rots = [int(x) for x in os.getenv("OCR_ROTATIONS", "0,90,270").split(",") if x.strip().isdigit()]
            ocr_result, best_bytes, best_wc, best_deg = None, png_bytes, -1, 0

            try:
                from PIL import Image, ImageOps, ImageFilter
                import io as _io
                def _prep(b, deg):
                    im = Image.open(_io.BytesIO(b)).convert("RGB")  # sin alfa
                    scale = float(os.getenv("OCR_SCALE", "2.0"))
                    w, h = im.size
                    im = im.resize((int(w * scale), int(h * scale)))
                    mw, mh = 5000, 5000
                    w2, h2 = im.size
                    if w2 > mw or h2 > mh:
                        r = min(mw / float(w2), mh / float(h2))
                        im = im.resize((int(w2 * r), int(h2 * r)))
                    im = ImageOps.autocontrast(im)
                    im = im.filter(ImageFilter.UnsharpMask(radius=1.0, percent=120, threshold=3))
                    if deg:
                        im = im.rotate(deg, expand=True)
                    outb = _io.BytesIO()
                    im.save(outb, format="PNG")
                    return outb.getvalue()
            except Exception:
                _prep = None

            early_stop_wc = int(os.getenv("OCR_EARLY_STOP_WC", "140"))
            stop_all = False
            for deg in rots:
                for j, tag in enumerate(lang_tags):
                    try:
                        # Primera pasada (deg=0, primer idioma) sin pre-proceso para acelerar
                        if _prep and not (deg == 0 and j == 0):
                            data = _prep(png_bytes, deg)
                        else:
                            data = png_bytes
                        res  = _run_ocr_sync(data, tag.strip())
                        wc   = 0
                        if res and getattr(res, "lines", None):
                            try:
                                wc = sum(len(ln.words) for ln in res.lines)
                            except Exception:
                                wc = 0
                        if res and getattr(res, "text", None) and wc > best_wc:
                            ocr_result, best_wc, best_deg, best_bytes = res, wc, deg, data
                        # Corta temprano si ya hay suficiente texto
                        if best_wc >= early_stop_wc:
                            stop_all = True
                            break
                    except Exception as e:
                        if dbg:
                            logging.info(f"[WINOCR] OCR fallo {tag} deg={deg}: {e}")
                        continue
                if stop_all:
                    break

            if dbg:
                logging.info(f"[WINOCR:DBG] page={i+1} (adjunto) best_deg={best_deg} best_wc={best_wc}")

            # tamaÃƒÂ±o de la imagen Ã¯Â¿Â½?oganadoraÃ¯Â¿Â½?Ã¯Â¿Â½ (por si rotÃƒÂ³)
            try:
                from PIL import Image as _Image
                import io as _io
                _imtmp = _Image.open(_io.BytesIO(best_bytes))
                img_w, img_h = _imtmp.size
            except Exception:
                pass

            # factores de escala imagen->PDF (Ã‚Â¡sin invertir Y!)
            sx = page_w / float(img_w)
            sy = page_h / float(img_h)

            # copiar pÃƒÂ¡gina original
            out.insert_pdf(src, from_page=i, to_page=i)
            newp = out[-1]

            # texto OCR seleccionable (debajo)
            if ocr_result and getattr(ocr_result, "lines", None):
                for line in ocr_result.lines:
                    for word in line.words:
                        try:
                            r = word.bounding_rect  # x,y,width,height (coords de la imagen)
                            x0 = r.x * sx
                            x1 = (r.x + r.width) * sx
                            y0 = r.y * sy
                            y1 = (r.y + r.height) * sy
                            rect = fitz.Rect(x0, y0, x1, y1)
                            _draw_word(newp, rect, word.text)
                        except Exception:
                            continue

            # Pegar la imagen de la pÃƒÂ¡gina *encima* (sin cuadros rojos)
            # Usamos el render original (png_bytes) para que calce 1:1 con la pÃƒÂ¡gina.
            newp.insert_image(fitz.Rect(0, 0, page_w, page_h), stream=png_bytes, overlay=True)

            # normalizar recursos/XObjects
            try:
                newp.wrap_contents()
            except Exception:
                pass

        out.save(str(dst), deflate=True, garbage=3)
        ok = dst.exists() and dst.stat().st_size > 1024
        if ok and dbg:
            try:
                logging.info(f"[WINOCR:DBG] OCGS: {out.get_ocgs()}")
                logging.info(f"[WINOCR:DBG] UI:   {out.layer_ui_configs()}")
            except Exception:
                pass
        return ok

    except Exception as e:
        logging.info(f"[WINOCR] Error procesando PDF: {e}")
        return False
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            out.close()
        except Exception:
            pass

def _maybe_ocr(pdf_in: Path, force: bool = False) -> Path:
    """
    OCR con Windows WinRT.
    - OCR_MODE=off   -> nunca
    - OCR_MODE=force -> siempre
    - OCR_MODE=auto  -> solo si detecta poco texto (_has_enough_text)
    - WINOCR_LANGS="es-AR+es-ES+en-US"
    - OCR_DPI=300 (por default)
    """
    mode = (os.getenv("OCR_MODE", "auto") or "").lower() or "auto"
    if mode == "off":
        return pdf_in

    # Solo forzar si se pide. En modo 'auto' se decide por contenido.
    need_ocr = bool(force)
    if mode == "auto":
        # Page-level scan ignoring headers; trigger OCR if any page lacks body text
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(str(pdf_in))
            need_ocr = False
            limit = min(doc.page_count, max(1, int(os.getenv("OCR_SCAN_MAX_PAGES", "200"))))
            # MÃƒÂ¡s estricto: requiere mÃƒÂ¡s texto en el cuerpo para saltar OCR
            min_chars = int(os.getenv("PAGE_BODY_MIN_CHARS", "80"))
            for i in range(limit):
                try:
                    if not _page_has_text(doc[i], min_chars=min_chars):
                        need_ocr = True
                        break
                except Exception:
                    # Be conservative if analysis fails
                    need_ocr = True
                    break
            doc.close()
        except Exception:
            # Fallback coarse check (sample more pages)
            try:
                need_ocr = not _has_enough_text(pdf_in, paginas=int(os.getenv("OCR_SAMPLE_PAGES", "10")))
            except Exception:
                need_ocr = True


    if not need_ocr:
        logging.info("[WINOCR] AUTO: suficiente texto; salto OCR")
        return pdf_in

    out = pdf_in.with_suffix(".ocr.pdf")
    langs = (os.getenv("WINOCR_LANGS", "es-AR+es-ES+en-US").split("+"))
    ok = _apply_winocr_to_pdf(pdf_in, out, langs, dpi=int(os.getenv("OCR_DPI", "450")))
    if ok:
        logging.info(f"[WINOCR] OK -> {out.name}")
        return out

    logging.info("[WINOCR] Falla/No disponible -> uso original")
    return pdf_in


# --------------------------- Helpers UI/DOM ----------------------------
def _pick_selector(page, candidates):
    for s in candidates:
        try:
            if page.query_selector(s):
                return s
        except Exception:
            pass
    return None


def _fill_first(page, candidates, value):
    s = _pick_selector(page, candidates)
    if not s:
        raise RuntimeError(f"No encontrÃƒÂ© control para {candidates}")
    page.fill(s, value)


def _click_first(page, candidates):
    s = _pick_selector(page, candidates)
    if s:
        page.click(s)
        return True
    return False


def _get_proxy_prefix(page) -> str:
    """
    Devuelve 'https://teletrabajo.justiciacordoba.gob.ar/proxy/<token>/' si existe.
    Si NO hay proxy (Intranet directa), devuelve cadena vacÃƒÂ­a "" (no explota).
    """
    import re

    def _scan_url(u: str):
        if not u:
            return None
        m = re.search(r"https://teletrabajo\.justiciacordoba\.gob\.ar/proxy/[^/]+/", u)
        if m:
            return m.group(0)
        m = re.search(r"^/proxy/[^/]+/", u)
        if m:
            return "https://teletrabajo.justiciacordoba.gob.ar" + m.group(0)
        return None

    # URL actual
    try:
        p = _scan_url(page.url)
        if p:
            return p
    except Exception:
        pass

    # Links de la pÃƒÂ¡gina
    try:
        for a in page.query_selector_all("a[href]"):
            p = _scan_url(a.get_attribute("href") or "")
            if p:
                return p
    except Exception:
        pass

    # Frames
    try:
        for fr in page.frames:
            p = _scan_url(getattr(fr, "url", None))
            if p:
                return p
    except Exception:
        pass

    # Sin proxy ? Intranet directa
    return ""


def _es_login_intranet(pg) -> bool:
    """Detecta login del portal viejo o del portal Angular."""
    u = (getattr(pg, "url", "") or "").lower()
    if ("portalweb/login/login.aspx" in u) or ("portalwebnet/#/login" in u):
        return True

    try:
        tiene_pwd = pg.locator("input[type='password']").first.count() > 0
    except Exception:
        tiene_pwd = False

    try:
        # texto tÃƒÂ­pico del portal clÃƒÂ¡sico
        tiene_texto = (
            pg.get_by_text(re.compile(r"ingrese\s+nombre\s+de\s+usuario\s+y\s+contraseÃƒÂ±a", re.I))
            .first.count()
            > 0
        )
    except Exception:
        tiene_texto = False

    return tiene_pwd and tiene_texto


def _page_requires_portal_login(pg) -> bool:
    u = (getattr(pg, "url", "") or "").lower()
    if (
        "portalweb/login/login.aspx" in u
        or "portalwebnet/#/login" in u
        or "sacinterior/login.aspx" in u
    ):
        return True
    try:
        return _es_login_intranet(pg)
    except Exception:
        return False


def _page_closed_or_invalid(pg) -> bool:
    try:
        return bool(pg.is_closed())
    except Exception:
        return True


def _is_page_closed_exc(exc: Exception) -> bool:
    try:
        msg = str(exc or "")
    except Exception:
        return False
    msg = msg.lower()
    return (
        "target page, context or browser has been closed" in msg
        or "page has been closed" in msg
        or "browser has been closed" in msg
        or "context has been closed" in msg
    )


def _sac_host_base(page) -> str:
    """
    Devuelve esquema+host del SAC real.
    - Con proxy Teletrabajo: extrae el host interno desde /proxy/.../https/<host>/...
    - Sin proxy: usa esquema+host de la URL actual.
    """
    import re

    u = getattr(page, "url", "") or ""
    try:
        pu = urlparse(u)
    except Exception:
        pu = None

    if "/proxy/" in u or "teletrabajo.justiciacordoba.gob.ar" in u:
        try:
            m = re.search(r"/proxy/[^/]+/https?(?:://|/)([^/]+)/", u)
            if m and m.group(1):
                return f"https://{m.group(1)}"
        except Exception:
            pass
        return "https://aplicaciones.tribunales.gov.ar"

    if pu and pu.scheme and pu.netloc:
        return f"{pu.scheme}://{pu.netloc}"

    return "https://aplicaciones.tribunales.gov.ar"


def _proxify_abs_url(proxy_prefix: str, abs_url: str) -> str:
    """
    Convierte URL absoluta a formato SSL-VPN estable:
    /proxy/<token>/<scheme>/<host>/<path>?query
    """
    if not abs_url:
        return abs_url
    if not proxy_prefix:
        return abs_url
    try:
        pu = urlparse(abs_url)
        if not (pu.scheme and pu.netloc):
            return proxy_prefix + abs_url.lstrip("/")
        path = pu.path or "/"
        q = f"?{pu.query}" if pu.query else ""
        return f"{proxy_prefix}{pu.scheme}/{pu.netloc}{path}{q}"
    except Exception:
        return proxy_prefix + abs_url.lstrip("/")


def _radiografia_candidate_urls(page) -> list[str]:
    proxy_prefix = _get_proxy_prefix(page)
    bases = []
    for b in [
        _sac_host_base(page),
        "https://aplicaciones.tribunales.gov.ar",
        "https://www.tribunales.gov.ar",
    ]:
        if b and b not in bases:
            bases.append(b)

    paths = [
        "/SacInterior/_Expedientes/Radiografia.aspx?ClearNavMenu=1",
        "/SacInterior/_Expedientes/Radiografia.aspx",
    ]

    out = []
    for b in bases:
        for pth in paths:
            absu = b.rstrip("/") + pth
            u = _proxify_abs_url(proxy_prefix, absu)
            if u not in out:
                out.append(u)
            # compatibilidad con formato legacy /proxy/<token>/https://host/...
            if proxy_prefix:
                legacy = proxy_prefix + absu
                if legacy not in out:
                    out.append(legacy)
    return out

def _handle_loginconfirm(page):
    """Si aparece 'Already Logged In', clic en 'Log in Anyway'."""
    if re.search(r"/remote/loginconfirm", page.url, re.I):
        for sel in ["text=Log in Anyway", "a:has-text('Log in Anyway')", "button:has-text('Log in Anyway')"]:
            try:
                page.locator(sel).first.click()
                page.wait_for_load_state("networkidle")
                break
            except Exception:
                pass


def _goto_portal_grid(page):
    # Aseguramos la grilla del portal
    page.goto("https://teletrabajo.justiciacordoba.gob.ar/static/sslvpn/portal/", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")


def _debug_dump(page, name="debug"):
    if _page_closed_or_invalid(page):
        try:
            logging.error(f"[DEBUG] {name}: pagina/contexto cerrados antes del dump")
        except Exception:
            pass
        return
    try:
        ss = BASE_PATH / f"{name}.png"
        html = BASE_PATH / f"{name}.html"
        page.screenshot(path=str(ss), full_page=True)
        with open(html, "w", encoding="utf-8") as f:
            f.write(page.content())
        logging.info(f"[DEBUG] {name}: url={page.url}")
    except Exception as e:
        logging.error(f"[DEBUG] dump fail: {e}")


def _is_proxy_error(page) -> bool:
    try:
        t = page.title()
        c = page.content()
        return ("SSL VPN Proxy Error" in t) or ("SSL VPN Proxy Error" in c)
    except Exception:
        return False


def _extract_url_from_js(js: str) -> str | None:
    if not js:
        return None
    m = re.search(r"https?://[^\s'\"()]+", js)
    if m:
        return m.group(0)
    m = re.search(r"/proxy/[^'\"()]+", js)
    if m:
        return "https://teletrabajo.justiciacordoba.gob.ar" + m.group(0)
    return None


def _fill_radiografia_y_buscar(page, nro_exp):
    import time

    if _page_closed_or_invalid(page):
        logging.info("[RADIO] La pagina de Radiografia ya estaba cerrada antes de buscar.")
        raise RuntimeError("RADIO_PAGE_CLOSED")

    if _page_requires_portal_login(page):
        logging.info("[RADIO] El portal sigue pidiendo login antes de abrir Radiografia.")
        raise RuntimeError("PORTAL_LOGIN_REQUIRED")

    def _iter_scopes(root):
        pendientes = [root]
        vistos = set()
        while pendientes:
            sc = pendientes.pop(0)
            k = id(sc)
            if k in vistos:
                continue
            vistos.add(k)
            yield sc
            try:
                pendientes.extend(list(getattr(sc, "frames", [])))
            except Exception:
                pass

    def _find_visible_field(timeout_ms=18000):
        sels = [
            "#txtNroExpediente",
            "input[id$='txtNroExpediente']",
            "input[name$='txtNroExpediente']",
            "input[id*='NroExpediente']",
            "input[name*='NroExpediente']",
            "xpath=//label[contains(normalize-space(.),'Expediente')]/following::input[1]",
            "xpath=//td[contains(normalize-space(.),'Expediente')]/following::input[1]",
            "xpath=//input[@type='text' and (contains(@id,'Expediente') or contains(@name,'Expediente'))]",
        ]

        deadline = time.time() + (max(1000, int(timeout_ms)) / 1000.0)
        while time.time() < deadline:
            if _page_closed_or_invalid(page):
                raise RuntimeError("RADIO_PAGE_CLOSED")
            scopes = list(_iter_scopes(page))
            for sc in scopes:
                for sel in sels:
                    try:
                        loc = sc.locator(sel).first
                        if not loc.count():
                            continue
                        try:
                            loc.wait_for(state="visible", timeout=600)
                        except Exception:
                            pass
                        if not loc.is_visible():
                            continue
                        try:
                            if loc.is_disabled():
                                continue
                        except Exception:
                            pass
                        return sc, loc
                    except Exception as e:
                        if _is_page_closed_exc(e):
                            raise RuntimeError("RADIO_PAGE_CLOSED") from e
                        continue
            try:
                page.wait_for_timeout(250)
            except Exception as e:
                if _is_page_closed_exc(e):
                    raise RuntimeError("RADIO_PAGE_CLOSED") from e
                time.sleep(0.25)

        return None, None

    try:
        _kill_overlays(page)
    except Exception as e:
        if _is_page_closed_exc(e):
            logging.info("[RADIO] La pagina se cerro al preparar la busqueda.")
            raise RuntimeError("RADIO_PAGE_CLOSED") from e
        raise
    sc, txt = _find_visible_field(timeout_ms=int(os.getenv("RADIO_FIELD_TIMEOUT_MS", "18000")))

    if not txt:
        # Si estamos en una pagina de error del proxy, reintentar Radiografia por URLs alternativas.
        try:
            if _is_proxy_error(page):
                logging.info("[RADIO] Detectado SSL VPN Proxy Error; reintentando Radiografia con URL alternativa")
        except Exception:
            pass

        for u in _radiografia_candidate_urls(page):
            if _page_closed_or_invalid(page):
                logging.info("[RADIO] La pagina se cerro antes de reintentar URLs de Radiografia.")
                raise RuntimeError("RADIO_PAGE_CLOSED")
            try:
                page.goto(u, wait_until="domcontentloaded")
            except Exception as e:
                if _is_page_closed_exc(e):
                    logging.info("[RADIO] La pagina se cerro durante goto a Radiografia.")
                    raise RuntimeError("RADIO_PAGE_CLOSED") from e
                continue
            if _is_proxy_error(page):
                try:
                    logging.info(f"[RADIO] URL bloqueada por proxy: {u}")
                except Exception:
                    pass
                continue
            _kill_overlays(page)
            sc, txt = _find_visible_field(timeout_ms=7000)
            if txt:
                break

    if not txt:
        _debug_dump(page, "no_txt_expediente")
        raise RuntimeError("No pude ubicar el campo 'Número de Expediente'.")
    try:
        txt.scroll_into_view_if_needed()
    except Exception as e:
        if _is_page_closed_exc(e):
            raise RuntimeError("RADIO_PAGE_CLOSED") from e
        pass

    try:
        txt.click(click_count=3)
    except Exception as e:
        if _is_page_closed_exc(e):
            raise RuntimeError("RADIO_PAGE_CLOSED") from e
        txt.click()
    try:
        txt.fill(str(nro_exp).strip())
    except Exception as e:
        if _is_page_closed_exc(e):
            raise RuntimeError("RADIO_PAGE_CLOSED") from e
        raise

    # Primero Enter en el mismo scope.
    try:
        txt.press("Enter")
        sc.wait_for_load_state("networkidle")
    except Exception as e:
        if _is_page_closed_exc(e):
            raise RuntimeError("RADIO_PAGE_CLOSED") from e
        pass

    # Si hace falta, usar boton Buscar en el mismo scope.
    btn = sc.locator("#btnBuscarExp, input[id$='btnBuscarExp']").first

    if not btn.count():
        import re
        btn = sc.get_by_role("button", name=re.compile(r"buscar", re.I)).first

    if not btn.count():
        btn = sc.locator(
            "input[type='submit'][value*='Buscar'], "
            "input[type='image'][alt*='Buscar'], "
            "input[title*='Buscar']"
        ).first

    if not btn.count():
        btn = sc.locator(
            "xpath=//input[( @type='image' or @type='submit') "
            "and (contains(@id,'Buscar') or contains(@value,'Buscar') "
            "or contains(@alt,'Buscar') or contains(@title,'Buscar'))]"
        ).first

    if btn.count():
        try:
            btn.click()
            sc.wait_for_load_state("networkidle")
        except Exception as e:
            if _is_page_closed_exc(e):
                raise RuntimeError("RADIO_PAGE_CLOSED") from e
            pass


# --- Usa el que ya funcionaba en Teletrabajo ---
def _abrir_libro_legacy(sac):
    """Abre '* Ver Expediente como Libro' y devuelve la Page del libro (flujo viejo)."""
    import re

    try:
        sac.get_by_text(re.compile(r"que\s+puedo\s+hacer\??", re.I)).first.click()
    except Exception:
        pass
    sac.wait_for_timeout(200)

    link = sac.get_by_role("link", name=re.compile(r"Expediente\s+como\s+Libro", re.I)).first
    if link.count():
        try:
            with sac.context.expect_page() as pop:
                link.click()
            libro = pop.value
            libro.wait_for_load_state("domcontentloaded")
            try:
                libro.set_default_timeout(90_000)
                libro.set_default_navigation_timeout(90_000)
            except Exception:
                pass
            return libro
        except Exception:
            try:
                with sac.expect_navigation(timeout=4000):
                    link.click()
                return sac
            except Exception:
                pass

    try:
        with sac.context.expect_page() as pop:
            sac.evaluate("() => window.ExpedienteLibro && window.ExpedienteLibro()")
        libro = pop.value
        libro.wait_for_load_state("domcontentloaded")
        try:
            libro.set_default_timeout(90_000)
            libro.set_default_navigation_timeout(90_000)
        except Exception:
            pass
        return libro
    except Exception:
        pass

    try:
        libro = sac.wait_for_event("popup", timeout=5000)
        libro.wait_for_load_state("domcontentloaded")
        try:
            libro.set_default_timeout(90_000)
            libro.set_default_navigation_timeout(90_000)
        except Exception:
            pass
        return libro
    except Exception:
        pass

    raise RuntimeError("No pude abrir 'Ver Expediente como Libro'.")


def _abrir_libro_intranet(sac, intra_user, intra_pass, nro_exp):
    import re

    # a) si nos mandÃƒÂ³ al login, loguear y volver a RadiografÃƒÂ­a + re-buscar
    def _volver_a_radiografia_y_buscar():
        _ir_a_radiografia(sac)
        if nro_exp:  # <- re-busca el expediente
            _fill_radiografia_y_buscar(sac, nro_exp)

    # -- Gate de RadiografÃƒÂ­a: Ã‚Â¿hay operaciones y puedo ver su contenido? --
    STRICT = _env_true("STRICT_ONLY_VISIBLE_OPS", "0")
    CHECK_ALL = _env_true("STRICT_CHECK_ALL_OPS", "0")

    op_ids_rad = _listar_ops_ids_radiografia(sac)  # ? antes decÃƒÂ­a p_ids_rad

    # 1) Ã‚Â¿Se ve alguna operaciÃƒÂ³n por DOM?
    hay_ops = bool(op_ids_rad)

    # 2) Fallback robusto: Ã‚Â¿puedo abrir alguna operaciÃƒÂ³n y leer su contenido?
    if not hay_ops:
        hay_ops = _puedo_abrir_alguna_operacion(sac)

    if STRICT and not hay_ops:
        logging.info("[SEC] Radiografia: no pude detectar operaciones -> sin acceso. Abortando.")
        messagebox.showwarning("Sin acceso", "No tenes acceso a este expediente (no aparecen operaciones).")
        return

    # Si tengo ids, verifico UNA (o todas, segÃƒÂºn CHECK_ALL); si no, ya validÃƒÂ© con el fallback
    perm_ok = True
    if op_ids_rad:
        ids_a_probar = op_ids_rad if CHECK_ALL else op_ids_rad[:1]
        # 1) Si ALGUNA operaciÃƒÂ³n probada muestra el cartel ? abortamos TODO
        if any(_op_denegada_en_radiografia(sac, _id) for _id in ids_a_probar):
            logging.info("[SEC] RadiografÃƒÂ­a mostrÃƒÂ³ 'sin permisos' en al menos una operaciÃƒÂ³n. Abortando.")
            messagebox.showwarning(
                "Sin acceso",
                "No tenÃƒÂ©s permisos para visualizar el contenido de este expediente "
                "(al menos una operaciÃƒÂ³n estÃƒÂ¡ bloqueada). No se descargarÃƒÂ¡ nada.",
            )
            return

        # 2) Si ninguna estÃƒÂ¡ denegada explÃƒÂ­citamente, exigimos que al menos una tenga contenido visible
        perm_ok = any(_op_visible_con_contenido_en_radiografia(sac, _id) for _id in ids_a_probar)
    elif not _puedo_abrir_alguna_operacion(sac):
        perm_ok = False

    if STRICT and not perm_ok:
        logging.info("[SEC] RadiografÃƒÂ­a: aparece grilla pero el contenido estÃƒÂ¡ bloqueado.")
        messagebox.showwarning(
            "Sin acceso", "No tenÃƒÂ©s permisos para visualizar el contenido de las operaciones. No se descargÃƒÂ³ nada."
        )
        return

    if "PortalWeb/LogIn/Login.aspx" in (sac.url or "") or "SacInterior/Login.aspx" in (sac.url or ""):
        _login_intranet(sac, intra_user, intra_pass)
        _volver_a_radiografia_y_buscar()

    # 0) por si el botÃƒÂ³n vive en "Ã‚Â¿QuÃƒÂ© puedo hacer?"
    try:
        sac.get_by_text(re.compile(r"que\s+puedo\s+hacer\??", re.I)).first.click()
        sac.wait_for_timeout(200)
    except Exception:
        pass

    # 1) Intento: click al link
    try:
        a = sac.locator("a[href*='ExpedienteLibro'], a[onclick*='ExpedienteLibro']").first
        if a.count():
            try:
                with sac.expect_navigation(timeout=5000):
                    a.click()
                if "PortalWeb/LogIn/Login.aspx" not in (sac.url or ""):
                    return sac

                # si cayÃƒÂ³ al login ? volver a RadiografÃƒÂ­a y reintentar una vez
                _login_intranet(sac, intra_user, intra_pass)
                _volver_a_radiografia_y_buscar()

                with sac.expect_navigation(timeout=5000):
                    sac.locator("a[href*='ExpedienteLibro'], a[onclick*='ExpedienteLibro']").first.click()
                if "PortalWeb/LogIn/Login.aspx" not in (sac.url or ""):
                    return sac
            except Exception:
                pass
    except Exception:
        pass

    # 2) Intento: ejecutar la funciÃƒÂ³n en page/frames (inline)
    for fr in [sac] + list(sac.frames):
        try:
            has_fn = fr.evaluate("() => typeof window.ExpedienteLibro === 'function'")
        except Exception:
            has_fn = False

        if has_fn:
            try:
                with fr.expect_navigation(timeout=5000):
                    fr.evaluate("() => window.ExpedienteLibro()")
                if "PortalWeb/LogIn/Login.aspx" not in (sac.url or ""):
                    return sac

                _login_intranet(sac, intra_user, intra_pass)
                _volver_a_radiografia_y_buscar()

                with fr.expect_navigation(timeout=5000):
                    fr.evaluate("() => window.ExpedienteLibro()")
                if "PortalWeb/LogIn/Login.aspx" not in (sac.url or ""):
                    return sac
            except Exception:
                pass

    # 3) Fallback: construir URL directa (AHORA sÃƒÂ­, estando en RadiografÃƒÂ­a)
    # si por algÃƒÂºn motivo volvimos a login, resolvelo primero
    if "PortalWeb/LogIn/Login.aspx" in (sac.url or "") or "SacInterior/Login.aspx" in (sac.url or ""):
        _login_intranet(sac, intra_user, intra_pass)
        _volver_a_radiografia_y_buscar()

    # lee los hidden en la pÃƒÂ¡gina correcta
    def _read_hidden_generic(page, key_patterns):
        sels = []
        for k in key_patterns:
            sels += [f"input[id*='{k}']", f"input[name*='{k}']"]

        for where in [page] + list(page.frames):
            for s in sels:
                try:
                    loc = where.locator(s).first
                    if loc.count():
                        v = loc.input_value() or where.eval_on_selector(s, "el=>el.value")
                        if v:
                            return (v or "").strip()
                except Exception:
                    pass
        return None

    exp_id = _read_hidden_generic(sac, ["hdIdExpediente", "hdExpedienteId"])
    if not exp_id:
        _debug_dump(sac, "no_hdIdExpediente")
        raise RuntimeError("No encontrÃƒÂ© el id del expediente (hdIdExpediente/hdExpedienteId).")

    key = _read_hidden_generic(sac, ["hdIdExpedienteKey"]) or ""
    lvl = _read_hidden_generic(sac, ["hdNivelAcceso"]) or ""

    proxy_prefix = _get_proxy_prefix(sac)
    base_host = _sac_host_base(sac)  # ? usa mismo host (aplicaciones...) si no hay proxy
    base = f"{base_host}/SacInterior/_Expedientes/ExpedienteLibro.aspx"
    qs = f"idExpediente={exp_id}" + (f"&key={key}" if key else "") + (f"&nivelAcceso={lvl}" if lvl else "")
    url = (proxy_prefix + base) if proxy_prefix else base
    url = url + "?" + qs

    try:
        # Abrir el Libro en una nueva pestaÃƒÂ±a para no perder la RadiografÃƒÂ­a
        with sac.context.expect_page() as pop:
            sac.evaluate("url => window.open(url, '_blank')", url)
        libro = pop.value
        libro.wait_for_load_state("domcontentloaded")
        try:
            libro.set_default_timeout(90_000)
            libro.set_default_navigation_timeout(90_000)
        except Exception:
            pass
        return libro
    except Exception:
        # Fallback: navegar en la pestaÃƒÂ±a actual (menos robusto)
        sac.goto(url, wait_until="domcontentloaded")
        try:
            libro = sac.wait_for_event("popup", timeout=1500)
            libro.wait_for_load_state("domcontentloaded")
            return libro
        except Exception:
            return sac


def _abrir_libro(sac, intra_user=None, intra_pass=None, nro_exp=None):
    u = (sac.url or "")
    if "teletrabajo.justiciacordoba.gob.ar" in u or "/proxy/" in u:
        return _abrir_libro_legacy(sac)  # Teletrabajo intacto
    return _abrir_libro_intranet(sac, intra_user, intra_pass, nro_exp)




def _descargar_adjuntos_de_operacion(libro, op_id: str, carpeta: Path) -> list[Path]:
    """
    Encuentra y descarga los adjuntos que cuelgan de UNA operaciÃƒÂ³n dentro del Libro.
    - Descarga por la UI (Playwright).
    - Convierte a PDF si hace falta.
    - Descarta respuestas sin permiso.
    - Evita duplicados exactos (nombre+tamaÃƒÂ±o).
    """
    pdfs: list[Path] = []
    vistos: set[tuple[str, int]] = set()

    scope = libro.locator(f"[id='{op_id}'], [data-codigo='{op_id}']")
    if not scope.count():
        return pdfs

    triggers = scope.locator(
        "[onclick*='VerAdjuntoFichero'], a[href*='Fichero.aspx'], a:has-text('Adjunto'), a[href*='VerAdjunto']"
    )
    try:
        n = triggers.count()
    except Exception:
        n = 0

    for i in range(n):
        link = triggers.nth(i)
        try:
            with libro.expect_download() as dl:
                try:
                    link.click()
                except Exception:
                    try:
                        link.evaluate("el => el.click()")
                    except Exception:
                        continue
            d = dl.value
            destino = carpeta / d.suggested_filename
            d.save_as(destino)

            # NormalizaciÃƒÂ³n a PDF
            if not _is_real_pdf(destino):
                pdf = _ensure_pdf_fast(destino) if '_ensure_pdf_fast' in globals() else _ensure_pdf(destino)
            else:
                pdf = destino

            # Validaciones
            if pdf.suffix.lower() != ".pdf" or not pdf.exists():
                continue
            if _pdf_contiene_mensaje_permiso(pdf):
                try:
                    pdf.unlink()
                except Exception:
                    pass
                continue

            # Deduplicar por (nombre, tamaÃƒÂ±o)
            try:
                key = (pdf.name, pdf.stat().st_size)
            except Exception:
                key = (pdf.name, 0)
            if key in vistos:
                continue
            vistos.add(key)
            pdfs.append(pdf)
        except Exception:
            # Si algo abre otra pestaÃƒÂ±a y falla, seguimos con el resto
            continue

    return pdfs


def _texto_celdas_fila(fila) -> list[str]:
    textos: list[str] = []
    try:
        celdas = fila.locator("td")
        total = celdas.count()
    except Exception:
        return textos

    for j in range(total):
        try:
            txt = _norm_ws(celdas.nth(j).inner_text() or "")
        except Exception:
            txt = ""
        if txt:
            textos.append(txt)
    return textos


def _titulo_item_radiografia(*parts, fallback: str = "") -> str:
    out: list[str] = []
    vistos: set[str] = set()
    for part in parts:
        txt = _norm_ws(part or "")
        if not txt or txt == "-" or txt in vistos:
            continue
        vistos.add(txt)
        out.append(txt)
    return " · ".join(out) or fallback


def _asegurar_seccion_adjuntos_visible(sac):
    try:
        cont = sac.locator("#divAdjuntos").first
        visible = False
        if cont.count():
            try:
                visible = cont.evaluate("el => getComputedStyle(el).display !== 'none'")
            except Exception:
                visible = False
        if visible:
            return
    except Exception:
        pass

    try:
        sac.evaluate("() => { try { Seccion && Seccion('Adjuntos'); } catch(e){} }")
    except Exception:
        pass

    try:
        toggle = sac.locator("a[href*=\"Seccion('Adjuntos')\"], a[onclick*=\"Seccion('Adjuntos')\"]").first
        if toggle.count():
            try:
                toggle.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                toggle.click()
            except Exception:
                try:
                    toggle.evaluate("el => el.click()")
                except Exception:
                    pass
    except Exception:
        pass

    try:
        sac.wait_for_timeout(250)
    except Exception:
        pass


def _adjuntos_rows_locator(sac):
    return sac.locator(
        "#cphDetalle_gvAdjuntos tr, "
        "table[id*='gvAdjuntos'] tr, "
        "#divAdjuntos table tr"
    )


def _adjunto_file_link_locator(fila):
    return fila.locator(
        "*[onclick*='VerAdjuntoFichero'], "
        "*[href*='VerAdjuntoFichero'], "
        "a[href*='Fichero.aspx'], "
        "a[href*='idFichero='], "
        "a:has(img[src*='pdf']), "
        "a:has(img[src*='adobe']), "
        "a:has(img[src*='Adobe'])"
    ).first


def _listar_adjuntos_grid_para_radiografia(
    sac,
    op_fecha_map: dict[str, str] | None = None,
    op_title_map: dict[str, str] | None = None,
) -> list[dict]:
    import re

    _asegurar_seccion_adjuntos_visible(sac)
    filas = None
    for _ in range(40):
        filas = _adjuntos_rows_locator(sac)
        try:
            total_tmp = filas.count() if filas else 0
        except Exception:
            total_tmp = 0
        try:
            cand_n = sac.locator(
                "#divAdjuntos *[onclick*='VerAdjuntoFichero'], "
                "#divAdjuntos *[href*='VerAdjuntoFichero'], "
                "#divAdjuntos a[href*='Fichero.aspx'], "
                "#divAdjuntos a[href*='idFichero=']"
            ).count()
        except Exception:
            cand_n = 0
        if total_tmp > 1 and cand_n > 0:
            break
        try:
            sac.wait_for_timeout(150)
        except Exception:
            pass

    total = filas.count() if filas else 0
    try:
        logging.info(f"[ADJ] Filas Adjuntos: {total}")
    except Exception:
        pass
    items: list[dict] = []

    for i in range(1, total):
        fila = filas.nth(i)
        file_link = _adjunto_file_link_locator(fila)
        if not file_link.count():
            continue

        op_id = None
        op_link = fila.locator("a[href*='VerDecretoHtml'], a[onclick*='VerDecretoHtml']").first
        if op_link.count():
            try:
                raw = f"{op_link.get_attribute('href') or ''} {op_link.get_attribute('onclick') or ''}"
                m = re.search(r"VerDecretoHtml\('([^']+)'\)", raw)
                if m:
                    op_id = m.group(1)
            except Exception:
                pass

        file_key = None
        try:
            raw = f"{file_link.get_attribute('href') or ''} {file_link.get_attribute('onclick') or ''}"
            m = re.search(r"VerAdjuntoFichero\(\s*['\"]([^'\"]+)['\"]\s*\)", raw, re.I)
            if not m:
                m = re.search(r"[?&](?:id|Id|file|archivo)=([^&'\" )]+)", raw)
            if m:
                file_key = m.group(1)
        except Exception:
            pass

        textos = _texto_celdas_fila(fila)
        fecha = (op_fecha_map or {}).get(op_id or "", "")
        titulo_op = (op_title_map or {}).get(op_id or "", "")
        link_txt = ""
        try:
            link_txt = _norm_ws(file_link.inner_text() or "")
        except Exception:
            link_txt = ""
        if not link_txt:
            try:
                link_txt = _norm_ws(file_link.get_attribute("title") or "")
            except Exception:
                link_txt = ""

        resto = [t for t in textos if t and t not in {fecha, titulo_op}]
        titulo = _titulo_item_radiografia(link_txt, *(resto[:2]), fallback="Adjunto")
        detalle = _titulo_item_radiografia(titulo_op, fallback=(op_id or "Sin operacion"))
        uid = f"adj:{file_key or (op_id or 'sin-op')}:{i}"
        items.append(
            {
                "uid": uid,
                "kind": "adjunto",
                "kind_label": "Adjunto",
                "fecha": fecha,
                "titulo": titulo,
                "detalle": detalle,
                "op_id": op_id,
                "_file_key": file_key or "",
                "_file_name_hint": link_txt or "",
                "_row": i,
            }
        )

    return items


def _descargar_adjuntos_grid_mapeado(
    sac,
    carpeta: Path,
    selected_uids: set[str] | None = None,
    return_items: bool = False,
    op_fecha_map: dict[str, str] | None = None,
    op_title_map: dict[str, str] | None = None,
):
    """
    Devuelve por defecto {op_id: [PDFs...]} leyendo la grilla de Adjuntos de Radiografia.
    Si return_items=True, devuelve {uid: {"path", "fecha", "titulo", "detalle", "op_id"}}.
    """
    def _filename_hint_for_item(item: dict[str, object]) -> str:
        raw = _norm_ws(
            item.get("_file_name_hint")
            or item.get("titulo")
            or ""
        )
        first = raw.split(" · ", 1)[0].strip()
        name = re.sub(r'[\\/:*?"<>|]+', "_", first or "")
        name = name.strip(" .")
        if "." not in Path(name or "").name:
            file_key = _norm_ws(item.get("_file_key") or "")
            fallback = f"adjunto_{file_key}" if file_key else "adjunto"
            name = f"{name or fallback}.pdf"
        return name or "adjunto.pdf"

    def _unique_path(base_dir: Path, filename: str) -> Path:
        dst = base_dir / filename
        stem = dst.stem or "adjunto"
        suffix = dst.suffix or ".pdf"
        i = 2
        while dst.exists():
            dst = base_dir / f"{stem} ({i}){suffix}"
            i += 1
        return dst

    def _filename_key(name: str) -> str:
        s = _repair_mojibake_text(_norm_ws(Path(name or "").name)).lower()
        stem = Path(s).stem
        return re.sub(r"\s+", "", stem)

    def _filename_key_sin_copia_browser(name: str) -> str:
        s = _repair_mojibake_text(_norm_ws(Path(name or "").name)).lower()
        stem = Path(s).stem
        stem = re.sub(r"\s+\(\d+\)$", "", stem)
        return re.sub(r"\s+", "", stem)

    def _download_name_matches_expected(expected: str, actual: str) -> bool:
        expected = Path(expected or "").name
        actual = Path(actual or "").name
        if not expected or not actual:
            return True
        if expected.lower().startswith("adjunto_"):
            return True
        e_ext = Path(expected).suffix.lower()
        a_ext = Path(actual).suffix.lower()
        if e_ext and a_ext and e_ext != a_ext:
            return False
        e_key = _filename_key(expected)
        a_key = _filename_key(actual)
        if not e_key or not a_key:
            return True
        if e_key == a_key:
            return True
        if a_ext in {".doc", ".docx"} and not re.search(r"\(\d+\)\s*$", Path(expected).stem):
            return e_key == _filename_key_sin_copia_browser(actual)
        return False

    def _requests_session_from_page(page):
        sess = requests.Session()
        try:
            ua = page.evaluate("() => navigator.userAgent") or ""
        except Exception:
            ua = ""
        headers = {"Accept": "*/*"}
        if ua:
            headers["User-Agent"] = ua
        try:
            referer = page.url or ""
        except Exception:
            referer = ""
        if referer:
            headers["Referer"] = referer
        sess.headers.update(headers)
        try:
            for ck in page.context.cookies():
                kw = {
                    "name": ck.get("name") or "",
                    "value": ck.get("value") or "",
                    "domain": ck.get("domain") or "",
                    "path": ck.get("path") or "/",
                    "secure": bool(ck.get("secure", False)),
                    "rest": {"HttpOnly": bool(ck.get("httpOnly", False))},
                }
                exp = ck.get("expires", None)
                if isinstance(exp, (int, float)) and exp > 0:
                    kw["expires"] = int(exp)
                try:
                    sess.cookies.set_cookie(requests.cookies.create_cookie(**kw))
                except Exception:
                    continue
        except Exception as e:
            try:
                logging.info(f"[ADJ] No pude copiar cookies al downloader directo: {e}")
            except Exception:
                pass
        return sess

    mapeo: dict[str, list[Path]] = {}
    out_items: dict[str, dict[str, object]] = {}
    vistos: set[tuple[str, int]] = set()
    selected_uids = set(selected_uids or [])

    items = _listar_adjuntos_grid_para_radiografia(
        sac,
        op_fecha_map=op_fecha_map,
        op_title_map=op_title_map,
    )
    filas = _adjuntos_rows_locator(sac)
    proxy_prefix = _get_proxy_prefix(sac)
    dl_session = _requests_session_from_page(sac)

    for item in items:
        uid = item["uid"]
        if selected_uids and uid not in selected_uids:
            continue

        try:
            fila = filas.nth(int(item["_row"]))
        except Exception:
            continue

        file_link = _adjunto_file_link_locator(fila)
        if not file_link.count():
            continue

        pdf = None
        try:
            direct_url = _extraer_url_de_link(file_link, proxy_prefix)
        except Exception:
            direct_url = None

        if direct_url:
            try:
                logging.info(f"[ADJ] {uid}: intento directo -> {direct_url}")
            except Exception:
                pass
            try:
                destino_directo = _unique_path(carpeta, _filename_hint_for_item(item))
                p = _descargar_archivo(dl_session, direct_url, destino_directo)
                if p and p.exists():
                    pdf = p
                    try:
                        logging.info(f"[ADJ] {uid}: descarga directa OK -> {pdf.name}")
                    except Exception:
                        pass
            except Exception as e:
                try:
                    logging.info(f"[ADJ] {uid}: descarga directa fallo -> {e}")
                except Exception:
                    pass

        if pdf is None:
            expected_click_name = _filename_hint_for_item(item)
            timeout_ms = int(os.getenv("ADJ_DOWNLOAD_TIMEOUT_MS", "45000") or "45000")
            for intento_click in range(1, 3):
                try:
                    with sac.expect_download(timeout=timeout_ms) as dl:
                        try:
                            file_link.evaluate("el => el.click()")
                        except Exception:
                            file_link.click(force=True, no_wait_after=True, timeout=3000)
                    d = dl.value
                    suggested = d.suggested_filename or ""
                    if not _download_name_matches_expected(expected_click_name, suggested):
                        try:
                            logging.info(
                                f"[ADJ] {uid}: descarga descartada por nombre; "
                                f"esperado={expected_click_name} recibido={suggested}"
                            )
                        except Exception:
                            pass
                        try:
                            d.delete()
                        except Exception:
                            pass
                        continue
                    destino_click = _unique_path(carpeta, suggested or expected_click_name)
                    d.save_as(destino_click)
                    pdf = destino_click
                    try:
                        logging.info(f"[ADJ] {uid}: descarga por click OK -> {pdf.name}")
                    except Exception:
                        pass
                    break
                except Exception as e:
                    try:
                        logging.info(f"[ADJ] {uid}: click sin download capturable -> {e}")
                    except Exception:
                        pass
                    break

        try:
            if not pdf or not pdf.exists():
                continue

            if not _is_real_pdf(pdf):
                pdf = _ensure_pdf_fast(pdf) if '_ensure_pdf_fast' in globals() else _ensure_pdf(pdf)

            if not pdf.exists() or not _is_real_pdf(pdf):
                try:
                    logging.info(f"[ADJ] {uid}: archivo descartado; no es PDF valido tras conversion")
                except Exception:
                    pass
                continue

            if _pdf_contiene_mensaje_permiso(pdf):
                try:
                    pdf.unlink()
                except Exception:
                    pass
                continue

            try:
                key = (pdf.name, pdf.stat().st_size)
            except Exception:
                key = (pdf.name, 0)
            if key in vistos:
                continue
            vistos.add(key)

            op_id = item.get("op_id") or "__SIN_OP__"
            mapeo.setdefault(op_id, []).append(pdf)
            out_items[uid] = {
                "path": pdf,
                "fecha": item.get("fecha") or "",
                "titulo": item.get("titulo") or pdf.name,
                "detalle": item.get("detalle") or "",
                "op_id": item.get("op_id"),
            }
        except Exception as e:
            try:
                logging.info(f"[ADJ] {uid}: error finalizando adjunto -> {e}")
            except Exception:
                pass
            continue

    return out_items if return_items else mapeo

def _mapear_fechas_operaciones_radiografia(sac) -> tuple[dict[str, str], list[str]]:
    """
    Lee la grilla de Operaciones en RadiografÃƒÂ­a y devuelve:
      - dict { op_id -> 'dd/mm/aaaa' }
      - lista de fechas ÃƒÂºnicas en el ORDEN en que aparecen en la grilla (para intercalar cronolÃƒÂ³gicamente)
    """
    import re
    fechas_por_op: dict[str, str] = {}
    orden_fechas: list[str] = []

    filas = sac.locator("#cphDetalle_gvOperaciones tr, table[id*='gvOperaciones'] tr")
    total = filas.count() if filas else 0
    if total <= 1:
        return fechas_por_op, orden_fechas

    for i in range(1, total):
        fila = filas.nth(i)
        # op_id
        link = fila.locator("a[href*='VerDecretoHtml'], a[onclick*='VerDecretoHtml']").first
        if not link.count():
            continue
        try:
            href = link.get_attribute("href") or ""
            oc = link.get_attribute("onclick") or ""
            m = re.search(r"VerDecretoHtml\('([^']+)'\)", href + " " + oc)
            if not m:
                continue
            op_id = m.group(1)
        except Exception:
            continue

        # fecha (buscar en celdas con patrÃƒÂ³n dd/mm/aaaa)
        fecha = ""
        try:
            celdas = fila.locator("td")
            ntd = celdas.count()
            for j in range(ntd):
                try:
                    txt = _norm_ws(celdas.nth(j).inner_text() or "")
                except Exception:
                    continue
                m2 = re.search(r"\b\d{2}/\d{2}/\d{4}\b", txt)
                if m2:
                    fecha = m2.group(0)
                    break
        except Exception:
            fecha = ""

        if fecha:
            fechas_por_op[op_id] = fecha
            if not orden_fechas or orden_fechas[-1] != fecha:
                orden_fechas.append(fecha)

    return fechas_por_op, orden_fechas

def _asegurar_seccion_informes_tecnicos_visible(sac):
    try:
        cont = sac.locator("#divInformesTecnicosMPF").first
        visible = False
        if cont.count():
            try:
                visible = cont.evaluate("el => getComputedStyle(el).display !== 'none'")
            except Exception:
                visible = False
        if visible:
            return
    except Exception:
        pass

    try:
        sac.evaluate("() => { try { Seccion && Seccion('InformesTecnicosMPF'); } catch(e){} }")
    except Exception:
        pass

    for sel in (
        "a[href*=\"Seccion('InformesTecnicosMPF')\"]",
        "a[onclick*=\"Seccion('InformesTecnicosMPF')\"]",
        "#imgInformesTecnicosMPF",
    ):
        try:
            loc = sac.locator(sel).first
            if not loc.count():
                continue
            try:
                loc.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                loc.click()
            except Exception:
                try:
                    loc.evaluate("el=>el.click()")
                except Exception:
                    pass
        except Exception:
            pass

    try:
        sac.wait_for_timeout(250)
    except Exception:
        pass


def _titulo_informe_tecnico_desde_fila(fila, fecha: str, fallback: str) -> str:
    textos = [t for t in _texto_celdas_fila(fila) if t and t != fecha]
    return _titulo_item_radiografia(*(textos[:3]), fallback=fallback)


def _listar_informes_tecnicos_para_radiografia(sac) -> list[dict]:
    import re

    _asegurar_seccion_informes_tecnicos_visible(sac)

    filas = None
    for _ in range(40):
        filas = sac.locator("table[id*='gvInformesTecnicos'] tr, table[id*='gvInformesTecnicosMPF'] tr")
        try:
            total_tmp = filas.count() if filas else 0
        except Exception:
            total_tmp = 0
        try:
            cand = sac.locator(
                "#divInformesTecnicosMPF *[onclick*='VerInforme'], "
                "#divInformesTecnicosMPF *[href*='VerInforme'], "
                "#divInformesTecnicosMPF a:has(img[src*='pdf']), "
                "#divInformesTecnicosMPF a:has(img[src*='adobe']), "
                "#divInformesTecnicosMPF a:has(img[src*='Adobe'])"
            )
            cand_n = cand.count()
        except Exception:
            cand_n = 0
        if (total_tmp > 1) and (cand_n > 0):
            break
        try:
            sac.wait_for_timeout(150)
        except Exception:
            pass

    total = filas.count() if filas else 0
    items: list[dict] = []
    for i in range(1, total):
        fila = filas.nth(i)
        link = fila.locator(
            "*[onclick*='VerInformeMPF'], *[href*='VerInformeMPF'], "
            "a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='pdf'])"
        ).first
        if not link.count():
            continue

        guid = None
        try:
            href = link.get_attribute("href") or ""
            oc = link.get_attribute("onclick") or ""
            m = re.search(r"VerInformeMPF\s*\(([^)]*)\)", f"{href} {oc}")
            if m:
                arg0 = m.group(1).split(",")[0].strip()
                if (len(arg0) >= 2) and arg0[0] == arg0[-1] and arg0[0] in "'\"":
                    arg0 = arg0[1:-1]
                guid = arg0 or None
        except Exception:
            pass

        fecha = ""
        try:
            celdas = fila.locator("td")
            ntd = celdas.count()
            for j in range(ntd):
                try:
                    txt = _norm_ws(celdas.nth(j).inner_text() or "")
                except Exception:
                    continue
                m = re.search(r"\b\d{2}/\d{2}/\d{4}\b", txt)
                if m:
                    fecha = m.group(0)
                    break
        except Exception:
            fecha = ""

        titulo = _titulo_informe_tecnico_desde_fila(fila, fecha, fallback=f"Informe tecnico MPF {i}")
        items.append(
            {
                "uid": f"infmpf:{guid or i}",
                "kind": "informe_mpf",
                "kind_label": "Informe MPF",
                "fecha": fecha,
                "titulo": titulo,
                "detalle": guid or "",
                "guid": guid,
                "_row": i,
            }
        )

    return items


def _descargar_informes_tecnicos(
    sac,
    carpeta: Path,
    selected_uids: set[str] | None = None,
    return_items: bool = False,
) -> list[tuple[Path, str]] | dict[str, dict[str, object]]:
    """
    Descarga informes tecnicos desde la seccion 'INFORMES TECNICOS MPF' y devuelve [(PDF, fecha_mov)].
    Con return_items=True devuelve {uid: {"path", "fecha", "titulo", "detalle"}}.
    """
    import re

    informes: list[tuple[Path, str]] = []
    out_items: dict[str, dict[str, object]] = {}
    vistos: set[tuple[str, int]] = set()
    selected_uids = set(selected_uids or [])

    _asegurar_seccion_informes_tecnicos_visible(sac)

    filas = None
    for _ in range(40):
        filas = sac.locator("table[id*='gvInformesTecnicos'] tr, table[id*='gvInformesTecnicosMPF'] tr")
        try:
            total_tmp = filas.count() if filas else 0
        except Exception:
            total_tmp = 0
        try:
            cand = sac.locator(
                "#divInformesTecnicosMPF *[onclick*='VerInforme'], "
                "#divInformesTecnicosMPF *[href*='VerInforme'], "
                "#divInformesTecnicosMPF a:has(img[src*='pdf']), "
                "#divInformesTecnicosMPF a:has(img[src*='adobe']), "
                "#divInformesTecnicosMPF a:has(img[src*='Adobe'])"
            )
            cand_n = cand.count()
        except Exception:
            cand_n = 0
        if (total_tmp > 1) and (cand_n > 0):
            break
        try:
            sac.wait_for_timeout(150)
        except Exception:
            pass

    total = filas.count() if filas else 0
    try:
        cand = sac.locator(
            "#divInformesTecnicosMPF *[onclick*='VerInforme'], "
            "#divInformesTecnicosMPF *[href*='VerInforme'], "
            "#divInformesTecnicosMPF a:has(img[src*='pdf']), "
            "#divInformesTecnicosMPF a:has(img[src*='adobe']), "
            "#divInformesTecnicosMPF a:has(img[src*='Adobe'])"
        )
        cand_n = cand.count()
        logging.info(f"[INF] Filas InformesTecnicosMPF: {total} | candidatos: {cand_n}")
    except Exception:
        pass
    if total <= 1:
        try:
            cont = sac.locator("#divInformesTecnicosMPF").first
            html = cont.inner_html() if cont and cont.count() else ""
            logging.info(f"[INF] Contenedor InformesTecnicosMPF HTML (recorte): {(html or '')[:2000]}")
        except Exception:
            pass
        return out_items if return_items else informes

    meta_by_row = {int(item["_row"]): item for item in _listar_informes_tecnicos_para_radiografia(sac)}

    for i in range(1, total):
        fila = filas.nth(i)
        meta = meta_by_row.get(i)
        if not meta:
            continue
        if selected_uids and meta["uid"] not in selected_uids:
            continue

        link = fila.locator(
            "*[onclick*='VerInformeMPF'], *[href*='VerInformeMPF'], "
            "a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='pdf'])"
        ).first
        if not link.count():
            try:
                logging.info(f"[INF] Fila {i}: sin link/icono de informe tecnico")
            except Exception:
                pass
            continue

        guid = meta.get("guid")
        fecha = meta.get("fecha") or ""
        try:
            logging.info(f"[INF] Fila {i}: GUID: {'si' if guid else 'no'}")
            logging.info(f"[INF] Fila {i}: fecha: {fecha or 'no detectada'}")
        except Exception:
            pass

        from playwright.sync_api import TimeoutError as PWTimeoutError
        ctx = sac.context

        def _filename_from_cd(cd: str | None) -> str | None:
            import re as _re, urllib.parse

            if not cd:
                return None
            m = _re.search(r'filename\*?=([^;]+)', cd)
            if not m:
                return None
            val = m.group(1).strip().strip('"')
            if val.lower().startswith("utf-8''"):
                val = urllib.parse.unquote(val[7:])
            return Path(val).name

        def _unique_path(base_dir: Path, filename: str | None) -> Path:
            raw = Path(filename or f"InformeTecnico_{i:03d}.pdf").name
            raw = re.sub(r'[\\/:*?"<>|]+', "_", raw).strip(" .") or f"InformeTecnico_{i:03d}.pdf"
            dst = base_dir / raw
            stem = dst.stem or "InformeTecnico"
            suffix = dst.suffix or ".pdf"
            n = 2
            while dst.exists():
                dst = base_dir / f"{stem} ({n}){suffix}"
                n += 1
            return dst

        def _guardar(bytes_, nombre_sugerido: str | None) -> Path | None:
            try:
                dst = _unique_path(carpeta, nombre_sugerido or f"InformeTecnico_{i:03d}.pdf")
                with open(dst, "wb") as f:
                    f.write(bytes_)
                return dst
            except Exception:
                return None

        def _capturar_desde_pagina(p) -> Path | None:
            try:
                d = p.wait_for_event("download", timeout=25000)
                dst = _unique_path(carpeta, d.suggested_filename)
                d.save_as(dst)
                return dst
            except PWTimeoutError:
                pass
            except Exception as e:
                try:
                    logging.info(f"[INF] popup/descarga cerrada o inválida: {e}")
                except Exception:
                    pass
                return None
            try:
                predicate = lambda r: any(
                    x in (r.headers.get("content-type", "").lower())
                    for x in ("application/pdf", "application/octet-stream")
                )
                if hasattr(p, "wait_for_response"):
                    resp = p.wait_for_response(predicate, timeout=25000)
                else:
                    with p.expect_response(predicate, timeout=25000) as resp_info:
                        pass
                    resp = resp_info.value
                nombre = _filename_from_cd((resp.headers or {}).get("content-disposition"))
                cuerpo = resp.body()
                return _guardar(cuerpo, nombre)
            except PWTimeoutError:
                return None
            except Exception as e:
                try:
                    logging.info(f"[INF] respuesta inline/popup inválida: {e}")
                except Exception:
                    pass
                return None

        destino = None

        try:
            try:
                logging.info(f"[INF] Fila {i}: click + expect_download")
            except Exception:
                pass
            try:
                _kill_overlays(sac)
            except Exception:
                pass
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            with sac.expect_download(timeout=12000) as dl:
                link.click(force=True)
            d = dl.value
            destino = _unique_path(carpeta, d.suggested_filename)
            d.save_as(destino)
            try:
                logging.info(f"[INF] Fila {i}: descargado -> {destino.name}")
            except Exception:
                pass
        except PWTimeoutError:
            try:
                logging.info(f"[INF] Fila {i}: expect_download timeout; probando popup/inline")
            except Exception:
                pass
            try:
                link.click(force=True)
            except Exception:
                pass

            try:
                pop = ctx.wait_for_event("page", timeout=6000)
                try:
                    destino = _capturar_desde_pagina(pop)
                finally:
                    try:
                        pop.close()
                    except Exception:
                        pass
                try:
                    logging.info(f"[INF] Fila {i}: popup -> {'OK ' + destino.name if destino else 'sin descarga'}")
                except Exception:
                    pass
            except PWTimeoutError:
                pass
            except Exception as e:
                try:
                    logging.info(f"[INF] Fila {i}: popup error: {e}")
                except Exception:
                    pass

            if not destino:
                try:
                    try:
                        logging.info(f"[INF] Fila {i}: esperando respuesta inline en misma pagina")
                    except Exception:
                        pass

                    def _is_pdf_resp_inline(r):
                        try:
                            ct = (r.headers or {}).get("content-type", "")
                            return ("application/pdf" in (ct or "").lower()) or (
                                "application/octet-stream" in (ct or "").lower()
                            )
                        except Exception:
                            return False

                    with sac.expect_response(_is_pdf_resp_inline, timeout=15000) as resp_info:
                        try:
                            link.click()
                        except Exception:
                            pass
                    resp = resp_info.value
                    try:
                        nombre = _filename_from_cd((resp.headers or {}).get("content-disposition"))
                    except Exception:
                        nombre = None
                    try:
                        cuerpo = resp.body()
                    except Exception:
                        cuerpo = b""
                    destino = _guardar(cuerpo, nombre)
                except PWTimeoutError:
                    pass
                except Exception as e:
                    try:
                        logging.info(f"[INF] Fila {i}: inline response error: {e}")
                    except Exception:
                        pass

            if not destino:
                if guid:
                    try:
                        def _is_pdf_resp_eval(r):
                            try:
                                ct = (r.headers or {}).get("content-type", "")
                                return ("application/pdf" in (ct or "").lower()) or (
                                    "application/octet-stream" in (ct or "").lower()
                                )
                            except Exception:
                                return False

                        with sac.expect_response(_is_pdf_resp_eval, timeout=15000) as resp_info:
                            sac.evaluate("g => { try { window.VerInformeMPF && window.VerInformeMPF(g); } catch(e){} }", guid)
                        resp = resp_info.value
                        nombre = _filename_from_cd((resp.headers or {}).get("content-disposition"))
                        cuerpo = resp.body()
                        destino = _guardar(cuerpo, nombre)
                    except PWTimeoutError:
                        try:
                            sac.evaluate("g => { try { window.VerInformeMPF && window.VerInformeMPF(g); } catch(e){} }", guid)
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            logging.info(f"[INF] Fila {i}: eval/response error: {e}")
                        except Exception:
                            pass

                for p in reversed(ctx.pages):
                    destino = _capturar_desde_pagina(p)
                    if destino:
                        break
                try:
                    logging.info(f"[INF] Fila {i}: inline/existing -> {'OK ' + destino.name if destino else 'no encontrado'}")
                except Exception:
                    pass

        if not destino or not destino.exists():
            try:
                logging.info(f"[INF] Fila {i}: sin archivo destino")
            except Exception:
                pass
            continue
        if destino.suffix.lower() != ".pdf":
            destino = _ensure_pdf_fast(destino) if '_ensure_pdf_fast' in globals() else _ensure_pdf(destino)
        if not destino or not destino.exists() or destino.suffix.lower() != ".pdf" or not _is_real_pdf(destino):
            try:
                logging.info(f"[INF] Fila {i}: conversion a PDF fallida")
            except Exception:
                pass
            continue
        if _pdf_contiene_mensaje_permiso(destino):
            try:
                destino.unlink()
            except Exception:
                pass
            try:
                logging.info(f"[INF] Fila {i}: PDF con mensaje de permisos -> descartado")
            except Exception:
                pass
            continue

        key = (destino.name, destino.stat().st_size if destino.exists() else 0)
        if key in vistos:
            continue
        vistos.add(key)

        informes.append((destino, fecha))
        out_items[meta["uid"]] = {
            "path": destino,
            "fecha": fecha,
            "titulo": meta.get("titulo") or destino.name,
            "detalle": meta.get("detalle") or "",
            "guid": guid,
        }
        try:
            logging.info(f"[INF] Fila {i}: agregado {destino.name}")
        except Exception:
            pass

    return out_items if return_items else informes


def _fecha_rnr_desde_pdf(pdf_path: Path) -> str | None:
    """
    Intenta extraer la fecha del Informe RNR desde el PDF.
    - Primero busca dd/mm/aaaa literal.
    - Luego busca "20 de septiembre de 2023" (mes en espaÃƒÂ±ol), con o sin dÃƒÂ­a de semana/lugar.
    - Si no hay texto (escaneado), aplica OCR best-effort y reintenta.
    Devuelve "dd/mm/aaaa" o None.
    """
    import unicodedata, re

    def _extraer_txt(p: Path) -> str:
        txt = ""
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(str(p))
            for i in range(min(2, doc.page_count)):
                try:
                    txt += doc[i].get_text("text") or ""
                except Exception:
                    continue
            try:
                doc.close()
            except Exception:
                pass
        except Exception:
            try:
                for pg in PdfReader(str(p)).pages[:2]:
                    try:
                        txt += pg.extract_text() or ""
                    except Exception:
                        continue
            except Exception:
                pass
        return txt or ""

    def _parse(txt: str) -> str | None:
        t = _norm_ws(txt or "")
        if not t:
            return None
        # 1) dd/mm/aaaa
        m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", t)
        if m:
            d, m_, a = m.groups()
            try:
                return f"{int(d):02d}/{int(m_):02d}/{int(a):04d}"
            except Exception:
                return f"{d}/{m_}/{a}"

        # 2) "20 de septiembre de 2023" (case/acento-insensible)
        def _deacc(s: str) -> str:
            return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").lower()

        t2 = _deacc(t)
        meses = {
            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
            "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
            "noviembre": 11, "diciembre": 12,
        }
        m2 = re.search(r"\b(\d{1,2})\s+de\s+([a-z\u00f1]+)\s+de\s+(\d{4})\b", t2)
        if m2:
            d = int(m2.group(1))
            mes_name = m2.group(2)
            anio = int(m2.group(3))
            mm = meses.get(mes_name)
            if mm:
                return f"{d:02d}/{mm:02d}/{anio:04d}"
        return None

    # Primer intento sin OCR
    fecha = _parse(_extraer_txt(pdf_path))
    if fecha:
        return fecha

    # Best-effort: OCR si estÃƒÂ¡ habilitado/posible
    try:
        pdf_ocr = _maybe_ocr(pdf_path)
        if pdf_ocr and Path(pdf_ocr).exists():
            fecha = _parse(_extraer_txt(Path(pdf_ocr)))
            if fecha:
                return fecha
    except Exception:
        pass
    return None


def _abrir_seccion_rnr(sac):
    try:
        logging.info("[RNR] Intentando abrir sección Reincidencias")
    except Exception:
        pass

    try:
        sac.evaluate("() => { try { Seccion('Reincidencias'); } catch(e){} }")
    except Exception:
        pass

    for sel in (
        "a[href*=\"Seccion('Reincidencias')\"]",
        "a[onclick*=\"Seccion('Reincidencias')\"]",
    ):
        try:
            a = sac.locator(sel).first
            if not a.count():
                continue
            try:
                a.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                a.click(timeout=3000)
            except Exception:
                try:
                    a.evaluate("el => el.click()")
                except Exception:
                    pass
        except Exception:
            pass

    try:
        sac.wait_for_timeout(800)
    except Exception:
        pass


def _leer_ajax_reincidencias(sac) -> list[dict]:
    import json
    import html as html_lib
    import xml.etree.ElementTree as ET

    try:
        raw_text = sac.evaluate(
            """async () => {
                const r = await fetch("Radiografia.aspx/GetReincidencias", {
                    method: "POST",
                    headers: { "Content-Type": "application/json; charset=utf-8" },
                    body: JSON.stringify({ idExpediente: idExpedienteCliente + "" })
                });
                return await r.text();
            }"""
        )
    except Exception as e:
        try:
            logging.info(f"[RNR] Error llamando GetReincidencias: {e}")
        except Exception:
            pass
        return []

    if not raw_text:
        return []

    try:
        payload = json.loads(raw_text)
        xml_escaped = payload.get("d", "") or ""
    except Exception as e:
        try:
            logging.info(f"[RNR] No se pudo parsear JSON de GetReincidencias: {e}")
        except Exception:
            pass
        return []

    if not xml_escaped:
        return []

    try:
        xml_text = html_lib.unescape(xml_escaped)
        root = ET.fromstring(xml_text)
    except Exception as e:
        try:
            logging.info(f"[RNR] No se pudo parsear XML de GetReincidencias: {e}")
            logging.info(f"[RNR] XML recorte: {(xml_escaped or '')[:1000]}")
        except Exception:
            pass
        return []

    filas = []
    for table in root.findall(".//Table"):
        item = {}
        for child in list(table):
            item[child.tag] = (child.text or "").strip()
        if item.get("IdPedidoAPKey"):
            filas.append(item)

    try:
        logging.info(f"[RNR] AJAX GetReincidencias devolvió {len(filas)} registros")
    except Exception:
        pass

    return filas


def _listar_informes_reincidencia_para_radiografia(sac) -> list[dict]:
    _abrir_seccion_rnr(sac)
    items: list[dict] = []
    for i, item in enumerate(_leer_ajax_reincidencias(sac), start=1):
        id_apkey = (item.get("IdPedidoAPKey") or "").strip()
        nombre = _norm_ws(item.get("Nombre") or "")
        ndoc = _norm_ws(item.get("NumeroDocumento") or "")
        if not id_apkey:
            continue
        titulo = _titulo_item_radiografia(nombre, f"DNI {ndoc}" if ndoc else "", fallback=f"Informe RNR {i}")
        detalle = _titulo_item_radiografia(
            _norm_ws(item.get("FechaNacimiento") or ""),
            id_apkey,
            fallback=id_apkey,
        )
        items.append(
            {
                "uid": f"rnr:{id_apkey}",
                "kind": "informe_rnr",
                "kind_label": "Informe RNR",
                "fecha": "",
                "titulo": titulo,
                "detalle": detalle,
                "id_apkey": id_apkey,
            }
        )
    return items


def _descargar_informes_reincidencia(
    sac,
    carpeta: Path,
    selected_uids: set[str] | None = None,
    return_items: bool = False,
) -> list[tuple[Path, str]] | dict[str, dict[str, object]]:
    """
    Descarga los Informes del Registro Nacional de Reincidencia (RNR) y devuelve [(PDF, fecha_informe)].

    Estrategia real del sitio:
    - abre la sección Reincidencias
    - llama al mismo endpoint AJAX que usa la página: Radiografia.aspx/GetReincidencias
    - extrae los IdPedidoAPKey desde response.d
    - ubica el anchor exacto javascript:VerReincidencia('<id>')
    - hace click sobre ESE anchor exacto
    - captura:
        a) descarga directa
        b) popup nuevo
    - si el popup devuelve HTML con “La respuesta no contiene archivo”, lo registra y sigue
    """

    import re
    import json
    import time
    import html as html_lib
    import xml.etree.ElementTree as ET
    from playwright.sync_api import TimeoutError as PWTimeoutError
    from urllib.parse import urljoin

    informes: list[tuple[Path, str]] = []
    out_items: dict[str, dict[str, object]] = {}
    vistos: set[tuple[str, int]] = set()
    selected_uids = set(selected_uids or [])
    ctx = sac.context

    def _filename_from_cd(cd: str | None) -> str | None:
        import urllib.parse
        if not cd:
            return None
        m = re.search(r'filename\*?=([^;]+)', cd)
        if not m:
            return None
        val = m.group(1).strip().strip('"')
        if val.lower().startswith("utf-8''"):
            val = urllib.parse.unquote(val[7:])
        return Path(val).name

    def _guardar_bytes_pdf(data: bytes, nombre: str | None, i: int) -> Path | None:
        try:
            dst = carpeta / (nombre or f"InformeRNR_{i:03d}.pdf")
            with open(dst, "wb") as f:
                f.write(data)
            if _is_real_pdf(dst):
                return dst
        except Exception:
            pass
        return None

    def _cerrar_popups_extra():
        for p in list(ctx.pages):
            try:
                if p == sac:
                    continue
                p.close()
            except Exception:
                pass

    def _abrir_seccion_rnr():
        try:
            logging.info("[RNR] Intentando abrir sección Reincidencias")
        except Exception:
            pass

        try:
            sac.evaluate("() => { try { Seccion('Reincidencias'); } catch(e){} }")
        except Exception:
            pass

        for sel in [
            "a[href*=\"Seccion('Reincidencias')\"]",
            "a[onclick*=\"Seccion('Reincidencias')\"]",
        ]:
            try:
                a = sac.locator(sel).first
                if a.count():
                    try:
                        a.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        a.click(timeout=3000)
                    except Exception:
                        try:
                            a.evaluate("el => el.click()")
                        except Exception:
                            pass
            except Exception:
                pass

        try:
            sac.wait_for_timeout(800)
        except Exception:
            pass

    def _leer_ajax_reincidencias() -> list[dict]:
        """
        Llama al mismo endpoint que usa DesplegarReincidencias y devuelve
        una lista de dicts con Nombre / NumeroDocumento / FechaNacimiento / IdPedidoAPKey.
        """
        try:
            raw_text = sac.evaluate(
                """async () => {
                    const r = await fetch("Radiografia.aspx/GetReincidencias", {
                        method: "POST",
                        headers: { "Content-Type": "application/json; charset=utf-8" },
                        body: JSON.stringify({ idExpediente: idExpedienteCliente + "" })
                    });
                    return await r.text();
                }"""
            )
        except Exception as e:
            try:
                logging.info(f"[RNR] Error llamando GetReincidencias: {e}")
            except Exception:
                pass
            return []

        if not raw_text:
            return []

        try:
            payload = json.loads(raw_text)
            xml_escaped = payload.get("d", "") or ""
        except Exception as e:
            try:
                logging.info(f"[RNR] No se pudo parsear JSON de GetReincidencias: {e}")
            except Exception:
                pass
            return []

        if not xml_escaped:
            return []

        try:
            xml_text = html_lib.unescape(xml_escaped)
            root = ET.fromstring(xml_text)
        except Exception as e:
            try:
                logging.info(f"[RNR] No se pudo parsear XML de GetReincidencias: {e}")
                logging.info(f"[RNR] XML recorte: {(xml_escaped or '')[:1000]}")
            except Exception:
                pass
            return []

        filas = []
        for table in root.findall(".//Table"):
            item = {}
            for child in list(table):
                item[child.tag] = (child.text or "").strip()
            if item.get("IdPedidoAPKey"):
                filas.append(item)

        try:
            logging.info(f"[RNR] AJAX GetReincidencias devolvió {len(filas)} registros")
        except Exception:
            pass

        return filas

    def _esperar_anchor_exacto(id_apkey: str):
        href_js = f"javascript:VerReincidencia('{id_apkey}')"

        for _ in range(20):
            try:
                link = sac.locator(f'a[href="{href_js}"]').first
                if link.count():
                    return link
            except Exception:
                pass
            try:
                sac.wait_for_timeout(200)
            except Exception:
                pass
        return None

    def _esperar_popup_real_tras_click(click_fn, timeout_ms: int = 15000):
        """
        Hace el click y luego busca la nueva pestaña/ventana REAL,
        ignorando about:blank, ':' y popups vacíos.
        """
        try:
            before = list(ctx.pages)
        except Exception:
            before = []

        before_ids = {id(p) for p in before}

        click_fn()

        deadline = time.time() + (timeout_ms / 1000.0)
        popup_real = None

        while time.time() < deadline:
            try:
                actuales = list(ctx.pages)
            except Exception:
                actuales = []

            nuevos = [p for p in actuales if id(p) not in before_ids]

            for p in nuevos:
                try:
                    try:
                        p.wait_for_load_state("domcontentloaded", timeout=1000)
                    except Exception:
                        pass

                    u = (p.url or "").strip()
                    if u and u != ":" and u != "about:blank":
                        popup_real = p
                        break
                except Exception:
                    continue

            if popup_real:
                break

            try:
                sac.wait_for_timeout(250)
            except Exception:
                pass

        return popup_real

    def _descargar_desde_popup(popup, i: int, id_apkey: str) -> Path | None:
        try:
            popup.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            pass

        try:
            logging.info(f"[RNR] Fila {i}: popup_url={popup.url}")
        except Exception:
            pass

        # 1) si el popup dispara download real
        try:
            d = popup.wait_for_event("download", timeout=8000)
            dst = carpeta / d.suggested_filename
            d.save_as(dst)
            if _is_real_pdf(dst):
                return dst
        except PWTimeoutError:
            pass
        except Exception:
            pass

        # 2) si terminó en File.aspx, esperar un poco más
        try:
            if "File.aspx?id=" in (popup.url or ""):
                popup.wait_for_timeout(2500)
        except Exception:
            pass

        # 3) si el popup quedó en HTML con error
        try:
            body_txt = (popup.locator("body").inner_text(timeout=5000) or "").strip()
            try:
                logging.info(f"[RNR] Fila {i}: popup_body={body_txt[:500]}")
            except Exception:
                pass
            if "La respuesta no contiene archivo" in body_txt:
                return None
        except Exception:
            pass

        # 4) si el contenido es HTML, lo logueamos para diagnóstico
        try:
            html = popup.content() or ""
            try:
                logging.info(f"[RNR] Fila {i}: body File.aspx={html[:800]}")
            except Exception:
                pass
        except Exception:
            pass

        # 5) buscar enlace real de descarga dentro del popup
        try:
            link = popup.locator("a[href*='Download'], a[href*='download'], a[href$='.pdf']").first
            if link.count():
                try:
                    with popup.expect_download(timeout=8000) as dl_info:
                        link.click()
                    d = dl_info.value
                    dst = carpeta / d.suggested_filename
                    d.save_as(dst)
                    if _is_real_pdf(dst):
                        return dst
                except Exception:
                    pass
        except Exception:
            pass

        return None

    def _descargar_en_misma_pestana(url_abs: str, i: int, id_apkey: str) -> Path | None:
        """
        Fuerza la descarga en la MISMA pestaña y la captura con expect_download.
        Esta es la rama correcta cuando el sitio dispara 'Download is starting'
        pero no deja una navegación HTML usable.
        """
        try:
            logging.info(f"[RNR] Fila {i}: intentando descarga en misma pestaña -> {url_abs}")
        except Exception:
            pass

        # intento 1: location.assign dentro de expect_download
        try:
            with sac.expect_download(timeout=20000) as dl_info:
                try:
                    sac.evaluate("(u) => { window.location.assign(u); }", url_abs)
                except Exception as e:
                    # a veces el contexto se destruye porque arranca la descarga
                    msg = str(e)
                    if "Execution context was destroyed" not in msg:
                        raise

            d = dl_info.value
            dst = carpeta / d.suggested_filename
            d.save_as(dst)

            try:
                logging.info(f"[RNR] Fila {i}: download capturado por location.assign -> {dst.name}")
            except Exception:
                pass

            if _is_real_pdf(dst):
                return dst
        except PWTimeoutError:
            try:
                logging.info(f"[RNR] Fila {i}: timeout esperando download con location.assign")
            except Exception:
                pass
        except Exception as e:
            try:
                logging.info(f"[RNR] Fila {i}: error con location.assign: {e}")
            except Exception:
                pass

        # intento 2: goto dentro de expect_download
        try:
            with sac.expect_download(timeout=20000) as dl_info:
                try:
                    sac.goto(url_abs, wait_until="commit", timeout=15000)
                except Exception as e:
                    msg = str(e)
                    # Esto NO es fallo real: indica que empezó la descarga
                    if "Download is starting" not in msg:
                        raise

            d = dl_info.value
            dst = carpeta / d.suggested_filename
            d.save_as(dst)

            try:
                logging.info(f"[RNR] Fila {i}: download capturado por goto -> {dst.name}")
            except Exception:
                pass

            if _is_real_pdf(dst):
                return dst
        except PWTimeoutError:
            try:
                logging.info(f"[RNR] Fila {i}: timeout esperando download con goto")
            except Exception:
                pass
        except Exception as e:
            try:
                logging.info(f"[RNR] Fila {i}: error con goto descargando: {e}")
            except Exception:
                pass

        # diagnóstico final
        try:
            logging.info(f"[RNR] Fila {i}: misma_pestana_url={sac.url}")
        except Exception:
            pass

        try:
            body = (sac.content() or "")[:1200]
            logging.info(f"[RNR] Fila {i}: misma_pestana_body={body}")
        except Exception:
            pass

        return None

    def _descargar_en_pagina_auxiliar(url_abs: str, i: int) -> Path | None:
        """
        Abre la URL File.aspx en una página NUEVA creada por Playwright
        y captura la descarga ahí, sin depender del popup del sitio.
        """
        page_aux = None
        try:
            page_aux = ctx.new_page()

            try:
                logging.info(f"[RNR] Fila {i}: abriendo página auxiliar -> {url_abs}")
            except Exception:
                pass

            # Intento principal: captura de download en la página auxiliar
            try:
                with page_aux.expect_download(timeout=20000) as dl_info:
                    try:
                        page_aux.goto(url_abs, wait_until="commit", timeout=15000)
                    except Exception as e:
                        msg = str(e)
                        if "Download is starting" not in msg:
                            raise

                d = dl_info.value
                dst = carpeta / d.suggested_filename
                d.save_as(dst)

                try:
                    logging.info(f"[RNR] Fila {i}: descarga capturada en página auxiliar -> {dst.name}")
                except Exception:
                    pass

                if _is_real_pdf(dst):
                    return dst

            except PWTimeoutError:
                try:
                    logging.info(f"[RNR] Fila {i}: timeout en página auxiliar esperando download")
                except Exception:
                    pass
            except Exception as e:
                try:
                    logging.info(f"[RNR] Fila {i}: error en página auxiliar: {e}")
                except Exception:
                    pass

            # Fallback: por si abrió HTML con error
            try:
                page_aux.goto(url_abs, wait_until="domcontentloaded", timeout=15000)
            except Exception as e:
                try:
                    logging.info(f"[RNR] Fila {i}: goto auxiliar falló: {e}")
                except Exception:
                    pass

            try:
                logging.info(f"[RNR] Fila {i}: auxiliar_url={page_aux.url}")
            except Exception:
                pass

            body = ""
            html = ""
            try:
                body = page_aux.locator("body").inner_text(timeout=4000) or ""
            except Exception:
                pass

            try:
                html = page_aux.content() or ""
                logging.info(f"[RNR] Fila {i}: auxiliar_body={html[:800]}")
            except Exception:
                pass

            if "La respuesta no contiene archivo" in body or "La respuesta no contiene archivo" in html:
                try:
                    logging.info(f"[RNR] Fila {i}: página auxiliar respondió sin archivo")
                except Exception:
                    pass
                return None

            return None

        finally:
            if page_aux is not None:
                try:
                    page_aux.close()
                except Exception:
                    pass

    def _click_anchor_exacto(link, id_apkey: str, i: int) -> Path | None:
        """
        Hace exactamente un click sobre el anchor javascript:VerReincidencia('<id>')
        y captura download/popup nuevo.

        Orden de intentos:
        1) descarga directa desde el click
        2) popup real
        3) URL capturada por window.open -> página auxiliar
        4) URL capturada por window.open -> misma pestaña
        """
        destino = None

        try:
            link.scroll_into_view_if_needed()
        except Exception:
            pass

        try:
            href = link.get_attribute("href") or ""
        except Exception:
            href = f"javascript:VerReincidencia('{id_apkey}')"

        # Hook para registrar la URL que intenta abrir window.open
        try:
            sac.evaluate(
                """() => {
                    try {
                        if (!window.__rnr_open_hook_installed) {
                            window.__rnr_last_open = "";
                            const __old_open = window.open;
                            window.open = function(url, target, features) {
                                try { window.__rnr_last_open = String(url || ""); } catch(e) {}
                                return __old_open.apply(this, arguments);
                            };
                            window.__rnr_open_hook_installed = true;
                        } else {
                            window.__rnr_last_open = "";
                        }
                    } catch(e) {}
                }"""
            )
        except Exception:
            pass

        try:
            logging.info(f"[RNR] Fila {i}: intento captura con un solo click")
        except Exception:
            pass

        # Intento 1: descarga directa
        try:
            with sac.expect_download(timeout=12000) as dl_info:
                link.click(timeout=4000, force=True)
            d = dl_info.value
            destino = carpeta / d.suggested_filename
            d.save_as(destino)
            if _is_real_pdf(destino):
                return destino
            destino = None
        except PWTimeoutError:
            destino = None
        except Exception:
            destino = None

        # Intento 2: popup real usando expect_popup del PAGE
        if not destino:
            popup = None
            try:
                with sac.expect_popup(timeout=15000) as pop_info:
                    try:
                        link.click(timeout=4000, force=True)
                    except Exception:
                        try:
                            link.evaluate("el => el.click()")
                        except Exception:
                            pass

                popup = pop_info.value

                try:
                    popup.wait_for_load_state("domcontentloaded", timeout=15000)
                except Exception:
                    pass

                try:
                    logging.info(f"[RNR] Fila {i}: popup_url={popup.url}")
                except Exception:
                    pass

                destino = _descargar_desde_popup(popup, i, id_apkey)

            except PWTimeoutError:
                popup = None
            except Exception as e:
                popup = None
                try:
                    logging.info(f"[RNR] Fila {i}: error capturando popup con expect_popup: {e}")
                except Exception:
                    pass
            finally:
                if popup is not None:
                    try:
                        popup.close()
                    except Exception:
                        pass

        # Intento 3: si Playwright no atrapó popup, leer la URL que pasó por window.open
        if not destino:
            try:
                opened_url = sac.evaluate("() => window.__rnr_last_open || ''") or ""
            except Exception:
                opened_url = ""

            try:
                logging.info(f"[RNR] Fila {i}: window.open url={opened_url}")
            except Exception:
                pass

            if opened_url:
                try:
                    try:
                        opened_url_abs = urljoin(sac.url, opened_url)
                    except Exception:
                        opened_url_abs = opened_url

                    try:
                        logging.info(f"[RNR] Fila {i}: window.open abs={opened_url_abs}")
                    except Exception:
                        pass

                    destino = _descargar_en_misma_pestana(opened_url_abs, i, id_apkey)

                except Exception as e:
                    try:
                        logging.info(f"[RNR] Fila {i}: error abriendo url capturada por window.open: {e}")
                    except Exception:
                        pass

        return destino


    _abrir_seccion_rnr()

    datos = _leer_ajax_reincidencias()
    if not datos:
        try:
            logging.info("[RNR] GetReincidencias no devolvió registros.")
        except Exception:
            pass
        return out_items if return_items else informes

    try:
        logging.info(f"[RNR] Registros RNR detectados: {len(datos)}")
    except Exception:
        pass

    for i, item in enumerate(datos, start=1):
        id_apkey = (item.get("IdPedidoAPKey") or "").strip()
        nombre = (item.get("Nombre") or "").strip()
        ndoc = (item.get("NumeroDocumento") or "").strip()
        uid = f"rnr:{id_apkey}" if id_apkey else ""
        titulo = _titulo_item_radiografia(nombre, f"DNI {ndoc}" if ndoc else "", fallback=f"Informe RNR {i}")

        if not id_apkey:
            continue
        if selected_uids and uid not in selected_uids:
            continue

        try:
            logging.info(f"[RNR] Procesando fila {i}")
            logging.info(f"[RNR] Fila {i}: nombre={nombre} doc={ndoc} id={id_apkey}")
        except Exception:
            pass

        _cerrar_popups_extra()

        link = _esperar_anchor_exacto(id_apkey)
        if not link:
            try:
                logging.info(f"[RNR] Fila {i}: no se encontró anchor exacto para id={id_apkey}")
            except Exception:
                pass
            continue

        try:
            href = link.get_attribute("href") or ""
            logging.info(f"[RNR] Fila {i}: link href={href}")
        except Exception:
            pass

        destino = _click_anchor_exacto(link, id_apkey, i)

        if not destino or not destino.exists():
            try:
                logging.info(f"[RNR] Fila {i}: no se obtuvo archivo")
            except Exception:
                pass
            continue

        if destino.suffix.lower() != ".pdf":
            destino = _ensure_pdf_fast(destino) if '_ensure_pdf_fast' in globals() else _ensure_pdf(destino)

        if not destino or not destino.exists() or destino.suffix.lower() != ".pdf" or not _is_real_pdf(destino):
            try:
                logging.info(f"[RNR] Fila {i}: archivo inválido tras conversión")
            except Exception:
                pass
            continue

        if _pdf_contiene_mensaje_permiso(destino):
            try:
                destino.unlink()
            except Exception:
                pass
            try:
                logging.info(f"[RNR] Fila {i}: PDF con mensaje de permisos, se descarta")
            except Exception:
                pass
            continue

        key = (destino.name, destino.stat().st_size if destino.exists() else 0)
        if key in vistos:
            try:
                logging.info(f"[RNR] Fila {i}: archivo duplicado, se omite")
            except Exception:
                pass
            continue
        vistos.add(key)

        fecha = _fecha_rnr_desde_pdf(destino) or ""
        informes.append((destino, fecha))
        out_items[uid] = {
            "path": destino,
            "fecha": fecha,
            "titulo": titulo,
            "detalle": id_apkey,
            "id_apkey": id_apkey,
        }

        try:
            logging.info(f"[RNR] Archivo agregado: {destino.name} (fecha {fecha})")
        except Exception:
            pass

    return out_items if return_items else informes

def _extraer_adjuntos_embebidos(pdf_in: Path, out_dir: Path) -> list[Path]:
    """
    Extrae archivos embebidos / adjuntos de un PDF (PyMuPDF o pikepdf si estÃƒÂ¡ disponible).
    Devuelve lista de paths extraÃƒÂ­dos.
    """
    extraidos: list[Path] = []
    # PyMuPDF primero (rÃƒÂ¡pido y suele venir instalado)
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(pdf_in))
        # nombres segun versiÃƒÂ³n
        try:
            names = list(doc.embedded_file_names())
        except Exception:
            try:
                names = list(doc.embeddedFileNames())
            except Exception:
                names = []
        for name in names:
            try:
                # obtener bytes
                try:
                    data = doc.embedded_file_get(name)
                except Exception:
                    data = doc.embeddedFileGet(name)  # compat
                if not data:
                    continue
                dst = out_dir / Path(name).name
                with open(dst, "wb") as f:
                    f.write(data if isinstance(data, (bytes, bytearray)) else bytes(data))
                extraidos.append(dst)
            except Exception:
                continue
        try: doc.close()
        except Exception: pass
        if extraidos:
            return extraidos
    except Exception:
        pass

    # Fallback: pikepdf
    try:
        import pikepdf
        with pikepdf.open(str(pdf_in)) as pdf:
            try:
                # pikepdf >=7
                for fname, fs in pdf.attachments.items():
                    dst = out_dir / Path(fname).name
                    fs.extract_to(dst)
                    extraidos.append(dst)
            except Exception:
                # Manual: recorrer EmbeddedFiles
                af = pdf.open_outline_root()
                # si no estÃƒÂ¡, omitimos
                pass
    except Exception:
        pass

    return extraidos



# --------------------- Portal ? Ã¯Â¿Â½?oPortal de Aplicaciones PJÃ¯Â¿Â½?Ã¯Â¿Â½ ------------
def _open_portal_aplicaciones_pj(page):
    """
    Abre el tile "Portal de Aplicaciones PJ" del portal SSL-VPN.
    Soporta variantes de DOM (titulo en <div> o <span>).
    """
    try:
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    selectors = [
        ".card-header:has(.card-title:has-text('INTRANET: Portal de Aplicaciones'))",
        ".card:has(.card-title:has-text('INTRANET: Portal de Aplicaciones'))",
        ".condensed-card:has-text('INTRANET: Portal de Aplicaciones')",
        "a:has-text('INTRANET: Portal de Aplicaciones')",
        "text=/INTRANET:\\Portal\\s+de\\s+Aplicaciones/i",
    ]

    target = None
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count():
                target = loc
                break
        except Exception:
            continue

    if not target or target.count() == 0:
        _debug_dump(page, "tile_not_found")
        raise RuntimeError("No encontre 'Portal de Aplicaciones PJ'.")

    try:
        target.scroll_into_view_if_needed()
    except Exception:
        pass

    # Misma pestana
    try:
        with page.expect_navigation(timeout=8000):
            target.click(force=True)
        return page
    except Exception:
        pass

    # Popup
    try:
        with page.context.expect_page() as pop:
            target.click(force=True)
        newp = pop.value
        newp.wait_for_load_state("domcontentloaded")
        return newp
    except Exception:
        pass

    # Onclick / href internos
    try:
        href, onclick = target.evaluate(
            """el=>{
                const a = el.closest('a[href]') || el.querySelector('a[href]');
                return [a ? a.getAttribute('href') : null,
                        (a && a.getAttribute('onclick')) || el.getAttribute('onclick') || ""];
            }"""
        )
        if href and not href.startswith("javascript:") and href.strip() != "#":
            if href.startswith("/"):
                href = "https://teletrabajo.justiciacordoba.gob.ar" + href
            page.goto(href, wait_until="domcontentloaded")
            return page

        real = _extract_url_from_js(onclick)
        if real:
            page.goto(real, wait_until="domcontentloaded")
            return page
    except Exception:
        pass

    # Fallback duro
    proxy_prefix = _get_proxy_prefix(page)
    page.goto(
        proxy_prefix + "https://www.tribunales.gov.ar/PortalWeb/LogIn.aspx",
        wait_until="domcontentloaded",
    )
    return page


# ------------------------- Intranet helpers ----------------------------
def _login_intranet(page, intra_user, intra_pass):
    import time

    logging.info("[LOGIN] Buscando formulario de Intranet")

    try:
        page.wait_for_load_state("domcontentloaded")
    except Exception:
        pass

    scopes = [page] + list(page.frames)
    must_login = _page_requires_portal_login(page)

    def _has_password(sc):
        try:
            return sc.locator("input[type='password']").first.count() > 0
        except Exception:
            return False

    def _logged_in(sc):
        # Sin password + link/acciÃƒÂ³n de salir visibles
        try:
            if _has_password(sc):
                return False
            logout = sc.locator(
                "a[href*='Logout'], a[href*='SignOut'], a[href*='logoff'], "
                "a:has-text('Desconectarse'), a:has-text('Salir')"
            ).first
            return logout.count() > 0
        except Exception:
            return False

    # ? Solo devolvÃƒÂ© Ã¯Â¿Â½?oya activoÃ¯Â¿Â½?Ã¯Â¿Â½ si de verdad vemos logout y no hay password en ningÃƒÂºn lado
    if any(_logged_in(sc) for sc in scopes):
        logging.info("[LOGIN] SesiÃƒÂ³n ya activa (logout visible).")
        return

    # Si hay cualquier password en la pÃƒÂ¡gina, vamos a completar el login
    target_scope = None
    user_box = None
    pass_box = None

    def _first_visible(sc, selectors):
        for sel in selectors:
            try:
                loc = sc.locator(sel).first
                if loc.count():
                    try:
                        loc.wait_for(state="visible", timeout=2000)
                    except Exception:
                        pass
                    if loc.is_visible():
                        return loc
            except Exception:
                pass
        return None

    login_deadline = time.time() + (12.0 if must_login else 0.0)
    while True:
        # Buscamos primero el password, luego el user en el mismo scope.
        for sc in scopes:
            p_ = _first_visible(sc, [
                "input[id$='Password']",
                "input[name$='Password']",
                "input[id*='Password']",
                "input[name*='Password']",
                "input[type='password']",
                "input[formcontrolname='password']",
                "input[name='password']",
            ])
            if not p_:
                continue
            u_ = _first_visible(sc, [
                "input[id$='UserName']",
                "input[name$='UserName']",
                "input[id$='txtUserName']",
                "input[name$='txtUserName']",
                "input[id*='UserName']",
                "input[name*='UserName']",
                "input[formcontrolname='username']",
                "input[name='username']",
                "input[type='email']",
                "input[type='text']",
            ])
            if u_:
                target_scope, user_box, pass_box = sc, u_, p_
                break
        if target_scope and user_box and pass_box:
            break
        if (not must_login) or (time.time() >= login_deadline):
            break
        try:
            page.wait_for_timeout(250)
        except Exception:
            time.sleep(0.25)
        try:
            scopes = [page] + list(page.frames)
        except Exception:
            scopes = [page]

    if not (target_scope and user_box and pass_box):
        if must_login:
            logging.info("[LOGIN] PÃ¡gina de login detectada pero no encontrÃ© el formulario visible.")
            _debug_dump(page, "login_intranet_no_form")
            raise RuntimeError("LOGIN_FORM_NOT_FOUND")
        logging.info("[LOGIN] No vi un formulario de login activo; continÃºo sin reloguear.")
        return

    def _smart_fill(sc, el, val):
        try:
            el.click()
            sc.wait_for_timeout(60)
        except Exception:
            pass
        try:
            el.fill(val)
        except Exception:
            try:
                sc.evaluate(
                    "(el,val)=>{el.value=''; el.dispatchEvent(new Event('input',{bubbles:true})); "
                    "el.focus(); el.value=val; el.dispatchEvent(new Event('input',{bubbles:true}));}",
                    el, val
                )
            except Exception:
                pass

    _kill_overlays(target_scope)
    _smart_fill(target_scope, user_box, intra_user)
    _smart_fill(target_scope, pass_box, intra_pass)

    # Enviar (Enter o botÃƒÂ³n submit)
    try:
        pass_box.press("Enter")
        target_scope.wait_for_load_state("networkidle")
    except Exception:
        pass

    btn = _first_visible(target_scope, [
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('Ingresar')",
        "button:has-text('Iniciar sesiÃƒÂ³n')",
        "xpath=//span[normalize-space()='Ingresar' or normalize-space()='Iniciar sesiÃƒÂ³n']/ancestor::button[1]"
    ])
    if btn and btn.count():
        try:
            target_scope.wait_for_function(
                "(b)=>!b.disabled && b.getAttribute('aria-disabled')!=='true'",
                arg=btn.element_handle(), timeout=4000
            )
        except Exception:
            pass
        try:
            btn.click(timeout=3000)
        except Exception:
            try:
                btn.click(force=True, timeout=2000)
            except Exception:
                pass

    try:
        target_scope.wait_for_load_state("networkidle")
    except Exception:
        pass

    if must_login:
        for _ in range(20):
            if not _page_requires_portal_login(page):
                break
            try:
                page.wait_for_timeout(300)
            except Exception:
                time.sleep(0.3)
        if _page_requires_portal_login(page):
            logging.info("[LOGIN] El portal sigue mostrando login tras enviar credenciales.")
            raise RuntimeError("LOGIN_STILL_REQUIRED")



def _kill_overlays(page):
    """Oculta/remueve cortinas/overlays que interceptan el click (jQuery UI / modales)."""
    try:
        page.evaluate(
            """
            () => {
                const sels = [
                    '#divDialogCourtian_0', '.divDialogCourtian', '.divDialogCortina',
                    '.ui-widget-overlay', '.ui-widget-shadow', '.modal-backdrop', '.modal[role=dialog]'
                ];
                for (const s of sels) {
                    document.querySelectorAll(s).forEach(el => {
                        el.style.pointerEvents = 'none';
                        el.style.display = 'none';
                        el.remove();
                    });
                }
            }
            """
        )
    except Exception:
        pass


def _ensure_public_apps(page):
    """
    Posiciona en PublicApps.aspx pero nunca sale del proxy.
    Si todavÃƒÂ­a no hay /proxy/<token>/ vuelve a la grilla y abre por tile.
    """
    proxy_prefix = _get_proxy_prefix(page)
    if not proxy_prefix:
        _goto_portal_grid(page)
        return _open_portal_aplicaciones_pj(page)

    # activa el proxy y vuelve dentro del portal
    page.goto(
        proxy_prefix + "https://www.tribunales.gov.ar/PortalWeb/PublicApps.aspx",
        wait_until="domcontentloaded",
    )
    page.wait_for_load_state("networkidle")
    return page


# ------------------------- CARGA DEL LIBRO -----------------------------
def _expandir_y_cargar_todo_el_libro(libro):
    S = _libro_scope(libro)
    try:
        S.wait_for_load_state("domcontentloaded")
        S.wait_for_load_state("networkidle")
    except Exception:
        pass

    # ? activar killer mientras tocamos el ÃƒÂ­ndice
    handler = _kill_spurious_popups(libro.context)
    try:
        items = _listar_operaciones_rapido(libro)
        orden = []
        for it in items:
            _mostrar_operacion(libro, it["id"], it.get("tipo", ""))
            cont = _buscar_contenedor_operacion(libro, it["id"])
            if cont:
                try:
                    cont.wait_for(state="visible", timeout=2000)
                except Exception:
                    pass
                orden.append(it)
        return orden
    finally:
        try:
            libro.context.off("page", handler)
        except Exception:
            pass


def _mostrar_operacion(libro, op_id: str, tipo: str):
    import re

    # 1) localizar el link del ÃƒÂ­ndice en cualquier frame
    link, link_scope = None, None
    for sc in _all_scopes(libro):
        try:
            _kill_overlays(sc)
        except Exception:
            pass
        for sel in (
            f"a[onclick*=\"onItemClick('{op_id}'\"]",
            f"a[onclick*=\"onItemClick(\\\"{op_id}\\\"\"]",
            f"a[href*=\"onItemClick('{op_id}'\"]",
            f"a[href*=\"onItemClick(\\\"{op_id}\\\"\"]",
            f"a[data-codigo='{op_id}']",
            f".nav-link.{op_id}",
            f"a[aria-controls*='{op_id}']",
        ):
            try:
                loc = sc.locator(sel).first
                if loc.count():
                    link, link_scope = loc, sc
                    break
            except Exception:
                continue
        if link:
            break

    # 2) si no vino 'tipo', intentÃƒÂ¡ inferirlo del link encontrado
    if (not tipo) and link:
        try:
            oc = (link.get_attribute("onclick") or "") + " " + (
                link.get_attribute("href") or ""
            )
            m = re.search(
                r"onItemClick\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]", oc
            )
            if m:
                tipo = m.group(1)
            else:
                tipo = link.get_attribute("data-tipo") or ""
        except Exception:
            pass

    # 3) intento principal: clic real en el link del ÃƒÂ­ndice
    clicked = False
    if link:
        try:
            link.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            link.evaluate("el=>{el.target='_self'; el.rel='noopener';}")
        except Exception:
            pass
        try:
            link.click()
            clicked = True
        except Exception:
            try:
                link.click(force=True)
                clicked = True
            except Exception:
                try:
                    link.evaluate("el=>el.click()")
                    clicked = True
                except Exception:
                    pass
    # 4) fallback: ejecutar onItemClick donde exista (pÃƒÂ¡gina o cualquier frame)
    if not clicked:
        for sc in _all_scopes(libro):
            try:
                has_fn = sc.evaluate("()=>typeof onItemClick==='function'")
            except Exception:
                has_fn = False
            if not has_fn:
                continue
            try:
                sc.evaluate("([id,t])=>onItemClick(id,t)", [op_id, tipo or ""])
                clicked = True
                break
            except Exception:
                continue

    # 5) ÃƒÂºltimo recurso: evento custom usado por algunas skins
    if not clicked and link_scope:
        try:
            link_scope.evaluate(
                "(id)=>{ const ev=new CustomEvent('SAC:onItemClick',{detail:{id}}); "
                "window.dispatchEvent(ev); }",
                op_id,
            )
        except Exception:
            pass


def _extraer_url_de_link(link, proxy_prefix: str) -> str | None:
    href = link.get_attribute("href") or ""
    oc = link.get_attribute("onclick") or ""

    # 1) Caso clÃƒÂ¡sico: URL absoluta o /proxy/ relativo
    url = _extract_url_from_js(href or oc)
    if url:
        if url.startswith("/proxy/"):
            url = "https://teletrabajo.justiciacordoba.gob.ar" + url
        if (
            url.startswith("https://www.tribunales.gov.ar/")
            or url.startswith("https://aplicaciones.tribunales.gov.ar/")
        ) and proxy_prefix:
            url = _proxify_abs_url(proxy_prefix, url)
        return url

    # 2) Nuevo: javascript:VerAdjuntoFichero('ID')
    if "VerAdjuntoFichero" in (href + oc):
        raw = href if "VerAdjuntoFichero" in href else oc
        u = _url_from_ver_adjunto(raw, proxy_prefix)
        if u:
            return u
    return None


def _descargar_archivo(session: requests.Session, url: str, destino: Path, _depth: int = 0) -> Path | None:
    from requests.exceptions import SSLError
    from urllib.parse import urlparse
    import urllib3
    import json

    nombre = Path(urlparse(url).path).name or destino.name
    host = (urlparse(url).hostname or "").lower()
    logging.info(f"[DL:START] {nombre} -> {destino.name}")

    def _guardar(data: bytes):
        with open(destino, "wb") as f:
            f.write(data)

    def _url_secundaria_valida(u: str | None) -> str | None:
        if not u:
            return None
        cand = (u or "").strip().strip("'\"").replace("&amp;", "&")
        if not cand:
            return None
        low = cand.lower()
        if "w3.org/" in low or "stylesheet" in low:
            return None
        try:
            pu = urlparse(cand)
        except Exception:
            return None
        host = (pu.hostname or "").lower()
        if host and not (
            host.endswith("tribunales.gov.ar")
            or host.endswith("justiciacordoba.gob.ar")
            or host == "teletrabajo.justiciacordoba.gob.ar"
        ):
            return None
        path_q = f"{pu.path}?{pu.query}".lower()
        if path_q.endswith((".css", ".xsd", ".dtd")):
            return None
        return cand

    def _urls_secundarias_en_texto(txt: str):
        vistos_url = set()
        for m in re.finditer(r"https?://[^\s'\"<>(),]+", txt or "", re.I):
            u = _url_secundaria_valida(m.group(0))
            if u and u not in vistos_url:
                vistos_url.add(u)
                yield u
        for m in re.finditer(r"/proxy/[^'\"<>(),]+", txt or "", re.I):
            u = "https://teletrabajo.justiciacordoba.gob.ar" + m.group(0)
            u = _url_secundaria_valida(u)
            if u and u not in vistos_url:
                vistos_url.add(u)
                yield u

    def _resolver_url_secundaria(payload: bytes) -> str | None:
        if not payload:
            return None
        txt = payload.decode("utf-8", errors="ignore")

        # Caso simple: el body ya contiene una URL completa.
        u = next(_urls_secundarias_en_texto(txt), None)
        if u:
            return u

        # Caso JSON (GetDownloadLink y variantes).
        try:
            obj = json.loads(txt)
        except Exception:
            obj = None

        def _walk(x):
            if isinstance(x, dict):
                for k, v in x.items():
                    if isinstance(v, str) and any(t in k.lower() for t in ("url", "link", "download")):
                        yield v
                    yield from _walk(v)
            elif isinstance(x, list):
                for it in x:
                    yield from _walk(it)
            elif isinstance(x, str):
                yield x

        if obj is not None:
            for cand in _walk(obj):
                u2 = next(_urls_secundarias_en_texto(cand), None)
                if u2:
                    return u2

        # Ultimo intento por regex plana.
        m = re.search(r'"(?:url|downloadurl|link)"\s*:\s*"([^"]+)"', txt, re.I)
        if m:
            u3 = next(_urls_secundarias_en_texto(m.group(1)), None)
            if u3:
                return u3

        return None

    def _payload_parece_respuesta_intermedia(payload: bytes) -> bool:
        head = (payload or b"")[:4096].lstrip().lower()
        if not head:
            return True
        if head.startswith((b"<!doctype", b"<html", b"<script", b"{", b"[")):
            return True
        if any(m in head for m in (b"<html", b"<body", b"</html", b"w3.org/1999/xhtml", b"<link", b"stylesheet")):
            return True
        if head.startswith((b"/*", b"@", b".", b"#", b"body", b"html")) and b"{" in head[:300]:
            return True
        marcadores = (
            b"ssl vpn proxy error",
            b"login.aspx",
            b"returnurl",
            b"form id=",
            b"<body",
            b"veradjunto",
            b"downloadurl",
        )
        return any(m in head for m in marcadores)

    def _payload_compatible_con_extension(payload: bytes, ext: str) -> bool:
        head = (payload or b"")[:32]
        ext = (ext or "").lower()
        if ext in {".docx", ".xlsx", ".pptx", ".odt", ".ods", ".odp"}:
            return head.startswith(b"PK")
        if ext in {".doc", ".xls", ".ppt"}:
            return head.startswith(b"\xd0\xcf\x11\xe0")
        if ext == ".rtf":
            return head.startswith(b"{\\rtf")
        if ext in {".jpg", ".jpeg"}:
            return head.startswith(b"\xff\xd8\xff")
        if ext == ".png":
            return head.startswith(b"\x89PNG\r\n\x1a\n")
        if ext in {".tif", ".tiff"}:
            return head.startswith((b"II*\x00", b"MM\x00*"))
        if ext == ".bmp":
            return head.startswith(b"BM")
        if ext in {".txt", ".csv"}:
            return not _payload_parece_respuesta_intermedia(payload)
        return not _payload_parece_respuesta_intermedia(payload)

    def _descarga_once(verify_tls: bool = True):
        return session.get(url, timeout=60, allow_redirects=True, verify=verify_tls)

    payload = b""
    try:
        r = _descarga_once(verify_tls=True)
        r.raise_for_status()
        payload = r.content or b""
    except SSLError as e:
        msg = str(e).lower()
        if host.endswith("tribunales.gov.ar") and (
            "self-signed" in msg or "certificate verify failed" in msg
        ):
            logging.info(f"[DL:WARN] SSL en {host}. Reintento sin verificacion TLS.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            try:
                r = _descarga_once(verify_tls=False)
                r.raise_for_status()
                payload = r.content or b""
            except Exception as e2:
                logging.info(f"[DL:ERR] {destino.name} | {e2}")
                return None
        else:
            logging.info(f"[DL:ERR] {destino.name} | {e}")
            return None
    except Exception as e:
        logging.info(f"[DL:ERR] {destino.name} | {e}")
        return None

    # Caso normal: PDF real.
    if payload[:4] == b"%PDF":
        _guardar(payload)
        sz = destino.stat().st_size if destino.exists() else 0
        logging.info(f"[DL:OK] {destino.name} ({sz} bytes)")
        return destino

    # Caso normal: archivo real no PDF (Office/imagen). Se guarda y luego se convierte.
    ext_destino = destino.suffix.lower()
    if payload and ext_destino and ext_destino != ".pdf" and _payload_compatible_con_extension(payload, ext_destino):
        _guardar(payload)
        sz = destino.stat().st_size if destino.exists() else 0
        logging.info(f"[DL:OK] {destino.name} ({sz} bytes, no PDF)")
        return destino

    # Si vino JSON/HTML con URL secundaria, seguirla una vez o dos.
    if _depth < 2:
        siguiente = _resolver_url_secundaria(payload)
        if siguiente and siguiente != url:
            try:
                logging.info(f"[DL:INFO] {destino.name}: respuesta no PDF; intento URL interna -> {siguiente}")
            except Exception:
                pass
            return _descargar_archivo(session, siguiente, destino, _depth=_depth + 1)

    # Guardar solo para debug y descartar para no romper el merge.
    try:
        _guardar(payload)
        if not _is_real_pdf(destino):
            destino.unlink(missing_ok=True)
    except Exception:
        pass

    logging.info(f"[DL:ERR] {destino.name} | respuesta no es PDF real")
    return None


def _imagen_a_pdf_fast(img: Path, margin_mm: float = 10.0) -> Path:
    """
    Convierte una imagen a PDF A4, manteniendo proporciones y con margen.
    Requiere img2pdf.
    """
    import img2pdf

    pdf = img.with_suffix(".pdf")
    # A4 en puntos (72 pt por pulgada) usando helpers de img2pdf
    a4 = (img2pdf.mm_to_pt(210), img2pdf.mm_to_pt(297))
    border = (img2pdf.mm_to_pt(margin_mm), img2pdf.mm_to_pt(margin_mm))
    layout_fun = img2pdf.get_layout_fun(
        pagesize=a4,
        border=border,
        fit=img2pdf.FitMode.SHRINK_TO_FIT,  # nunca agranda mÃƒÂ¡s de A4; conserva relaciÃƒÂ³n de aspecto
        auto_orient=True,
    )
    with open(pdf, "wb") as f:
        f.write(img2pdf.convert(str(img), layout_fun=layout_fun))
    return pdf


def _ensure_pdf_fast(path: Path) -> Path:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return path

    if ext in {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}:
        pdf = _imagen_a_pdf_fast(path)
        return pdf

    soffice = _shutil.which("soffice") or _shutil.which("soffice.exe") or r"C:\Program Files\LibreOffice\program\soffice.exe"
    if soffice and Path(str(soffice)).exists():
        outdir = path.parent
        dst = path.with_suffix(".pdf")
        logging.info(f"[CNV:OFF] {path.name} -> {dst.name}")
        try:
            subprocess.run(
                [soffice, "--headless", "--convert-to", "pdf", "--outdir", str(outdir), str(path)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                **_subprocess_hidden_kwargs(),
            )
            pdf = path.with_suffix(".pdf")
            if pdf.exists():
                logging.info(f"[CNV:OK ] {pdf.name}")
                return pdf
        except Exception as e:
            logging.info(f"[CNV:ERR] {path.name} Ã‚Â· {e}")
    else:
        logging.info(f"[CNV:OFF] LibreOffice no encontrado; no puedo convertir {path.name}")
    word_pdf = _convert_office_with_word(path)
    if word_pdf:
        return word_pdf
    docx_pdf = _convert_docx_text_to_pdf(path)
    if docx_pdf:
        return docx_pdf
    return path


def _open_sac_desde_portal_teletrabajo(page):
    """
    *** SOLO Teletrabajo ***
    Abre el menÃƒÂº 'Aplicaciones' (img#imgMenuServiciosPrivadas) y entra a 'SAC Multifuero'.
    Es el flujo que ya te funcionaba y NO usa navegaciÃƒÂ³n directa sin proxy.
    """
    logging.info("[NAV] Intentando abrir 'SAC Multifuero' desde portal actual")
    import re
    # Si ya estamos en PublicApps.aspx (bajo proxy), delegÃƒÂ¡
    if re.search(r"/PortalWeb/(Pages/)?PublicApps\.aspx", (page.url or ""), re.I):
        return _open_sac_desde_portal_intranet(page)
    try:
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    scopes = [page] + page.frames
    trigger = None
    scope = page
    for sc in scopes:
        trig = sc.locator("#imgMenuServiciosPrivadas").first
        if not trig.count():
            trig = sc.locator("img[alt*='Aplicaciones'][alt*='Privadas']").first
        if trig.count():
            trigger = trig
            scope = sc
            break

    if not trigger:
        _debug_dump(page, "no_trigger_aplicaciones")
        raise RuntimeError(
            "No encontrÃƒÂ© el botÃƒÂ³n 'Aplicaciones' (id imgMenuServiciosPrivadas)."
        )

    try:
        trigger.scroll_into_view_if_needed()
    except Exception:
        pass

    matcher = re.compile(r"SAC\s*Multifueros?", re.I)
    link = None
    for _ in range(3):
        try:
            trigger.click(force=True)
        except Exception:
            logging.info("[NAV] Link a SAC Multifuero localizado; abriendo...")
            try:
                trigger.evaluate("el => el.click()")
            except Exception:
                pass
        scope.wait_for_timeout(250)
        link = scope.get_by_role("link", name=matcher)
        if not link.count():
            link = scope.locator("a", has_text=matcher)
        if link.first.count():
            link = link.first
            break

    if not link or not link.count():
        _debug_dump(page, "apps_menu_sin_sac")
        raise RuntimeError(
            "No encontrÃƒÂ© el enlace a 'SAC Multifuero' dentro de Aplicaciones."
        )

    # Puede ser popup o misma pestaÃƒÂ±a
    try:
        with page.context.expect_page() as pop:
            link.click()
        sac = pop.value
        sac.wait_for_load_state("domcontentloaded")
        logging.info("[NAV] SAC abierto desde portal")
        return sac
    except Exception:
        pass

    try:
        with scope.expect_navigation(timeout=7000):
            link.click()
        return scope
    except Exception:
        pass

    # Ã¯Â¿Â½sltimo recurso: seguir href/onclick del link
    try:
        href, onclick = link.evaluate(
            "el => [el.getAttribute('href'), el.getAttribute('onclick') || '']"
        )
        if href and href.strip() not in ("#", "javascript:void(0)"):
            if href.startswith("/"):
                href = "https://teletrabajo.justiciacordoba.gob.ar" + href
            page.goto(href, wait_until="domcontentloaded")
            return page

        real = _extract_url_from_js(onclick)
        if real:
            page.goto(real, wait_until="domcontentloaded")
            return page
    except Exception:
        pass

    _debug_dump(page, "click_sac_fail")
    raise RuntimeError(
        "No pude abrir 'SAC Multifuero' pese a desplegar el menÃƒÂº (ver click_sac_fail.*)."
    )


def _open_sac_desde_portal_intranet(page):
    """
    *** SOLO Intranet directa / pÃƒÂ¡gina ya proxificada ***
    Busca enlace 'SAC Multifuero'. Si no aparece, navega al menÃƒÂº del SAC:
    - con proxy_prefix si estamos proxificados,
    - o directo si la URL actual ya es tribunales.gov.ar.
    """
    import re

    try:
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    matcher = re.compile(r"SAC\s*Multifueros?", re.I)
    link = page.get_by_role("link", name=matcher).first
    if not link.count():
        link = page.locator("a", has_text=matcher).first
    if not link.count():
        link = page.get_by_text(matcher).first

    if not link.count():
        for fr in page.frames:
            try:
                lk = fr.get_by_role("link", name=matcher).first
                if lk.count():
                    link = lk
                    page = fr
                    break
                lk = fr.locator("a", has_text=matcher).first
                if lk.count():
                    link = lk
                    page = fr
                    break
                lk = fr.get_by_text(matcher).first
                if lk.count():
                    link = lk
                    page = fr
                    break
            except Exception:
                continue

    if link.count():
        try:
            with (page.context if hasattr(page, "context") else page).expect_page(
                timeout=7000
            ) as pop:
                link.click()
            sac = pop.value
            sac.wait_for_load_state("domcontentloaded")
            return sac
        except Exception:
            try:
                with page.expect_navigation(timeout=7000):
                    link.click()
                return page
            except Exception:
                pass

    # Fallback seguro: solo directo si ya estamos en tribunales, o via proxy si hay proxy.
    proxy_prefix = _get_proxy_prefix(page)  # "" si no hay proxy
    if not proxy_prefix and not _is_tribunales(page.url):
        _debug_dump(page, "sac_fallback_blocked_no_proxy")
        raise RuntimeError(
            "No hallÃƒÂ© link a SAC y no hay proxy activo; evito navegaciÃƒÂ³n directa en Teletrabajo."
        )

    base_host = _sac_host_base(page).rstrip("/")
    dest = _proxify_abs_url(proxy_prefix, f"{base_host}/SacInterior/Menu/Default.aspx")
    try:
        page.goto(dest, wait_until="domcontentloaded")
    except Exception:
        # fallback estable en SSL-VPN
        alt = _proxify_abs_url(proxy_prefix, "https://aplicaciones.tribunales.gov.ar/SacInterior/Menu/Default.aspx")
        page.goto(alt, wait_until="domcontentloaded")
    return page


def _open_sac_desde_portal(page):
    import re
    u = (page.url or "")
    ul = u.lower()

    # Portal nuevo/Angular o PublicApps (aunque este bajo /proxy/) -> flujo Intranet.
    if (
        re.search(r"/portalweb/(pages/)?publicapps\.aspx", ul, re.I)
        or "aplicaciones.tribunales.gov.ar/portalwebnet" in ul
        or "/portalwebnet/#/" in ul
        or "/portalweb/login/login.aspx" in ul
    ):
        logging.info("[NAV] Contexto portalweb detectado: uso flujo Intranet para abrir SAC")
        return _open_sac_desde_portal_intranet(page)

    # Portal clasico SSL-VPN: menu Aplicaciones privadas.
    if ("teletrabajo.justiciacordoba.gob.ar" in ul) or ("/proxy/" in ul):
        logging.info("[NAV] Contexto SSL-VPN clasico detectado: uso flujo Teletrabajo")
        return _open_sac_desde_portal_teletrabajo(page)

    return _open_sac_desde_portal_intranet(page)

def _ir_a_radiografia(sac):
    """
    Preferir el menÃƒÂº de SAC ? Ã¯Â¿Â½?oRadiografÃƒÂ­aÃ¯Â¿Â½?Ã¯Â¿Â½. Si no aparece, usar URL con el mismo /proxy/.
    """
    import re

    if _page_requires_portal_login(sac):
        logging.info("[RADIO] SAC devolviÃ³ login del portal; pospongo RadiografÃ­a hasta reautenticar.")
        return sac

    try:
        sac.wait_for_load_state("domcontentloaded")
    except Exception:
        pass

    try:
        matcher = re.compile(r"Radiograf[ÃƒÂ­i]a", re.I)
        link = sac.get_by_role("link", name=matcher).first
        if not link.count():
            link = sac.locator("a", has_text=matcher).first
        if link.count():
            link.click()
            try:
                sac.wait_for_load_state("domcontentloaded")
            except Exception:
                pass
            if "Radiografia.aspx" in (sac.url or ""):
                return sac
    except Exception:
        pass

    for dest in _radiografia_candidate_urls(sac):
        try:
            sac.goto(dest, wait_until="domcontentloaded")
            if _is_proxy_error(sac):
                continue
            return sac
        except Exception:
            continue

    # ultimo fallback directo
    sac.goto(URL_RADIOGRAFIA, wait_until="domcontentloaded")
    return sac


# ----------------------- Flujo principal de login ----------------------
def abrir_sac_via_teletrabajo(context, tele_user, tele_pass, intra_user, intra_pass):
    page = context.new_page()
    page.set_default_timeout(int(os.getenv("OPEN_TIMEOUT_MS", "45000")))
    page.set_default_navigation_timeout(int(os.getenv("OPEN_NAV_TIMEOUT_MS", "60000")))

    page.goto(TELETRABAJO_URL, wait_until="domcontentloaded")

    def _is_portal_grid(pg):
        try:
            u = pg.url or ""
            # grilla SSLVPN o cards del portal
            return ("static/sslvpn/portal" in u) or (pg.locator(".card .card-title span").first.count() > 0)
        except Exception:
            return False

    # Login solo si NO estamos ya en el portal
    if not _is_portal_grid(page):
        try:
            _fill_first(page, ['#username','input[name="username"]','input[name="UserName"]','input[type="text"]'], tele_user)
            _fill_first(page, ['#password','input[name="password"]','input[type="password"]'], tele_pass)
            if not _click_first(page, ['text=Continuar','button[type="submit"]','input[type="submit"]']):
                page.keyboard.press("Enter")
            page.wait_for_load_state("networkidle")
            _handle_loginconfirm(page)
        except Exception as e:
            # Si no hay formulario pero sÃƒÂ­ vemos el portal, seguimos; si no, re-lanzamos
            if not _is_portal_grid(page):
                raise

    # Traer grilla del portal (activa el proxy) y abrir el tile
    _goto_portal_grid(page)
    portal = _open_portal_aplicaciones_pj(page)

    _login_intranet(portal, intra_user, intra_pass)
    sac = _open_sac_desde_portal(portal)
    if _page_requires_portal_login(sac):
        logging.info("[OPEN] SAC pidiÃ³ re-login del portal; intento reautenticar en el retorno.")
        _login_intranet(sac, intra_user, intra_pass)
    if _page_requires_portal_login(sac) or _is_proxy_error(sac):
        logging.info("[OPEN] Reabro Portal de Aplicaciones tras sesiÃ³n vencida o proxy bloqueado.")
        _goto_portal_grid(portal)
        portal = _open_portal_aplicaciones_pj(portal)
        _login_intranet(portal, intra_user, intra_pass)
        sac = _open_sac_desde_portal(portal)
        if _page_requires_portal_login(sac):
            _login_intranet(sac, intra_user, intra_pass)

    return _ir_a_radiografia(sac)


def abrir_sac(context, tele_user, tele_pass, intra_user, intra_pass):
    page = context.new_page()
    page.set_default_timeout(int(os.getenv("OPEN_TIMEOUT_MS", "45000")))
    page.set_default_navigation_timeout(int(os.getenv("OPEN_NAV_TIMEOUT_MS", "60000")))
    ALLOW_DIRECT_INTRANET = _env_true("ALLOW_DIRECT_INTRANET", "1")
    prefer_tele = bool(tele_user and tele_pass and _env_true("PREFER_TELE", "1"))

    def _try_open(fn, label):
        last = None
        for i in range(2):  # 2 intentos ligeros
            try:
                logging.info(f"[OPEN] {label} intento {i+1}")
                return fn()
            except Exception as e:
                last = e
                logging.info(f"[OPEN:{label}:ERR] intento {i+1} Ã‚Â· {e}")
                try:
                    page.wait_for_timeout(800 * (i + 1))
                except Exception:
                    pass
        raise last if last else RuntimeError(f"{label} fallÃƒÂ³")

    # 1) Si hay credenciales de Tele, ir por Tele primero
    if prefer_tele:
        try:
            return _try_open(
                lambda: abrir_sac_via_teletrabajo(
                    context, tele_user, tele_pass, intra_user, intra_pass
                ),
                "TELETRABAJO",
            )
        except Exception as e:
            logging.info("[OPEN] Teletrabajo fallÃƒÂ³; pruebo Intranet directa")
            if not ALLOW_DIRECT_INTRANET:
                raise e  # si ALLOW_DIRECT_INTRANET=1, reciÃƒÂ©n ahÃƒÂ­ proba el bloque de INTRANET

    # 2) Intranet directa
    try:
        def _open_intranet():
            pg = context.new_page()
            pg.set_default_timeout(int(os.getenv("OPEN_TIMEOUT_MS", "45000")))
            pg.set_default_navigation_timeout(int(os.getenv("OPEN_NAV_TIMEOUT_MS", "60000")))

            # Si la URL de Intranet no resuelve o estÃƒÂ¡ caÃƒÂ­da, disparamos un error reconocible
            try:
                pg.goto(INTRANET_LOGIN_URL, wait_until="domcontentloaded")
            except Exception as e:
                # DNS / conectividad: net::ERR_NAME_NOT_RESOLVED, ERR_CONNECTION_*
                if "ERR_NAME_NOT_RESOLVED" in str(e) or "ERR_CONNECTION" in str(e):
                    raise RuntimeError("INTRANET_NO_RESOLVE")
                raise

            _login_intranet(pg, intra_user, intra_pass)
            if "aplicaciones.tribunales.gov.ar" not in (pg.url or ""):
                _ensure_public_apps(pg)
            sac = _open_sac_desde_portal(pg)
            return _ir_a_radiografia(sac)

        return _try_open(_open_intranet, "INTRANET")
    except Exception as e:
        # Fallback explÃƒÂ­cito a Teletrabajo si Intranet no estÃƒÂ¡ disponible
        if tele_user and tele_pass:
            logging.info("[OPEN] INTRANET inaccesible; redirijo a Teletrabajo")
            return _try_open(
                lambda: abrir_sac_via_teletrabajo(
                    context, tele_user, tele_pass, intra_user, intra_pass
                ),
                "TELETRABAJO",
            )
        # Si no hay credenciales de Teletrabajo, re-lanzamos el error original
        raise

    # 3) Ã¯Â¿Â½sltimo intento por Tele si no lo probamos primero
    if not prefer_tele and tele_user and tele_pass:
        return _try_open(
            lambda: abrir_sac_via_teletrabajo(context, tele_user, tele_pass, intra_user, intra_pass),
            "TELETRABAJO",
        )

    raise RuntimeError("No pude abrir el SAC ni por Intranet ni por Teletrabajo.")


def _cerrar_indice_libro(libro):
    """
    Cierra el panel ÃƒÂndice usando los toggles de la UI (sin ocultarlo por CSS).
    Soporta distintas variantes (pestaÃƒÂ±a vertical, hamburguesa, chevrons, etc.).
    """
    S = _libro_scope(libro)

    def visible():
        nav = S.locator("#indice, .indice, .nav-container").first
        if not nav.count():
            return False
        try:
            # visible y con ancho ÃƒÂºtil (>40px para distinguir handle)
            bb = nav.bounding_box()
            return bool(bb and bb.get("width", 0) > 40 and nav.is_visible())
        except Exception:
            return False

    if not S.locator("#indice, .indice, .nav-container").first.count():
        return

    toggles = [
        "text=/^\\s*ÃƒÂndice\\s*$/i",
        "button:has-text('ÃƒÂndice')",
        "a:has-text('ÃƒÂndice')",
        ".indice-toggle, .indice .toggle, .indice [role=button]",
        ".nav-container .navbar-toggler",
        ".nav-container .fa-chevron-left, .nav-container .fa-angle-left, .nav-container .fa-angle-double-left",
        ".btn-indice, #btnIndice, #indiceTab, #indice-tab",
        "xpath=//*[contains(translate(normalize-space(.),'ÃƒÂNDICE','ÃƒÂ­ndice'),'ÃƒÂ­ndice')]",
    ]

    # Probar mÃƒÂºltiples toggles un par de veces
    for _ in range(6):
        if not visible():
            break
        for sel in toggles:
            try:
                t = S.locator(sel).first
                if not t.count():
                    continue
                try:
                    t.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    t.click()
                except Exception:
                    try:
                        t.evaluate("el => el.click()")
                    except Exception:
                        continue
                S.wait_for_timeout(200)
                if not visible():
                    break
            except Exception:
                continue


def _imprimir_libro_a_pdf(libro, context, tmp_dir: Path, p) -> Path | None:
    """
    Intenta obtener el PDF del 'Expediente como Libro'.
    1) Click en 'Imprimir / Imprimir SelecciÃƒÂ³n' y captura download si el sitio genera PDF.
    2) Si abre el diÃƒÂ¡logo del navegador (no automatable), fallback: PDF por CDP en un Chromium
       HEADLESS con el mismo estado de sesiÃƒÂ³n.
    """
    S = _libro_scope(libro)
    _cerrar_indice_libro(libro)
    out = tmp_dir / "libro.pdf"

    # Asegurar foco y scrollear al fondo (botÃƒÂ³n suele estar abajo a la derecha)
    try:
        libro.bring_to_front()
    except Exception:
        pass
    try:
        S.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        S.wait_for_timeout(300)
    except Exception:
        pass

    # 1) Intento: botÃƒÂ³n que dispare download del backend
    btn_selectors = [
        "text=/\\bImprimir SelecciÃƒÂ³n\\b/i",
        "text=/\\bImprimir\\b/i",
        "button:has-text('Imprimir SelecciÃƒÂ³n')",
        "button:has-text('Imprimir')",
        "a[onclick*='Imprimir']",
        "button[onclick*='Imprimir']",
        "a[href*='Imprimir']",
    ]
    for sel in btn_selectors:
        try:
            loc = S.locator(sel).last
            if not loc.count():
                continue
            try:
                loc.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                with libro.expect_download(timeout=20000) as dl:
                    try:
                        loc.click(force=True)
                    except Exception:
                        loc.evaluate("el => el.click()")
                d = dl.value
                d.save_as(out)
                # despuÃƒÂ©s de d.save_as(out) o de hp.pdf(...)
                if out.exists() and out.stat().st_size > 1024:
                    if _pdf_es_login_portal(out):
                        logging.info(
                            "[PRINT:DL] Ignorado: es login del portal (no Libro)."
                        )
                        try:
                            out.unlink()
                        except Exception:
                            pass
                        return None
                    logging.info(f"[PRINT:DL] PDF libro guardado: {out.name}")
                    return out
            except Exception:
                # Si abriÃƒÂ³ el diÃƒÂ¡logo del navegador, no habrÃƒÂ¡ download ? seguimos al plan B
                pass
        except Exception:
            continue

    # justo antes de lanzar headless: stor
    stor = libro.evaluate(
        """() => ({
            local: Object.fromEntries(Object.entries(localStorage)),
            session: Object.fromEntries(Object.entries(sessionStorage)),
        })"""
    )
    state_file = tmp_dir / "state.json"
    context.storage_state(path=str(state_file))
    hbrowser = _launch_chromium(
        p.chromium,
        headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
    )
    hctx = hbrowser.new_context(
        storage_state=str(state_file), viewport={"width": 1366, "height": 900}
    )
    hp = hctx.new_page()
    # reinyectar storages ANTES de navegar
    import json

    hp.add_init_script(
        f""" (function() {{
            try {{
                localStorage.clear();
                const L = {json.dumps(stor["local"])};
                for (const k in L) localStorage.setItem(k, L[k]);
                sessionStorage.clear();
                const S = {json.dumps(stor["session"])};
                for (const k in S) sessionStorage.setItem(k, S[k]);
            }} catch (e) {{}}
        }})(); """
    )
    hp.goto(libro.url, wait_until="networkidle")
    hp.emulate_media(media="print")
    hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)

    # Si la exportaciÃƒÂ³n headless terminÃƒÂ³ en el login, descartalo
    try:
        if out.exists() and _pdf_es_login_portal(out):
            logging.info("[PRINT:HEADLESS] Detectado login en PDF; se descarta.")
            out.unlink(missing_ok=True)
            return None
    except Exception:
        pass

    # 2) Fallback HEADLESS: mismo estado de sesiÃƒÂ³n + Page.pdf()
    try:
        state_file = tmp_dir / "state.json"
        context.storage_state(path=str(state_file))
        hbrowser = _launch_chromium(
            p.chromium,
            headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        hctx = hbrowser.new_context(
            storage_state=str(state_file), viewport={"width": 1366, "height": 900}
        )
        hp = hctx.new_page()
        hp.goto(libro.url, wait_until="networkidle")
        # Cargar/expandir como hicimos en la pestaÃƒÂ±a visible
        try:
            _expandir_y_cargar_todo_el_libro(hp)
        except Exception:
            pass
        try:
            _cerrar_indice_libro(hp)
        except Exception:
            pass
        try:
            hp.emulate_media(media="print")
        except Exception:
            pass
        hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
        try:
            hctx.close()
            hbrowser.close()
        except Exception:
            pass

        if out.exists() and out.stat().st_size > 1024 and not _pdf_es_login_portal(out):
            logging.info(f"[PRINT:HEADLESS] PDF libro guardado: {out.name}")
            return out
    except Exception as e:
        logging.info(f"[PRINT:HEADLESS-ERR] {e}")

    logging.info("[PRINT] No pude obtener el PDF del Libro ni por botÃƒÂ³n ni por fallback headless.")
    return None


def _guardar_libro_como_html(libro, tmp_dir: Path) -> Path | None:
    """
    Snapshot del 'Expediente como Libro' a un .html en disco, parecido a
    'Guardar comoÃ¯Â¿Â½?Ã¯Â¿Â½ / PÃƒÂ¡gina web completa'. Inyecta <base> (para recursos relativos vÃƒÂ­a /proxy/)
    y CSS de impresiÃƒÂ³n para ocultar el ÃƒÂ­ndice/menus.
    """
    try:
        S = _libro_scope(libro)
        _cerrar_indice_libro(libro)

        # HTML actual del frame donde vive el Libro
        html = S.content()

        # Prefijo del proxy de Teletrabajo y base del sitio
        proxy_prefix = _get_proxy_prefix(libro)
        base_href = proxy_prefix + "https://www.tribunales.gov.ar/"

        # CSS para vista de impresiÃƒÂ³n
        extra_css = """
            @page { size: A4; margin: 12mm; }
            html, body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
            #indice, .indice, .nav-container, .menuLateral, .navbar, .goup, .go-top, .scrollup, [onclick*='Imprimir'] {
                display: none !important;
            }
        """

        # Inyectar <base> + <style> al <head>
        try:
            html = re.sub(
                r"(?i)<head([^>]*)>",
                lambda m: f"<head{m.group(1)}><base href=\"{base_href}\"><style>{extra_css}</style>",
                html,
                count=1,
            )
            if "<base " not in html.lower():
                html = html.replace(
                    "<head>",
                    f"<head><base href=\"{base_href}\"><style>{extra_css}</style>",
                    1,
                )
        except Exception:
            html = f"<base href=\"{base_href}\"><style>{extra_css}</style>" + html

        out_html = tmp_dir / "libro_guardado.html"
        with open(out_html, "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"[SAVE HTML] {out_html.name}")
        return out_html
    except Exception as e:
        logging.info(f"[SAVE HTML:ERR] {e}")
        return None


def _convertir_html_a_pdf(html_path: Path, context, p, tmp_dir: Path) -> Path | None:
    """
    Abre el HTML guardado usando Chromium headless con el mismo storage_state
    (cookies del proxy/tribunales) y lo exporta como PDF.
    """
    try:
        out_pdf = tmp_dir / "libro_desde_html.pdf"

        # Guardamos el estado de sesiÃƒÂ³n del contexto actual
        state_file = tmp_dir / "state.json"
        context.storage_state(path=str(state_file))

        hbrowser = _launch_chromium(
            p.chromium,
            headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        hctx = hbrowser.new_context(
            storage_state=str(state_file), viewport={"width": 1366, "height": 900}
        )
        hp = hctx.new_page()

        # Cargar el archivo local; los recursos relativos se resuelven con el <base> inyectado
        hp.goto(f"file:///{html_path.as_posix()}", wait_until="domcontentloaded")
        try:
            hp.emulate_media(media="print")
        except Exception:
            pass
        hp.pdf(path=str(out_pdf), format="A4", print_background=True, prefer_css_page_size=True)
        try:
            hctx.close()
            hbrowser.close()
        except Exception:
            pass

        if out_pdf.exists() and out_pdf.stat().st_size > 1024:
            logging.info(f"[HTML->PDF] {out_pdf.name}")
            return out_pdf
    except Exception as e:
        logging.info(f"[HTML->PDF:ERR] {e}")
    return None


def _render_operacion_a_pdf_paginas(libro, op_id: str, context, p, tmp_dir: Path, hctx=None, hp=None) -> Path | None:
    cont = _buscar_contenedor_operacion(libro, op_id)
    if not cont:
        return None
    try:
        cont.wait_for(state="visible", timeout=6000)
    except Exception:
        return None

    outer = cont.evaluate("el => el.outerHTML") or ""
    if not outer:
        return None

    # Quitar 'page-break' del wrapper (lo mismo que hace ImprimirOperacion)
    outer = re.sub(
        r'(?i)(class\s*=\s*["\'])([^"\']*?)\bpage-break\b([^"\']*?)(["\'])',
        r'\1\2 \3\4',
        outer,
    )

    S = _libro_scope(libro)
    proxy_prefix = _get_proxy_prefix(libro)
    base_href = proxy_prefix + "https://www.tribunales.gov.ar/"

    try:
        head_html = S.evaluate(
            """() => {
                const head = document.head ? document.head.cloneNode(true) : document.createElement('head');
                head.querySelectorAll('script').forEach((node) => node.remove());
                return head.innerHTML || '';
            }"""
        ) or ""
    except Exception:
        head_html = ""
    try:
        body_class = S.evaluate(
            "() => document.body ? (document.body.getAttribute('class') || '') : ''"
        ) or ""
    except Exception:
        body_class = ""

    head_html = re.sub(r"(?is)<script\b[^>]*>.*?</script>", "", head_html or "")
    head_html = re.sub(r"(?is)<base\b[^>]*>", "", head_html or "")

    css = """
        @page { size: A4; margin: 10mm; }
        html, body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        #indice, .indice, .nav-container, .menuLateral, .navbar, .goup, .go-top, .scrollup {
            display: none !important;
        }
        .noprint { display: none !important; }
        .enable-print { display: block !important; }
        img, table.signature-block { page-break-inside: avoid; break-inside: avoid; }
        table { page-break-inside: avoid; break-inside: avoid-page; page-break-after: avoid; }
        #codex-op-print-root { margin: 0 !important; padding: 0 !important; }
    """
    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<base href="{base_href}">
{head_html}
<style>{css}</style>
</head>
<body class="{body_class}"><div id="codex-op-print-root">{outer}</div></body>
</html>"""

    state_file = tmp_dir / f"state_{op_id}.json"
    context.storage_state(path=str(state_file))
    out = tmp_dir / f"op_{op_id}.pdf"

    if hctx is None or hp is None:
        hbrowser = _launch_chromium(
            p.chromium,
            headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        try:
            hctx = hbrowser.new_context(
                storage_state=str(state_file), viewport={"width": 1366, "height": 900}
            )
            hp = hctx.new_page()
            hp.set_content(html, wait_until="domcontentloaded")
            try:
                hp.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            hp.wait_for_timeout(250)
            try:
                hp.emulate_media(media="print")
            except Exception:
                pass
            hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
        finally:
            try:
                hctx.close()
            except Exception:
                pass
            try:
                hbrowser.close()
            except Exception:
                pass
    else:
        try:
            hp.set_content(html, wait_until="domcontentloaded")
            try:
                hp.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            hp.wait_for_timeout(250)
            try:
                hp.emulate_media(media="print")
            except Exception:
                pass
            hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
        except Exception as e:
            logging.info(f"[HTML->PDF:REUSE-ERR] {e}")
            return None

    try:
        logging.info(f"[OP:REALCSS] {op_id} -> {out.name}")
    except Exception:
        pass
    return out if out.exists() and out.stat().st_size > 500 else None


def _render_caratula_a_pdf(libro, context, p, tmp_dir: Path, hctx=None, hp=None) -> Path | None:
    """
    Nueva forma: NO navega a ImprimirCaratula.aspx.
    Toma el HTML del bloque #caratula dentro del Libro, lo aÃƒÂ­sla en una pÃƒÂ¡gina en blanco
    con <base> al proxy y lo exporta a PDF en headless. AsÃƒÂ­ no aparece el ÃƒÂ­ndice ni overlays
    y se evita el proxy error.
    """
    S = _libro_scope(libro)

    # 1) Asegurar que la carÃƒÂ¡tula estÃƒÂ© poblada por el front-end del SAC
    try:
        S.evaluate("() => { try { if (window.Encabezado) Encabezado(); } catch(e) {} }")
    except Exception:
        pass

    # 2) Tomar el HTML del bloque de carÃƒÂ¡tula (outerHTML)
    html = None
    for sel in ("#caratula", "#encabezado", "div[id*='carat']"):
        try:
            loc = S.locator(sel).first
            if loc.count():
                html = loc.evaluate("el => el.outerHTML")
                if html:
                    break
        except Exception:
            continue
    if not html:
        return None

    # 3) Construir documento autÃƒÂ³nomo con base al proxy (para recursos relativos)
    base_href = _get_proxy_prefix(libro) + "https://www.tribunales.gov.ar/"
    css = """
        @page { size: A4; margin: 12mm; }
        html, body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        /* Sin sombras ni menÃƒÂºs; aseguramos ancho fluido */
        * { box-shadow: none !important; }
        body { width: auto !important; }
    """
    html_doc = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<base href='{base_href}'><style>{css}</style></head>"
        f"<body>{html}</body></html>"
    )
    # 4) Render headless a PDF usando el MISMO storage_state (cookies del proxy)
    out = tmp_dir / "caratula.pdf"
    state_file = tmp_dir / "state_caratula.json"
    context.storage_state(path=str(state_file))
    if hctx is None or hp is None:
        hbrowser = _launch_chromium(
            p.chromium,
            headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        try:
            hctx = hbrowser.new_context(
                storage_state=str(state_file), viewport={"width": 900, "height": 1200}
            )
            hp = hctx.new_page()
            hp.set_content(html_doc, wait_until="domcontentloaded")
            try:
                hp.emulate_media(media="print")
            except Exception:
                pass
            hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
        finally:
            try:
                hctx.close()
            except Exception:
                pass
            try:
                hbrowser.close()
            except Exception:
                pass
    else:
        try:
            hp.set_content(html_doc, wait_until="domcontentloaded")
            try:
                hp.emulate_media(media="print")
            except Exception:
                pass
            hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
        except Exception as e:
            logging.info(f"[CARATULA:REUSE-ERR] {e}")
            return None

    # 5) Limpieza opcional si hubiera pÃƒÂ¡gina en blanco
    if out.exists() and out.stat().st_size > 1024:
        try:
            return _pdf_sin_blancos(out)
        except Exception:
            return out
    return None


def _pdf_sin_blancos(pdf_path: Path, thresh: float = 0.995) -> Path:
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logging.info("[BLANK] PyMuPDF no disponible; omito limpieza de pÃƒÂ¡ginas en blanco.")
        return pdf_path

    doc = fitz.open(str(pdf_path))
    out = fitz.open()
    for i in range(doc.page_count):
        pg = doc[i]
        txt = (pg.get_text("text") or "").strip()
        imgs = pg.get_images(full=True)
        draws = pg.get_drawings()
        try:
            pm = pg.get_pixmap(dpi=36)
            sample = memoryview(pm.samples)[::8]
            white_count = sum(1 for b in sample if b == 255)
            ratio = white_count / len(sample) if len(sample) else 0.0
        except Exception:
            ratio = 0.0

        is_blank = (not txt) and (len(imgs) == 0) and (len(draws) == 0) and (ratio >= thresh)
        if not is_blank:
            out.insert_pdf(doc, from_page=i, to_page=i)

    if out.page_count == 0:
        doc.close()
        out.close()
        return pdf_path

    cleaned = pdf_path.with_suffix(".clean.pdf")
    out.save(str(cleaned), deflate=True, garbage=3)
    doc.close()
    out.close()
    try:
        pdf_path.unlink(missing_ok=True)
    except Exception:
        pass
    return cleaned

def _agregar_fojas(pdf_in: Path, start_after: int = 1, cada_dos: bool = True,
                   numero_inicial: int = 1, fijo: str | None = None) -> Path:
    """
    Estampa numeraciÃƒÂ³n de fojas (arriba-derecha) en el PDF:
      - start_after: pÃƒÂ¡ginas iniciales SIN numerar (1 = dejar carÃƒÂ¡tula sin nÃƒÂºmero)
      - cada_dos: si True, numera sÃƒÂ³lo una de cada dos pÃƒÂ¡ginas (recto)
      - numero_inicial: valor inicial (1 por defecto)
      - fijo: texto fijo (si querÃƒÂ©s que siempre diga p.ej. "1")
    """
    try:
        import fitz  # PyMuPDF (rÃƒÂ¡pido)
        import unicodedata

        doc = fitz.open(str(pdf_in))
        folio = numero_inicial
        for i in range(doc.page_count):
            pg = doc[i]
            # Evitar foliar pÃƒÂ¡ginas que correspondan al ÃƒÂ­ndice
            try:
                raw_text = pg.get_text("text") or ""
                text_norm = unicodedata.normalize("NFKD", raw_text)
                text_norm = text_norm.encode("ascii", "ignore").decode("ascii").lower()
                if "indice" in text_norm:
                    continue
            except Exception:
                pass
            if i <= (start_after - 1):
                continue
            if cada_dos and ((i - start_after) % 2 == 1):
                continue  # sÃƒÂ³lo una cara por hoja
            margen = 18
            # tamaÃƒÂ±o de letra proporcional (12Ã¯Â¿Â½?"18 pt)
            try:
                sz = max(12, min(18, pg.rect.height * 0.018))
            except Exception:
                sz = 14
            texto = fijo if fijo is not None else str(folio)
            # medir ancho para alinear a la derecha
            try:
                tw = fitz.get_text_length(texto, fontname="helv", fontsize=sz)
            except Exception:
                tw = sz * max(1, len(texto)) * 0.6
            x = max(margen, pg.rect.width - margen - tw)
            y = margen + sz  # baseline desde arriba
            pg.insert_text(fitz.Point(x, y), texto,
                           fontsize=sz, fontname="helv", color=(0, 0, 0))
            if fijo is None:
                folio += 1
        tmp = pdf_in.with_suffix(".fojas.pdf")
        doc.save(str(tmp), deflate=True, garbage=3)
        doc.close()
        shutil.move(tmp, pdf_in)
        return pdf_in
    except Exception:
        # Fallback con PyPDF2 + reportlab
        r = PdfReader(str(pdf_in))
        w = PdfWriter()
        folio = numero_inicial
        temps = []
        from reportlab.pdfbase import pdfmetrics
        import unicodedata
        for i, p in enumerate(r.pages):
            pw = float(p.mediabox.width)
            ph = float(p.mediabox.height)
            try:
                raw_text = p.extract_text() or ""
                text_norm = unicodedata.normalize("NFKD", raw_text)
                text_norm = text_norm.encode("ascii", "ignore").decode("ascii").lower()
                is_index = "indice" in text_norm
            except Exception:
                is_index = False
            if is_index:
                w.add_page(p)
                continue
            if i >= start_after and (not cada_dos or ((i - start_after) % 2 == 0)):
                tmp = Path(tempfile.mkstemp(suffix=".foja.pdf")[1])
                c = canvas.Canvas(str(tmp), pagesize=(pw, ph))
                sz = max(12, min(18, ph * 0.018))
                c.setFont("Helvetica-Bold", sz)
                texto = fijo if fijo is not None else str(folio)
                tw = pdfmetrics.stringWidth(texto, "Helvetica-Bold", sz)
                x = max(18, pw - 18 - tw)
                y = ph - 18 - sz  # desde abajo, para Ã¯Â¿Â½?oarribaÃ¯Â¿Â½?Ã¯Â¿Â½
                c.drawString(x, y, texto)
                c.save()
                overlay = PdfReader(str(tmp)).pages[0]
                p.merge_page(overlay)
                temps.append(tmp)
                if fijo is None:
                    folio += 1
            w.add_page(p)
        tmpout = pdf_in.with_suffix(".fojas.pdf")
        with open(tmpout, "wb") as f:
            w.write(f)
        for t in temps:
            Path(t).unlink(missing_ok=True)
        shutil.move(tmpout, pdf_in)
        return pdf_in


def _agregar_numeracion_paginas(pdf_in: Path, numero_inicial: int = 1) -> Path:
    try:
        import fitz

        doc = fitz.open(str(pdf_in))
        for i in range(doc.page_count):
            pg = doc[i]
            try:
                sz = max(10, min(14, pg.rect.height * 0.015))
            except Exception:
                sz = 12
            texto = str(numero_inicial + i)
            try:
                tw = fitz.get_text_length(texto, fontname="helv", fontsize=sz)
            except Exception:
                tw = sz * max(1, len(texto)) * 0.6
            margen = 18
            x = max(margen, pg.rect.width - margen - tw)
            y = max(sz + margen, pg.rect.height - margen)
            pg.insert_text(
                fitz.Point(x, y),
                texto,
                fontsize=sz,
                fontname="helv",
                color=(0, 0, 0),
            )
        tmp = pdf_in.with_suffix(".paginas.pdf")
        doc.save(str(tmp), deflate=True, garbage=3)
        doc.close()
        shutil.move(tmp, pdf_in)
        return pdf_in
    except Exception:
        r = PdfReader(str(pdf_in))
        w = PdfWriter()
        temps = []
        from reportlab.pdfbase import pdfmetrics
        for i, p in enumerate(r.pages):
            pw = float(p.mediabox.width)
            ph = float(p.mediabox.height)
            tmp = Path(tempfile.mkstemp(suffix=".pagina.pdf")[1])
            c = canvas.Canvas(str(tmp), pagesize=(pw, ph))
            sz = max(10, min(14, ph * 0.015))
            c.setFont("Helvetica-Bold", sz)
            texto = str(numero_inicial + i)
            tw = pdfmetrics.stringWidth(texto, "Helvetica-Bold", sz)
            x = max(18, pw - 18 - tw)
            y = 18
            c.drawString(x, y, texto)
            c.save()
            overlay = PdfReader(str(tmp)).pages[0]
            p.merge_page(overlay)
            temps.append(tmp)
            w.add_page(p)
        tmpout = pdf_in.with_suffix(".paginas.pdf")
        with open(tmpout, "wb") as f:
            w.write(f)
        for t in temps:
            Path(t).unlink(missing_ok=True)
        shutil.move(tmpout, pdf_in)
        return pdf_in


# ----------------------- DESCARGA PRINCIPAL ----------------------------
def _env_true(name: str, default="0"):
    return os.getenv(name, default).lower() in ("1", "true", "t", "yes", "y", "si")


def _indice_prefix_for_item(item: dict) -> str:
    kind = _norm_ws((item or {}).get("kind") or "").lower()
    if kind == "adjunto":
        return "ADJUNTO"
    if kind == "informe_mpf":
        return "INFORME TECNICO MPF"
    if kind == "informe_rnr":
        return "INFORME RNR"
    return "OPERACION"


def _indice_nombre_for_item(item: dict) -> str:
    raw = _norm_ws((item or {}).get("index_name") or "")
    if raw:
        return raw
    return _norm_ws((item or {}).get("titulo") or "") or _norm_ws((item or {}).get("kind_label") or "") or "Documento"


def _indice_toc_title_for_item(item: dict) -> str:
    prefix = _indice_prefix_for_item(item)
    name = _indice_nombre_for_item(item)
    return f"{prefix} - {name}" if name else prefix


def _armar_items_radiografia(
    ops: list[dict],
    op_fecha_map: dict[str, str],
    adj_items: list[dict],
    informes_tecnicos: list[dict],
    informes_rnr: list[dict],
) -> list[dict]:
    from collections import defaultdict

    adj_por_op = defaultdict(list)
    adj_sin_op: list[dict] = []

    for raw in adj_items:
        item = dict(raw)
        op_id = item.get("op_id")
        if op_id and op_id in {op["id"] for op in ops}:
            adj_por_op[op_id].append(item)
        else:
            adj_sin_op.append(item)

    items: list[dict] = []
    for op in ops:
        op_id = op["id"]
        items.append(
            {
                "uid": f"op:{op_id}",
                "kind": "operacion",
                "kind_label": "Operacion",
                "fecha": op_fecha_map.get(op_id, ""),
                "titulo": (op.get("titulo") or "").strip() or f"Operacion {op_id}",
                "detalle": _norm_ws(op.get("tipo") or ""),
                "op_id": op_id,
                "op_tipo": op.get("tipo") or "",
            }
        )
        items.extend(adj_por_op.get(op_id, []))

    items.extend(dict(item) for item in informes_tecnicos)
    items.extend(dict(item) for item in informes_rnr)
    items.extend(adj_sin_op)

    for idx, item in enumerate(items):
        item["default_order"] = idx
        item.setdefault("selected", True)
        item.setdefault("index_name", _norm_ws(item.get("titulo") or ""))

    return items


def _cargar_timeline_descarga_completa(
    sac,
    libro,
    temp_dir: Path,
    ops: list[dict],
    op_fecha_map: dict[str, str],
    incluir_adjuntos: bool,
    stamp: bool,
    context,
    p,
    hctx,
    hp,
    push_pdf,
    mf,
):
    if incluir_adjuntos:
        etapa("Descargando adjuntos desde Radiografia")
        try:
            sac.bring_to_front()
        except Exception:
            pass
        pdfs_grid = _descargar_adjuntos_grid_mapeado(sac, temp_dir)
        logging.info(f"[ADJ/GRID] Mapeo adjuntos por operación: { {k: len(v) for k, v in pdfs_grid.items()} }")
    else:
        etapa("Adjuntos omitidos por configuración")
        pdfs_grid = {}
        logging.info("[ADJ] Omitidos por opción de usuario (sin adjuntos).")

    def _agregar_adjuntos_de_op(op_id: str, titulo: str, fecha_op: str | None):
        if not incluir_adjuntos:
            return
        pdfs_op: list[Path] = []
        try:
            pdfs_op.extend(_descargar_adjuntos_de_operacion(libro, op_id, temp_dir))
        except Exception:
            pass
        pdfs_op.extend(pdfs_grid.get(op_id, []))
        for ap in pdfs_op:
            pth = (
                ap
                if ap.suffix.lower() == ".pdf"
                else (_ensure_pdf_fast(ap) if "_ensure_pdf_fast" in globals() else _ensure_pdf(ap))
            )
            if not pth or not pth.exists() or pth.suffix.lower() != ".pdf":
                continue
            mf(f"ADJUNTO · {titulo} · {pth.name}")
            hdr = (f"ADJUNTO - {titulo}") if stamp else None
            push_pdf(pth, hdr, fecha=fecha_op, toc_title=f"ADJUNTO - {titulo}")

    op_pdfs_capturados = 0
    etapa("Capturando operaciones visibles del Libro")
    for o in ops:
        op_id = o["id"]
        op_tipo = o["tipo"]
        titulo = (o.get("titulo") or "").strip() or f"Operación {op_id}"
        fecha_op = op_fecha_map.get(op_id, None)
        logging.info(
            f"[OP] Procesando operación · id={op_id} · tipo='{op_tipo}' · titulo='{titulo}' · fecha='{fecha_op or '-'}'"
        )

        _mostrar_operacion(libro, op_id, op_tipo)
        cont = _esperar_contenedor_operacion(libro, op_id, timeout_ms=4500)
        if not cont:
            logging.info(f"[OP] {op_id}: contenedor no encontrado; se continúa con adjuntos.")
            _agregar_adjuntos_de_op(op_id, titulo, fecha_op)
            continue

        try:
            pdf_op = _render_operacion_a_pdf_paginas(libro, op_id, context, p, temp_dir, hctx=hctx, hp=hp)
        except Exception as e:
            logging.info(f"[OP:ERR] {op_id}: {e}")
            pdf_op = None

        if pdf_op and pdf_op.exists():
            mf(f"OPERACION · {titulo} · {pdf_op.name}")
            push_pdf(pdf_op, None, fecha=fecha_op, toc_title=f"OPERACION - {titulo}")
            op_pdfs_capturados += 1
            logging.info(f"[OP] {op_id}: agregado (renderer de páginas)")
        else:
            logging.info(f"[OP] {op_id}: no se pudo renderizar (se continúa con adjuntos).")

        _agregar_adjuntos_de_op(op_id, titulo, fecha_op)

    etapa("Descargando informes técnicos MPF")
    try:
        sac.bring_to_front()
    except Exception:
        pass
    informes_tecnicos = _descargar_informes_tecnicos(sac, temp_dir)
    logging.info(f"[INF] Informes técnicos descargados: {len(informes_tecnicos)}")
    for it_path, it_fecha in informes_tecnicos:
        pth = (
            it_path
            if it_path.suffix.lower() == ".pdf"
            else (_ensure_pdf_fast(it_path) if "_ensure_pdf_fast" in globals() else _ensure_pdf(it_path))
        )
        if not pth or not pth.exists() or pth.suffix.lower() != ".pdf":
            continue
        mf(f"INF_TEC · {it_fecha} · {pth.name}")
        hdr = (f"INFORME TECNICO · {it_fecha}") if stamp else None
        toc_it = f"INFORME TECNICO MPF - {it_fecha}" if it_fecha else "INFORME TECNICO MPF"
        push_pdf(pth, hdr, fecha=it_fecha, toc_title=toc_it)

        if incluir_adjuntos:
            try:
                anexos = _extraer_adjuntos_embebidos(pth, temp_dir) if "_extraer_adjuntos_embebidos" in globals() else []
            except Exception:
                anexos = []
            for an in anexos:
                an = Path(an)
                an_pdf = (
                    an if an.suffix.lower() == ".pdf"
                    else (_ensure_pdf_fast(an) if "_ensure_pdf_fast" in globals() else _ensure_pdf(an))
                )
                if not an_pdf or not Path(an_pdf).exists() or Path(an_pdf).suffix.lower() != ".pdf":
                    continue
                mf(f"INF_TEC/ANEXO · {it_fecha} · {Path(an_pdf).name}")
                hdr_an = (f"INFORME TECNICO - ANEXO - {it_fecha}") if stamp else None
                toc_an = f"INFORME TECNICO MPF - ANEXO - {it_fecha}" if it_fecha else "INFORME TECNICO MPF - ANEXO"
                push_pdf(Path(an_pdf), hdr_an, fecha=it_fecha, toc_title=toc_an)

    if op_pdfs_capturados == 0:
        logging.info("[FALLBACK] Ninguna operación pudo renderizarse; intento PDF del Libro.")
        libro_pdf = _imprimir_libro_a_pdf(libro, context, temp_dir, p)
        if not (libro_pdf and libro_pdf.exists() and libro_pdf.stat().st_size > 1024):
            html_snap = _guardar_libro_como_html(libro, temp_dir)
            if html_snap and html_snap.exists():
                libro_pdf = _convertir_html_a_pdf(html_snap, context, p, temp_dir)
        if libro_pdf and libro_pdf.exists() and libro_pdf.stat().st_size > 1024:
            try:
                libro_pdf = _pdf_sin_blancos(libro_pdf)
            except Exception:
                pass
            mf(f"LIBRO · {libro_pdf.name}")
            push_pdf(libro_pdf, None, fecha=None, toc_title="LIBRO")
        else:
            logging.info("[FALLBACK] No se pudo obtener PDF del Libro por ningún método.")

    etapa("Descargando informes RNR")
    try:
        sac.bring_to_front()
    except Exception:
        pass
    try:
        informes_rnr = _descargar_informes_reincidencia(sac, temp_dir)
    except Exception as e:
        logging.info(f"[RNR] Error en descarga de informes RNR: {e}")
        logging.exception("[RNR] Traceback de descarga RNR")
        informes_rnr = []
    logging.info(f"[RNR] Informes RNR descargados: {len(informes_rnr)}")
    for rnr_path, rnr_fecha in informes_rnr:
        pth = (
            rnr_path
            if rnr_path.suffix.lower() == ".pdf"
            else (_ensure_pdf_fast(rnr_path) if "_ensure_pdf_fast" in globals() else _ensure_pdf(rnr_path))
        )
        if not pth or not pth.exists() or pth.suffix.lower() != ".pdf":
            continue
        mf(f"RNR - {rnr_fecha or '-'} - {pth.name}")
        hdr = (f"INFORME RNR - {rnr_fecha}") if stamp and rnr_fecha else ("INFORME RNR" if stamp else None)
        push_pdf(pth, hdr, fecha=(rnr_fecha or None), toc_title=("INFORME RNR - " + (rnr_fecha or "")))
        logging.info(f"[MERGE] RNR - {pth.name} (fecha {rnr_fecha or '-'})")

    if incluir_adjuntos:
        adj_sin = pdfs_grid.get("__SIN_OP__", [])
        if adj_sin:
            logging.info(f"[ADJ] SIN_OP · {len(adj_sin)} archivo(s)")
            for pdf in adj_sin:
                pth = (
                    pdf
                    if pdf.suffix.lower() == ".pdf"
                    else (_ensure_pdf_fast(pdf) if "_ensure_pdf_fast" in globals() else _ensure_pdf(pdf))
                )
                if not pth or not pth.exists() or pth.suffix.lower() != ".pdf":
                    continue
                mf(f"ADJUNTO · (sin operación) · {pth.name}")
                hdr = ("ADJUNTO · (sin operación)") if stamp else None
                push_pdf(pth, hdr, fecha=None, toc_title=hdr)

def _preview_uid_slug(uid: str) -> str:
    try:
        raw = str(uid or "").strip()
    except Exception:
        raw = "item"
    raw = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._")
    return raw or "item"


def _preview_size_text(size_bytes: int) -> str:
    try:
        size = float(max(0, int(size_bytes)))
    except Exception:
        return "-"
    units = ["B", "KB", "MB", "GB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.1f} {units[idx]}" if idx else f"{int(size)} {units[idx]}"


def _render_pdf_preview_png(pdf_path: Path, png_path: Path) -> tuple[Path | None, int]:
    doc = None
    try:
        import fitz  # PyMuPDF

        png_path.parent.mkdir(parents=True, exist_ok=True)
        doc = fitz.open(str(pdf_path))
        if doc.page_count <= 0:
            return None, 0
        page = doc.load_page(0)
        rect = page.rect
        max_dim = max(float(rect.width or 1), float(rect.height or 1))
        zoom = 2.2 if max_dim < 900 else 1.8
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
        pix.save(str(png_path))
        return (png_path if png_path.exists() else None), int(doc.page_count)
    except Exception as e:
        try:
            logging.info(f"[RADIOPREVIEW] No se pudo rasterizar {pdf_path.name}: {e}")
        except Exception:
            pass
        return None, 0
    finally:
        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass


def _generar_preview_real_radiografia(
    item: dict,
    sac,
    libro,
    preview_dir: Path,
    ops_by_id: dict[str, dict],
    op_fecha_map: dict[str, str],
    op_title_map: dict[str, str],
    context,
    p,
    hctx=None,
    hp=None,
) -> dict[str, object]:
    uid = str(item.get("uid") or "")
    kind = str(item.get("kind") or "")
    preview_dir.mkdir(parents=True, exist_ok=True)

    def _resp_error(msg: str) -> dict[str, object]:
        return {
            "uid": uid,
            "kind": kind,
            "ok": False,
            "message": msg,
        }

    try:
        pdf_path = None
        if kind == "operacion":
            op_id = item.get("op_id")
            op = ops_by_id.get(op_id)
            if not op_id or not op:
                return _resp_error("No encontré la operación en el Libro para armar la vista previa.")
            try:
                _mostrar_operacion(libro, op_id, op.get("tipo") or item.get("op_tipo") or "")
                cont = _esperar_contenedor_operacion(libro, op_id, timeout_ms=4500)
            except Exception as e:
                try:
                    logging.info(f"[RADIOPREVIEW] Operación {op_id}: error preparando preview: {e}")
                except Exception:
                    pass
                cont = None
            if not cont:
                return _resp_error("No pude abrir la operación para generar su vista previa real.")
            pdf_path = _render_operacion_a_pdf_paginas(libro, op_id, context, p, preview_dir, hctx=hctx, hp=hp)
        else:
            return _resp_error("La vista previa real sólo está disponible para operaciones.")

        if not pdf_path or not Path(pdf_path).exists():
            return _resp_error("No se pudo generar el archivo temporal para esta vista previa.")

        pdf_path = Path(pdf_path)
        if pdf_path.suffix.lower() != ".pdf" or not _is_real_pdf(pdf_path):
            return _resp_error("El archivo temporal generado no es un PDF válido para mostrar.")

        png_name = f"{_preview_uid_slug(uid)}.png"
        image_path, page_count = _render_pdf_preview_png(pdf_path, preview_dir / png_name)
        size_bytes = int(pdf_path.stat().st_size) if pdf_path.exists() else 0

        return {
            "uid": uid,
            "kind": kind,
            "ok": True,
            "pdf_path": str(pdf_path),
            "image_path": str(image_path) if image_path else None,
            "page_count": int(page_count or 1),
            "file_name": pdf_path.name,
            "size_bytes": size_bytes,
            "size_text": _preview_size_text(size_bytes),
            "message": "Vista previa real generada desde un archivo temporal de la sesión actual.",
        }
    except Exception as e:
        try:
            logging.info(f"[RADIOPREVIEW] Error generando preview real para {uid}: {e}")
        except Exception:
            pass
        return _resp_error(f"No pude generar la vista previa real: {e}")


def _crear_contexto_headless_reutilizable(context, p, temp_dir: Path, chromium_args: list[str]):
    hbrowser = hctx = hp = None
    try:
        state_print = temp_dir / "state_print.json"
        try:
            context.storage_state(path=str(state_print))
        except Exception:
            pass
        hbrowser = _launch_chromium(p.chromium, headless=True, args=chromium_args)
        hctx = hbrowser.new_context(
            storage_state=str(state_print),
            viewport={"width": 900, "height": 1200},
        )
        hp = hctx.new_page()
        try:
            hp.emulate_media(media="print")
        except Exception:
            pass
        return hbrowser, hctx, hp
    except Exception:
        try:
            if hp:
                hp.close()
        except Exception:
            pass
        try:
            if hctx:
                hctx.close()
        except Exception:
            pass
        try:
            if hbrowser:
                hbrowser.close()
        except Exception:
            pass
        return None, None, None


def _cargar_timeline_radiografia_custom(
    sac,
    libro,
    temp_dir: Path,
    ops: list[dict],
    plan_items: list[dict],
    op_fecha_map: dict[str, str],
    incluir_adjuntos: bool,
    stamp: bool,
    context,
    p,
    hctx,
    hp,
    push_pdf,
    mf,
):
    seleccionados_por_tipo = {
        "operacion": sum(1 for item in plan_items if item.get("kind") == "operacion"),
        "adjunto": sum(1 for item in plan_items if item.get("kind") == "adjunto"),
        "informe_mpf": sum(1 for item in plan_items if item.get("kind") == "informe_mpf"),
        "informe_rnr": sum(1 for item in plan_items if item.get("kind") == "informe_rnr"),
    }
    ops_by_id = {op["id"]: op for op in ops}
    op_title_map = {
        op["id"]: ((op.get("titulo") or "").strip() or f"Operación {op['id']}")
        for op in ops
    }

    def _rehidratar_libro() -> bool:
        nonlocal ops_by_id, op_title_map
        try:
            refreshed_ops = _expandir_y_cargar_todo_el_libro(libro)
        except Exception as e:
            logging.info(f"[RADIOPLAN] No pude recargar el Libro antes de capturar operaciones: {e}")
            return False
        if not refreshed_ops:
            logging.info("[RADIOPLAN] Rehidratación del Libro sin operaciones visibles")
            return False
        ops_by_id = {op["id"]: op for op in refreshed_ops}
        op_title_map = {
            op["id"]: ((op.get("titulo") or "").strip() or f"Operación {op['id']}")
            for op in refreshed_ops
        }
        logging.info(f"[RADIOPLAN] Libro rehidratado para captura selectiva: ops_visibles={len(refreshed_ops)}")
        return True

    adj_uids = {item["uid"] for item in plan_items if item.get("kind") == "adjunto"}
    mpf_uids = {item["uid"] for item in plan_items if item.get("kind") == "informe_mpf"}
    rnr_uids = {item["uid"] for item in plan_items if item.get("kind") == "informe_rnr"}

    adj_descargados: dict[str, dict[str, object]] = {}
    mpf_descargados: dict[str, dict[str, object]] = {}
    rnr_descargados: dict[str, dict[str, object]] = {}
    agregados_por_tipo = {"operacion": 0, "adjunto": 0, "informe_mpf": 0, "informe_rnr": 0}
    agregados_uids = {"operacion": set(), "adjunto": set(), "informe_mpf": set(), "informe_rnr": set()}

    def _merge_descargas(dst: dict[str, dict[str, object]], extra: dict[str, dict[str, object]] | None):
        if not extra:
            return
        for k, v in extra.items():
            if v:
                dst[k] = v

    def _missing_uids(expected: set[str], actual_map: dict[str, dict[str, object]]) -> list[str]:
        return sorted(set(expected) - set(actual_map.keys()))

    def _retry_descargas(tipo_label: str, expected: set[str], current: dict[str, dict[str, object]], fn):
        faltan = _missing_uids(expected, current)
        if not faltan:
            return current
        try:
            logging.info(f"[RADIOPLAN] {tipo_label}: faltan {len(faltan)} item(s); reintento selectivo")
        except Exception:
            pass
        try:
            sac.bring_to_front()
        except Exception:
            pass
        try:
            extra = fn(set(faltan))
        except Exception as e:
            logging.info(f"[RADIOPLAN] {tipo_label}: reintento fallido: {e}")
            extra = {}
        _merge_descargas(current, extra)
        faltan = _missing_uids(expected, current)
        if faltan:
            try:
                logging.info(f"[RADIOPLAN] {tipo_label}: siguen faltando {len(faltan)} item(s) tras reintento")
            except Exception:
                pass
        return current

    if incluir_adjuntos and adj_uids:
        etapa("Descargando adjuntos seleccionados desde Radiografia")
        try:
            sac.bring_to_front()
        except Exception:
            pass
        adj_descargados = _descargar_adjuntos_grid_mapeado(
            sac,
            temp_dir,
            selected_uids=adj_uids,
            return_items=True,
            op_fecha_map=op_fecha_map,
            op_title_map=op_title_map,
        )
        adj_descargados = _retry_descargas(
            "Adjuntos",
            adj_uids,
            adj_descargados,
            lambda missing: _descargar_adjuntos_grid_mapeado(
                sac,
                temp_dir,
                selected_uids=missing,
                return_items=True,
                op_fecha_map=op_fecha_map,
                op_title_map=op_title_map,
            ),
        )

    if incluir_adjuntos and mpf_uids:
        etapa("Descargando informes técnicos MPF seleccionados")
        try:
            sac.bring_to_front()
        except Exception:
            pass
        mpf_descargados = _descargar_informes_tecnicos(
            sac,
            temp_dir,
            selected_uids=mpf_uids,
            return_items=True,
        )
        mpf_descargados = _retry_descargas(
            "Informes MPF",
            mpf_uids,
            mpf_descargados,
            lambda missing: _descargar_informes_tecnicos(
                sac,
                temp_dir,
                selected_uids=missing,
                return_items=True,
            ),
        )

    if incluir_adjuntos and rnr_uids:
        etapa("Descargando informes RNR seleccionados")
        try:
            sac.bring_to_front()
        except Exception:
            pass
        rnr_descargados = _descargar_informes_reincidencia(
            sac,
            temp_dir,
            selected_uids=rnr_uids,
            return_items=True,
        )
        rnr_descargados = _retry_descargas(
            "Informes RNR",
            rnr_uids,
            rnr_descargados,
            lambda missing: _descargar_informes_reincidencia(
                sac,
                temp_dir,
                selected_uids=missing,
                return_items=True,
            ),
        )

    orden_idx = 0

    def _push_ordenado(pth: Path, hdr: str | None, toc_title: str | None):
        nonlocal orden_idx
        fecha_orden = (datetime.date(1900, 1, 1) + datetime.timedelta(days=orden_idx)).strftime("%d/%m/%Y")
        if push_pdf(pth, hdr, fecha=fecha_orden, toc_title=toc_title):
            orden_idx += 1
            return True
        return False

    if seleccionados_por_tipo["operacion"] > 0:
        etapa("Reacomodando el Libro para capturar operaciones seleccionadas")
        _rehidratar_libro()

    etapa("Capturando contenido seleccionado en Radiografia del expediente")
    for item in plan_items:
        kind = item.get("kind")
        titulo = (item.get("titulo") or "").strip()
        if kind == "operacion":
            op_id = item.get("op_id")
            op = ops_by_id.get(op_id)
            if not op:
                logging.info(f"[RADIOPLAN] Operación no encontrada en Libro: {op_id}")
                continue
            op_tipo = op.get("tipo") or item.get("op_tipo") or ""
            titulo = titulo or op_title_map.get(op_id, f"Operación {op_id}")
            try:
                _mostrar_operacion(libro, op_id, op_tipo)
                cont = _esperar_contenedor_operacion(libro, op_id, timeout_ms=4500)
            except Exception as e:
                logging.info(f"[RADIOPLAN] Operación {op_id}: error mostrando contenedor: {e}")
                cont = None
            if not cont:
                try:
                    logging.info(f"[RADIOPLAN] Operación {op_id}: reintentando tras rehidratar Libro")
                    if _rehidratar_libro():
                        op = ops_by_id.get(op_id, op)
                        op_tipo = op.get("tipo") or item.get("op_tipo") or op_tipo
                        _mostrar_operacion(libro, op_id, op_tipo)
                        cont = _esperar_contenedor_operacion(libro, op_id, timeout_ms=6000)
                except Exception as e:
                    logging.info(f"[RADIOPLAN] Operación {op_id}: reintento fallido: {e}")
            if not cont:
                logging.info(f"[RADIOPLAN] Operación {op_id}: contenedor no encontrado")
                continue
            try:
                pdf_op = _render_operacion_a_pdf_paginas(libro, op_id, context, p, temp_dir, hctx=hctx, hp=hp)
            except Exception as e:
                logging.info(f"[RADIOPLAN] Operación {op_id}: error renderizando: {e}")
                pdf_op = None
            if not (pdf_op and pdf_op.exists()):
                try:
                    logging.info(f"[RADIOPLAN] Operación {op_id}: reintento final de render")
                    if _rehidratar_libro():
                        op = ops_by_id.get(op_id, op)
                        op_tipo = op.get("tipo") or item.get("op_tipo") or op_tipo
                        _mostrar_operacion(libro, op_id, op_tipo)
                        cont = _esperar_contenedor_operacion(libro, op_id, timeout_ms=6500)
                        if cont:
                            pdf_op = _render_operacion_a_pdf_paginas(libro, op_id, context, p, temp_dir, hctx=hctx, hp=hp)
                except Exception as e:
                    logging.info(f"[RADIOPLAN] Operación {op_id}: reintento final fallido: {e}")
            if pdf_op and pdf_op.exists():
                mf(f"OPERACION · {titulo} · {pdf_op.name}")
                if _push_ordenado(pdf_op, None, _indice_toc_title_for_item(item)):
                    agregados_por_tipo["operacion"] += 1
                    agregados_uids["operacion"].add(str(item.get("uid") or ""))
            continue

        if kind == "adjunto":
            meta = adj_descargados.get(item["uid"])
            if not meta:
                logging.info(f"[RADIOPLAN] Adjunto no descargado o no disponible: {item['uid']}")
                continue
            pth = Path(meta["path"])
            mf(f"ADJUNTO · {titulo} · {pth.name}")
            hdr = (f"ADJUNTO - {titulo}") if stamp else None
            if _push_ordenado(pth, hdr, _indice_toc_title_for_item(item)):
                agregados_por_tipo["adjunto"] += 1
                agregados_uids["adjunto"].add(str(item.get("uid") or ""))
            continue

        if kind == "informe_mpf":
            meta = mpf_descargados.get(item["uid"])
            if not meta:
                logging.info(f"[RADIOPLAN] Informe MPF no descargado o no disponible: {item['uid']}")
                continue
            pth = Path(meta["path"])
            mf(f"INF_TEC · {titulo} · {pth.name}")
            hdr = (f"INFORME TECNICO MPF - {titulo}") if stamp else None
            if _push_ordenado(pth, hdr, _indice_toc_title_for_item(item)):
                agregados_por_tipo["informe_mpf"] += 1
                agregados_uids["informe_mpf"].add(str(item.get("uid") or ""))
            continue

        if kind == "informe_rnr":
            meta = rnr_descargados.get(item["uid"])
            if not meta:
                logging.info(f"[RADIOPLAN] Informe RNR no descargado o no disponible: {item['uid']}")
                continue
            pth = Path(meta["path"])
            mf(f"RNR - {titulo} - {pth.name}")
            hdr = (f"INFORME RNR - {titulo}") if stamp else None
            if _push_ordenado(pth, hdr, _indice_toc_title_for_item(item)):
                agregados_por_tipo["informe_rnr"] += 1
                agregados_uids["informe_rnr"].add(str(item.get("uid") or ""))

    try:
        logging.info(
            "[RADIOPLAN] Resumen seleccion/agregado: "
            f"ops={agregados_por_tipo['operacion']}/{seleccionados_por_tipo['operacion']} · "
            f"adj={agregados_por_tipo['adjunto']}/{seleccionados_por_tipo['adjunto']} · "
            f"mpf={agregados_por_tipo['informe_mpf']}/{seleccionados_por_tipo['informe_mpf']} · "
            f"rnr={agregados_por_tipo['informe_rnr']}/{seleccionados_por_tipo['informe_rnr']} · "
            f"bloques={orden_idx}"
        )
    except Exception:
        pass

    faltantes_items: list[dict] = []
    for item in plan_items:
        kind = str(item.get("kind") or "")
        uid = str(item.get("uid") or "")
        if kind in agregados_uids and uid and uid not in agregados_uids[kind]:
            faltantes_items.append(item)
    if faltantes_items:
        resumen = [
            _norm_ws(f"{item.get('kind_label') or item.get('kind')} · {item.get('titulo') or item.get('uid')}")
            for item in faltantes_items[:8]
        ]
        if len(faltantes_items) > 8:
            resumen.append(f"... y {len(faltantes_items) - 8} item(s) más")
        detalle = " | ".join(resumen)
        try:
            logging.info(f"[RADIOPLAN] FALTANTES: {detalle}")
        except Exception:
            pass
        raise RuntimeError(
            "No pude descargar o capturar todos los elementos seleccionados en la radiografía. "
            f"Faltantes: {detalle}"
        )


# ----------------------- DESCARGA PRINCIPAL ----------------------------
def descargar_expediente(
    tele_user,
    tele_pass,
    intra_user,
    intra_pass,
    nro_exp,
    carpeta_salida,
    incluir_adjuntos: bool = True,
    aplicar_ocr: bool = True,
    radiografia_selector=None,
):
    SHOW_BROWSER = _env_true("SHOW_BROWSER", "0")
    CHROMIUM_ARGS = ["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
    KEEP_WORK = _env_true("KEEP_WORK", "0")
    STAMP = _env_true("STAMP_HEADERS", "1")
    INCLUIR_ADJUNTOS = bool(incluir_adjuntos)
    APLICAR_OCR = bool(aplicar_ocr)

    work_dir = Path(carpeta_salida) / f"Exp_{nro_exp}_work"
    temp_ctx = TemporaryDirectory() if not KEEP_WORK else contextlib.nullcontext(work_dir)
    with temp_ctx as tmp_name:
        temp_dir = Path(tmp_name)
        if KEEP_WORK:
            temp_dir.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("TMP", str(temp_dir))
        os.environ.setdefault("TEMP", str(temp_dir))

        def _mf(line: str):
            logging.info(line)

        logging.info(f"[CONFIG] Adjuntos={'si' if INCLUIR_ADJUNTOS else 'no'} | OCR={'si' if APLICAR_OCR else 'no'}")
        etapa("Preparando entorno local y sesion de descarga")

        with sync_playwright() as p:
            etapa("Iniciando navegador automatizado")
            browser = _launch_chromium(
                p.chromium,
                headless=not SHOW_BROWSER,
                args=CHROMIUM_ARGS,
                slow_mo=0,
            )
            logging.info("[NAV] Chromium lanzado")

            if SHOW_BROWSER:
                context = browser.new_context(
                    accept_downloads=True,
                    viewport={"width": 1366, "height": 900},
                )
                logging.info("[NAV] Contexto de navegador creado")
            else:
                # Evitar grabar video por defecto: impacta mucho en performance.
                # Si necesitÃƒÂ¡s video, exportÃƒÂ¡ RECORD_VIDEO=1
                if _env_true("RECORD_VIDEO", "0"):
                    vid_dir = temp_dir / "video"
                    vid_dir.mkdir(parents=True, exist_ok=True)
                    context = browser.new_context(
                        accept_downloads=True,
                        viewport={"width": 1366, "height": 900},
                        record_video_dir=str(vid_dir),
                    )
                else:
                    context = browser.new_context(
                        accept_downloads=True,
                        viewport={"width": 1366, "height": 900},
                    )

            try:
                etapa("Ingresando a Teletrabajo/Intranet y abriendo SAC")
                # 1) Login -> Radiografia
                sac = abrir_sac(context, tele_user, tele_pass, intra_user, intra_pass)
                logging.info(f"[SAC] Abierto SAC / Radiografia: url={sac.url}")

                if _page_requires_portal_login(sac):
                    logging.info("[OPEN] El SAC devolvio login antes de buscar el expediente; reautenticando.")
                    _login_intranet(sac, intra_user, intra_pass)
                    sac = _ir_a_radiografia(sac)

                if _page_requires_portal_login(sac):
                    messagebox.showerror("Error de sesion", "El SAC pidio re-login y no pude recuperar la sesion. Proba nuevamente.")
                    return

                # 2) Buscar expediente
                buscado_ok = False
                for intento_busqueda in range(2):
                    etapa(f"Buscando expediente Nro {nro_exp} en Radiografia")
                    try:
                        _fill_radiografia_y_buscar(sac, nro_exp)
                        logging.info(f"[RADIO] Buscado expediente Nro {nro_exp}")
                        buscado_ok = True
                        break
                    except RuntimeError as e:
                        motivo = str(e or "")
                        if motivo == "PORTAL_LOGIN_REQUIRED":
                            logging.info("[RADIO] El SAC redirigio a login al buscar; reintento una vez.")
                            _login_intranet(sac, intra_user, intra_pass)
                            sac = _ir_a_radiografia(sac)
                            if _page_requires_portal_login(sac):
                                messagebox.showerror("Error de sesion", "El SAC volvio a pedir login al buscar el expediente. Proba nuevamente.")
                                return
                            continue
                        if motivo == "RADIO_PAGE_CLOSED" and intento_busqueda == 0:
                            logging.info("[RADIO] La pestaña de Radiografia se cerro sola; reabro SAC y reintento.")
                            sac = abrir_sac(context, tele_user, tele_pass, intra_user, intra_pass)
                            logging.info(f"[SAC] Reabierto SAC / Radiografia: url={sac.url}")
                            if _page_requires_portal_login(sac):
                                _login_intranet(sac, intra_user, intra_pass)
                                sac = _ir_a_radiografia(sac)
                            continue
                        raise

                if not buscado_ok:
                    raise RuntimeError("No pude completar la busqueda en Radiografia.")

                # >>> GATE DESDE RADIOGRAFIA <<<
                CHECK_ALL = _env_true("STRICT_CHECK_ALL_OPS", "0")
                etapa("Verificando acceso al contenido del expediente")
    
                # Esperar a que cargue la vista (caratula + grillas)
                _esperar_radiografia_listo(sac, timeout=int(os.getenv("RADIO_TIMEOUT_MS", "150")))
                logging.info("[RADIO] Vista de Radiografia cargada (caratula/operaciones/adjuntos visibles)")
    
                # Listar operaciones rÃƒÂ¡pido (con frames)
                op_ids_rad = _listar_ops_ids_radiografia(
                    sac,
                    wait_ms=int(os.getenv("RADIO_OPS_WAIT_MS", "150")),
                    scan_frames=True,
                )
    
                # VerificaciÃƒÂ³n de acceso:
                acceso_ok = False
                if op_ids_rad:
                    ids_a_probar = op_ids_rad if CHECK_ALL else op_ids_rad[:1]
                    # 1) Si alguna operaciÃƒÂ³n probada estÃƒÂ¡ denegada ? abortar
                    if any(_op_denegada_en_radiografia(sac, _id) for _id in ids_a_probar):
                        logging.info("[SEC] RadiografÃƒÂ­a mostrÃƒÂ³ 'sin permisos' en al menos una operaciÃƒÂ³n. Abortando.")
                        messagebox.showwarning(
                            "Sin acceso",
                            "No tenÃƒÂ©s permisos para visualizar el contenido de este expediente "
                            "(al menos una operaciÃƒÂ³n estÃƒÂ¡ bloqueada). No se descargarÃƒÂ¡ nada.",
                        )
                        return
                    # 2) Al menos una visible con contenido
                    acceso_ok = any(_op_visible_con_contenido_en_radiografia(sac, _id) for _id in ids_a_probar)
                else:
                    acceso_ok = _puedo_abrir_alguna_operacion(sac)
    
                if not acceso_ok:
                    logging.info("[SEC] No hay acceso real al contenido de las operaciones (bloqueando descarga).")
                    messagebox.showwarning(
                        "Sin acceso",
                        "No tenÃƒÂ©s permisos para visualizar el contenido del expediente (operaciones bloqueadas). "
                        "No se descargarÃƒÂ¡ nada.",
                    )
                    return
                # >>> GATE DESDE RADIOGRAFIA <<<
    
                # === 3.a) NUEVO: fechas por operaciÃƒÂ³n + timeline ===
                op_fecha_map, orden_fechas = _mapear_fechas_operaciones_radiografia(sac)
                from collections import defaultdict
                timeline = defaultdict(list)   # { 'dd/mm/aaaa' -> [(Path, header), ...] }
                ya_agregados: set[tuple[str, int]] = set()
                caratula_block: tuple[Path, str | None] | None = None
    
                def _push_pdf(pth: Path, hdr: str | None, fecha: str | None, toc_title: str | None = None):
                    if not pth or not pth.exists() or pth.suffix.lower() != ".pdf":
                        return False
                    if not _is_real_pdf(Path(pth)):
                        try:
                            logging.info(f"[MERGE:SKIP] {Path(pth).name} no es un PDF valido")
                        except Exception:
                            pass
                        return False
                    try:
                        key = (pth.name, pth.stat().st_size)
                    except Exception:
                        key = (pth.name, 0)
                    if key in ya_agregados:
                        return False
                    ya_agregados.add(key)
    
                    # Limpieza de pÃƒÂ¡ginas en blanco (best-effort)
                    try:
                        pth = _pdf_sin_blancos(pth)
                    except Exception:
                        pass
                    if not pth or not Path(pth).exists():
                        return False
     
                    timeline[(fecha or "__NOFECHA__")].append((pth, hdr, toc_title))
                    if fecha and fecha not in orden_fechas:
                        orden_fechas.append(fecha)
                    return True
    
                # 3) Abrir Libro y listar operaciones VISIBLES (sin forzar)
                etapa("Abriendo vista 'Expediente como Libro'")
                libro = _abrir_libro(sac, intra_user, intra_pass, nro_exp)
                if _es_login_intranet(libro):
                    _login_intranet(libro, intra_user, intra_pass)
                if "ExpedienteLibro.aspx" not in (libro.url or ""):
                    libro = _abrir_libro(sac, intra_user, intra_pass, nro_exp)
    
                etapa("Leyendo ÃƒÂ­ndice del Libro y orden cronolÃƒÂ³gico")
                ops = _expandir_y_cargar_todo_el_libro(libro)
                # Fallback de fechas por operacion desde el Indice del Libro
                try:
                    import re as _re
                    op_fecha_map_alt: dict[str, str] = {}
                    orden_fechas_alt: list[str] = []
                    for it in ops:
                        try:
                            t = _norm_ws(it.get("titulo") or "")
                        except Exception:
                            t = ""
                        m = _re.search(r"\b\d{2}/\d{2}/\d{4}\b", t)
                        if m:
                            d = m.group(0)
                            op_fecha_map_alt[it["id"]] = d
                            if not orden_fechas_alt or orden_fechas_alt[-1] != d:
                                orden_fechas_alt.append(d)
                    for k, v in op_fecha_map_alt.items():
                        if k not in op_fecha_map:
                            op_fecha_map[k] = v
                    if (not orden_fechas) and orden_fechas_alt:
                        orden_fechas = orden_fechas_alt
                except Exception:
                    pass
                logging.info(f"[LIBRO] ÃƒÂndice cargado Ã‚Â· operaciones visibles={len(ops)}")
                if not ops:
                    logging.info("[SEC] La UI no muestra operaciones en el ÃƒÂndice. Se continÃƒÂºa SIN operaciones.")
                    ops = []
                logging.info(f"[OPS] Encontradas {len(ops)} operaciones visibles en el ÃƒÂ­ndice.")
                radiografia_plan = None
                if radiografia_selector:
                    etapa("Preparando radiografia del expediente")
                    op_title_map = {
                        op["id"]: ((op.get("titulo") or "").strip() or f"Operación {op['id']}")
                        for op in ops
                    }
                    if INCLUIR_ADJUNTOS:
                        try:
                            sac.bring_to_front()
                        except Exception:
                            pass
                        adj_meta = _listar_adjuntos_grid_para_radiografia(
                            sac,
                            op_fecha_map=op_fecha_map,
                            op_title_map=op_title_map,
                        )
                        informes_mpf_meta = _listar_informes_tecnicos_para_radiografia(sac)
                        informes_rnr_meta = _listar_informes_reincidencia_para_radiografia(sac)
                        try:
                            logging.info(
                                f"[RADIOPLAN] Meta detectada: adj={len(adj_meta)} · "
                                f"mpf={len(informes_mpf_meta)} · rnr={len(informes_rnr_meta)}"
                            )
                        except Exception:
                            pass
                    else:
                        adj_meta = []
                        informes_mpf_meta = []
                        informes_rnr_meta = []
                    radiografia_items = _armar_items_radiografia(
                        ops,
                        op_fecha_map,
                        adj_meta,
                        informes_mpf_meta,
                        informes_rnr_meta,
                    )
                    etapa("Radiografia del expediente: seleccionando contenido y orden")
                    preview_state = {
                        "done": threading.Event(),
                        "result": None,
                        "preview_request_q": queue.Queue(),
                        "preview_response_q": queue.Queue(),
                        "preview_cache": {},
                    }
                    selector_state = radiografia_selector(radiografia_items, preview_state)
                    if isinstance(selector_state, dict) and hasattr(selector_state.get("done"), "is_set"):
                        preview_state = selector_state
                        preview_dir = temp_dir / "_radio_preview"
                        ops_by_id = {op["id"]: op for op in ops}
                        preview_hbrowser = preview_hctx = preview_hp = None
                        try:
                            preview_hbrowser, preview_hctx, preview_hp = _crear_contexto_headless_reutilizable(
                                context,
                                p,
                                preview_dir,
                                CHROMIUM_ARGS,
                            )
                            while not preview_state["done"].wait(0.12):
                                try:
                                    while True:
                                        req = preview_state["preview_request_q"].get_nowait()
                                        if (req or {}).get("action") != "preview":
                                            continue
                                        uid_req = str(req.get("uid") or "")
                                        force_req = bool(req.get("force"))
                                        if not uid_req:
                                            continue
                                        cached = preview_state["preview_cache"].get(uid_req)
                                        if force_req:
                                            preview_state["preview_cache"].pop(uid_req, None)
                                            cached = None
                                        if not cached:
                                            item_req = next((it for it in radiografia_items if str(it.get("uid") or "") == uid_req), None)
                                            if item_req:
                                                cached = _generar_preview_real_radiografia(
                                                    item_req,
                                                    sac,
                                                    libro,
                                                    preview_dir,
                                                    ops_by_id,
                                                    op_fecha_map,
                                                    op_title_map,
                                                    context,
                                                    p,
                                                    hctx=preview_hctx,
                                                    hp=preview_hp,
                                                )
                                            else:
                                                cached = {
                                                    "uid": uid_req,
                                                    "ok": False,
                                                    "message": "El item ya no está disponible para generar preview.",
                                                }
                                            preview_state["preview_cache"][uid_req] = cached
                                        preview_state["preview_response_q"].put(cached)
                                except queue.Empty:
                                    pass
                        finally:
                            try:
                                if preview_hp:
                                    preview_hp.close()
                            except Exception:
                                pass
                            try:
                                if preview_hctx:
                                    preview_hctx.close()
                            except Exception:
                                pass
                            try:
                                if preview_hbrowser:
                                    preview_hbrowser.close()
                            except Exception:
                                pass
                        radiografia_plan = preview_state.get("result")
                    else:
                        radiografia_plan = selector_state
                    if radiografia_plan is None:
                        logging.info("[RADIOPLAN] Selección cancelada por el usuario")
                        return
                    if not radiografia_plan:
                        logging.info("[RADIOPLAN] No se seleccionaron items para descargar")
                        return
                    logging.info(f"[RADIOPLAN] Items seleccionados: {len(radiografia_plan)}")
                    try:
                        from collections import Counter as _Counter
                        by_kind = _Counter((item.get("kind") or "?") for item in radiografia_plan)
                        logging.info(
                            "[RADIOPLAN] Seleccion por tipo: "
                            f"ops={by_kind.get('operacion', 0)} · "
                            f"adj={by_kind.get('adjunto', 0)} · "
                            f"mpf={by_kind.get('informe_mpf', 0)} · "
                            f"rnr={by_kind.get('informe_rnr', 0)}"
                        )
                        muestra = [
                            _norm_ws(f"{item.get('kind_label') or item.get('kind')}: {item.get('titulo') or ''}")[:120]
                            for item in radiografia_plan[:8]
                        ]
                        logging.info(f"[RADIOPLAN] Muestra seleccion: {muestra}")
                    except Exception:
                        pass
                # 4) Preparar contexto headless reutilizable para HTML->PDF (carÃƒÂ¡tula + operaciones)
                hbrowser = hctx = hp = None
                try:
                    state_print = temp_dir / "state_print.json"
                    try:
                        context.storage_state(path=str(state_print))
                    except Exception:
                        pass
                    hbrowser = _launch_chromium(p.chromium, headless=True, args=CHROMIUM_ARGS)
                    hctx = hbrowser.new_context(
                        storage_state=str(state_print), viewport={"width": 900, "height": 1200}
                    )
                    hp = hctx.new_page()
                    try:
                        hp.emulate_media(media="print")
                    except Exception:
                        pass
                except Exception:
                    hbrowser = hctx = hp = None
                # 4) CarÃƒÂ¡tula (guardada aparte para que quede primera)
                etapa("Renderizando carÃƒÂ¡tula del expediente en PDF")
                try:
                    caratula_pdf = _render_caratula_a_pdf(libro, context, p, temp_dir, hctx=hctx, hp=hp)
                    if caratula_pdf and caratula_pdf.exists():
                        _mf(f"CARATULA Ã‚Â· {caratula_pdf.name}")
                        caratula_block = (caratula_pdf, None)
                        logging.info("[CARATULA] capturada")
                    else:
                        logging.info("[CARATULA] no se pudo capturar (se continÃƒÂºa)")
                except Exception as e:
                    logging.info(f"[CARATULA:ERR] {e}")
    
                try:
                    if radiografia_plan is not None:
                        _cargar_timeline_radiografia_custom(
                            sac,
                            libro,
                            temp_dir,
                            ops,
                            radiografia_plan,
                            op_fecha_map,
                            INCLUIR_ADJUNTOS,
                            STAMP,
                            context,
                            p,
                            hctx,
                            hp,
                            _push_pdf,
                            _mf,
                        )
                    else:
                        _cargar_timeline_descarga_completa(
                            sac,
                            libro,
                            temp_dir,
                            ops,
                            op_fecha_map,
                            INCLUIR_ADJUNTOS,
                            STAMP,
                            context,
                            p,
                            hctx,
                            hp,
                            _push_pdf,
                            _mf,
                        )
                finally:
                    try:
                        if hp:
                            hp.close()
                    except Exception:
                        pass
                    try:
                        if hctx:
                            hctx.close()
                    except Exception:
                        pass
                    try:
                        if hbrowser:
                            hbrowser.close()
                    except Exception:
                        pass
    
                # === 3.e) ConstrucciÃƒÂ³n final en orden cronolÃƒÂ³gico ===
                hay_algo = any(timeline.values()) or bool(caratula_block)
                if not hay_algo:
                    raise RuntimeError("No hubo nada para fusionar (no se pudo capturar operaciones ni adjuntos).")
    
                # Reordenar listas por fecha para que el ÃƒÂ­ndice quede de las
                # operaciones mÃƒÂ¡s antiguas a las mÃƒÂ¡s recientes.
                for k in list(timeline.keys()):
                    timeline[k].reverse()
                def _key_fecha(s: str):
                    try:
                        d, m, a = s.split("/")
                        return (int(a), int(m), int(d))
                    except Exception:
                        return (9999, 99, 99)
                orden_fechas = sorted(orden_fechas, key=_key_fecha)
    
                bloques_final = []  # list of (Path, header, toc_title?)
                if caratula_block:
                    bloques_final.append(caratula_block)
    
                # 1) Fechas en el orden que aparece la grilla de operaciones
                for f in orden_fechas:
                    bloques_final.extend(timeline.get(f, []))
    
                # 2) Fechas que no estaban en la grilla (p.ej. sÃƒÂ³lo IT)
                restantes = [f for f in timeline.keys() if f not in set(orden_fechas) and f != "__NOFECHA__"]
                for f in sorted(restantes, key=_key_fecha):
                    bloques_final.extend(timeline.get(f, []))
    
                # 3) Elementos sin fecha ? al final
                bloques_final.extend(timeline.get("__NOFECHA__", []))
                # 9) FusiÃƒÂ³n final
                # Reordenar bloques por fechas con fallback si falta orden desde Radiografia
                try:
                    bloques_final2: list[tuple[Path, str | None]] = []
                    if caratula_block:
                        bloques_final2.append(caratula_block)
                    fechas_keys = [f for f in timeline.keys() if f != "__NOFECHA__"]
                    if orden_fechas:
                        _set_ord = set(orden_fechas)
                        restantes = [f for f in set(fechas_keys) if f not in _set_ord]
                        fechas_iter = list(orden_fechas) + sorted(restantes, key=_key_fecha)
                    else:
                        fechas_iter = sorted(set(fechas_keys), key=_key_fecha)
                    for f in fechas_iter:
                        bloques_final2.extend(timeline.get(f, []))
                    bloques_final2.extend(timeline.get("__NOFECHA__", []))
                    bloques_final = bloques_final2
                except Exception:
                    pass
    
                out = Path(carpeta_salida) / f"Exp_{nro_exp}.pdf"
                out_sin_links = out
                front_matter_pages = _contar_paginas_pdf(caratula_block[0]) if caratula_block else 0
                idx_pages, idx_map = fusionar_bloques_con_indice(
                    bloques_final,
                    out,
                    index_title="INDICE",
                    keep_sidecar=_env_true("KEEP_TOC", "0"),
                    front_matter_pages=front_matter_pages,
                    skip_first_block_in_index=bool(caratula_block),
                )
                first_index_page = max(1, front_matter_pages + 1) if idx_pages else 1
    
                # DiagnÃƒÂ³stico inicial: links presentes justo tras fusionar
                try:
                    _log_links_en_pagina(out, first_index_page, "INDICE/ANTES_POST")
                except Exception:
                    pass
    
                # Intentar aplicar OCR al PDF final
                if APLICAR_OCR:
                    ocr_out = _maybe_ocr(out)
                    if ocr_out != out:
                        shutil.move(ocr_out, out)

                    # DiagnÃƒÂ³stico despuÃƒÂ©s del OCR Ã¯Â¿Â½?oligeroÃ¯Â¿Â½?Ã¯Â¿Â½
                    try:
                        _log_links_en_pagina(out, first_index_page, "INDICE/DESPUES_OCR")
                    except Exception:
                        pass

                    if _env_true("OCR_FINAL_FORCE"):
                        try:
                            tmp_out = out.with_name(out.stem + "_ocr.pdf")
                            subprocess.run(
                                [
                                    "ocrmypdf",
                                    "--force-ocr",
                                    "--language", "spa",
                                    "--image-dpi", "300",
                                    "--deskew",
                                    "--rotate-pages",
                                    "--optimize", "3",
                                    str(out),
                                    str(tmp_out),
                                ],
                                check=True,
                                **_subprocess_hidden_kwargs(),
                            )
                            shutil.move(tmp_out, out)
                        except Exception:
                            logging.exception("[OCR] FallÃƒÂ³ OCR final")

                        # DiagnÃƒÂ³stico despuÃƒÂ©s del OCR forzado
                        try:
                            _log_links_en_pagina(out, first_index_page, "INDICE/DESPUES_OCR_FORCE")
                        except Exception:
                            pass
                else:
                    logging.info("[OCR] Omitido por opciÃƒÂ³n de usuario (sin OCR).")
    
                # === NUMERACIÓN DE PÁGINAS ===
                try:
                    _agregar_numeracion_paginas(out, numero_inicial=1)
                    logging.info("[PAGINAS] Numeración por página aplicada")
                except Exception as e:
                    logging.info(f"[PAGINAS] No se pudo estampar numeración de páginas: {e}")
    
                # DiagnÃƒÂ³stico tras fojas (antes de reinyectar)
                try:
                    _log_links_en_pagina(out, first_index_page, "INDICE/ANTES_RELINK")
                except Exception:
                    pass
    
                # Reinyectar links (por si OCR/fojas los borraron)
                try:
                    ok, out_final = _relink_indice_con_fitz(out, idx_map)
                    logging.info(f"[INDICE/LINK] reinyectado={ok} items={len(idx_map)}")
                    if out_final != out_sin_links:
                        try:
                            out_sin_links.unlink(missing_ok=True)
                            logging.info(
                                f"[INDICE/LINK] duplicado sin links eliminado: {out_sin_links.name}"
                            )
                        except Exception as e_del:
                            logging.info(
                                f"[INDICE/LINK] no se pudo eliminar duplicado: {e_del}"
                            )
                    out = out_final
                except Exception as e:
                    logging.info(f"[INDICE/LINK:ERR] {e}")
    
                # DiagnÃƒÂ³stico final
                try:
                    _log_links_en_pagina(out, first_index_page, "INDICE/FINAL")
                except Exception:
                    pass
    
                _mf(f"==> PDF FINAL: {out.name} (total bloques={len(bloques_final)})")
                logging.info(f"[OK] PDF final creado: {out} | bloques={len(bloques_final)}")
                etapa("Listo: PDF final creado")
                messagebox.showinfo("Éxito", f"PDF creado en:\n{out}")
    
            finally:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass




def _create_root():
    """Crea la ventana principal con ttkbootstrap cuando esta disponible."""
    if _TTKBOOTSTRAP_OK and tb is not None:
        try:
            return tb.Window(themename=UI_THEME)
        except Exception:
            pass
    return Tk()


def _apply_ui_theme(master):
    """Aplica una apariencia moderna sin tocar la logica de negocio."""
    style = ttk.Style(master)
    if _TTKBOOTSTRAP_OK:
        try:
            style.theme_use(UI_THEME)
        except Exception:
            pass
    else:
        for theme in ("vista", "clam", "default"):
            try:
                style.theme_use(theme)
                break
            except Exception:
                pass

    font = "Segoe UI"
    title_font = "Segoe UI Variable Display"
    bg = "#F5F7FA"
    surface = "#FFFFFF"
    text = "#172033"
    muted = "#64748B"
    primary = "#0F766E"
    primary_active = "#115E59"
    border = "#D9E2EC"

    try:
        master.configure(bg=bg)
    except Exception:
        pass

    try:
        style.configure(".", font=(font, 10))
        style.configure("TFrame", background=bg)
        style.configure("TLabel", background=bg, foreground=text)
        style.configure("Title.TLabel", font=(title_font, 20, "bold"), foreground="#0F172A", background=bg)
        style.configure("Hint.TLabel", foreground=muted, background=bg)
        style.configure("Status.TLabel", font=(font, 10, "bold"), foreground="#1E293B", background=bg)
        style.configure("Card.TLabelframe", padding=(14, 10, 14, 12), background=surface, borderwidth=1, relief="solid")
        style.configure("Card.TLabelframe.Label", font=(font, 10, "bold"), foreground="#334155", background=bg)
        style.configure("TEntry", padding=(8, 6), fieldbackground=surface)
        style.configure("TCheckbutton", background=bg, foreground=text, padding=(2, 4))
        style.configure("Accent.TButton", padding=(16, 9), font=(font, 10, "bold"), foreground="#FFFFFF", background=primary, borderwidth=0)
        style.map(
            "Accent.TButton",
            background=[("active", primary_active), ("pressed", primary_active), ("disabled", "#CBD5E1")],
            foreground=[("disabled", "#F8FAFC")],
        )
        style.configure("TButton", padding=(12, 7), font=(font, 10))
        style.configure("TProgressbar", thickness=10)
        style.configure("Radiografia.Treeview", rowheight=34, font=(font, 10), background=surface, fieldbackground=surface, foreground=text, bordercolor=border)
        style.configure("Radiografia.Treeview.Heading", font=(font, 10, "bold"), foreground="#0F172A", background="#E8F0F7")
        style.map("Radiografia.Treeview", background=[("selected", "#DDF4EF")], foreground=[("selected", "#0F172A")])
        style.configure("TypeFilterMenu.TMenubutton", padding=(12, 7), font=(font, 10, "bold"))
        style.configure("TypeFilterMenu.TButton", padding=(12, 7), font=(font, 10, "bold"))
    except Exception:
        pass
    return style


class ProgressWin(Toplevel):
    """Ventana de progreso con estado claro y bitacora tecnica."""

    def __init__(self, master, q, title="Progreso"):
        super().__init__(master)
        self.title(title)
        self.geometry("920x500")
        self.minsize(880, 460)
        self.q = q

        cont = ttk.Frame(self, padding=(12, 12, 12, 12))
        cont.pack(fill="both", expand=True)

        self.lbl = ttk.Label(cont, text="Estado actual: iniciando...", style="Status.TLabel")
        self.lbl.pack(anchor="w")

        self.sub = ttk.Label(
            cont,
            text="Preparando entorno. Vas a ver cada etapa con una explicacion simple.",
            style="Hint.TLabel",
            wraplength=860,
            justify="left",
        )
        self.sub.pack(anchor="w", pady=(4, 8))

        self.pb = ttk.Progressbar(cont, mode="indeterminate")
        self.pb.pack(fill="x", pady=(0, 10))
        self.pb.start(12)

        log_box = ttk.LabelFrame(cont, text="Bitacora tecnica", padding=(8, 8, 8, 8))
        log_box.pack(fill="both", expand=True)

        self.text = ScrolledText(log_box, wrap="word", height=19, font=("Consolas", 10))
        self.text.pack(fill="both", expand=True)

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._poll()

    def _detalle_etapa(self, etapa_txt: str) -> str:
        t = (etapa_txt or "").lower()
        if "seleccionando contenido y orden" in t or "radiografia del expediente" in t:
            return "Revisando operaciones, adjuntos e informes para elegir que descargar y en que orden."
        if "teletrabajo" in t or "intranet" in t or "sac" in t:
            return "Iniciando sesion y entrando al SAC."
        if "radiografia" in t:
            return "Buscando el expediente en Radiografia y validando acceso."
        if "indice" in t or "libro" in t:
            return "Leyendo el indice para respetar el orden del expediente."
        if "adjuntos" in t:
            return "Descargando adjuntos y vinculandolos a su operacion."
        if "operaciones" in t:
            return "Capturando operaciones visibles y convirtiendolas a PDF."
        if "tecnicos" in t or "rnr" in t:
            return "Descargando informes complementarios."
        if "caratula" in t:
            return "Generando portada del expediente."
        if "listo" in t:
            return "Proceso finalizado. El PDF quedo guardado en la carpeta elegida."
        return "Procesando. Mira la bitacora de abajo para detalle tecnico en vivo."

    def _poll(self):
        try:
            while True:
                msg = _repair_mojibake_text(self.q.get_nowait())
                if "[ETAPA] " in msg:
                    etapa_txt = _repair_mojibake_text(msg.split("[ETAPA] ", 1)[1].strip())
                    self.lbl.config(text=f"Estado actual: {etapa_txt}")
                    self.sub.config(text=self._detalle_etapa(etapa_txt))
                if "[CONFIG]" in msg:
                    conf = _repair_mojibake_text(msg.split("[CONFIG]", 1)[1].strip())
                    self.sub.config(text=f"Opciones activas: {conf}")
                self.text.insert("end", msg + "\n")
                self.text.see("end")
        except queue.Empty:
            pass
        self.after(100, self._poll)

    def _on_close(self):
        # Solo oculta la ventana. La descarga sigue en segundo plano.
        self.withdraw()


class RadiografiaDialog(Toplevel):
    def __init__(self, master, items: list[dict], title="Radiografia del expediente", preview_session: dict | None = None):
        super().__init__(master)
        self.title(title)
        screen_w = max(1200, int(self.winfo_screenwidth() or 1500))
        screen_h = max(800, int(self.winfo_screenheight() or 900))
        win_w = max(1260, screen_w - 60)
        win_h = max(760, screen_h - 150)
        self.geometry(f"{win_w}x{win_h}+18+18")
        self.minsize(1180, 680)
        self.resizable(True, True)
        self.result = None
        self.items = [dict(item) for item in items]
        self._items_by_uid = {str(item.get("uid") or ""): item for item in self.items}
        self._default_uid_order = [str(item.get("uid") or "") for item in self.items]
        self._drag_state = None
        self._drag_ghost = None
        self._drop_indicator = None
        self._type_filter_popover = None
        self._type_filter_body = None
        self.summary_var = StringVar(value="")
        self.type_filter_summary_var = StringVar(value="")
        self.preview_title_var = StringVar(value="")
        self.preview_meta_var = StringVar(value="")
        self.preview_status_var = StringVar(value="")
        self.index_name_var = StringVar(value="")
        self._preview_session = preview_session
        self._preview_model = None
        self._preview_photo = None
        self._preview_local_cache: dict[str, dict] = {}
        self._preview_poll_job = None
        self._preview_debounce_job = None
        self._type_filter_buttons: dict[str, str] = {}
        self._syncing_index_name = False
        try:
            self.index_name_var.trace_add("write", self._on_index_name_var_changed)
        except Exception:
            pass

        _apply_ui_theme(self)

        cont = ttk.Frame(self, padding=(14, 14, 14, 14))
        cont.pack(fill="both", expand=True)
        cont.columnconfigure(0, weight=1)
        cont.rowconfigure(1, weight=1)

        head = ttk.Frame(cont)
        head.grid(row=0, column=0, sticky="ew")
        head.columnconfigure(0, weight=1)

        ttk.Label(
            head,
            text="Elegí qué descargar y ordenalo como querés verlo en el PDF final.",
            style="Status.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            head,
            text="Usá el selector visual de cada fila para incluir o quitar contenido. Arrastrá filas para reordenar el PDF final.",
            style="Hint.TLabel",
            wraplength=1400,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(4, 2))
        ttk.Label(
            head,
            textvariable=self.summary_var,
            style="Hint.TLabel",
            wraplength=1400,
            justify="left",
        ).grid(row=2, column=0, sticky="w")

        toolbar = ttk.Frame(head)
        toolbar.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        for col in range(7):
            toolbar.columnconfigure(col, weight=0)
        toolbar.columnconfigure(6, weight=1)

        ttk.Button(toolbar, text="Seleccionar todo", command=self._mark_all).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(toolbar, text="Limpiar todo", command=self._clear_all).grid(row=0, column=1, padx=6)
        ttk.Button(toolbar, text="Borrar desmarcados", command=self._remove_unselected).grid(row=0, column=2, padx=6)
        ttk.Button(toolbar, text="Restaurar orden", command=self._restore_order).grid(row=0, column=3, padx=6)
        self.type_filter_btn = ttk.Button(
            toolbar,
            text="Tipos de operación ▾",
            style="TypeFilterMenu.TButton",
            command=self._toggle_type_filter_popover,
        )
        self.type_filter_btn.grid(row=0, column=4, padx=(12, 6), sticky="w")
        ttk.Label(
            toolbar,
            textvariable=self.type_filter_summary_var,
            style="Hint.TLabel",
            wraplength=500,
            justify="left",
        ).grid(row=0, column=5, sticky="w", padx=(4, 0))

        type_filters = ttk.Frame(head)
        type_filters.grid(row=4, column=0, sticky="ew", pady=(3, 0))
        type_filters.columnconfigure(0, weight=1)
        self.type_filters = type_filters
        self.type_filters.grid_remove()

        paned = ttk.Panedwindow(cont, orient="horizontal")
        paned.grid(row=1, column=0, sticky="nsew", pady=(12, 10))
        self.paned = paned

        left = ttk.Frame(paned)
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)
        paned.add(left, weight=33)

        preview = ttk.LabelFrame(paned, text="Vista previa", style="Card.TLabelframe")
        preview.columnconfigure(0, weight=1)
        preview.rowconfigure(3, weight=1)
        paned.add(preview, weight=67)

        tree_box = ttk.Frame(left)
        tree_box.grid(row=0, column=0, sticky="nsew")
        tree_box.columnconfigure(0, weight=1)
        tree_box.rowconfigure(0, weight=1)
        self.tree_box = tree_box

        self.tree = ttk.Treeview(
            tree_box,
            columns=("orden", "sel", "tipo", "fecha", "titulo", "detalle"),
            show="headings",
            selectmode="extended",
            style="Radiografia.Treeview",
        )
        self.tree.heading("orden", text="#")
        self.tree.heading("sel", text="○")
        self.tree.heading("tipo", text="Tipo")
        self.tree.heading("fecha", text="Fecha")
        self.tree.heading("titulo", text="Documento")
        self.tree.heading("detalle", text="Detalle")
        self.tree.column("orden", width=42, anchor="center", stretch=False)
        self.tree.column("sel", width=54, anchor="center", stretch=False)
        self.tree.column("tipo", width=126, anchor="w", stretch=False)
        self.tree.column("fecha", width=95, anchor="center", stretch=False)
        self.tree.column("titulo", width=430, anchor="w")
        self.tree.column("detalle", width=360, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._update_preview())
        self.tree.bind("<space>", self._on_space)
        self.tree.bind("<ButtonPress-1>", self._on_tree_press)
        self.tree.bind("<B1-Motion>", self._on_tree_motion)
        self.tree.bind("<ButtonRelease-1>", self._on_tree_release)

        self.tree.tag_configure("selected_even", font=("Segoe UI", 10, "bold"), foreground="#0F172A", background="#F7FBFF")
        self.tree.tag_configure("selected_odd", font=("Segoe UI", 10, "bold"), foreground="#0F172A", background="#EEF5FB")
        self.tree.tag_configure("muted_even", font=("Segoe UI", 10), foreground="#8A94A6", background="#FBFCFD")
        self.tree.tag_configure("muted_odd", font=("Segoe UI", 10), foreground="#8A94A6", background="#F4F6F8")
        self.tree.tag_configure("drag_even", font=("Segoe UI", 10, "bold"), foreground="#64748B", background="#E8EEF5")
        self.tree.tag_configure("drag_odd", font=("Segoe UI", 10, "bold"), foreground="#64748B", background="#E2E8F0")

        yscroll = ttk.Scrollbar(tree_box, orient="vertical", command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=yscroll.set)

        ttk.Label(
            left,
            text="Consejo: seleccioná una o varias filas y arrastralas. Mientras movés, el bloque queda tenue y aparece una guía negra donde se insertará al soltar.",
            style="Hint.TLabel",
            wraplength=820,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))

        ttk.Label(
            preview,
            text="Vista previa del documento",
            style="Hint.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            preview,
            text="La vista previa real se genera automáticamente sólo para operaciones. Adjuntos e informes muestran información descriptiva.",
            style="Hint.TLabel",
            wraplength=760,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(4, 8))

        preview_tools = ttk.Frame(preview)
        preview_tools.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        preview_tools.columnconfigure(1, weight=1)

        self.preview_btn = ttk.Button(
            preview_tools,
            text="Regenerar preview",
            command=lambda: self._load_real_preview(force=True),
        )
        self.preview_btn.grid(row=0, column=0, sticky="w")
        ttk.Label(
            preview_tools,
            textvariable=self.preview_status_var,
            style="Hint.TLabel",
            wraplength=700,
            justify="left",
        ).grid(row=0, column=1, sticky="w", padx=(10, 0))
        ttk.Label(
            preview_tools,
            text="Nombre en índice:",
            style="Hint.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.index_name_entry = ttk.Entry(preview_tools, textvariable=self.index_name_var)
        self.index_name_entry.grid(row=1, column=1, sticky="ew", padx=(10, 0), pady=(10, 0))

        preview_body = ttk.Frame(preview)
        preview_body.grid(row=3, column=0, sticky="nsew")
        preview_body.columnconfigure(0, weight=1)
        preview_body.rowconfigure(0, weight=1)

        self.preview_canvas = Canvas(
            preview_body,
            background="#E7EDF3",
            highlightthickness=0,
            bd=0,
        )
        self.preview_canvas.grid(row=0, column=0, sticky="nsew")
        self.preview_scroll = ttk.Scrollbar(preview_body, orient="vertical", command=self.preview_canvas.yview)
        self.preview_scroll.grid(row=0, column=1, sticky="ns")
        self.preview_canvas.configure(yscrollcommand=self.preview_scroll.set)
        self.preview_canvas.bind("<Configure>", self._render_preview_canvas)
        self.preview_canvas.bind("<MouseWheel>", self._on_preview_mousewheel)
        self.preview_canvas.bind("<Button-4>", self._on_preview_mousewheel)
        self.preview_canvas.bind("<Button-5>", self._on_preview_mousewheel)

        ttk.Separator(cont, orient="horizontal").grid(row=2, column=0, sticky="ew", pady=(0, 8))

        btns = ttk.Frame(cont)
        btns.grid(row=3, column=0, sticky="ew", pady=(0, 2))
        btns.columnconfigure(0, weight=1)
        btns.columnconfigure(1, weight=0)
        btns.columnconfigure(2, weight=0)

        ttk.Button(btns, text="Cancelar", command=self._cancel).grid(row=0, column=1, sticky="e", padx=(6, 6))
        ttk.Button(btns, text="Aceptar", command=self._accept).grid(row=0, column=2, sticky="e")

        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self.bind("<Escape>", lambda _e: self._cancel())
        self.after(80, self._position_panes)
        self._refresh()

    def _item_by_uid(self, uid: str) -> dict | None:
        for item in self.items:
            if item.get("uid") == uid:
                return item
        return None

    def _selected_uids(self) -> list[str]:
        return [str(uid) for uid in self.tree.selection()]

    def _position_panes(self):
        paned = getattr(self, "paned", None)
        if paned is None:
            return
        try:
            total_w = max(int(paned.winfo_width() or 0), 1180)
            paned.sashpos(0, int(total_w * 0.32))
        except Exception:
            pass

    def _update_summary(self):
        from collections import Counter

        selected_items = [item for item in self.items if item.get("selected", True)]
        by_kind = Counter((item.get("kind") or "?") for item in selected_items)
        self.summary_var.set(
            f"Incluidos {len(selected_items)}/{len(self.items)} · "
            f"Operaciones {by_kind.get('operacion', 0)} · "
            f"Adjuntos {by_kind.get('adjunto', 0)} · "
            f"Informes MPF {by_kind.get('informe_mpf', 0)} · "
            f"Informes RNR {by_kind.get('informe_rnr', 0)}"
        )

    def _category_for_item(self, item: dict) -> str:
        kind = (item.get("kind") or "").strip().lower()
        if kind == "adjunto":
            return "Adjuntos"
        if kind == "informe_mpf":
            return "Informes MPF"
        if kind == "informe_rnr":
            return "Informes RNR"

        raw_title = _norm_ws(item.get("titulo") or "")
        raw_tipo = _norm_ws(item.get("op_tipo") or "")
        text = f"{raw_title} {raw_tipo}".strip()
        text_norm = text.lower()

        patterns = [
            (r"declaraci[oó]n testimonial|testimonial", "Declaraciones testimoniales"),
            (r"certific", "Certificados"),
            (r"declaraci[oó]n", "Declaraciones"),
            (r"oficio", "Oficios"),
            (r"decreto", "Decretos"),
            (r"constancia", "Constancias"),
            (r"audiencia", "Audiencias"),
            (r"resoluci[oó]n", "Resoluciones"),
            (r"notificaci[oó]n", "Notificaciones"),
            (r"c[ée]dula|cedula", "Cédulas"),
            (r"pericia|pericial", "Pericias"),
            (r"informe", "Informes"),
            (r"presentaci[oó]n", "Presentaciones"),
            (r"escrito", "Escritos"),
        ]
        for pattern, label in patterns:
            if re.search(pattern, text_norm, re.I):
                return label

        cleaned = re.sub(r"^\d{2}/\d{2}/\d{4}\s*[-·]\s*", "", raw_title).strip(" -·")
        cleaned = re.sub(r"\s+", " ", cleaned)
        if cleaned:
            return cleaned[:80]
        if raw_tipo:
            return raw_tipo[:80]
        return "Operaciones"

    def _category_stats(self) -> list[dict]:
        buckets: dict[str, dict] = {}
        order: list[str] = []
        for item in self.items:
            label = self._category_for_item(item)
            key = label.casefold()
            if key not in buckets:
                buckets[key] = {"label": label, "total": 0, "selected": 0}
                order.append(key)
            buckets[key]["total"] += 1
            if item.get("selected", True):
                buckets[key]["selected"] += 1
        return [buckets[key] for key in order]

    def _refresh_type_filter_buttons(self):
        host = getattr(self, "type_filters", None)
        if host is None:
            return

        self._type_filter_buttons = {}
        stats = self._category_stats()
        for idx, stat in enumerate(stats):
            label = stat["label"]
            total = int(stat["total"])
            selected = int(stat["selected"])
            if selected <= 0:
                prefix = "○"
                detail = f"{total}"
            elif selected >= total:
                prefix = "●"
                detail = f"{total}"
            else:
                prefix = "◐"
                detail = f"{selected}/{total}"
            text = f"{prefix} {label} ({detail})"
            self._type_filter_buttons[label] = text

        try:
            abierto = False
            try:
                abierto = bool(self.type_filters.winfo_ismapped())
            except Exception:
                abierto = False
            self.type_filter_btn.configure(text=f"Tipos de operación {'▴' if abierto else '▾'} ({len(stats)})")
        except Exception:
            pass
        self.type_filter_summary_var.set(
            f"{len(stats)} tipo(s) detectados. Abrí el desplegable y marcá varios sin que se cierre."
        )
        try:
            panel_abierto = bool(host.winfo_ismapped())
        except Exception:
            panel_abierto = False
        if panel_abierto:
            body = getattr(self, "_type_filter_body", None)
            try:
                body_ok = bool(body is not None and body.winfo_exists())
            except Exception:
                body_ok = False
            if body_ok:
                self._render_type_filter_popover()
            else:
                self._show_type_filter_popover()

    def _toggle_type_filter_popover(self):
        host = getattr(self, "type_filters", None)
        if host is None:
            return
        try:
            if host.winfo_ismapped():
                self._hide_type_filter_popover()
                return
        except Exception:
            pass
        self._show_type_filter_popover()

    def _show_type_filter_popover(self):
        self._hide_type_filter_popover()
        host = getattr(self, "type_filters", None)
        if host is None:
            return
        try:
            host.grid()
        except Exception:
            pass
        shell = Frame(host, bg="#CBD5E1", bd=1, relief="solid")
        shell.grid(row=0, column=0, sticky="ew")
        body = ttk.Frame(shell, padding=(12, 12, 12, 12))
        body.pack(fill="both", expand=True)
        self._type_filter_popover = shell
        self._type_filter_body = body
        self._render_type_filter_popover()
        try:
            self.type_filter_btn.configure(text=self.type_filter_btn.cget("text").replace("▾", "▴"))
        except Exception:
            pass

    def _reposition_type_filter_popover(self):
        return

    def _hide_type_filter_popover(self):
        pop = getattr(self, "_type_filter_popover", None)
        self._type_filter_popover = None
        self._type_filter_body = None
        if pop is not None:
            try:
                pop.destroy()
            except Exception:
                pass
        host = getattr(self, "type_filters", None)
        if host is not None:
            try:
                host.grid_remove()
            except Exception:
                pass
        try:
            txt = self.type_filter_btn.cget("text") or "Tipos de operación ▾"
            self.type_filter_btn.configure(text=txt.replace("▴", "▾"))
        except Exception:
            pass

    def _render_type_filter_popover(self):
        body = getattr(self, "_type_filter_body", None)
        if body is None:
            return
        try:
            if not body.winfo_exists():
                return
        except Exception:
            return
        for child in body.winfo_children():
            try:
                child.destroy()
            except Exception:
                pass
        body.columnconfigure(0, weight=1)
        ttk.Label(
            body,
            text="Marcá o limpiá grupos completos sin cerrar este desplegable.",
            style="Hint.TLabel",
            wraplength=760,
            justify="left",
        ).grid(row=0, column=0, columnspan=3, sticky="w")
        stats = self._category_stats()
        for col in range(3):
            body.columnconfigure(col, weight=1)
        for idx, stat in enumerate(stats, start=1):
            label = stat["label"]
            total = int(stat["total"])
            selected = int(stat["selected"])
            if selected <= 0:
                prefix = "○"
                detail = f"{total}"
            elif selected >= total:
                prefix = "●"
                detail = f"{total}"
            else:
                prefix = "◐"
                detail = f"{selected}/{total}"
            row = 1 + ((idx - 1) // 3)
            col = (idx - 1) % 3
            ttk.Button(
                body,
                text=f"{prefix} {label} ({detail})",
                command=lambda current=label: self._toggle_category_selection(current),
            ).grid(row=row, column=col, sticky="ew", padx=(0 if col == 0 else 8, 0), pady=(8 if row == 1 else 6, 0))

    def _preview_theme(self, kind: str, selected: bool) -> dict[str, str]:
        kind = (kind or "").strip().lower()
        theme = {
            "operacion": {"badge": "OPERACIÓN", "badge_fill": "#DBEAFE", "badge_text": "#1D4ED8"},
            "adjunto": {"badge": "ADJUNTO", "badge_fill": "#DCFCE7", "badge_text": "#166534"},
            "informe_mpf": {"badge": "INFORME MPF", "badge_fill": "#FEF3C7", "badge_text": "#92400E"},
            "informe_rnr": {"badge": "INFORME RNR", "badge_fill": "#FCE7F3", "badge_text": "#9D174D"},
        }.get(kind, {"badge": "DOCUMENTO", "badge_fill": "#E5E7EB", "badge_text": "#334155"})
        if selected:
            theme["status"] = "Incluido en la descarga"
            theme["status_fill"] = "#DCFCE7"
            theme["status_text"] = "#166534"
        else:
            theme["status"] = "Omitido del PDF final"
            theme["status_fill"] = "#FEE2E2"
            theme["status_text"] = "#991B1B"
        return theme

    def _set_preview_model(self, model: dict):
        self._preview_model = model
        self._render_preview_canvas()
        try:
            self.preview_canvas.yview_moveto(0)
        except Exception:
            pass

    def _render_preview_canvas(self, _event=None):
        canvas = getattr(self, "preview_canvas", None)
        if canvas is None:
            return
        canvas.delete("all")
        self._preview_photo = None

        width = max(int(canvas.winfo_width() or 0), 760)
        height = max(int(canvas.winfo_height() or 0), 660)
        outer_margin = 14
        page_x0 = outer_margin
        page_x1 = max(page_x0 + 320, width - outer_margin)
        page_y0 = 18

        model = self._preview_model or {
            "badge": "SIN SELECCIÓN",
            "badge_fill": "#E5E7EB",
            "badge_text": "#334155",
            "status": "Esperando selección",
            "status_fill": "#E2E8F0",
            "status_text": "#475569",
            "title": "Elegí un documento",
            "subtitle": "La vista previa se actualiza al cambiar la fila activa.",
            "sections": [
                ("Cómo usar el selector", [
                    "Usá el selector ●/○ para incluir o quitar.",
                    "Arrastrá una o varias filas para cambiar el orden.",
                    "La vista real de operaciones se genera en segundo plano apenas elegís la fila.",
                ])
            ],
            "footer": "Panel de lectura de Radiografía",
        }

        inner_left = page_x0 + 24
        inner_right = page_x1 - 24
        text_w = max(120, inner_right - inner_left)
        cursor_y = page_y0 + 22

        def _draw_wrapped_text(x, y, text, font, fill, width_px, anchor="nw"):
            item_id = canvas.create_text(
                x,
                y,
                text=text,
                anchor=anchor,
                width=width_px,
                font=font,
                fill=fill,
            )
            bbox = canvas.bbox(item_id) or (x, y, x, y + 16)
            return bbox[3]

        badge_w = max(86, len(model.get("badge", "")) * 8 + 20)
        badge_h = 24
        canvas.create_rectangle(
            inner_left,
            cursor_y,
            inner_left + badge_w,
            cursor_y + badge_h,
            fill=model.get("badge_fill", "#E5E7EB"),
            outline=model.get("badge_fill", "#E5E7EB"),
        )
        canvas.create_text(
            inner_left + badge_w / 2,
            cursor_y + badge_h / 2,
            text=model.get("badge", "DOCUMENTO"),
            font=("Segoe UI", 9, "bold"),
            fill=model.get("badge_text", "#334155"),
        )

        status = model.get("status", "")
        status_w = max(96, len(status) * 6 + 18)
        status_h = 22
        status_x1 = inner_right
        status_x0 = max(inner_left + badge_w + 12, status_x1 - status_w)
        canvas.create_rectangle(
            status_x0,
            cursor_y + 1,
            status_x1,
            cursor_y + 1 + status_h,
            fill=model.get("status_fill", "#E2E8F0"),
            outline=model.get("status_fill", "#E2E8F0"),
        )
        canvas.create_text(
            (status_x0 + status_x1) / 2,
            cursor_y + 1 + status_h / 2,
            text=status,
            font=("Segoe UI", 8, "bold"),
            fill=model.get("status_text", "#475569"),
        )
        cursor_y += 38

        cursor_y = _draw_wrapped_text(
            inner_left,
            cursor_y,
            model.get("title", "Documento"),
            ("Segoe UI", 14, "bold"),
            "#0F172A",
            text_w,
        ) + 8

        subtitle = model.get("subtitle", "")
        if subtitle:
            cursor_y = _draw_wrapped_text(
                inner_left,
                cursor_y,
                subtitle,
                ("Segoe UI", 9),
                "#64748B",
                text_w,
            ) + 14

        canvas.create_line(inner_left, cursor_y, inner_right, cursor_y, fill="#E2E8F0", width=1)
        cursor_y += 14

        image_path = model.get("image_path")
        if image_path:
            try:
                img = Image.open(str(image_path))
                avail_w = max(280, int(text_w))
                target_w = avail_w
                if img.width > 0 and img.width != target_w:
                    target_h = max(1, int((img.height * target_w) / img.width))
                    img = img.resize((target_w, target_h), Image.LANCZOS)
                self._preview_photo = ImageTk.PhotoImage(img)
                img_x = inner_left
                canvas.create_image(img_x, cursor_y, anchor="nw", image=self._preview_photo)
                canvas.create_rectangle(
                    img_x - 1,
                    cursor_y - 1,
                    img_x + img.width + 1,
                    cursor_y + img.height + 1,
                    outline="#CBD5E1",
                    width=1,
                )
                cursor_y += img.height + 16
            except Exception as e:
                try:
                    logging.info(f"[RADIOPREVIEW] Error cargando imagen en canvas: {e}")
                except Exception:
                    pass

        for heading, lines in model.get("sections", []):
            cursor_y = _draw_wrapped_text(
                inner_left,
                cursor_y,
                heading.upper(),
                ("Segoe UI", 8, "bold"),
                "#475569",
                text_w,
            ) + 8
            for line in lines:
                cursor_y = _draw_wrapped_text(
                    inner_left,
                    cursor_y,
                    f"• {line}",
                    ("Segoe UI", 10),
                    "#0F172A",
                    text_w,
                ) + 6
            cursor_y += 8

        footer = model.get("footer", "")
        if footer:
            footer_y = cursor_y + 4
            canvas.create_line(inner_left, footer_y, inner_right, footer_y, fill="#E2E8F0", width=1)
            cursor_y = _draw_wrapped_text(
                inner_left,
                footer_y + 10,
                footer,
                ("Segoe UI", 8),
                "#94A3B8",
                text_w,
            )
        total_h = max(height, int(cursor_y + 28))
        paper = canvas.create_rectangle(
            page_x0,
            page_y0,
            page_x1,
            max(page_y0 + 320, total_h - 18),
            fill="#FFFFFF",
            outline="#CBD5E1",
            width=1,
        )
        canvas.tag_lower(paper)
        canvas.configure(scrollregion=(0, 0, width, total_h))

    def _build_item_preview_model(self, item: dict, order_idx: int) -> dict:
        kind = item.get("kind") or ""
        kind_label = item.get("kind_label") or kind or "Documento"
        fecha = item.get("fecha") or "Sin fecha"
        titulo = _norm_ws(item.get("titulo") or kind_label)
        detalle = _norm_ws(item.get("detalle") or "")
        theme = self._preview_theme(kind, bool(item.get("selected", True)))

        resumen = detalle or item.get("op_tipo") or "No hay detalle adicional disponible en esta etapa."
        metadatos = [
            f"Posición en el PDF final: {order_idx + 1} de {len(self.items)}",
            f"Fecha visible en Radiografía: {fecha}",
            f"Referencia interna: {item.get('uid')}",
            f"Nombre en índice: {_indice_toc_title_for_item(item)}",
        ]
        if item.get("op_id"):
            metadatos.append(f"ID de operación: {item.get('op_id')}")
        if item.get("op_tipo"):
            metadatos.append(f"Tipo de operación: {item.get('op_tipo')}")

        accion = "Se agregará al PDF final respetando el orden elegido."
        if kind == "operacion":
            accion = "Se abrirá en el Libro y se convertirá a PDF antes de fusionarla."
        elif kind == "adjunto":
            accion = "Se descargará desde la grilla de Radiografía y luego se insertará en el merge."
        elif kind == "informe_mpf":
            accion = "Se descargará desde la sección de Informes Técnicos MPF."
        elif kind == "informe_rnr":
            accion = "Se descargará desde la sección de Reincidencias."

        return {
            **theme,
            "title": titulo,
            "subtitle": f"{kind_label} · {fecha}",
            "sections": [
                ("Resumen", [resumen]),
                ("Metadatos", metadatos),
                ("Acción al descargar", [accion]),
            ],
            "footer": "Vista previa preliminar generada antes de descargar el archivo real.",
        }

    def _build_real_item_preview_model(self, item: dict, order_idx: int, preview_resp: dict) -> dict:
        model = self._build_item_preview_model(item, order_idx)
        real_lines = [
            f"Archivo temporal: {preview_resp.get('file_name') or '-'}",
            f"Páginas detectadas: {preview_resp.get('page_count') or '-'}",
            f"Tamaño: {preview_resp.get('size_text') or '-'}",
        ]
        if preview_resp.get("message"):
            real_lines.append(str(preview_resp.get("message")))
        model["sections"] = [("Archivo real", real_lines)] + model.get("sections", [])
        model["footer"] = "Vista previa real temporal obtenida antes de confirmar la descarga."
        if preview_resp.get("image_path"):
            model["image_path"] = preview_resp.get("image_path")
        return model

    def _build_preview_error_model(self, item: dict, order_idx: int, message: str) -> dict:
        kind = item.get("kind") or ""
        model = self._build_item_preview_model(item, order_idx)
        theme = self._preview_theme(kind, bool(item.get("selected", True)))
        model.update({
            "badge": theme.get("badge", "DOCUMENTO"),
            "badge_fill": "#FEE2E2",
            "badge_text": "#991B1B",
            "status": "Sin vista previa",
            "status_fill": "#FEE2E2",
            "status_text": "#991B1B",
            "sections": [
                ("Resultado", [message or "No se pudo construir la vista previa real."]),
                ("Documento", [
                    f"Título: {_norm_ws(item.get('titulo') or theme.get('badge', 'Documento'))}",
                    f"Tipo: {item.get('kind_label') or kind or 'Documento'}",
                    f"Fecha: {item.get('fecha') or 'Sin fecha'}",
                ]),
            ],
            "footer": "No fue posible obtener el archivo temporal para esta fila.",
        })
        model.pop("image_path", None)
        return model

    def _build_multi_preview_model(self, items: list[dict]) -> dict:
        lines = []
        for pos, item in enumerate(items[:8], start=1):
            estado = "Incluido" if item.get("selected", True) else "Omitido"
            kind = item.get("kind_label") or item.get("kind") or "Documento"
            fecha = item.get("fecha") or "Sin fecha"
            titulo = _norm_ws(item.get("titulo") or "")
            lines.append(f"{pos}. [{estado}] {kind} · {fecha} · {titulo}")
        if len(items) > 8:
            lines.append(f"... y {len(items) - 8} fila(s) más en la selección actual.")

        return {
            "badge": "SELECCIÓN",
            "badge_fill": "#E0E7FF",
            "badge_text": "#3730A3",
            "status": "Movimiento conjunto",
            "status_fill": "#DBEAFE",
            "status_text": "#1D4ED8",
            "title": f"{len(items)} documentos seleccionados",
            "subtitle": "Si arrastrás ahora, todas estas filas se mueven juntas manteniendo su orden relativo.",
            "sections": [
                ("Filas activas", lines),
                ("Qué podés hacer ahora", [
                    "Arrastrar la selección para cambiar el bloque completo de posición.",
                    "Usar el selector ●/○ de cada fila o el menú superior para marcar o limpiar grupos.",
                ]),
            ],
            "footer": "Vista previa agrupada de la selección actual.",
        }

    def _current_single_selected_item(self) -> dict | None:
        selected_uids = self._selected_uids()
        if len(selected_uids) != 1:
            return None
        return self._item_by_uid(selected_uids[0])

    def _can_generate_real_preview(self, item: dict | None) -> bool:
        return bool(item and self._preview_session and (item.get("kind") or "") == "operacion")

    def _preview_unavailable_message(self, item: dict | None) -> str:
        kind = (item or {}).get("kind") or ""
        if kind == "adjunto":
            return "La vista previa de adjuntos no está disponible."
        if kind in {"informe_mpf", "informe_rnr"}:
            return "La vista previa de informes no está disponible."
        return "La vista previa real sólo está disponible para operaciones."

    def _build_preview_unavailable_model(self, item: dict, order_idx: int, message: str) -> dict:
        model = self._build_item_preview_model(item, order_idx)
        model["sections"] = [("Vista previa", [message])] + model.get("sections", [])
        model["footer"] = "Sólo las operaciones pueden generar una vista previa real desde el Libro."
        model.pop("image_path", None)
        return model

    def _cancel_preview_jobs(self):
        for attr in ("_preview_poll_job", "_preview_debounce_job"):
            job = getattr(self, attr, None)
            if job:
                try:
                    self.after_cancel(job)
                except Exception:
                    pass
                setattr(self, attr, None)
        self._set_busy_cursor(False)

    def _update_preview_button_state(self):
        item = self._current_single_selected_item()
        enabled = self._can_generate_real_preview(item)
        try:
            self.preview_btn.config(state=("normal" if enabled else "disabled"))
        except Exception:
            pass
        if not enabled and not self.preview_status_var.get():
            self.preview_status_var.set("")

    def _update_index_name_editor(self):
        item = self._current_single_selected_item()
        text = _indice_nombre_for_item(item) if item else ""
        self._syncing_index_name = True
        try:
            self.index_name_var.set(text)
        finally:
            self._syncing_index_name = False
        try:
            self.index_name_entry.configure(state=("normal" if item else "disabled"))
        except Exception:
            pass

    def _refresh_preview_for_current_item(self, item: dict):
        uid = str((item or {}).get("uid") or "")
        if not uid:
            return
        order_idx = next((idx for idx, cur in enumerate(self.items) if cur.get("uid") == uid), 0)
        cached = self._preview_local_cache.get(uid)
        if cached and cached.get("ok"):
            self._set_preview_model(self._build_real_item_preview_model(item, order_idx, cached))
            return
        if cached and not cached.get("ok"):
            self._set_preview_model(self._build_preview_error_model(item, order_idx, cached.get("message") or "No se pudo generar la vista previa real."))
            return
        if self._can_generate_real_preview(item):
            self._set_preview_model(self._build_item_preview_model(item, order_idx))
            return
        msg = self._preview_unavailable_message(item)
        self._set_preview_model(self._build_preview_unavailable_model(item, order_idx, msg))

    def _on_index_name_var_changed(self, *_args):
        if getattr(self, "_syncing_index_name", False):
            return
        item = self._current_single_selected_item()
        if not item:
            return
        item["index_name"] = _norm_ws(self.index_name_var.get() or "")
        self._refresh_preview_for_current_item(item)

    def _drain_preview_queue(self):
        session = self._preview_session or {}
        q = session.get("preview_response_q")
        if q is None:
            return
        while True:
            try:
                resp = q.get_nowait()
            except queue.Empty:
                break
            except Exception:
                break
            uid = str(resp.get("uid") or "")
            if uid:
                self._preview_local_cache[uid] = resp

    def _apply_preview_response(self, item: dict, preview_resp: dict):
        self._set_busy_cursor(False)
        order_idx = next((idx for idx, cur in enumerate(self.items) if cur.get("uid") == item.get("uid")), 0)
        if preview_resp.get("ok"):
            self.preview_status_var.set("")
            self._set_preview_model(self._build_real_item_preview_model(item, order_idx, preview_resp))
        else:
            self.preview_status_var.set(preview_resp.get("message") or "")
            self._set_preview_model(self._build_preview_error_model(item, order_idx, preview_resp.get("message") or "No se pudo generar la vista previa real."))

    def _set_busy_cursor(self, busy: bool):
        cursor = "watch" if busy else ""
        for widget in (
            self,
            getattr(self, "preview_canvas", None),
            getattr(self, "preview_scroll", None),
            getattr(self, "tree", None),
        ):
            if widget is None:
                continue
            try:
                widget.configure(cursor=cursor)
            except Exception:
                pass

    def _on_preview_mousewheel(self, event):
        canvas = getattr(self, "preview_canvas", None)
        if canvas is None:
            return "break"
        try:
            if getattr(event, "delta", 0):
                canvas.yview_scroll(int(-event.delta / 120), "units")
            elif getattr(event, "num", None) == 4:
                canvas.yview_scroll(-3, "units")
            elif getattr(event, "num", None) == 5:
                canvas.yview_scroll(3, "units")
        except Exception:
            pass
        return "break"

    def _poll_preview_for_uid(self, uid: str, deadline: datetime.datetime):
        self._preview_poll_job = None
        item = self._current_single_selected_item()
        if not item or str(item.get("uid") or "") != uid:
            return
        self._drain_preview_queue()
        cached = self._preview_local_cache.get(uid)
        if cached:
            self._apply_preview_response(item, cached)
            return
        if datetime.datetime.now() >= deadline:
            msg = "La vista previa de la operación tardó demasiado y no se pudo mostrar."
            self._set_busy_cursor(False)
            self.preview_status_var.set(msg)
            order_idx = next((idx for idx, cur in enumerate(self.items) if cur.get("uid") == uid), 0)
            self._set_preview_model(self._build_preview_error_model(item, order_idx, msg))
            return
        self._preview_poll_job = self.after(120, lambda: self._poll_preview_for_uid(uid, deadline))

    def _request_real_preview_async(self, uid: str, force: bool = False, timeout_ms: int = 25000):
        item = self._current_single_selected_item()
        if not item or item.get("uid") != uid or not self._can_generate_real_preview(item):
            return

        cached = self._preview_local_cache.get(uid)
        if cached and not force:
            self._apply_preview_response(item, cached)
            return
        if force:
            self._preview_local_cache.pop(uid, None)

        try:
            self._preview_session["preview_request_q"].put_nowait({"action": "preview", "uid": uid, "force": bool(force)})
        except Exception:
            self.preview_status_var.set("No pude solicitar la vista previa real.")
            return

        self.preview_status_var.set("Generando vista previa de la operación...")
        self._set_busy_cursor(True)
        deadline = datetime.datetime.now() + datetime.timedelta(milliseconds=max(1000, int(timeout_ms)))
        self._preview_poll_job = self.after(120, lambda: self._poll_preview_for_uid(uid, deadline))

    def _load_real_preview(self, force: bool = False):
        self._cancel_preview_jobs()
        item = self._current_single_selected_item()
        self._update_preview_button_state()
        if not item or not self._can_generate_real_preview(item):
            return
        uid = str(item.get("uid") or "")
        if not uid:
            return
        self._request_real_preview_async(uid, force=force)

    def _update_preview(self):
        self._drain_preview_queue()
        selected_uids = self._selected_uids()
        if not selected_uids:
            self.preview_status_var.set("")
            self._cancel_preview_jobs()
            self._set_preview_model(None)
            self._update_preview_button_state()
            self._update_index_name_editor()
            return

        if len(selected_uids) > 1:
            selected_items = [self._item_by_uid(uid) for uid in selected_uids]
            selected_items = [item for item in selected_items if item]
            self.preview_status_var.set("")
            self._cancel_preview_jobs()
            self._set_preview_model(self._build_multi_preview_model(selected_items))
            self._update_preview_button_state()
            self._update_index_name_editor()
            return

        uid = selected_uids[0]
        item = self._item_by_uid(uid)
        if not item:
            return
        self._update_index_name_editor()
        self._update_preview_button_state()
        self._cancel_preview_jobs()
        order_idx = next((idx for idx, cur in enumerate(self.items) if cur.get("uid") == uid), 0)
        if self._can_generate_real_preview(item):
            self.preview_status_var.set("Generando vista previa de la operación...")
            self._set_preview_model(self._build_item_preview_model(item, order_idx))
            self._preview_debounce_job = self.after(120, lambda: self._load_real_preview(force=False))
        else:
            msg = self._preview_unavailable_message(item)
            self.preview_status_var.set(msg)
            self._set_preview_model(self._build_preview_unavailable_model(item, order_idx, msg))

    def _refresh(self, preserve_selection: list[str] | None = None, refresh_preview: bool = True):
        preserve_selection = preserve_selection or self._selected_uids()
        yview = self.tree.yview()
        dragging_selected = set()
        if self._drag_state and self._drag_state.get("dragging"):
            dragging_selected = set(self._selected_uids())
        self.tree.delete(*self.tree.get_children())
        for idx, item in enumerate(self.items, start=1):
            uid = item["uid"]
            enabled = bool(item.get("selected", True))
            if uid in dragging_selected:
                row_tag = "drag_even" if idx % 2 == 0 else "drag_odd"
            else:
                row_tag = (
                    "selected_even" if enabled and idx % 2 == 0 else
                    "selected_odd" if enabled else
                    "muted_even" if idx % 2 == 0 else
                    "muted_odd"
                )
            self.tree.insert(
                "",
                "end",
                iid=uid,
                tags=(row_tag,),
                values=(
                    idx,
                    "●" if enabled else "○",
                    item.get("kind_label") or "",
                    item.get("fecha") or "",
                    item.get("titulo") or "",
                    item.get("detalle") or "",
                ),
            )
        valid = [uid for uid in preserve_selection if self.tree.exists(uid)]
        if valid:
            self.tree.selection_set(valid)
            try:
                self.tree.focus(valid[0])
            except Exception:
                pass
        elif self.items:
            first_uid = self.items[0]["uid"]
            self.tree.selection_set(first_uid)
            try:
                self.tree.focus(first_uid)
            except Exception:
                pass
        try:
            if yview:
                self.tree.yview_moveto(yview[0])
        except Exception:
            pass
        self._refresh_type_filter_buttons()
        self._update_summary()
        if refresh_preview:
            self._update_preview()

    def _toggle_selected_uids(self, uids):
        uid_set = {str(uid) for uid in (uids or []) if uid}
        if not uid_set:
            return
        for item in self.items:
            if str(item.get("uid") or "") in uid_set:
                item["selected"] = not bool(item.get("selected", True))
        self._refresh(list(uid_set))

    def _set_selected(self, selected: bool):
        selected_uids = set(self._selected_uids())
        if not selected_uids:
            return
        for item in self.items:
            if item["uid"] in selected_uids:
                item["selected"] = selected
        self._refresh(list(selected_uids))

    def _toggle_category_selection(self, category: str):
        category = _norm_ws(category or "")
        if not category:
            return
        touched = []
        matching = [item for item in self.items if self._category_for_item(item) == category]
        if not matching:
            return
        target_value = not all(item.get("selected", True) for item in matching)
        for item in self.items:
            if self._category_for_item(item) == category:
                item["selected"] = target_value
                touched.append(str(item.get("uid") or ""))
        self._refresh(touched or None)

    def _toggle_selected(self):
        selected_uids = set(self._selected_uids())
        if not selected_uids:
            return
        for item in self.items:
            if item["uid"] in selected_uids:
                item["selected"] = not bool(item.get("selected", True))
        self._refresh(list(selected_uids))

    def _mark_all(self):
        for item in self.items:
            item["selected"] = True
        self._refresh()

    def _clear_all(self):
        for item in self.items:
            item["selected"] = False
        self._refresh()

    def _remove_unselected(self):
        selected = self._selected_uids()
        before = len(self.items)
        self.items = [item for item in self.items if item.get("selected", True)]
        if len(self.items) == before:
            return
        keep = [uid for uid in selected if any(str(item.get("uid") or "") == uid for item in self.items)]
        self._refresh(keep or None)

    def _move(self, delta: int):
        selected = self._selected_uids()
        if not selected:
            return
        selected_set = set(selected)
        if delta < 0:
            for idx in range(1, len(self.items)):
                if self.items[idx]["uid"] in selected_set and self.items[idx - 1]["uid"] not in selected_set:
                    self.items[idx - 1], self.items[idx] = self.items[idx], self.items[idx - 1]
        else:
            for idx in range(len(self.items) - 2, -1, -1):
                if self.items[idx]["uid"] in selected_set and self.items[idx + 1]["uid"] not in selected_set:
                    self.items[idx + 1], self.items[idx] = self.items[idx], self.items[idx + 1]
        self._refresh(selected)

    def _restore_order(self):
        preserve = self._selected_uids()
        self.items = [self._items_by_uid[uid] for uid in self._default_uid_order if uid in self._items_by_uid]
        self._refresh(preserve)

    def _on_tree_double_click(self, event):
        iid = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)
        if iid and col == "#2":
            self.tree.selection_set(iid)
            self._toggle_selected_uids([iid])
            return "break"

    def _on_space(self, _event):
        self._toggle_selected()
        return "break"

    def _on_tree_press(self, event):
        self._hide_type_filter_popover()
        iid = self.tree.identify_row(event.y)
        region = self.tree.identify_region(event.x, event.y)
        col = self.tree.identify_column(event.x)
        if iid and region == "cell" and col == "#2":
            self.tree.selection_set(iid)
            self._toggle_selected_uids([iid])
            self._drag_state = None
            self._hide_drop_indicator()
            return "break"
        if iid and region in {"cell", "tree"}:
            self._drag_state = {
                "iid": iid,
                "x": event.x,
                "y": event.y,
                "dragging": False,
                "drop_target": None,
            }
        else:
            self._drag_state = None
            self._hide_drop_indicator()
        self.after_idle(self._update_preview)

    def _drag_ghost_text(self, selected: list[str]) -> str:
        items = [self._item_by_uid(uid) for uid in selected]
        items = [item for item in items if item]
        if not items:
            return "Moviendo documento"
        first = _norm_ws(items[0].get("titulo") or items[0].get("kind_label") or "Documento")
        if len(items) == 1:
            return first[:88]
        return f"{first[:64]}  +{len(items) - 1} más"

    def _show_drag_ghost(self, selected: list[str], x_root: int, y_root: int):
        self._hide_drag_ghost()
        ghost = Toplevel(self)
        ghost.overrideredirect(True)
        try:
            ghost.attributes("-topmost", True)
        except Exception:
            pass
        try:
            ghost.attributes("-alpha", 0.76)
        except Exception:
            pass
        frame = Frame(ghost, bg="#0F172A", bd=1, relief="solid")
        frame.pack()
        label = Label(
            frame,
            text=self._drag_ghost_text(selected),
            bg="#F1F5F9",
            fg="#0F172A",
            font=("Segoe UI", 10, "bold"),
            padx=12,
            pady=7,
            anchor="w",
            justify="left",
        )
        label.pack(fill="both", expand=True)
        self._drag_ghost = ghost
        self._move_drag_ghost(x_root, y_root)

    def _move_drag_ghost(self, x_root: int, y_root: int):
        ghost = getattr(self, "_drag_ghost", None)
        if ghost is None:
            return
        try:
            ghost.geometry(f"+{int(x_root) + 16}+{int(y_root) + 18}")
        except Exception:
            pass

    def _hide_drag_ghost(self):
        ghost = getattr(self, "_drag_ghost", None)
        self._drag_ghost = None
        if ghost is None:
            return
        try:
            ghost.destroy()
        except Exception:
            pass

    def _set_drop_indicator(self, y_pos: int | None):
        line = getattr(self, "_drop_indicator", None)
        if y_pos is None:
            if line is not None:
                try:
                    line.place_forget()
                except Exception:
                    pass
            return
        if line is None:
            line = Frame(self.tree_box, bg="#111111", height=2)
            self._drop_indicator = line
        try:
            width = max(int(self.tree.winfo_width() or 0) - 6, 40)
            x = int(self.tree.winfo_x() or 0) + 3
            y = int(self.tree.winfo_y() or 0) + max(0, int(y_pos) - 1)
            line.place(x=x, y=y, width=width, height=2)
            line.lift()
        except Exception:
            pass

    def _hide_drop_indicator(self):
        self._set_drop_indicator(None)

    def _auto_scroll_tree(self, y_pos: int):
        try:
            tree_h = int(self.tree.winfo_height() or 0)
        except Exception:
            tree_h = 0
        if tree_h <= 0:
            return
        try:
            if y_pos < 28:
                self.tree.yview_scroll(-1, "units")
            elif y_pos > tree_h - 28:
                self.tree.yview_scroll(1, "units")
        except Exception:
            pass

    def _compute_drop_target(self, y_pos: int, selected: list[str] | None = None):
        selected = selected or self._selected_uids()
        if not selected:
            return None
        selected_set = set(selected)
        dragged = [item for item in self.items if item["uid"] in selected_set]
        remaining = [item for item in self.items if item["uid"] not in selected_set]
        if not dragged:
            return None
        if not remaining:
            return {"kind": "all", "insert_at": 0, "line_y": 1}

        target_uid = self.tree.identify_row(y_pos)
        if not target_uid:
            if y_pos <= 0:
                return {"kind": "top", "insert_at": 0, "line_y": 1}
            return {
                "kind": "bottom",
                "insert_at": len(remaining),
                "line_y": max(int(self.tree.winfo_height() or 0) - 2, 1),
            }
        if target_uid in selected_set:
            return None

        bbox = self.tree.bbox(target_uid)
        after = False
        line_y = None
        if bbox:
            after = y_pos > (bbox[1] + (bbox[3] / 2.0))
            line_y = int(bbox[1] + (bbox[3] if after else 0))

        target_idx = next((idx for idx, item in enumerate(remaining) if item["uid"] == target_uid), None)
        if target_idx is None:
            return None
        insert_at = target_idx + (1 if after else 0)
        return {
            "kind": "between",
            "target_uid": target_uid,
            "after": bool(after),
            "insert_at": insert_at,
            "line_y": line_y,
        }

    def _apply_drop_target(self, target: dict | None, selected: list[str] | None = None):
        selected = selected or self._selected_uids()
        if not selected or not target:
            return False
        selected_set = set(selected)
        dragged = [item for item in self.items if item["uid"] in selected_set]
        remaining = [item for item in self.items if item["uid"] not in selected_set]
        if not dragged:
            return False
        insert_at = int(target.get("insert_at", len(remaining)))
        insert_at = max(0, min(insert_at, len(remaining)))
        new_items = remaining[:insert_at] + dragged + remaining[insert_at:]
        if [item["uid"] for item in new_items] == [item["uid"] for item in self.items]:
            return False
        self.items = new_items
        return True

    def _on_tree_motion(self, event):
        state = self._drag_state
        if not state or not state.get("iid"):
            return
        if not state.get("dragging"):
            if abs(event.y - state["y"]) < 6 and abs(event.x - state["x"]) < 6:
                return
            if state["iid"] not in self._selected_uids():
                self.tree.selection_set(state["iid"])
            state["dragging"] = True
            try:
                self.tree.configure(cursor="fleur")
            except Exception:
                pass
            self._refresh(self._selected_uids(), refresh_preview=False)
            self._show_drag_ghost(self._selected_uids(), event.x_root, event.y_root)
        self._auto_scroll_tree(event.y)
        self._move_drag_ghost(event.x_root, event.y_root)
        target = self._compute_drop_target(event.y, self._selected_uids())
        state["drop_target"] = target
        self._set_drop_indicator((target or {}).get("line_y"))
        return "break"

    def _on_tree_release(self, event):
        state = self._drag_state
        self._drag_state = None
        self._hide_drag_ghost()
        self._hide_drop_indicator()
        try:
            self.tree.configure(cursor="")
        except Exception:
            pass
        if not state:
            return
        if state.get("dragging"):
            selected = self._selected_uids()
            changed = self._apply_drop_target(state.get("drop_target"), selected)
            self._refresh(selected if changed else selected, refresh_preview=False)
            try:
                if selected:
                    self.tree.see(selected[0])
            except Exception:
                pass
            self._update_preview()
            return "break"
        self._update_preview()

    def _accept(self):
        self._cancel_preview_jobs()
        self._hide_drag_ghost()
        self._hide_drop_indicator()
        self._hide_type_filter_popover()
        selected_items = [dict(item) for item in self.items if item.get("selected", True)]
        if not selected_items:
            messagebox.showwarning("Sin seleccion", "Marca al menos un item para descargar.", parent=self)
            return
        self.result = selected_items
        self.destroy()

    def _cancel(self):
        self._cancel_preview_jobs()
        self._hide_drag_ghost()
        self._hide_drop_indicator()
        self._hide_type_filter_popover()
        self.result = None
        self.destroy()

    def show(self):
        try:
            self.grab_set()
        except Exception:
            pass
        try:
            self.deiconify()
            self.lift()
            self.focus_force()
        except Exception:
            pass
        self.wait_window()
        return self.result


# ------------------------- INTERFAZ ttkbootstrap ----------------------------
class App:
    def __init__(self, master):
        master.title("Descargador de expediente SAC")
        master.geometry("1020x780")
        master.minsize(920, 700)
        master.columnconfigure(0, weight=1)
        master.rowconfigure(0, weight=1)
        load_dotenv()

        _apply_ui_theme(master)

        shell = ttk.Frame(master, padding=(18, 16, 18, 14))
        shell.grid(row=0, column=0, sticky="nsew")
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(5, weight=1)

        ttk.Label(shell, text="Descarga de Expediente SAC", style="Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            shell,
            text="Ingresá credenciales, definí opciones y luego elegí la carpeta de destino.",
            style="Hint.TLabel",
            wraplength=960,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(2, 10))

        form = ttk.LabelFrame(shell, text="Datos de acceso", style="Card.TLabelframe")
        form.grid(row=2, column=0, sticky="ew")
        form.columnconfigure(1, weight=1)

        self.tele_user = StringVar(value=os.getenv("TELE_USER", ""))
        self.tele_pwd = StringVar(value=os.getenv("TELE_PASS", ""))
        self.intra_user = StringVar(value=os.getenv("INTRA_USER", os.getenv("SAC_USER", "")))
        self.intra_pwd = StringVar(value=os.getenv("INTRA_PASS", os.getenv("SAC_PASS", "")))
        self.exp = StringVar()
        self.incluir_adjuntos = BooleanVar(value=_env_true("UI_INCLUIR_ADJUNTOS", "1"))
        ocr_default = (os.getenv("OCR_MODE", "auto") or "auto").strip().lower() != "off"
        self.aplicar_ocr = BooleanVar(value=ocr_default)

        ttk.Label(form, text="Usuario Teletrabajo (opcional):").grid(row=0, column=0, sticky="w", pady=3)
        ttk.Entry(form, textvariable=self.tele_user).grid(row=0, column=1, sticky="ew", padx=(12, 0), pady=3)

        ttk.Label(form, text="Clave Teletrabajo (opcional):").grid(row=1, column=0, sticky="w", pady=3)
        ttk.Entry(form, textvariable=self.tele_pwd, show="*").grid(row=1, column=1, sticky="ew", padx=(12, 0), pady=3)

        ttk.Label(form, text="Usuario Intranet:").grid(row=2, column=0, sticky="w", pady=3)
        ttk.Entry(form, textvariable=self.intra_user).grid(row=2, column=1, sticky="ew", padx=(12, 0), pady=3)

        ttk.Label(form, text="Clave Intranet:").grid(row=3, column=0, sticky="w", pady=3)
        ttk.Entry(form, textvariable=self.intra_pwd, show="*").grid(row=3, column=1, sticky="ew", padx=(12, 0), pady=3)

        ttk.Label(form, text="Número de expediente:").grid(row=4, column=0, sticky="w", pady=3)
        ttk.Entry(form, textvariable=self.exp).grid(row=4, column=1, sticky="ew", padx=(12, 0), pady=3)

        opts = ttk.LabelFrame(shell, text="Opciones de descarga", style="Card.TLabelframe")
        opts.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        opts.columnconfigure(0, weight=1)

        ttk.Checkbutton(
            opts,
            text="Incluir adjuntos (adjuntos, informes técnicos MPF e informes RNR)",
            variable=self.incluir_adjuntos,
        ).grid(row=0, column=0, sticky="w", pady=2)

        ttk.Checkbutton(
            opts,
            text="Aplicar OCR al PDF final (texto seleccionable)",
            variable=self.aplicar_ocr,
        ).grid(row=1, column=0, sticky="w", pady=2)

        ttk.Label(
            opts,
            text="Si desactivás OCR, el PDF se genera más rápido, pero páginas escaneadas pueden quedar sin búsqueda.",
            style="Hint.TLabel",
            wraplength=920,
            justify="left",
        ).grid(row=2, column=0, sticky="w", pady=(4, 0))

        actions = ttk.Frame(shell)
        actions.grid(row=4, column=0, sticky="ew", pady=(12, 0))
        actions.columnconfigure(0, weight=1)

        ttk.Label(
            actions,
            text="El progreso y la bitácora técnica se muestran abajo, dentro de esta misma ventana.",
            style="Hint.TLabel",
            wraplength=760,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        self.btn_radiografia = ttk.Button(
            actions,
            text="Radiografía del expediente",
            command=self.run_radiografia,
            style="Accent.TButton",
        )
        self.btn_radiografia.grid(row=0, column=1, sticky="e", padx=(12, 0))

        self.btn = ttk.Button(actions, text="Descargar expediente", command=self.run, style="Accent.TButton")
        self.btn.grid(row=0, column=2, sticky="e", padx=(12, 0))

        progress = ttk.LabelFrame(shell, text="Progreso y bitácora", style="Card.TLabelframe")
        progress.grid(row=5, column=0, sticky="nsew", pady=(14, 0))
        progress.columnconfigure(0, weight=1)
        progress.rowconfigure(3, weight=1)
        self.progress_panel = progress

        self.progress_lbl = ttk.Label(progress, text="Estado actual: en espera", style="Status.TLabel")
        self.progress_lbl.grid(row=0, column=0, sticky="w")

        self.progress_sub = ttk.Label(
            progress,
            text="La descarga todavía no empezó.",
            style="Hint.TLabel",
            wraplength=940,
            justify="left",
        )
        self.progress_sub.grid(row=1, column=0, sticky="w", pady=(4, 8))

        self.progress_pb = ttk.Progressbar(progress, mode="indeterminate")
        self.progress_pb.grid(row=2, column=0, sticky="ew", pady=(0, 10))

        log_box = ttk.LabelFrame(progress, text="Bitácora técnica", padding=(8, 8, 8, 8))
        log_box.grid(row=3, column=0, sticky="nsew")
        log_box.columnconfigure(0, weight=1)
        log_box.rowconfigure(0, weight=1)

        self.progress_text = ScrolledText(log_box, wrap="word", height=16, font=("Consolas", 10))
        self.progress_text.grid(row=0, column=0, sticky="nsew")
        self.progress_text.configure(state="disabled")

        self._log_queue = None
        self._ui_handler = None
        self._progress_win = None
        self._progress_poll_job = None
        self._progress_active = False

    def _detalle_etapa_ui(self, etapa_txt: str) -> str:
        t = (etapa_txt or "").lower()
        if "seleccionando contenido y orden" in t or "radiografia del expediente" in t:
            return "Revisando operaciones, adjuntos e informes para elegir qué descargar y en qué orden."
        if "teletrabajo" in t or "intranet" in t or "sac" in t:
            return "Iniciando sesión y entrando al SAC."
        if "radiografia" in t or "radiografía" in t:
            return "Buscando el expediente en Radiografía y validando acceso."
        if "indice" in t or "índice" in t or "libro" in t:
            return "Leyendo el índice para respetar el orden del expediente."
        if "adjuntos" in t:
            return "Descargando adjuntos y vinculándolos a su operación."
        if "operaciones" in t:
            return "Capturando operaciones visibles y convirtiéndolas a PDF."
        if "tecnicos" in t or "técnicos" in t or "rnr" in t:
            return "Descargando informes complementarios."
        if "caratula" in t or "carátula" in t:
            return "Generando la portada del expediente."
        if "listo" in t:
            return "Proceso finalizado. El PDF quedó guardado en la carpeta elegida."
        return "Procesando. Mirá la bitácora de abajo para detalle técnico en vivo."

    def _set_progress_active(self, active: bool):
        self._progress_active = bool(active)
        try:
            if active:
                self.progress_pb.start(12)
            else:
                self.progress_pb.stop()
        except Exception:
            pass

    def _append_progress_log(self, msg: str):
        msg = _repair_mojibake_text(msg)
        try:
            self.progress_text.configure(state="normal")
            self.progress_text.insert("end", msg + "\n")
            self.progress_text.see("end")
            self.progress_text.configure(state="disabled")
        except Exception:
            pass

    def _poll_progress_queue(self):
        self._progress_poll_job = None
        q = self._log_queue
        try:
            while q is not None:
                msg = _repair_mojibake_text(q.get_nowait())
                if "[ETAPA] " in msg:
                    etapa_txt = _repair_mojibake_text(msg.split("[ETAPA] ", 1)[1].strip())
                    self.progress_lbl.config(text=f"Estado actual: {etapa_txt}")
                    self.progress_sub.config(text=self._detalle_etapa_ui(etapa_txt))
                elif "[CONFIG]" in msg:
                    conf = _repair_mojibake_text(msg.split("[CONFIG]", 1)[1].strip())
                    self.progress_sub.config(text=f"Opciones activas: {conf}")
                self._append_progress_log(msg)
        except queue.Empty:
            pass
        if self._progress_active or (self._log_queue is not None):
            self._progress_poll_job = self.btn.master.after(100, self._poll_progress_queue)

    def _reset_progress_panel(self):
        self.progress_lbl.config(text="Estado actual: iniciando...")
        self.progress_sub.config(text="Preparando entorno. Vas a ver cada etapa con una explicación simple.")
        try:
            self.progress_text.configure(state="normal")
            self.progress_text.delete("1.0", "end")
            self.progress_text.configure(state="disabled")
        except Exception:
            pass
        self._set_progress_active(True)
        if not self._progress_poll_job:
            self._progress_poll_job = self.btn.master.after(100, self._poll_progress_queue)

    def _finish_run_ui(self):
        try:
            while self._log_queue is not None:
                msg = _repair_mojibake_text(self._log_queue.get_nowait())
                if "[ETAPA] " in msg:
                    etapa_txt = _repair_mojibake_text(msg.split("[ETAPA] ", 1)[1].strip())
                    self.progress_lbl.config(text=f"Estado actual: {etapa_txt}")
                    self.progress_sub.config(text=self._detalle_etapa_ui(etapa_txt))
                elif "[CONFIG]" in msg:
                    conf = _repair_mojibake_text(msg.split("[CONFIG]", 1)[1].strip())
                    self.progress_sub.config(text=f"Opciones activas: {conf}")
                self._append_progress_log(msg)
        except queue.Empty:
            pass
        self.btn.config(state="normal")
        self.btn_radiografia.config(state="normal")
        self._set_progress_active(False)
        self._log_queue = None
        try:
            if self._ui_handler:
                logging.getLogger().removeHandler(self._ui_handler)
            self._ui_handler = None
        except Exception:
            pass

    def run(self):
        self._run_internal(usar_radiografia=False)

    def run_radiografia(self):
        self._run_internal(usar_radiografia=True)

    def _run_internal(self, usar_radiografia: bool):
        if not all([
            self.intra_user.get().strip(),
            self.intra_pwd.get().strip(),
            self.exp.get().strip(),
        ]):
            messagebox.showerror(
                "Faltan datos",
                "Completá usuario/clave de Intranet y número de expediente. "
                "Teletrabajo es opcional (solo si entrás por VPN).",
            )
            return

        carpeta = filedialog.askdirectory(title="Carpeta destino")
        if not carpeta:
            return

        incluir_adjuntos = bool(self.incluir_adjuntos.get())
        aplicar_ocr = bool(self.aplicar_ocr.get())

        self.btn.config(state="disabled")
        self.btn_radiografia.config(state="disabled")

        self._log_queue = queue.Queue()
        self._reset_progress_panel()

        if self._ui_handler:
            logging.getLogger().removeHandler(self._ui_handler)

        self._ui_handler = TkQueueHandler(self._log_queue)
        self._ui_handler.setFormatter(
            logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S")
        )
        logging.getLogger().addHandler(self._ui_handler)

        threading.Thread(
            target=lambda: self._ejecutar(Path(carpeta), incluir_adjuntos, aplicar_ocr, usar_radiografia),
            daemon=True,
        ).start()

    def _pedir_radiografia(self, items: list[dict], preview_state: dict | None = None):
        session = preview_state or {
            "done": threading.Event(),
            "result": None,
            "preview_request_q": queue.Queue(),
            "preview_response_q": queue.Queue(),
            "preview_cache": {},
        }

        def _open():
            try:
                dlg = RadiografiaDialog(
                    self.btn.master,
                    items,
                    title=f"Radiografia del expediente - Exp. {self.exp.get().strip()}",
                    preview_session=session,
                )
                session["result"] = dlg.show()
            finally:
                try:
                    session["done"].set()
                except Exception:
                    pass

        self.btn.master.after(0, _open)
        return session

    def _ejecutar(self, carpeta: Path, incluir_adjuntos: bool, aplicar_ocr: bool, usar_radiografia: bool):
        try:
            descargar_expediente(
                self.tele_user.get().strip(),
                self.tele_pwd.get().strip(),
                self.intra_user.get().strip(),
                self.intra_pwd.get().strip(),
                self.exp.get().strip(),
                carpeta,
                incluir_adjuntos=incluir_adjuntos,
                aplicar_ocr=aplicar_ocr,
                radiografia_selector=self._pedir_radiografia if usar_radiografia else None,
            )
        except Exception as e:
            self.btn.master.after(0, lambda m=str(e): messagebox.showerror("Error", m))
        finally:
            self.btn.master.after(0, self._finish_run_ui)


# ---------------------------- MAIN -------------------------------------
LOG = BASE_PATH / "debug.log"
logging.basicConfig(
    filename=str(LOG),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)

# Filtro para silenciar logs de diagnÃƒÂ³stico detallado de Informes TÃƒÂ©cnicos.
# Se puede reactivar seteando EXPEDIENTE_DIAG_INFORMES=1 en el entorno.
class _InfDiagFilter(logging.Filter):
    def filter(self, record):
        try:
            msg = record.getMessage() or ""
        except Exception:
            return True
        if os.getenv("EXPEDIENTE_DIAG_INFORMES", "").strip().lower() in {"1", "true", "yes", "on"}:
            return True
        prefixes = (
            "[INF] Fila ",
            "[INF] Abriendo secciÃƒÂ³n",
            "[INF] Filas InformesTecnicosMPF:",
            "[INF] Contenedor InformesTecnicosMPF",
        )
        return not any(msg.startswith(p) for p in prefixes)

logging.getLogger().addFilter(_InfDiagFilter())

import builtins as _bi
def _print_to_log(*args, **kwargs):
    try:
        logging.info(" ".join(str(a) for a in args))
    except Exception:
        pass
_bi.print = _print_to_log


def _set_win_appusermodelid(appid="SACDownloader.CBA"):
    """Establece el AppUserModelID en Windows para usar el ÃƒÂ­cono de la app."""
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(appid)
    except Exception:
        pass


def _set_tk_icon(root):
    """Intenta usar .ico; si falla, hace *fallback* a iconphoto."""
    ico = BASE_PATH / "icono3.ico"
    if not ico.exists():
        return
    try:
        root.iconbitmap(default=str(ico))
        return
    except Exception:
        pass
    # Fallback: usar PhotoImage (sirve si el .ico trae varias resoluciones)
    try:
        from PIL import Image, ImageTk
        img = Image.open(ico)
        root.iconphoto(True, ImageTk.PhotoImage(img))
    except Exception:
        pass
    # Si el mÃƒÂ©todo anterior falla, no hacemos nada mÃƒÂ¡s.


if __name__ == "__main__":
    # Inicializa la aplicaciÃƒÂ³n de escritorio.
    _set_win_appusermodelid("SACDownloader.CBA")
    root = _create_root()
    _set_tk_icon(root)  # usa icono3.ico desde BASE_PATH si estÃƒÂ¡ disponible
    App(root)
    root.mainloop()
# Nota: Al ejecutar con OCR_MODE=force, los adjuntos siempre salen con capa de texto.






























