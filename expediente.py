# -*- coding: utf-8 -*-


#!/usr/bin/env python3


"""
Descarga un expediente del SAC (vía Teletrabajo -> Portal de Aplicaciones -> Intranet),
adjuntos incluidos, y arma un único PDF.
"""

import os, sys, tempfile, shutil, datetime, threading, re, logging, contextlib
from pathlib import Path
from tkinter import Tk, Label, Entry, Button, StringVar, filedialog, messagebox
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from PyPDF2 import PdfReader, PdfWriter, PdfMerger
from reportlab.pdfgen import canvas
from PIL import Image
import requests, mimetypes
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import queue
from tkinter import Toplevel, ttk
from tkinter.scrolledtext import ScrolledText
from tempfile import TemporaryDirectory
import subprocess
import asyncio
# --- OCR WinRT: compatibilidad winsdk (Py 3.12+) y winrt (Py 3.8–3.11)
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

# Playwright buscará el navegador empaquetado aquí (portabiliza el .exe)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BASE_PATH / "ms-playwright")

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry




def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))

# --------- Seguridad/Permisos ---------
PERM_MSG = "El usuario no tiene los permisos suficientes para visualizar este contenido."


def _norm_ws(s: str) -> str:
    # normaliza nbsp, tabs y saltos ? 1 espacio; recorta extremos
    import re

    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _tiene_mensaje_permiso(texto: str) -> bool:
    # detectar el mensaje aunque esté rodeado de otros textos (p. ej. título del modal)
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

    # heurística por frases clave
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
    return ("ingrese nombre de usuario y contraseña" in t) or ("portal" in t and "intranet" in t)


def _pdf_contiene_mensaje_permiso(path: Path) -> bool:
    """Heurística: si el PDF trae el cartel de 'no tiene permisos', lo descartamos."""
    txt = ""
    try:
        # PyMuPDF rápido si está
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
    Considera válido todo contenido que NO sea el mensaje de permisos.
    (Hay operaciones muy cortas —p.ej. 'Se declara confidencial'— que antes se filtraban por longitud.)
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
    """Muestra la sección 'OPERACIONES' si está colapsada y la desplaza a la vista."""
    try:
        # toggles típicos
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

        # desplazar título/tabla a la vista
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
    logging.info(f"[ETAPA] {msg}")


def _esperar_radiografia_listo(page, timeout=120):
    """
    Espera a que Radiografía termine de cargar luego de la búsqueda.
    Considera AJAX: esperamos a ver carátula/fojas y que 'Operaciones' o 'Adjuntos' estén
    renderizados (o, al menos, que el encabezado del expediente cambie).
    """
    import time, re

    t0 = time.time()

    # algo de vida en la carátula
    pistas_ok = [
        "text=/\\bEXPEDIENTE N°\\b/i",
        "text=/\\bCarátula\\b/i",
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
            # ‘or True’ ? si carátula ya cargó, damos unos ms extra y seguimos
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
                    return loc  # no exijo is_visible: a veces está fuera de viewport
            except Exception:
                continue
    return None


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
    Caso contrario, deja el archivo como está (no rompe).
    """
    ext = path.suffix.lower()
    if ext == ".pdf":
        return path

    # imágenes
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
            )
            pdf = path.with_suffix(".pdf")
            if pdf.exists():
                return pdf
        except Exception:
            pass

    # si no pudimos convertir, devolvemos el original (se omitirá en la fusión si no es PDF)
    return path




# --- MERGE TURBO con fitz y fallback agrupado con PyPDF2 ---
try:
    import fitz  # PyMuPDF

    def fusionar_bloques_inline(bloques, destino: Path):
        """
        Fast path con PyMuPDF:
        - insert_pdf para cada bloque (ultra rápido).
        - Si header_text, dibuja marco+cabecera en las páginas recién insertadas.
        """
        dst = fitz.open()
        margin = 18
        for pdf_path, header_text in bloques:
            try:
                src = fitz.open(str(pdf_path))
            except Exception as e:
                logging.info(f"[MERGE:SKIP] {Path(pdf_path).name} · {e}")
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
                f"[MERGE:+FITZ] {Path(pdf_path).name} · páginas={end-start} · header={'sí' if header_text else 'no'}"
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

                # agregamos los paths tal cual (concatena rapidísimo)
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
                logging.info(f"[MERGE:HDR-ERR] {Path(pdf_path).name} · {e}")
            i += 1

        # Concat único
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

def fusionar_bloques_con_indice(bloques, destino: Path, index_title: str = "INDICE", keep_sidecar: bool = False):
    """
    Fusiona bloques PDF, inserta un índice clickable detrás de la carátula y devuelve
    (idx_page_count, relink_items) donde:
      - idx_page_count: cantidad de páginas del índice insertadas
      - relink_items  : [{'title', 'start', 'target', 'y'}] para re-inyectar links post-OCR
    También escribe <destino>.toc.json con ese mapeo.
    El archivo auxiliar se elimina automáticamente salvo que keep_sidecar sea True.
    """
    try:
        import fitz  # PyMuPDF
    except Exception:
        # Sin PyMuPDF: fusión simple sin índice
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

    # --- Inserción de bloques ---
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

    # --- Índice ---
    idx_page_count = 0
    relink_items = []

    if dst.page_count > 0 and len(items_info) > 1:
        try:
            first_rect = dst[0].rect
            pw, ph = first_rect.width, first_rect.height
            entries = sorted(items_info[1:], key=lambda x: x[1])  # salteo carátula
            if entries:
                fs = 12
                x_left = margin + 6
                x_right = pw - margin - 12
                title_y = margin + 10
                y_start = title_y + 22

                def _calc_pages(n_items: int) -> int:
                    if n_items <= 0:
                        return 0
                    y = y_start
                    pages = 1
                    for _ in range(n_items):
                        if y > ph - margin - 24:
                            pages += 1
                            y = y_start
                        y += fs + 8
                    return pages

                idx_page_count = _calc_pages(len(entries))
                index_pages = []
                for i in range(idx_page_count):
                    pno = 1 + i
                    try:
                        dst.new_page(pno=pno, width=pw, height=ph)
                        pg = dst.load_page(pno)
                    except Exception as e:
                        try: logging.warning(f"[INDICE] no se pudo crear/cargar página {pno}: {e}")
                        except Exception: pass
                        continue
                    if getattr(pg, "parent", None) is None:
                        try: logging.warning(f"[INDICE] página {pno} sin parent; se omite")
                        except Exception: pass
                        continue
                    index_pages.append(pg)

                if not index_pages:
                    try: logging.warning("[INDICE] no se generaron páginas válidas de índice")
                    except Exception: pass
                else:
                    idx_page_count = len(index_pages)
                    try: logging.info(f"[INDICE] entries={len(entries)} idx_pages={idx_page_count}")
                    except Exception: pass

                    def _foja_for_page(p: int):
                        # p es 0-based del PDF final
                        skip = 1 + idx_page_count  # carátula + índice
                        if p < skip:
                            return None
                        return 1 + ((p - skip) // 2)

                    use_foja_numbers = _env_true("FOJAS", "1")

                    page_idx = 0
                    idx_page = index_pages[page_idx]
                    try: idx_page.insert_text((x_left, title_y), index_title, fontsize=16)
                    except Exception: pass
                    y = y_start
                    toc_outline = []

                    for title, start_page in entries:
                        if y > ph - margin - 24:
                            page_idx += 1
                            if page_idx >= len(index_pages):
                                try:
                                    logging.warning("[INDICE] páginas de índice insuficientes; se detiene la generación")
                                except Exception:
                                    pass
                                break
                            idx_page = index_pages[page_idx]
                            try:
                                idx_page.insert_text((x_left, title_y), index_title + " (cont.)", fontsize=16)
                            except Exception:
                                pass
                            y = y_start

                        t = str(title)[:120]
                        try:
                            idx_page.insert_text((x_left, y), t, fontname="helv", fontsize=fs)
                        except Exception as e:
                            try:
                                logging.info(f"[INDICE] insert_text error: {e}")
                            except Exception:
                                pass
                            continue

                        # ancho título (punteado)
                        try:
                            tw_title = fitz.get_text_length(t, fontname="helv", fontsize=fs)
                        except Exception:
                            tw_title = fs * max(1, len(t)) * 0.6

                        target_page = start_page + idx_page_count  # 0-based
                        toc_outline.append([1, t, target_page + 1])

                        try:
                            logging.info(f"[INDICE] item title={t[:50]} start={start_page} target={target_page} y={y}")
                        except Exception:
                            pass

                        fj = _foja_for_page(target_page) if use_foja_numbers else (target_page + 1)
                        fj_txt = str(fj) if fj is not None else "-"

                        try:
                            tw = fitz.get_text_length(fj_txt, fontname="helv", fontsize=fs)
                        except Exception:
                            tw = fs * max(1, len(fj_txt)) * 0.6

                        left_end = x_left + tw_title + 6
                        dot_area_right = x_right - tw - 6
                        if dot_area_right > left_end:
                            try:
                                tw_dot = fitz.get_text_length(".", fontname="helv", fontsize=fs)
                            except Exception:
                                tw_dot = fs * 0.6
                            if tw_dot > 0:
                                n = int((dot_area_right - left_end) / tw_dot)
                                if n > 2:
                                    try:
                                        idx_page.insert_text((left_end, y), "." * n, fontname="helv", fontsize=fs)
                                    except Exception:
                                        pass

                        try:
                            idx_page.insert_text((x_right - tw, y), fj_txt, fontname="helv", fontsize=fs)
                        except Exception:
                            pass

                        # Rect clickable
                        link_rect = fitz.Rect(x_left - 2, y - fs, x_right, y + fs)
                        ok_link = _add_goto_link(idx_page, link_rect, target_page)
                        try:
                            logging.info(f"[INDICE] link_{'ok' if ok_link else 'fail'} {t[:50]} -> p{target_page}")
                        except Exception:
                            pass

                        relink_items.append({
                            "title": t,
                            "start": (idx_page.number + 1),   # 1-based
                            "target": (target_page + 1),      # 1-based
                            "y": float(y)
                        })
                        y += fs + 8

                    try:
                        if toc_outline:
                            dst.set_toc(toc_outline)
                    except Exception:
                        pass

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

                    # Diagnóstico: contar links por página del índice
                    try:
                        for p in index_pages:
                            ln, c = p.first_link, 0
                            while ln:
                                c += 1
                                ln = ln.next
                            logging.info(f"[INDICE] links en página {p.number+1}: {c}")
                    except Exception:
                        pass
            else:
                try: logging.info("[INDICE] sin entradas; no se genera índice")
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
                            left=36, right=36, line_h=20, pad_top=3, pad_bottom=3):
    """
    Reinyecta anotaciones clickeables en las páginas de índice tras OCR/fojas.
    items: [{'start':1,'target':7,'y':70,'title':'...'}, ...]
    """
    import fitz, math, os, time
    if not items:
        return True
    try:
        doc = fitz.open(str(pdf_path))

        # 1) limpiar links existentes en páginas de índice
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
                    time.sleep(0.5)
                    os.replace(str(tmp), str(pdf_path))
                finally:
                    tmp.unlink(missing_ok=True)
            else:
                doc.close()
                raise
        else:
            doc.close()
        return True
    except Exception as e:
        logging.info(f"[INDICE/LINK:ERR] {e}")
        return False


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
            logging.info(f"[{etiqueta}] links en página {pagina_1b}: {n}")
        doc.close()
    except Exception as e:
        logging.info(f"[{etiqueta}] no se pudo contar links: {e}")


def _listar_ops_ids_radiografia(sac, wait_ms: int | None = None, scan_frames: bool = True) -> list[str]:
    """
    Busca ids de operaciones en Radiografía de forma rápida.
    - Espera como máx. RADIO_OPS_WAIT_MS (default 1200 ms) en la page principal.
    - Si no encuentra, escanea frames con una espera mínima (300 ms c/u).
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
            m = re.search(r"VerDecretoHtml\('([^']+)'", href or oc)  # acepta GUID o numérico
            if m:
                ids.add(m.group(1))

    # Asegurar que la sección esté visible y hacer una pasada rápida
    try:
        _asegurar_seccion_operaciones_visible(sac)
    except Exception:
        pass

    # Salto rápido si se pide saltar el gate completo
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

    # Si aún no hay ids y está permitido, frames express (300 ms c/u, corta al primer hallazgo)
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
    # Si se solicita saltar el gate, asumir que podemos abrir
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
                        loc.evaluate("el=>el.click()")
                    except Exception:
                        continue

            import re

            dialog = sac.locator(
                ".ui-dialog, .modal, [role='dialog'], div[id*='TextoOp'], div[id*='TextoOperacion']"
            ).filter(has_text=re.compile(r"operaci[oó]n", re.I)).last

            try:
                dialog.wait_for(state="visible", timeout=180)
                contenido = (dialog.inner_text() or "")
            except Exception:
                contenido = ""
            finally:
                try:
                    dialog.locator(".ui-dialog-titlebar-close, .close, button[aria-label='Close']").first.click()
                except Exception:
                    pass

            # ? AQUÍ TAMBIÉN: sólo vale si hay contenido real
            return _contenido_operacion_valido(contenido)

    return False


def _texto_modal_operacion(dialog, timeout=500) -> str:
    """
    Devuelve el texto del modal 'TEXTO DE LA OPERACIÓN'.
    Si el contenido viene en un <iframe>, lee el body del frame.
    Hace polling corto hasta que haya contenido.
    """
    try:
        dialog.wait_for(state="visible", timeout=timeout)
    except Exception:
        pass

    # 1) ¿Tiene iframe?
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
                # esperar a que el body tenga algo de texto
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

    # 2) Fallback: texto del propio contenedor del modal
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

    # 3) Último recurso: HTML -> texto plano
    try:
        html = dialog.inner_html() or ""
        import re

        return re.sub(r"<[^>]+>", " ", html)
    except Exception:
        return ""


def _op_visible_con_contenido_en_radiografia(sac, op_id: str) -> bool:
    _kill_overlays(sac)

    def _abrir_via_click_o_js(sc):
        link = sc.locator(
            f"[href*=\"VerDecretoHtml('{op_id}')\"], [onclick*=\"VerDecretoHtml('{op_id}')\"]"
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
        return False  # ? si ni siquiera pudimos disparar el modal, NO hay acceso

    dialog = sac.locator(
        ".ui-dialog:has-text('TEXTO DE LA OPERACIÓN'), .modal:has-text('TEXTO DE LA OPERACIÓN')"
    ).last

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
    """Devuelve True si el modal de la operación muestra el cartel de permisos insuficientes."""
    # intentar abrir el modal igual que en _op_visible_con_contenido_en_radiografia
    for sc in [sac] + list(sac.frames):
        link = sc.locator(
            f"[href*=\"VerDecretoHtml('{op_id}')\"], [onclick*=\"VerDecretoHtml('{op_id}')\"]"
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

            dialog = sac.locator(
                ".ui-dialog:has-text('TEXTO DE LA OPERACIÓN'), .modal:has-text('TEXTO DE LA OPERACIÓN')"
            ).last
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

    return False  # si ni siquiera pudimos abrir, no afirmamos denegación explícita


# ------------------------- UTILIDADES PDF ------------------------------
def _estampar_header(origen: Path, destino: Path, texto="ADJUNTO"):
    """
    Dibuja un marco en todo el borde y un texto (e.g. 'ADJUNTO – archivo.pdf')
    en la parte superior de CADA página del PDF 'origen', y lo guarda en 'destino'.
    """
    # Camino rápido con PyMuPDF (fitz)
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
    Devuelve el frame/página que realmente contiene el Libro:
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

    # 1) página principal
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

    # 3) último recurso: el primer frame con anchors de operaciones
    for fr in getattr(libro, "frames", []):
        try:
            if fr.locator("a[onclick*='onItemClick'], [data-codigo]").first.count():
                return fr
        except Exception:
            continue

    return libro


def _all_scopes(root):
    """Itera la página y todos sus frames (profundidad)."""
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
        # SOLO dentro del contenedor del índice
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
            return  # no tocar nada fuera del índice

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
        # SOLO scrolleo del índice (nada de wheel global)
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
            # tabs/pills que guardan relación por aria-controls / clases con GUID
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

    # si el índice está en pestaña "Índice", mostrarla
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
    Convierte "javascript:VerAdjuntoFichero('29229802')" en una URL real,
    preservando el mismo /proxy/.
    """
    m = re.search(r"VerAdjuntoFichero\('(\d+)'\)", js_call or "")
    if not m:
        return None

    file_id = m.group(1)
    # Ruta real usada por SAC para un adjunto individual:
    real = f"https://www.tribunales.gov.ar/SacInterior/_Expedientes/Fichero.aspx?idFichero={file_id}"
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
    Usa pdfminer si está; si no, PyPDF2. Devuelve un entero.
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
    # Umbral por defecto más alto para ser estrictos al considerar que ya hay texto
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
    """Convierte cada página de un PDF en un archivo de imagen independiente.

    Se intentará usar `pdfimages (Poppler) si está disponible en el sistema.
    Si no se encuentra, se probará `pdftoppm. Como último recurso, se
    utilizará PyMuPDF <https://pymupdf.readthedocs.io/>_ (`fitz).

    Los archivos resultantes se nombran `page_001.png, page_002.png,
    etc. y se guardan en `out_dir.

    Parameters
    ----------
    pdf_path:
        Ruta al archivo PDF de origen.
    out_dir:
        Directorio donde se guardarán las imágenes.
    formato:
        Formato de salida: `"png" (por defecto) o "tiff".
    dpi:
        Resolución para el renderizado cuando se utiliza PyMuPDF o `pdftoppm.

    Returns
    -------
    list[str]
        Lista con las rutas de las imágenes generadas.

    Raises
    ------
    FileNotFoundError
        Si `pdf_path no existe.
    ValueError
        Si `formato no es "png" ni "tiff".
    RuntimeError
        Si no hay herramientas disponibles para realizar la conversión.
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
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return _renombrar_salida()
        except Exception:
            pass

    # 2) Intento: pdftoppm
    if shutil.which("pdftoppm"):
        cmd = ["pdftoppm", f"-{formato}", "-r", str(dpi), str(pdf_path), str(tmp_base)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return _renombrar_salida()
        except Exception:
            pass

    # 3) Fallback: PyMuPDF
    try:
        import fitz
    except Exception as e:  # pragma: no cover - se ejecuta solo si falta fitz
        raise RuntimeError(
            "No se encontraron 'pdfimages', 'pdftoppm' ni la librería PyMuPDF"
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
    Solo realiza OCR sobre “adjuntos” (páginas escaneadas / sin texto útil en el cuerpo).
    Probado con PyMuPDF 1.26.4 (MuPDF 1.26.7) en Windows / Python 3.12.

    ENV opcionales:
      OCR_DEBUG=1                -> logs extra
      OCR_INVISIBLE=0/1          -> si 1, texto invisible (no recomendado para selección)
      OCR_VISIBLE_TEXT=1         -> fuerza texto visible
      OCR_ROTATIONS="0,90,270"   -> rotaciones a probar
      OCR_SCALE=2.0              -> escalado previo para OCR
      PAGE_BODY_MIN_CHARS=50     -> umbral para “página ya tiene texto”
      OCR_USE_OCG=0/1            -> si 1, intenta capa OCG
      OCR_FONT="helv"            -> fuente PDF estándar a usar
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
            # Nota: algunos visores no permiten selección con render_mode=3
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
        """Heurística: sin texto de cuerpo + presencia/área de imagen relevante."""
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
            # adjunto si la/s imagen/es cubren una parte importante de la página
            if page_area > 0 and (img_area / page_area) > 0.35:
                return True
        except Exception:
            pass
        # último recurso: si no hay texto y hay al menos una imagen embebida
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

        # OCG opcional (no recomendado para compatibilidad de selección)
        ocr_layer = None
        if use_ocg:
            try:
                ocr_layer = out.add_ocg("OCR Layer", on=True, intent="View")
            except Exception as e:
                logging.info(f"[WINOCR] add_ocg falló, sigo sin OCG: {e}")
                ocr_layer = None

        # Recorrer páginas y hacer OCR SOLO en adjuntos
        for i in range(src.page_count):
            pg = src[i]

            # Si NO es adjunto -> copiar tal cual, sin OCR
            if not _is_attachment_page(pg):
                out.insert_pdf(src, from_page=i, to_page=i)
                if dbg:
                    logging.info(f"[WINOCR:DBG] page={i+1} sin OCR (no es adjunto)")
                continue

            # Renderizar imagen de esa página (solo para este adjunto)
            try:
                zoom = dpi / 72.0
                mat = fitz.Matrix(zoom, zoom)
                pix = pg.get_pixmap(matrix=mat, alpha=False)
                png_bytes = pix.tobytes("png")
                img_w, img_h = pix.width, pix.height
            except Exception as e:
                logging.info(f"[WINOCR] No pude rasterizar página {i+1}: {e}")
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

            # tamaño de la imagen “ganadora” (por si rotó)
            try:
                from PIL import Image as _Image
                import io as _io
                _imtmp = _Image.open(_io.BytesIO(best_bytes))
                img_w, img_h = _imtmp.size
            except Exception:
                pass

            # factores de escala imagen->PDF (¡sin invertir Y!)
            sx = page_w / float(img_w)
            sy = page_h / float(img_h)

            # copiar página original
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

            # Pegar la imagen de la página *encima* (sin cuadros rojos)
            # Usamos el render original (png_bytes) para que calce 1:1 con la página.
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
            # Más estricto: requiere más texto en el cuerpo para saltar OCR
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
        raise RuntimeError(f"No encontré control para {candidates}")
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
    Si NO hay proxy (Intranet directa), devuelve cadena vacía "" (no explota).
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

    # Links de la página
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
        # texto típico del portal clásico
        tiene_texto = (
            pg.get_by_text(re.compile(r"ingrese\s+nombre\s+de\s+usuario\s+y\s+contraseña", re.I))
            .first.count()
            > 0
        )
    except Exception:
        tiene_texto = False

    return tiene_pwd and tiene_texto


def _sac_host_base(page) -> str:
    """
    Si estamos proxificados (Teletrabajo), devolvés www.tribunales.gov.ar.
    Si NO hay proxy, devolvés esquema+host de la URL actual (p.ej. aplicaciones.tribunales.gov.ar).
    """
    u = getattr(page, "url", "") or ""
    try:
        pu = urlparse(u)
    except Exception:
        pu = None

    # Con proxy ? siempre www.tribunales.gov.ar (lo antepone _get_proxy_prefix)
    if "/proxy/" in u or "teletrabajo.justiciacordoba.gob.ar" in u:
        return "https://www.tribunales.gov.ar"

    # Sin proxy ? quedate en el mismo host (aplicaciones.tribunales.gov.ar)
    if pu and pu.scheme and pu.netloc:
        return f"{pu.scheme}://{pu.netloc}"

    # Fallback conservador
    return "https://aplicaciones.tribunales.gov.ar"


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
    def _first_visible_in_scopes(scopes, selectors):
        for sc in scopes:
            for sel in selectors:
                try:
                    loc = sc.locator(sel).first
                    if loc.count():
                        try:
                            loc.wait_for(state="visible", timeout=1500)
                        except Exception:
                            pass
                        if loc.is_visible():
                            return sc, loc
                except Exception:
                    pass
        return None, None

    scopes = [page] + list(page.frames)
    _kill_overlays(page)

    sc, txt = _first_visible_in_scopes(scopes, [
        "#txtNroExpediente",
        "input[id$='txtNroExpediente']",
        "input[name$='txtNroExpediente']",
        "xpath=//label[normalize-space()='Número de Expediente:']/following::input[1]",
        "xpath=//td[contains(normalize-space(.),'Número de Expediente')]/following::input[1]",
        "xpath=//input[@type='text' and (contains(@id,'Expediente') or contains(@name,'Expediente'))]",
        "input[type='text']"
    ])
    if not txt:
        _debug_dump(page, "no_txt_expediente")
        raise RuntimeError("No pude ubicar el campo 'Número de Expediente'.")

    try:
        txt.scroll_into_view_if_needed()
    except Exception:
        pass
    txt.click()
    txt.fill(str(nro_exp))

    # Enter o botón Buscar en el MISMO scope
    try:
        txt.press("Enter")
        sc.wait_for_load_state("networkidle")
    except Exception:
        pass

    # Enter ya se probó arriba. Si hace falta botón, buscá en pasos.
    btn = sc.locator("#btnBuscarExp, input[id$='btnBuscarExp']").first

    if not btn.count():
        # Botones 'Buscar' (por accesibilidad/role)
        import re
        btn = sc.get_by_role("button", name=re.compile(r"buscar", re.I)).first

    if not btn.count():
        # Inputs que suelen usarse como botón de búsqueda
        btn = sc.locator(
            "input[type='submit'][value*='Buscar'], "
            "input[type='image'][alt*='Buscar'], "
            "input[title*='Buscar']"
        ).first

    if not btn.count():
        # XPath en un selector **separado**, nunca mezclado con CSS
        btn = sc.locator(
            "xpath=//input[( @type='image' or @type='submit') "
            "and (contains(@id,'Buscar') or contains(@value,'Buscar') "
            "or contains(@alt,'Buscar') or contains(@title,'Buscar'))]"
        ).first

    if btn.count():
        try:
            btn.click()
            sc.wait_for_load_state("networkidle")
        except Exception:
            pass



# --- Usa el que ya funcionaba en Teletrabajo ---
def _abrir_libro_legacy(sac):
    """Abre '* Ver Expediente como Libro' y devuelve la Page del libro (flujo viejo)."""
    import re

    try:
        sac.locator("text=¿Qué puedo hacer?").first.click()
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

    # a) si nos mandó al login, loguear y volver a Radiografía + re-buscar
    def _volver_a_radiografia_y_buscar():
        proxy_prefix = _get_proxy_prefix(sac)
        sac.goto(proxy_prefix + URL_RADIOGRAFIA, wait_until="domcontentloaded")
        if nro_exp:  # <- re-busca el expediente
            _fill_radiografia_y_buscar(sac, nro_exp)

    # -- Gate de Radiografía: ¿hay operaciones y puedo ver su contenido? --
    STRICT = _env_true("STRICT_ONLY_VISIBLE_OPS", "0")
    CHECK_ALL = _env_true("STRICT_CHECK_ALL_OPS", "0")

    op_ids_rad = _listar_ops_ids_radiografia(sac)  # ? antes decía p_ids_rad

    # 1) ¿Se ve alguna operación por DOM?
    hay_ops = bool(op_ids_rad)

    # 2) Fallback robusto: ¿puedo abrir alguna operación y leer su contenido?
    if not hay_ops:
        hay_ops = _puedo_abrir_alguna_operacion(sac)

    if STRICT and not hay_ops:
        logging.info("[SEC] Radiografía: no pude detectar operaciones ? sin acceso. Abortando.")
        messagebox.showwarning("Sin acceso", "No tenés acceso a este expediente (no aparecen operaciones).")
        return

    # Si tengo ids, verifico UNA (o todas, según CHECK_ALL); si no, ya validé con el fallback
    perm_ok = True
    if op_ids_rad:
        ids_a_probar = op_ids_rad if CHECK_ALL else op_ids_rad[:1]
        # 1) Si ALGUNA operación probada muestra el cartel ? abortamos TODO
        if any(_op_denegada_en_radiografia(sac, _id) for _id in ids_a_probar):
            logging.info("[SEC] Radiografía mostró 'sin permisos' en al menos una operación. Abortando.")
            messagebox.showwarning(
                "Sin acceso",
                "No tenés permisos para visualizar el contenido de este expediente "
                "(al menos una operación está bloqueada). No se descargará nada.",
            )
            return

        # 2) Si ninguna está denegada explícitamente, exigimos que al menos una tenga contenido visible
        perm_ok = any(_op_visible_con_contenido_en_radiografia(sac, _id) for _id in ids_a_probar)
    elif not _puedo_abrir_alguna_operacion(sac):
        perm_ok = False

    if STRICT and not perm_ok:
        logging.info("[SEC] Radiografía: aparece grilla pero el contenido está bloqueado.")
        messagebox.showwarning(
            "Sin acceso", "No tenés permisos para visualizar el contenido de las operaciones. No se descargó nada."
        )
        return

    if "PortalWeb/LogIn/Login.aspx" in (sac.url or "") or "SacInterior/Login.aspx" in (sac.url or ""):
        _login_intranet(sac, intra_user, intra_pass)
        _volver_a_radiografia_y_buscar()

    # 0) por si el botón vive en "¿Qué puedo hacer?"
    try:
        sac.locator("text=¿Qué puedo hacer?").first.click()
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

                # si cayó al login ? volver a Radiografía y reintentar una vez
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

    # 2) Intento: ejecutar la función en page/frames (inline)
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

    # 3) Fallback: construir URL directa (AHORA sí, estando en Radiografía)
    # si por algún motivo volvimos a login, resolvelo primero
    if "PortalWeb/LogIn/Login.aspx" in (sac.url or "") or "SacInterior/Login.aspx" in (sac.url or ""):
        _login_intranet(sac, intra_user, intra_pass)
        _volver_a_radiografia_y_buscar()

    # lee los hidden en la página correcta
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
        raise RuntimeError("No encontré el id del expediente (hdIdExpediente/hdExpedienteId).")

    key = _read_hidden_generic(sac, ["hdIdExpedienteKey"]) or ""
    lvl = _read_hidden_generic(sac, ["hdNivelAcceso"]) or ""

    proxy_prefix = _get_proxy_prefix(sac)
    base_host = _sac_host_base(sac)  # ? usa mismo host (aplicaciones...) si no hay proxy
    base = f"{base_host}/SacInterior/_Expedientes/ExpedienteLibro.aspx"
    qs = f"idExpediente={exp_id}" + (f"&key={key}" if key else "") + (f"&nivelAcceso={lvl}" if lvl else "")
    url = (proxy_prefix + base) if proxy_prefix else base
    url = url + "?" + qs

    try:
        # Abrir el Libro en una nueva pestaña para no perder la Radiografía
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
        # Fallback: navegar en la pestaña actual (menos robusto)
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
    Encuentra y descarga los adjuntos que cuelgan de UNA operación dentro del Libro.
    - Descarga por la UI (Playwright).
    - Convierte a PDF si hace falta.
    - Descarta respuestas sin permiso.
    - Evita duplicados exactos (nombre+tamaño).
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

            # Normalización a PDF
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

            # Deduplicar por (nombre, tamaño)
            try:
                key = (pdf.name, pdf.stat().st_size)
            except Exception:
                key = (pdf.name, 0)
            if key in vistos:
                continue
            vistos.add(key)
            pdfs.append(pdf)
        except Exception:
            # Si algo abre otra pestaña y falla, seguimos con el resto
            continue

    return pdfs


def _descargar_adjuntos_grid_mapeado(sac, carpeta: Path) -> dict[str, list[Path]]:
    """
    Devuelve { op_id: [PDFs...] } leyendo la grilla “Adjuntos” de Radiografía.
    - Descarga cada adjunto por la UI (lo mismo que hacés a mano).
    - Convierte a PDF si hace falta.
    - Descarta respuestas sin permiso.
    - Evita duplicados exactos (nombre+tamaño).
    """
    mapeo: dict[str, list[Path]] = {}
    vistos: set[tuple[str, int]] = set()

    # Asegurar que la sección 'Adjuntos' esté visible
    try:
        toggle = sac.locator("a[href*=\"Seccion('Adjuntos')\"], a[onclick*=\"Seccion('Adjuntos')\"]").first
        cont = sac.locator("#divAdjuntos").first
        oculto = False
        if cont.count():
            try:
                oculto = cont.evaluate("el => getComputedStyle(el).display === 'none'")
            except Exception:
                pass
        if oculto and toggle.count():
            toggle.click()
            sac.wait_for_timeout(250)
        elif toggle.count():
            toggle.click()
            sac.wait_for_timeout(250)
    except Exception:
        pass

    filas = sac.locator("#cphDetalle_gvAdjuntos tr")
    total = filas.count() if filas else 0

    for i in range(1, total):  # saltear header
        fila = filas.nth(i)

        # op_id en la col. “Operación – Tipo de Operación”
        op_link = fila.locator("a[href*='VerDecretoHtml'], a[onclick*='VerDecretoHtml']").first
        op_id = None
        if op_link.count():
            href = op_link.get_attribute("href") or ""
            oc = op_link.get_attribute("onclick") or ""
            m = re.search(r"VerDecretoHtml\('([^']+)'\)", href or oc)
            if m:
                op_id = m.group(1)

        # link de adjunto
        file_link = fila.locator(
            "a[href*='VerAdjuntoFichero'], a[onclick*='VerAdjuntoFichero'], a[href*='Fichero.aspx']"
        ).first
        if not file_link.count():
            continue

        try:
            with sac.expect_download() as dl:
                try:
                    file_link.click()
                except Exception:
                    file_link.evaluate("el => el.click()")
            d = dl.value
            destino = carpeta / d.suggested_filename
            d.save_as(destino)

            # Normalización a PDF
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

            # Deduplicar por (nombre, tamaño)
            try:
                key = (pdf.name, pdf.stat().st_size)
            except Exception:
                key = (pdf.name, 0)
            if key in vistos:
                continue
            vistos.add(key)

            # Guardar en el mapeo
            mapeo.setdefault(op_id or "__SIN_OP__", []).append(pdf)
        except Exception:
            continue

    return mapeo

def _mapear_fechas_operaciones_radiografia(sac) -> tuple[dict[str, str], list[str]]:
    """
    Lee la grilla de Operaciones en Radiografía y devuelve:
      - dict { op_id -> 'dd/mm/aaaa' }
      - lista de fechas únicas en el ORDEN en que aparecen en la grilla (para intercalar cronológicamente)
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

        # fecha (buscar en celdas con patrón dd/mm/aaaa)
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

def _descargar_informes_tecnicos(sac, carpeta: Path) -> list[tuple[Path, str]]:
    """
    Descarga informes técnicos desde la sección 'INFORMES TÉCNICOS MPF' y devuelve [(PDF, fecha_mov)].
    - Abre SOLO esa sección (no toca Adjuntos).
    - Click en el ícono PDF (javascript:VerInformeMPF(...)).
    - Extrae 'Fecha Movimiento' de la fila (dd/mm/aaaa).
    """
    import re
    informes: list[tuple[Path, str]] = []
    vistos: set[tuple[str, int]] = set()

    # --- 1) Abrir específicamente la sección InformesTecnicosMPF ---
    try:
        # Si ya está visible, no hacemos nada
        cont = sac.locator("#divInformesTecnicosMPF").first
        visible = False
        if cont.count():
            try:
                visible = cont.evaluate("el => getComputedStyle(el).display !== 'none'")
            except Exception:
                visible = False

        if not visible:
            # Prioridad 1: ejecutar la función exacta del sitio
            try:
                sac.evaluate("() => { try { Seccion && Seccion('InformesTecnicosMPF'); } catch(e){} }")
            except Exception:
                pass

            # Prioridad 2: click en el anchor correcto (el que llama Seccion('InformesTecnicosMPF'))
            if cont.count():
                try:
                    # Subimos hasta el contenedor, luego buscamos el <a> con esa sección
                    sac.locator("a[href*=\"Seccion('InformesTecnicosMPF')\"], a[onclick*=\"Seccion('InformesTecnicosMPF')\"]").first.scroll_into_view_if_needed()
                except Exception:
                    pass
            try:
                a = sac.locator("a[href*=\"Seccion('InformesTecnicosMPF')\"], a[onclick*=\"Seccion('InformesTecnicosMPF')\"]").first
                if a.count():
                    try:
                        a.click()
                    except Exception:
                        a.evaluate("el=>el.click()")
            except Exception:
                pass

            # Prioridad 3: click directo en el ícono +/expandir de esa sección (id único)
            try:
                img = sac.locator("#imgInformesTecnicosMPF").first
                if img.count():
                    try:
                        img.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        img.click()
                    except Exception:
                        img.evaluate("el=>el.click()")
            except Exception:
                pass

            # Espera corta a que se materialice la grilla
            try:
                sac.wait_for_timeout(250)
            except Exception:
                pass
    except Exception:
        pass

    # --- 2) Asegurar que la tabla esté y tenga filas ---
    # (algunas skins tardan un poco en pintar la grilla)
    filas = None
    for _ in range(40):  # ~6s total
        filas = sac.locator("table[id*='gvInformesTecnicos'] tr, table[id*='gvInformesTecnicosMPF'] tr")

        # esperamos a que haya filas de datos y candidatos a link
        try:
            total_tmp = filas.count() if filas else 0
        except Exception:
            total_tmp = 0
        try:
            cand = sac.locator("#divInformesTecnicosMPF *[onclick*='VerInforme'], #divInformesTecnicosMPF *[href*='VerInforme'], #divInformesTecnicosMPF a:has(img[src*='pdf']), #divInformesTecnicosMPF a:has(img[src*='adobe']), #divInformesTecnicosMPF a:has(img[src*='Adobe'])")
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
        cand = sac.locator("#divInformesTecnicosMPF *[onclick*='VerInforme'], #divInformesTecnicosMPF *[href*='VerInforme'], #divInformesTecnicosMPF a:has(img[src*='pdf']), #divInformesTecnicosMPF a:has(img[src*='adobe']), #divInformesTecnicosMPF a:has(img[src*='Adobe'])")
        cand_n = cand.count()
        logging.info(f"[INF] Filas InformesTecnicosMPF: {total} | candidatos: {cand_n}")
    except Exception:
        pass
    if total <= 1:
        try:
            cont = sac.locator("#divInformesTecnicosMPF").first
            html = cont.inner_html() if cont and cont.count() else ""
            logging.info(f"[INF] Contenedor InformesTecnicosMPF HTML (recorte): { (html or '')[:2000] }")
        except Exception:
            pass
        return informes  # no hay filas

    # --- 3) Recorrer filas ---
    for i in range(1, total):  # saltear header
        fila = filas.nth(i)

        # Link del PDF (ícono Adobe) + GUID para invocación directa
        import re
        # Selector robusto: match por onclick/href que contenga VerInformeMPF y por ícono PDF
        link = fila.locator(
            "*[onclick*='VerInformeMPF'], *[href*='VerInformeMPF'], a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='pdf'])"
        ).first
        if not link.count():
            try:
                logging.info(f"[INF] Fila {i}: sin link/ícono de informe técnico")
                try:
                    html = fila.inner_html()
                    logging.info(f"[INF] Fila {i}: HTML fila (recortado): {html[:1000]}")
                except Exception:
                    pass
            except Exception:
                pass
            continue

        guid = None
        try:
            href = link.get_attribute("href") or ""
            oc = link.get_attribute("onclick") or ""
            m = re.search(r"VerInformeMPF\s*\(([^)]*)\)", f"{href} {oc}")
            if m:
                args = m.group(1)
                # tomar el primer argumento (entre comillas simples o dobles, o crudo)
                arg0 = args.split(",")[0].strip()
                if (len(arg0) >= 2) and ((arg0[0] == arg0[-1]) and arg0[0] in "'\""):
                    arg0 = arg0[1:-1]
                guid = arg0 or None
        except Exception:
            pass
        try:
            logging.info(f"[INF] Fila {i}: GUID: {'sí' if guid else 'no'}")
        except Exception:
            pass



        # Fecha Movimiento (buscar un dd/mm/aaaa en las celdas)
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
        try:
            logging.info(f"[INF] Fila {i}: fecha: {fecha or 'no detectada'}")
        except Exception:
            pass

        # Click -> download (directa, popup nuevo, popup reutilizado e inline PDF)
        from playwright.sync_api import TimeoutError as PWTimeoutError
        ctx = sac.context  # BrowserContext

        def _filename_from_cd(cd: str | None) -> str | None:
            import re, urllib.parse
            if not cd:
                return None
            m = re.search(r'filename\*?=([^;]+)', cd)
            if not m:
                return None
            val = m.group(1).strip().strip('"')
            if val.lower().startswith("utf-8''"):
                val = urllib.parse.unquote(val[7:])
            return Path(val).name

        def _guardar(bytes_, nombre_sugerido: str | None) -> Path | None:
            try:
                dst = carpeta / (nombre_sugerido or f"InformeTecnico_{i:03d}.pdf")
                with open(dst, "wb") as f:
                    f.write(bytes_)
                return dst
            except Exception:
                return None

        def _capturar_desde_pagina(p) -> Path | None:
            # a) evento de descarga
            try:
                d = p.wait_for_event("download", timeout=25000)
                dst = carpeta / d.suggested_filename
                d.save_as(dst)
                return dst
            except PWTimeoutError:
                pass
            # b) respuesta inline con content-type: application/pdf
            try:
                resp = p.wait_for_response(
                    lambda r: any(x in (r.headers.get("content-type", "").lower()) for x in ("application/pdf", "application/octet-stream")),
                    timeout=25000,
                )
                nombre = _filename_from_cd((resp.headers or {}).get("content-disposition"))
                cuerpo = resp.body()
                return _guardar(cuerpo, nombre)
            except PWTimeoutError:
                return None

        destino = None

        # 1) intento: descarga directa en la misma page
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
                link.click(force=True)  # fuerza el click por si hay overlays
            d = dl.value
            destino = carpeta / d.suggested_filename
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
            # 2) si no hubo download, disparamos el click otra vez y tratamos popup
            try:
                link.click(force=True)
            except Exception:
                pass

            # 2.a) popup nuevo
            try:
                pop = ctx.wait_for_event("page", timeout=6000)
                try:
                    destino = _capturar_desde_pagina(pop)
                finally:
                    try: pop.close()
                    except Exception: pass
                try:
                    logging.info(f"[INF] Fila {i}: popup -> {'OK '+destino.name if destino else 'sin descarga'}")
                except Exception:
                    pass
            except PWTimeoutError:
                pass

            # 2.b) si no hubo popup nuevo, intentar respuesta inline en la misma página
            if not destino:
                try:
                    try:
                        logging.info(f"[INF] Fila {i}: esperando respuesta inline en misma página")
                    except Exception:
                        pass
                    def _is_pdf_resp_inline(r):
                        try:
                            ct = (r.headers or {}).get('content-type', '')
                            return ('application/pdf' in (ct or '').lower()) or ('application/octet-stream' in (ct or '').lower())
                        except Exception:
                            return False
                    with sac.expect_response(_is_pdf_resp_inline, timeout=15000) as resp_info:
                        try:
                            link.click()
                        except Exception:
                            pass
                    resp = resp_info.value
                    try:
                        nombre = _filename_from_cd((resp.headers or {}).get('content-disposition'))
                    except Exception:
                        nombre = None
                    try:
                        cuerpo = resp.body()
                    except Exception:
                        cuerpo = b''
                    destino = _guardar(cuerpo, nombre)
                    try:
                        logging.info(f"[INF] Fila {i}: inline misma página -> {'OK '+destino.name if destino else 'sin descarga'}")
                    except Exception:
                        pass
                except PWTimeoutError:
                    pass

            # 2.c) si sigue sin destino, puede reutilizar uno existente o invocar directo la función
            if not destino:
                # invocación directa de la función del sitio como último recurso
                if guid:
                    try:
                        def _is_pdf_resp_eval(r):
                            try:
                                ct = (r.headers or {}).get('content-type', '')
                                return ('application/pdf' in (ct or '').lower()) or ('application/octet-stream' in (ct or '').lower())
                            except Exception:
                                return False
                        with sac.expect_response(_is_pdf_resp_eval, timeout=15000) as resp_info:
                            sac.evaluate("g => { try { window.VerInformeMPF && window.VerInformeMPF(g); } catch(e){} }", guid)
                        resp = resp_info.value
                        nombre = _filename_from_cd((resp.headers or {}).get('content-disposition'))
                        cuerpo = resp.body()
                        destino = _guardar(cuerpo, nombre)
                    except PWTimeoutError:
                        try:
                            sac.evaluate("g => { try { window.VerInformeMPF && window.VerInformeMPF(g); } catch(e){} }", guid)
                        except Exception:
                            pass

                # buscamos en todas las páginas del contexto (incluida la actual)
                for p in reversed(ctx.pages):
                    destino = _capturar_desde_pagina(p)
                    if destino:
                        break
                try:
                    logging.info(f"[INF] Fila {i}: inline/existing -> {'OK '+destino.name if destino else 'no encontrado'}")
                except Exception:
                    pass

        # Validaciones finales del PDF (igual que tenías)
        if not destino or not destino.exists():
            try:
                logging.info(f"[INF] Fila {i}: sin archivo destino")
            except Exception:
                pass
            continue
        if destino.suffix.lower() != ".pdf":
            destino = _ensure_pdf_fast(destino) if '_ensure_pdf_fast' in globals() else _ensure_pdf(destino)
            if not destino or not destino.exists() or destino.suffix.lower() != ".pdf":
                try:
                    logging.info(f"[INF] Fila {i}: conversión a PDF fallida")
                except Exception:
                    pass
                continue
        if _pdf_contiene_mensaje_permiso(destino):
            try: destino.unlink()
            except Exception: pass
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
        try:
            logging.info(f"[INF] Fila {i}: agregado {destino.name}")
        except Exception:
            pass

    return informes


def _fecha_rnr_desde_pdf(pdf_path: Path) -> str | None:
    """
    Intenta extraer la fecha del Informe RNR desde el PDF.
    - Primero busca dd/mm/aaaa literal.
    - Luego busca "20 de septiembre de 2023" (mes en español), con o sin día de semana/lugar.
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

    # Best-effort: OCR si está habilitado/posible
    try:
        pdf_ocr = _maybe_ocr(pdf_path)
        if pdf_ocr and Path(pdf_ocr).exists():
            fecha = _parse(_extraer_txt(Path(pdf_ocr)))
            if fecha:
                return fecha
    except Exception:
        pass
    return None


def _descargar_informes_reincidencia(sac, carpeta: Path) -> list[tuple[Path, str]]:
    """
    Descarga los Informes del Registro Nacional de Reincidencia (RNR) y devuelve [(PDF, fecha_informe)].
    - Abre específicamente la sección "INFORMES REGISTRO NACIONAL DE REINCIDENCIA".
    - Hace click en el ícono PDF de cada fila y captura descarga/popup/inline.
    - La fecha se extrae desde el propio PDF (arriba a la izquierda), no de la grilla.
    """
    import re
    informes: list[tuple[Path, str]] = []
    vistos: set[tuple[str, int]] = set()

    # 1) Intentar mostrar la sección (varias variantes conocidas)
    try:
        try:
            logging.info("[RNR] Intentando abrir sección Reincidencias")
        except Exception:
            pass
        # a) llamada directa a la función del sitio (varios nombres probables)
        for sec in [
            'InformesReincidencia',
            'InformesRegistroNacionalReincidencia',
            'InformesRegistroNacionalDeReincidencia',
            'InformesRNR',
            'Reincidencias',
        ]:
            try:
                sac.evaluate("s => { try { window.Seccion && window.Seccion(s); } catch(e){} }", sec)
            except Exception:
                pass
        # b) anchor/imagen de toggle
        for sel in [
            "a[href*=\"Seccion('InformesReincidencia')\"], a[onclick*=\"Seccion('InformesReincidencia')\"]",
            "a[href*=\"Seccion('InformesRegistroNacionalReincidencia')\"], a[onclick*=\"Seccion('InformesRegistroNacionalReincidencia')\"]",
            "a[href*=\"Seccion('InformesRegistroNacionalDeReincidencia')\"], a[onclick*=\"Seccion('InformesRegistroNacionalDeReincidencia')\"]",
            "a[href*=\"Seccion('Reincidencias')\"], a[onclick*=\"Seccion('Reincidencias')\"]",
        ]:
            try:
                a = sac.locator(sel).first
                if a and a.count():
                    try:
                        a.click()
                    except Exception:
                        a.evaluate("el=>el.click()")
            except Exception:
                pass
        for imgsel in ["#imgInformesReincidencia", "#imgInformesRegistroNacionalReincidencia", "#imgInformesRegistroNacionalDeReincidencia", "#imgReincidencias"]:
            try:
                img = sac.locator(imgsel).first
                if img and img.count():
                    try:
                        img.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        img.click()
                    except Exception:
                        img.evaluate("el=>el.click()")
            except Exception:
                pass
        # Si existe el hidden de estado (#hdReincidencias) y está en 0, forzar un click más
        try:
            hd = sac.locator("#hdReincidencias").first
            if hd and hd.count():
                val = hd.get_attribute("value") or ""
                if val.strip() == "0":
                    try:
                        sac.locator("#imgReincidencias").first.click()
                    except Exception:
                        sac.locator("#imgReincidencias").first.evaluate("el=>el.click()")
        except Exception:
            pass
        try:
            sac.wait_for_timeout(300)
        except Exception:
            pass
    except Exception:
        pass

    # 2) Localizar contenedor/tabla
    cont = None
    for sel in [
        "#divInformesReincidencia",
        "#divInformesRegistroNacionalReincidencia",
        "#divInformesRegistroNacionalDeReincidencia",
        "#cphDetalle_gvReincidencias",
        "table[id*='gv'][id*='Reincid']",
        "div[id*='Informes'][id*='Reincidencia']",
        "div[id*='Informes'][id*='Registro'][id*='Reincid']",
        "div[id*='Reincid']",
    ]:
        try:
            c = sac.locator(sel).first
            if c and c.count():
                cont = c
                break
        except Exception:
            continue

    # fallback débil: buscar por texto visible del rótulo de sección y luego el parent que contenga una tabla
    if not cont:
        try:
            lab = sac.locator(r"text=/INFORMES\s+REGISTRO\s+NACIONAL.*REINCIDENCIAS?/i").first
            if lab and lab.count():
                # usar el ancestro próximo con una tabla
                for _ in range(4):
                    try:
                        lab = lab.locator("xpath=..")
                        if lab and lab.count():
                            t = lab.locator("table").first
                            if t and t.count():
                                cont = lab
                                break
                    except Exception:
                        break
        except Exception:
            pass

    if not cont:
        try:
            logging.info("[RNR] No se halló contenedor por ID conocidos ni por rótulo.")
        except Exception:
            pass
        return informes

    # 3) Esperar filas y candidatos a link
    filas = None
    # Preferir la grilla concreta (gvReincidencias)
    try:
        grid = sac.locator("table[id*='gv'][id*='Reincid']").first
        if not (grid and grid.count()) and cont and cont.count():
            grid = cont.locator("table[id*='gv'][id*='Reincid']").first
    except Exception:
        grid = None
    for _ in range(40):  # ~6s
        try:
            try:
                cont.scroll_into_view_if_needed()
            except Exception:
                pass
            target = grid if (grid and grid.count()) else cont
            filas = target.locator("tr") if target else None
            total_tmp = filas.count() if filas else 0
        except Exception:
            total_tmp = 0
        try:
            scope = (grid if (grid and grid.count()) else cont) or sac
            cand = scope.locator(
                "*[onclick*='Reincidencia'], *[href*='Reincidencia'], *[onclick*='InformeRNR'], *[href*='InformeRNR'], "
                "a:has(img[src*='pdf']), a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='Adobe16']), "
                "a[href*='__doPostBack'][href*='gv'][href*='Informes'], a[onclick*='__doPostBack'][onclick*='gv'][onclick*='Informes']"
            )
            cand_n = cand.count()
            try:
                logging.info(f"[RNR] Espera grilla: filas_tmp={total_tmp} cand={cand_n}")
            except Exception:
                pass
        except Exception:
            cand_n = 0
        if (total_tmp > 1) and (cand_n > 0):
            break
        try:
            sac.wait_for_timeout(150)
        except Exception:
            pass

    total = 0
    try:
        total = filas.count() if filas else 0
    except Exception:
        total = 0
    if total <= 1:
        try:
            s = (grid if (grid and grid.count()) else cont)
            html = s.inner_html() if s and s.count() else ""
            logging.info(f"[RNR] Contenedor RNR HTML (recorte): { (html or '')[:2000] }")
            logging.info(f"[RNR] Filas={total} candidatos=0 (no se detectaron)")
        except Exception:
            pass
        return informes
    else:
        try:
            s2 = (grid if (grid and grid.count()) else cont) or sac
            cand = s2.locator(
                "*[onclick*='Reincidencia'], *[href*='Reincidencia'], *[onclick*='InformeRNR'], *[href*='InformeRNR'], "
                "a:has(img[src*='pdf']), a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='Adobe16']), "
                "a[href*='__doPostBack'][href*='gv'][href*='Informes'], a[onclick*='__doPostBack'][onclick*='gv'][onclick*='Informes']"
            )
            cand_n = cand.count()
            logging.info(f"[RNR] Filas RNR: {total} | candidatos: {cand_n}")
        except Exception:
            pass

    # 4) Recorrer filas y descargar cada PDF
    from playwright.sync_api import TimeoutError as PWTimeoutError
    ctx = sac.context

    def _filename_from_cd(cd: str | None) -> str | None:
        import re, urllib.parse
        if not cd:
            return None
        m = re.search(r"filename\*?=([^;]+)", cd)
        if not m:
            return None
        val = m.group(1).strip().strip('"')
        if val.lower().startswith("utf-8''"):
            val = urllib.parse.unquote(val[7:])
        return Path(val).name

    def _guardar(bytes_, nombre_sugerido: str | None, i: int) -> Path | None:
        try:
            dst = carpeta / (nombre_sugerido or f"InformeRNR_{i:03d}.pdf")
            with open(dst, "wb") as f:
                f.write(bytes_)
            return dst
        except Exception:
            return None

    def _capturar_desde_pagina(p, i: int) -> Path | None:
        # a) download event
        try:
            d = p.wait_for_event("download", timeout=25000)
            dst = carpeta / d.suggested_filename
            d.save_as(dst)
            return dst
        except PWTimeoutError:
            pass
        # b) inline response with application/pdf
        try:
            resp = p.wait_for_response(
                lambda r: any(x in (r.headers.get("content-type", "").lower()) for x in ("application/pdf", "application/octet-stream")),
                timeout=25000,
            )
            nombre = _filename_from_cd((resp.headers or {}).get("content-disposition"))
            cuerpo = resp.body()
            return _guardar(cuerpo, nombre, i)
        except PWTimeoutError:
            return None

    for i in range(1, total):  # saltear header
        try:
            fila = filas.nth(i)
        except Exception:
            continue

        # Link/ícono candidato en la fila
        link = fila.locator(
            "*[onclick*='Reincidencia'], *[href*='Reincidencia'], *[onclick*='InformeRNR'], *[href*='InformeRNR'], "
            "a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='Adobe16']), a:has(img[src*='pdf']), "
            "a[href*='__doPostBack'][href*='gv'][href*='Informes'], a[onclick*='__doPostBack'][onclick*='gv'][onclick*='Informes']"
        ).first
        img_only = None
        if not (link and link.count()):
            # Fallback: intentar con el IMG directamente
            img_only = fila.locator("img[src*='adobe'], img[src*='Adobe'], img[src*='Adobe16'], img[src*='pdf']").first
            if not (img_only and img_only.count()):
                try:
                    html_f = fila.inner_html()
                    logging.info(f"[RNR] Fila {i}: sin link/ícono (recorte): {(html_f or '')[:800]}")
                except Exception:
                    pass
                continue

        # GUID/ID (si hubiera) para invocación directa
        guid = None
        try:
            href = (link.get_attribute("href") if link and link.count() else None) or ""
            oc = (link.get_attribute("onclick") if link and link.count() else None) or ""
            m = re.search(r"(VerInforme\w*|VerReincid\w*|Reinciden\w*)\s*\(([^)]*)\)", f"{href} {oc}")
            if m:
                args = m.group(2)
                arg0 = args.split(",")[0].strip()
                if (len(arg0) >= 2) and ((arg0[0] == arg0[-1]) and arg0[0] in "'\""):
                    arg0 = arg0[1:-1]
                guid = arg0 or None
        except Exception:
            pass

        destino = None

        # 1) descarga directa en la misma page
        try:
            try:
                _kill_overlays(sac)
            except Exception:
                pass
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            with sac.expect_download(timeout=12000) as dl:
                if link and link.count():
                    link.click(force=True)
                elif img_only and img_only.count():
                    try:
                        img_only.click(force=True)
                    except Exception:
                        # click en el padre <a>
                        try:
                            img_only.evaluate("el => el.closest('a') && el.closest('a').click()")
                        except Exception:
                            raise
            d = dl.value
            destino = carpeta / d.suggested_filename
            d.save_as(destino)
        except PWTimeoutError:
            # 2) popup
            try:
                if link and link.count():
                    link.click(force=True)
                elif img_only and img_only.count():
                    try:
                        img_only.click(force=True)
                    except Exception:
                        try:
                            img_only.evaluate("el => el.closest('a') && el.closest('a').click()")
                        except Exception:
                            pass
            except Exception:
                pass
            try:
                pop = ctx.wait_for_event("page", timeout=6000)
                try:
                    destino = _capturar_desde_pagina(pop, i)
                finally:
                    try:
                        pop.close()
                    except Exception:
                        pass
            except PWTimeoutError:
                pass

            # 3) inline en la misma página
            if not destino:
                try:
                    def _is_pdf_resp_inline(r):
                        try:
                            ct = (r.headers or {}).get('content-type', '')
                            return ('application/pdf' in (ct or '').lower()) or ('application/octet-stream' in (ct or '').lower())
                        except Exception:
                            return False
                    with sac.expect_response(_is_pdf_resp_inline, timeout=15000) as resp_info:
                        try:
                            if link and link.count():
                                link.click()
                            elif img_only and img_only.count():
                                try:
                                    img_only.click()
                                except Exception:
                                    img_only.evaluate("el => el.closest('a') && el.closest('a').click()")
                        except Exception:
                            pass
                    resp = resp_info.value
                    nombre = _filename_from_cd((resp.headers or {}).get('content-disposition'))
                    cuerpo = resp.body()
                    destino = _guardar(cuerpo, nombre, i)
                except PWTimeoutError:
                    pass

            # 4) invocación directa por JS si tenemos guid
            if not destino and guid:
                try:
                    def _is_pdf_resp_eval(r):
                        try:
                            ct = (r.headers or {}).get('content-type', '')
                            return ('application/pdf' in (ct or '').lower()) or ('application/octet-stream' in (ct or '').lower())
                        except Exception:
                            return False
                    with sac.expect_response(_is_pdf_resp_eval, timeout=15000) as resp_info:
                        sac.evaluate("g => { try { window.VerInformeRNR && window.VerInformeRNR(g); } catch(e){} }", guid)
                    resp = resp_info.value
                    nombre = _filename_from_cd((resp.headers or {}).get('content-disposition'))
                    cuerpo = resp.body()
                    destino = _guardar(cuerpo, nombre, i)
                except PWTimeoutError:
                    pass
                except Exception:
                    pass

        # Validaciones finales
        if not destino or not destino.exists():
            continue
        if destino.suffix.lower() != ".pdf":
            destino = _ensure_pdf_fast(destino) if '_ensure_pdf_fast' in globals() else _ensure_pdf(destino)
        if not destino or not destino.exists() or destino.suffix.lower() != ".pdf":
            continue
        if _pdf_contiene_mensaje_permiso(destino):
            try:
                destino.unlink()
            except Exception:
                pass
            continue

        key = (destino.name, destino.stat().st_size if destino.exists() else 0)
        if key in vistos:
            continue
        vistos.add(key)

        # Fecha desde el contenido del PDF
        fecha = _fecha_rnr_desde_pdf(destino) or ""
        informes.append((destino, fecha))

    # Fallback: si no se capturó nada por filas, intentar por candidatos globales en el contenedor
    if not informes:
        try:
            cand = cont.locator(
                "a:has(img[src*='adobe']), a:has(img[src*='Adobe']), a:has(img[src*='Adobe16']), a:has(img[src*='pdf'])"
            )
            n = cand.count()
        except Exception:
            n = 0
        from playwright.sync_api import TimeoutError as PWTimeoutError
        for j in range(n):
            link = cand.nth(j)
            destino = None
            try:
                with sac.expect_download(timeout=12000) as dl:
                    try:
                        link.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    link.click(force=True)
                d = dl.value
                destino = carpeta / d.suggested_filename
                d.save_as(destino)
            except PWTimeoutError:
                try:
                    pop = sac.context.wait_for_event("page", timeout=6000)
                    try:
                        destino = _capturar_desde_pagina(pop, j)
                    finally:
                        try:
                            pop.close()
                        except Exception:
                            pass
                except PWTimeoutError:
                    pass
            if not destino or not destino.exists():
                continue
            if destino.suffix.lower() != ".pdf":
                destino = _ensure_pdf_fast(destino) if '_ensure_pdf_fast' in globals() else _ensure_pdf(destino)
            if not destino or not destino.exists() or destino.suffix.lower() != ".pdf":
                continue
            if _pdf_contiene_mensaje_permiso(destino):
                try:
                    destino.unlink()
                except Exception:
                    pass
                continue
            key = (destino.name, destino.stat().st_size if destino.exists() else 0)
            if key in vistos:
                continue
            vistos.add(key)
            fecha = _fecha_rnr_desde_pdf(destino) or ""
            informes.append((destino, fecha))

    return informes


def _extraer_adjuntos_embebidos(pdf_in: Path, out_dir: Path) -> list[Path]:
    """
    Extrae archivos embebidos / adjuntos de un PDF (PyMuPDF o pikepdf si está disponible).
    Devuelve lista de paths extraídos.
    """
    extraidos: list[Path] = []
    # PyMuPDF primero (rápido y suele venir instalado)
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(pdf_in))
        # nombres segun versión
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
                # si no está, omitimos
                pass
    except Exception:
        pass

    return extraidos



# --------------------- Portal ? “Portal de Aplicaciones PJ” ------------
def _open_portal_aplicaciones_pj(page):
    """
    Abre el tile “Portal de Aplicaciones PJ” (NO el que empieza con INTRANET).
    El portal es Angular; el texto vive en un <span> dentro de una card.
    """
    try:
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    header = page.locator(
        ".card-header:has(.card-title span:has-text('Portal de Aplicaciones PJ'))"
    ).first
    card = page.locator(
        ".card:has(.card-title span:has-text('Portal de Aplicaciones PJ'))"
    ).first
    target = header if header.count() else card
    if not target or target.count() == 0:
        _debug_dump(page, "tile_not_found")
        raise RuntimeError("No encontré 'Portal de Aplicaciones PJ'.")

    try:
        target.scroll_into_view_if_needed()
    except Exception:
        pass

    # Misma pestaña
    try:
        with page.expect_navigation(timeout=7000):
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
                const a = el.querySelector('a[href]');
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
    logging.info("[LOGIN] Buscando formulario de Intranet")

    try:
        page.wait_for_load_state("domcontentloaded")
    except Exception:
        pass

    scopes = [page] + list(page.frames)

    def _has_password(sc):
        try:
            return sc.locator("input[type='password']").first.count() > 0
        except Exception:
            return False

    def _logged_in(sc):
        # Sin password + link/acción de salir visibles
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

    # ? Solo devolvé “ya activo” si de verdad vemos logout y no hay password en ningún lado
    if any(_logged_in(sc) for sc in scopes):
        logging.info("[LOGIN] Sesión ya activa (logout visible).")
        return

    # Si hay cualquier password en la página, vamos a completar el login
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

    # Buscamos primero el password, luego el user en el mismo scope.
    for sc in scopes:
        p_ = _first_visible(sc, [
            "input[id$='Password']",
            "input[name$='Password']",
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
            "input[formcontrolname='username']",
            "input[name='username']",
            "input[type='text']",
        ])
        if u_:
            target_scope, user_box, pass_box = sc, u_, p_
            break

    if not (target_scope and user_box and pass_box):
        logging.info("[LOGIN] No se encontró un par usuario/clave visible; continúo sin login.")
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

    # Enviar (Enter o botón submit)
    try:
        pass_box.press("Enter")
        target_scope.wait_for_load_state("networkidle")
    except Exception:
        pass

    btn = _first_visible(target_scope, [
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('Ingresar')",
        "button:has-text('Iniciar sesión')",
        "xpath=//span[normalize-space()='Ingresar' or normalize-space()='Iniciar sesión']/ancestor::button[1]"
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
    Si todavía no hay /proxy/<token>/ vuelve a la grilla y abre por tile.
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

    # ? activar killer mientras tocamos el índice
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

    # 1) localizar el link del índice en cualquier frame
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

    # 2) si no vino 'tipo', intentá inferirlo del link encontrado
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

    # 3) intento principal: clic real en el link del índice
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

    # 4) fallback: ejecutar onItemClick donde exista (página o cualquier frame)
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

    # 5) último recurso: evento custom usado por algunas skins
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

    # 1) Caso clásico: URL absoluta o /proxy/ relativo
    url = _extract_url_from_js(href or oc)
    if url:
        if url.startswith("/proxy/"):
            url = "https://teletrabajo.justiciacordoba.gob.ar" + url
        if url.startswith("https://www.tribunales.gov.ar/") and proxy_prefix:
            url = proxy_prefix + url
        return url

    # 2) Nuevo: javascript:VerAdjuntoFichero('ID')
    if "VerAdjuntoFichero" in (href + oc):
        u = _url_from_ver_adjunto(href or oc, proxy_prefix)
        if u:
            return u
    return None


def _descargar_archivo(session: requests.Session, url: str, destino: Path) -> Path | None:
    from requests.exceptions import SSLError
    from urllib.parse import urlparse
    import urllib3

    nombre = Path(urlparse(url).path).name or destino.name
    host = (urlparse(url).hostname or "").lower()
    logging.info(f"[DL:START] {nombre} ? {destino.name}")

    def _stream_to_file(resp):
        with open(destino, "wb") as f:
            for chunk in resp.iter_content(256 * 1024):
                if chunk:
                    f.write(chunk)

    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            _stream_to_file(r)
        sz = destino.stat().st_size if destino.exists() else 0
        logging.info(f"[DL:OK] {destino.name} ({sz} bytes)")
        return destino
    except SSLError as e:
        msg = str(e).lower()
        # fallback SOLO si es el host de tribunales y el problema es verificación de cert
        if host.endswith("tribunales.gov.ar") and (
            "self-signed" in msg or "certificate verify failed" in msg
        ):
            logging.info(
                f"[DL:WARN] SSL en {host} (self-signed). Reintento sin verificación TLS."
            )
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            try:
                with session.get(url, stream=True, timeout=60, verify=False) as r:
                    r.raise_for_status()
                    _stream_to_file(r)
                sz = destino.stat().st_size if destino.exists() else 0
                logging.info(f"[DL:OK/INSECURE] {destino.name} ({sz} bytes)")
                return destino
            except Exception as e2:
                logging.info(f"[DL:ERR] {destino.name} · {e2}")
                return None
        # cualquier otro SSLError
        logging.info(f"[DL:ERR] {destino.name} · {e}")
        return None
    except Exception as e:
        logging.info(f"[DL:ERR] {destino.name} · {e}")
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
        fit=img2pdf.FitMode.SHRINK_TO_FIT,  # nunca agranda más de A4; conserva relación de aspecto
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
        logging.info(f"[CNV:OFF] {path.name} ? {dst.name}")
        try:
            subprocess.run(
                [soffice, "--headless", "--convert-to", "pdf", "--outdir", str(outdir), str(path)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            pdf = path.with_suffix(".pdf")
            if pdf.exists():
                logging.info(f"[CNV:OK ] {pdf.name}")
                return pdf
        except Exception as e:
            logging.info(f"[CNV:ERR] {path.name} · {e}")
    return path


def _open_sac_desde_portal_teletrabajo(page):
    """
    *** SOLO Teletrabajo ***
    Abre el menú 'Aplicaciones' (img#imgMenuServiciosPrivadas) y entra a 'SAC Multifuero'.
    Es el flujo que ya te funcionaba y NO usa navegación directa sin proxy.
    """
    logging.info("[NAV] Intentando abrir 'SAC Multifuero' desde portal actual")
    import re
    # Si ya estamos en PublicApps.aspx (bajo proxy), delegá
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
            "No encontré el botón 'Aplicaciones' (id imgMenuServiciosPrivadas)."
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
            logging.info("[NAV] Link a 'SAC Multifuero' localizado; abriendo…")
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
            "No encontré el enlace a 'SAC Multifuero' dentro de Aplicaciones."
        )

    # Puede ser popup o misma pestaña
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

    # Último recurso: seguir href/onclick del link
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
        "No pude abrir 'SAC Multifuero' pese a desplegar el menú (ver click_sac_fail.*)."
    )


def _open_sac_desde_portal_intranet(page):
    """
    *** SOLO Intranet directa / página ya proxificada ***
    Busca enlace 'SAC Multifuero'. Si no aparece, navega al menú del SAC:
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
            "No hallé link a SAC y no hay proxy activo; evito navegación directa en Teletrabajo."
        )

    dest = (proxy_prefix or "") + "https://www.tribunales.gov.ar/SacInterior/Menu/Default.aspx"
    page.goto(dest, wait_until="domcontentloaded")
    return page


def _open_sac_desde_portal(page):
    import re
    u = page.url or ""
    if re.search(r"/PortalWeb/(Pages/)?PublicApps\.aspx", u, re.I):
        return _open_sac_desde_portal_intranet(page)
    if ("teletrabajo.justiciacordoba.gob.ar" in u) or ("/proxy/" in u):
        return _open_sac_desde_portal_teletrabajo(page)
    return _open_sac_desde_portal_intranet(page)

def _ir_a_radiografia(sac):
    """
    Preferir el menú de SAC ? “Radiografía”. Si no aparece, usar URL con el mismo /proxy/.
    """
    import re

    try:
        sac.wait_for_load_state("domcontentloaded")
    except Exception:
        pass

    try:
        matcher = re.compile(r"Radiograf[íi]a", re.I)
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

    proxy_prefix = _get_proxy_prefix(sac)
    sac.goto(proxy_prefix + URL_RADIOGRAFIA, wait_until="domcontentloaded")
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
            # Si no hay formulario pero sí vemos el portal, seguimos; si no, re-lanzamos
            if not _is_portal_grid(page):
                raise

    # Traer grilla del portal (activa el proxy) y abrir el tile
    _goto_portal_grid(page)
    portal = _open_portal_aplicaciones_pj(page)

    _login_intranet(portal, intra_user, intra_pass)
    sac = _open_sac_desde_portal(portal)
    if _is_proxy_error(sac):
        _goto_portal_grid(portal)
        portal = _open_portal_aplicaciones_pj(portal)
        _login_intranet(portal, intra_user, intra_pass)
        sac = _open_sac_desde_portal(portal)

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
                logging.info(f"[OPEN:{label}:ERR] intento {i+1} · {e}")
                try:
                    page.wait_for_timeout(800 * (i + 1))
                except Exception:
                    pass
        raise last if last else RuntimeError(f"{label} falló")

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
            logging.info("[OPEN] Teletrabajo falló; pruebo Intranet directa")
            if not ALLOW_DIRECT_INTRANET:
                raise e  # si ALLOW_DIRECT_INTRANET=1, recién ahí proba el bloque de INTRANET

    # 2) Intranet directa
    try:
        def _open_intranet():
            pg = context.new_page()
            pg.set_default_timeout(int(os.getenv("OPEN_TIMEOUT_MS", "45000")))
            pg.set_default_navigation_timeout(int(os.getenv("OPEN_NAV_TIMEOUT_MS", "60000")))

            # Si la URL de Intranet no resuelve o está caída, disparamos un error reconocible
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
        # Fallback explícito a Teletrabajo si Intranet no está disponible
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

    # 3) Último intento por Tele si no lo probamos primero
    if not prefer_tele and tele_user and tele_pass:
        return _try_open(
            lambda: abrir_sac_via_teletrabajo(context, tele_user, tele_pass, intra_user, intra_pass),
            "TELETRABAJO",
        )

    raise RuntimeError("No pude abrir el SAC ni por Intranet ni por Teletrabajo.")


def _cerrar_indice_libro(libro):
    """
    Cierra el panel Índice usando los toggles de la UI (sin ocultarlo por CSS).
    Soporta distintas variantes (pestaña vertical, hamburguesa, chevrons, etc.).
    """
    S = _libro_scope(libro)

    def visible():
        nav = S.locator("#indice, .indice, .nav-container").first
        if not nav.count():
            return False
        try:
            # visible y con ancho útil (>40px para distinguir handle)
            bb = nav.bounding_box()
            return bool(bb and bb.get("width", 0) > 40 and nav.is_visible())
        except Exception:
            return False

    if not S.locator("#indice, .indice, .nav-container").first.count():
        return

    toggles = [
        "text=/^\\s*Índice\\s*$/i",
        "button:has-text('Índice')",
        "a:has-text('Índice')",
        ".indice-toggle, .indice .toggle, .indice [role=button]",
        ".nav-container .navbar-toggler",
        ".nav-container .fa-chevron-left, .nav-container .fa-angle-left, .nav-container .fa-angle-double-left",
        ".btn-indice, #btnIndice, #indiceTab, #indice-tab",
        "xpath=//*[contains(translate(normalize-space(.),'ÍNDICE','índice'),'índice')]",
    ]

    # Probar múltiples toggles un par de veces
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
    1) Click en 'Imprimir / Imprimir Selección' y captura download si el sitio genera PDF.
    2) Si abre el diálogo del navegador (no automatable), fallback: PDF por CDP en un Chromium
       HEADLESS con el mismo estado de sesión.
    """
    S = _libro_scope(libro)
    _cerrar_indice_libro(libro)
    out = tmp_dir / "libro.pdf"

    # Asegurar foco y scrollear al fondo (botón suele estar abajo a la derecha)
    try:
        libro.bring_to_front()
    except Exception:
        pass
    try:
        S.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        S.wait_for_timeout(300)
    except Exception:
        pass

    # 1) Intento: botón que dispare download del backend
    btn_selectors = [
        "text=/\\bImprimir Selección\\b/i",
        "text=/\\bImprimir\\b/i",
        "button:has-text('Imprimir Selección')",
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
                # después de d.save_as(out) o de hp.pdf(...)
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
                # Si abrió el diálogo del navegador, no habrá download ? seguimos al plan B
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
    hbrowser = p.chromium.launch(
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

    # Si la exportación headless terminó en el login, descartalo
    try:
        if out.exists() and _pdf_es_login_portal(out):
            logging.info("[PRINT:HEADLESS] Detectado login en PDF; se descarta.")
            out.unlink(missing_ok=True)
            return None
    except Exception:
        pass

    # 2) Fallback HEADLESS: mismo estado de sesión + Page.pdf()
    try:
        state_file = tmp_dir / "state.json"
        context.storage_state(path=str(state_file))
        hbrowser = p.chromium.launch(
            headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        hctx = hbrowser.new_context(
            storage_state=str(state_file), viewport={"width": 1366, "height": 900}
        )
        hp = hctx.new_page()
        hp.goto(libro.url, wait_until="networkidle")
        # Cargar/expandir como hicimos en la pestaña visible
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

    logging.info("[PRINT] No pude obtener el PDF del Libro ni por botón ni por fallback headless.")
    return None


def _guardar_libro_como_html(libro, tmp_dir: Path) -> Path | None:
    """
    Snapshot del 'Expediente como Libro' a un .html en disco, parecido a
    'Guardar como… / Página web completa'. Inyecta <base> (para recursos relativos vía /proxy/)
    y CSS de impresión para ocultar el índice/menus.
    """
    try:
        S = _libro_scope(libro)
        _cerrar_indice_libro(libro)

        # HTML actual del frame donde vive el Libro
        html = S.content()

        # Prefijo del proxy de Teletrabajo y base del sitio
        proxy_prefix = _get_proxy_prefix(libro)
        base_href = proxy_prefix + "https://www.tribunales.gov.ar/"

        # CSS para vista de impresión
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

        # Guardamos el estado de sesión del contexto actual
        state_file = tmp_dir / "state.json"
        context.storage_state(path=str(state_file))

        hbrowser = p.chromium.launch(
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
            logging.info(f"[HTML?PDF] {out_pdf.name}")
            return out_pdf
    except Exception as e:
        logging.info(f"[HTML?PDF:ERR] {e}")
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

    proxy_prefix = _get_proxy_prefix(libro)
    base_href = proxy_prefix + "https://www.tribunales.gov.ar/"

    # CSS alineado con el 'Imprimir...' del SAC (sin * {break-inside:avoid})
    css = """
        @page { size: A4; margin: 10mm; }
        /* estilos impresora del sitio */
        .A4 { box-shadow: none; width: auto; height: auto; margin: 0; padding: 0.3cm; min-height: 25.7cm; }
        .row { margin: 15px 3px; display: block; width: 100%; }
        .PieDePagina { border-top: 1pt solid; left: 5%; text-align: center; bottom: 50px; width: 90%; }
        .text-center { text-align: center; }
        .noprint { display: none; }
        .enable-print { display: block; }
        .font-weight-bold { font-weight: bold; }
        .dataLabel { margin-right: 10px; display: inline; }
        /* Evitar sólo cortes feos en imágenes/firmas/mesas; permitir flujo normal */
        img, table.signature-block { page-break-inside: avoid; break-inside: avoid; }
        table { page-break-inside: avoid; break-inside: avoid-page; page-break-after: avoid; }
        /* ? mantiene junto el cuadro de 'Protocolo…' con el primer bloque siguiente si hay espacio */
    """
    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<base href="{base_href}">
<style>{css}</style>
</head>
<body>{outer}</body>
</html>"""

    state_file = tmp_dir / f"state_{op_id}.json"
    context.storage_state(path=str(state_file))
    out = tmp_dir / f"op_{op_id}.pdf"

    if hctx is None or hp is None:
        hbrowser = p.chromium.launch(
            headless=True, args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        try:
            hctx = hbrowser.new_context(
                storage_state=str(state_file), viewport={"width": 1366, "height": 900}
            )
            hp = hctx.new_page()
            hp.set_content(html, wait_until="domcontentloaded")
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
                hp.emulate_media(media="print")
            except Exception:
                pass
            hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
        except Exception as e:
            logging.info(f"[HTML?PDF:REUSE-ERR] {e}")
            return None

    return out if out.exists() and out.stat().st_size > 500 else None


def _render_caratula_a_pdf(libro, context, p, tmp_dir: Path, hctx=None, hp=None) -> Path | None:
    """
    Nueva forma: NO navega a ImprimirCaratula.aspx.
    Toma el HTML del bloque #caratula dentro del Libro, lo aísla en una página en blanco
    con <base> al proxy y lo exporta a PDF en headless. Así no aparece el índice ni overlays
    y se evita el proxy error.
    """
    S = _libro_scope(libro)

    # 1) Asegurar que la carátula esté poblada por el front-end del SAC
    try:
        S.evaluate("() => { try { if (window.Encabezado) Encabezado(); } catch(e) {} }")
    except Exception:
        pass

    # 2) Tomar el HTML del bloque de carátula (outerHTML)
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

    # 3) Construir documento autónomo con base al proxy (para recursos relativos)
    base_href = _get_proxy_prefix(libro) + "https://www.tribunales.gov.ar/"
    css = """
        @page { size: A4; margin: 12mm; }
        html, body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        /* Sin sombras ni menús; aseguramos ancho fluido */
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
        hbrowser = p.chromium.launch(
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

    # 5) Limpieza opcional si hubiera página en blanco
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
        logging.info("[BLANK] PyMuPDF no disponible; omito limpieza de páginas en blanco.")
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
    Estampa numeración de fojas (arriba-derecha) en el PDF:
      - start_after: páginas iniciales SIN numerar (1 = dejar carátula sin número)
      - cada_dos: si True, numera sólo una de cada dos páginas (recto)
      - numero_inicial: valor inicial (1 por defecto)
      - fijo: texto fijo (si querés que siempre diga p.ej. "1")
    """
    try:
        import fitz  # PyMuPDF (rápido)
        import unicodedata

        doc = fitz.open(str(pdf_in))
        folio = numero_inicial
        for i in range(doc.page_count):
            pg = doc[i]
            # Evitar foliar páginas que correspondan al índice
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
                continue  # sólo una cara por hoja
            margen = 18
            # tamaño de letra proporcional (12–18 pt)
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
                y = ph - 18 - sz  # desde abajo, para “arriba”
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


# ----------------------- DESCARGA PRINCIPAL ----------------------------
def _env_true(name: str, default="0"):
    return os.getenv(name, default).lower() in ("1", "true", "t", "yes", "y", "si", "sí")


# ----------------------- DESCARGA PRINCIPAL ----------------------------
def descargar_expediente(tele_user, tele_pass, intra_user, intra_pass, nro_exp, carpeta_salida):
    SHOW_BROWSER = _env_true("SHOW_BROWSER", "0")
    CHROMIUM_ARGS = ["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
    KEEP_WORK = _env_true("KEEP_WORK", "0")
    STAMP = _env_true("STAMP_HEADERS", "1")

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

        etapa("Preparando entorno y navegador")

        with sync_playwright() as p:
            etapa("Inicializando navegador")
            browser = p.chromium.launch(
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
                # Si necesitás video, exportá RECORD_VIDEO=1
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
                etapa("Accediendo a Teletrabajo/Intranet y abriendo SAC")
                # 1) Login ? Radiografía
                sac = abrir_sac(context, tele_user, tele_pass, intra_user, intra_pass)
                logging.info(f"[SAC] Abierto SAC / Radiografía: url={sac.url}")
    
                # 2) Buscar expediente
                etapa(f"Entrando a Radiografía y buscando expediente N° {nro_exp}")
                _fill_radiografia_y_buscar(sac, nro_exp)
                logging.info(f"[RADIO] Buscado expediente N° {nro_exp}")
    
                if "SacInterior/Login.aspx" in sac.url:
                    messagebox.showerror("Error de sesión", "El SAC pidió re-login. Probá nuevamente.")
                    return
    
                if "PortalWeb/LogIn/Login.aspx" in (sac.url or ""):
                    _login_intranet(sac, intra_user, intra_pass)
                    sac = _ir_a_radiografia(sac)
                    _fill_radiografia_y_buscar(sac, nro_exp)
    
                # >>> GATE DESDE RADIOGRAFÍA <<<
                CHECK_ALL = _env_true("STRICT_CHECK_ALL_OPS", "0")
                etapa("Esperando carga de Radiografía y verificando acceso a operaciones")
    
                # Esperar a que cargue la vista (carátula + grillas)
                _esperar_radiografia_listo(sac, timeout=int(os.getenv("RADIO_TIMEOUT_MS", "150")))
                logging.info("[RADIO] Vista de Radiografía cargada (carátula/operaciones/adjuntos visibles)")
    
                # Listar operaciones rápido (con frames)
                op_ids_rad = _listar_ops_ids_radiografia(
                    sac,
                    wait_ms=int(os.getenv("RADIO_OPS_WAIT_MS", "150")),
                    scan_frames=True,
                )
    
                # Verificación de acceso:
                acceso_ok = False
                if op_ids_rad:
                    ids_a_probar = op_ids_rad if CHECK_ALL else op_ids_rad[:1]
                    # 1) Si alguna operación probada está denegada ? abortar
                    if any(_op_denegada_en_radiografia(sac, _id) for _id in ids_a_probar):
                        logging.info("[SEC] Radiografía mostró 'sin permisos' en al menos una operación. Abortando.")
                        messagebox.showwarning(
                            "Sin acceso",
                            "No tenés permisos para visualizar el contenido de este expediente "
                            "(al menos una operación está bloqueada). No se descargará nada.",
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
                        "No tenés permisos para visualizar el contenido del expediente (operaciones bloqueadas). "
                        "No se descargará nada.",
                    )
                    return
                # <<< FIN GATE DESDE RADIOGRAFÍA >>>
    
                # === 3.a) NUEVO: fechas por operación + timeline ===
                op_fecha_map, orden_fechas = _mapear_fechas_operaciones_radiografia(sac)
                from collections import defaultdict
                timeline = defaultdict(list)   # { 'dd/mm/aaaa' -> [(Path, header), ...] }
                ya_agregados: set[tuple[str, int]] = set()
                caratula_block: tuple[Path, str | None] | None = None
    
                def _push_pdf(pth: Path, hdr: str | None, fecha: str | None, toc_title: str | None = None):
                    if not pth or not pth.exists() or pth.suffix.lower() != ".pdf":
                        return
                    try:
                        key = (pth.name, pth.stat().st_size)
                    except Exception:
                        key = (pth.name, 0)
                    if key in ya_agregados:
                        return
                    ya_agregados.add(key)
    
                    # Limpieza de páginas en blanco (best-effort)
                    try:
                        pth = _pdf_sin_blancos(pth)
                    except Exception:
                        pass
                    if not pth or not Path(pth).exists():
                        return
    
                    timeline[(fecha or "__NOFECHA__")].append((pth, hdr, toc_title))
                    if fecha and fecha not in orden_fechas:
                        orden_fechas.append(fecha)
    
                # 3) Abrir Libro y listar operaciones VISIBLES (sin forzar)
                etapa("Abriendo 'Expediente como Libro'")
                libro = _abrir_libro(sac, intra_user, intra_pass, nro_exp)
                if _es_login_intranet(libro):
                    _login_intranet(libro, intra_user, intra_pass)
                if "ExpedienteLibro.aspx" not in (libro.url or ""):
                    libro = _abrir_libro(sac, intra_user, intra_pass, nro_exp)
    
                etapa("Cargando índice del Libro")
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
                logging.info(f"[LIBRO] Índice cargado · operaciones visibles={len(ops)}")
                if not ops:
                    logging.info("[SEC] La UI no muestra operaciones en el Índice. Se continúa SIN operaciones.")
                    ops = []
                logging.info(f"[OPS] Encontradas {len(ops)} operaciones visibles en el índice.")
    
                # 4) Preparar contexto headless reutilizable para HTML->PDF (carátula + operaciones)
                hbrowser = hctx = hp = None
                try:
                    state_print = temp_dir / "state_print.json"
                    try:
                        context.storage_state(path=str(state_print))
                    except Exception:
                        pass
                    hbrowser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
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
    
                # 4) Carátula (guardada aparte para que quede primera)
                etapa("Renderizando carátula del expediente")
                try:
                    caratula_pdf = _render_caratula_a_pdf(libro, context, p, temp_dir, hctx=hctx, hp=hp)
                    if caratula_pdf and caratula_pdf.exists():
                        _mf(f"CARATULA · {caratula_pdf.name}")
                        caratula_block = (caratula_pdf, None)
                        logging.info("[CARATULA] capturada")
                    else:
                        logging.info("[CARATULA] no se pudo capturar (se continúa)")
                except Exception as e:
                    logging.info(f"[CARATULA:ERR] {e}")
    
                # 5) Adjuntos del GRID (mapeados por operación)
                etapa("Descargando adjuntos desde Radiografía (grilla)")
                try:
                    sac.bring_to_front()
                except Exception:
                    pass
                pdfs_grid = _descargar_adjuntos_grid_mapeado(sac, temp_dir)  # {op_id: [Path, ...]}
                logging.info(f"[ADJ/GRID] Mapeo adjuntos por operación: { {k: len(v) for k, v in pdfs_grid.items()} }")
    
                # Helper: adjuntos de operación (Libro + Grid) — se depositan en el día de la operación
                def _agregar_adjuntos_de_op(op_id: str, titulo: str, fecha_op: str | None):
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
                        _mf(f"ADJUNTO · {titulo} · {pth.name}")
                        hdr = (f"ADJUNTO · {titulo}") if STAMP else None
                        hdr = (f"ADJUNTO - {titulo}") if STAMP else None
                        _push_pdf(pth, hdr, fecha=fecha_op, toc_title=f"ADJUNTO - {titulo}")
    
                # 6) Operaciones (render por páginas sólo si están visibles)
                op_pdfs_capturados = 0
                etapa("Procesando operaciones visibles del Libro")
                for o in ops:
                    op_id = o["id"]
                    op_tipo = o["tipo"]
                    titulo = (o.get("titulo") or "").strip() or f"Operación {op_id}"
                    fecha_op = op_fecha_map.get(op_id, None)  # fecha desde la grilla
                    logging.info(
                        f"[OP] Procesando operación · id={op_id} · tipo='{op_tipo}' · titulo='{titulo}' · fecha='{fecha_op or '-'}'"
                    )
    
                    # Mostrar y chequear visibilidad real del contenedor de la operación
                    _mostrar_operacion(libro, op_id, op_tipo)
                    S = _libro_scope(libro)
                    cont = _buscar_contenedor_operacion(S, op_id)
                    if not cont:
                        logging.info(f"[OP] {op_id}: contenedor no encontrado; se continúa con adjuntos.")
                        _agregar_adjuntos_de_op(op_id, titulo, fecha_op)
                        continue
    
                    visible = False
                    try:
                        if cont.count() and cont.is_visible():
                            bb = cont.bounding_box()
                            visible = bool(bb and bb.get("width", 0) > 40 and bb.get("height", 0) > 40)
                    except Exception:
                        visible = False
    
                    if not visible:
                        logging.info(
                            f"[OP] {op_id}: contenedor no visible; se omite render de operación (se agregan adjuntos igual)."
                        )
                        _agregar_adjuntos_de_op(op_id, titulo, fecha_op)
                        continue
    
                    # Render HTML ? PDF
                    try:
                        pdf_op = _render_operacion_a_pdf_paginas(libro, op_id, context, p, temp_dir, hctx=hctx, hp=hp)
                    except Exception as e:
                        logging.info(f"[OP:ERR] {op_id}: {e}")
                        pdf_op = None
    
                    if pdf_op and pdf_op.exists():
                        _mf(f"OPERACION · {titulo} · {pdf_op.name}")
                        _push_pdf(pdf_op, None, fecha=fecha_op, toc_title=f"OPERACION - {titulo}")
                        op_pdfs_capturados += 1
                        logging.info(f"[OP] {op_id}: agregado (renderer de páginas)")
                    else:
                        logging.info(f"[OP] {op_id}: no se pudo renderizar (se continúa con adjuntos).")
    
                    # Adjuntos de esta operación (con la misma fecha de la operación)
                    _agregar_adjuntos_de_op(op_id, titulo, fecha_op)
    
                # Cerrar contexto headless reutilizado
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
    
                # 5b) Informes técnicos — DESPUÉS de operaciones ? quedan al final del día
                etapa("Descargando informes técnicos")
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
                    _mf(f"INF_TEC · {it_fecha} · {pth.name}")
                    hdr = (f"INFORME TÉCNICO · {it_fecha}") if STAMP else None
                    toc_it = f"INFORME TECNICO MPF - {it_fecha}" if it_fecha else "INFORME TECNICO MPF"
                    _push_pdf(pth, hdr, fecha=it_fecha, toc_title=toc_it)
    
                    # Extraer anexos embebidos (si existe la función)
                    try:
                        anexos = _extraer_adjuntos_embebidos(pth, temp_dir) if '_extraer_adjuntos_embebidos' in globals() else []
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
                        _mf(f"INF_TEC/ANEXO · {it_fecha} · {Path(an_pdf).name}")
                        hdr_an = (f"INFORME TÉCNICO – ANEXO · {it_fecha}") if STAMP else None
                        hdr_an = (f"INFORME TECNICO - ANEXO - {it_fecha}") if STAMP else None
                        toc_an = f"INFORME TECNICO MPF - ANEXO - {it_fecha}" if it_fecha else "INFORME TECNICO MPF - ANEXO"
                        _push_pdf(Path(an_pdf), hdr_an, fecha=it_fecha, toc_title=toc_an)
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
                        _mf(f"LIBRO · {libro_pdf.name}")
                        _push_pdf(libro_pdf, None, fecha=None, toc_title="LIBRO")
                    else:
                        logging.info("[FALLBACK] No se pudo obtener PDF del Libro por ningún método.")
    
                # 8) Adjuntos sin operación mapeada ? al final (sin fecha)
                # 5c) Informes Registro Nacional de Reincidencia (RNR)
                etapa("Descargando informes RNR")
                try:
                    sac.bring_to_front()
                except Exception:
                    pass
                try:
                    informes_rnr = _descargar_informes_reincidencia(sac, temp_dir)
                except Exception:
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
                    _mf(f"RNR - {rnr_fecha or '-'} - {pth.name}")
                    hdr = (f"INFORME RNR - {rnr_fecha}") if STAMP and rnr_fecha else ("INFORME RNR" if STAMP else None)
                    _push_pdf(pth, hdr, fecha=(rnr_fecha or None), toc_title=("INFORME RNR - " + (rnr_fecha or "")))
                    logging.info(f"[MERGE] RNR - {pth.name} (fecha {rnr_fecha or '-'})")
    
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
                        _mf(f"ADJUNTO · (sin operación) · {pth.name}")
                        hdr = ("ADJUNTO · (sin operación)") if STAMP else None
                        _push_pdf(pth, hdr, fecha=None, toc_title=hdr)
    
                # === 3.e) Construcción final en orden cronológico ===
                hay_algo = any(timeline.values()) or bool(caratula_block)
                if not hay_algo:
                    raise RuntimeError("No hubo nada para fusionar (no se pudo capturar operaciones ni adjuntos).")
    
                # Reordenar listas por fecha para que el índice quede de las
                # operaciones más antiguas a las más recientes.
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
    
                # 2) Fechas que no estaban en la grilla (p.ej. sólo IT)
                restantes = [f for f in timeline.keys() if f not in set(orden_fechas) and f != "__NOFECHA__"]
                for f in sorted(restantes, key=_key_fecha):
                    bloques_final.extend(timeline.get(f, []))
    
                # 3) Elementos sin fecha ? al final
                bloques_final.extend(timeline.get("__NOFECHA__", []))
                # 9) Fusión final
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
                idx_pages, idx_map = fusionar_bloques_con_indice(
                    bloques_final,
                    out,
                    index_title="INDICE",
                    keep_sidecar=_env_true("KEEP_TOC", "0"),
                )
    
                # Diagnóstico inicial: links presentes justo tras fusionar
                try:
                    _log_links_en_pagina(out, 1, "INDICE/ANTES_POST")
                except Exception:
                    pass
    
                # Intentar aplicar OCR al PDF final
                ocr_out = _maybe_ocr(out)
                if ocr_out != out:
                    shutil.move(ocr_out, out)
    
                # Diagnóstico después del OCR “ligero”
                try:
                    _log_links_en_pagina(out, 1, "INDICE/DESPUES_OCR")
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
                        )
                        shutil.move(tmp_out, out)
                    except Exception:
                        logging.exception("[OCR] Falló OCR final")
    
                    # Diagnóstico después del OCR forzado
                    try:
                        _log_links_en_pagina(out, 1, "INDICE/DESPUES_OCR_FORCE")
                    except Exception:
                        pass
    
                # === FOJAS (numeración de hojas) ===
                try:
                    # Deja la carátula y el índice sin número; numera desde la siguiente,
                    # sólo una de cada dos (recto): 1, (sin), 2, (sin)...
                    _agregar_fojas(
                        out,
                        start_after=1 + idx_pages,
                        cada_dos=True,
                        numero_inicial=1,
                    )
                    logging.info("[FOJAS] Numeración de fojas aplicada")
                except Exception as e:
                    logging.info(f"[FOJAS] No se pudo estampar fojas: {e}")
    
                # Diagnóstico tras fojas (antes de reinyectar)
                try:
                    _log_links_en_pagina(out, 1, "INDICE/ANTES_RELINK")
                except Exception:
                    pass
    
                # Reinyectar links (por si OCR/fojas los borraron)
                try:
                    ok = _relink_indice_con_fitz(out, idx_map)
                    logging.info(f"[INDICE/LINK] reinyectado={ok} items={len(idx_map)}")
                except Exception as e:
                    logging.info(f"[INDICE/LINK:ERR] {e}")
    
                # Diagnóstico final
                try:
                    _log_links_en_pagina(out, 1, "INDICE/FINAL")
                except Exception:
                    pass
    
                _mf(f"==> PDF FINAL: {out.name} (total bloques={len(bloques_final)})")
                logging.info(f"[OK] PDF final creado: {out} · bloques={len(bloques_final)}")
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




class ProgressWin(Toplevel):
    """Ventana simple que muestra los logs en vivo."""
    def __init__(self, master, q, title="Progreso"):
        super().__init__(master)
        self.title(title)
        self.geometry("820x400")
        self.q = q

        self.lbl = ttk.Label(self, text="Espere...")
        self.lbl.pack(anchor="w", padx=8, pady=(8, 0))

        self.text = ScrolledText(self, wrap="word", height=20)
        self.text.pack(fill="both", expand=True, padx=8, pady=8)

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._poll()

    def _poll(self):
        try:
            while True:
                msg = self.q.get_nowait()
                # Si es una etapa, actualizo el label de estado “en curso”
                if msg.startswith("[ETAPA] "):
                    etapa_txt = msg.replace("[ETAPA] ", "").strip()
                    self.lbl.config(text=f"Etapa: {etapa_txt}")
                # En todos los casos, lo dejo asentado en el panel de texto
                self.text.insert("end", msg + "\n")
                self.text.see("end")
        except queue.Empty:
            pass
        self.after(100, self._poll)

    def _on_close(self):
        # Solo oculta la ventana; los logs siguen en debug.log
        self.withdraw()


# ------------------------- INTERFAZ Tkinter ----------------------------
class App:
    def __init__(self, master):
        master.title("Descargar expediente SAC")
        load_dotenv()

        Label(master, text="Usuario Teletrabajo (si corresponde):").grid(row=0, column=0, sticky="e")
        Label(master, text="Clave Teletrabajo (si corresponde):").grid(row=1, column=0, sticky="e")
        Label(master, text="Usuario Intranet:").grid(row=2, column=0, sticky="e")
        Label(master, text="Clave Intranet:").grid(row=3, column=0, sticky="e")
        Label(master, text="Nº Expediente:").grid(row=4, column=0, sticky="e")

        self.tele_user = StringVar(value=os.getenv("TELE_USER", ""))
        self.tele_pwd = StringVar(value=os.getenv("TELE_PASS", ""))
        self.intra_user = StringVar(value=os.getenv("INTRA_USER", os.getenv("SAC_USER", "")))
        self.intra_pwd = StringVar(value=os.getenv("INTRA_PASS", os.getenv("SAC_PASS", "")))
        self.exp = StringVar()

        Entry(master, textvariable=self.tele_user, width=26).grid(row=0, column=1)
        Entry(master, textvariable=self.tele_pwd, width=26, show="*").grid(row=1, column=1)
        Entry(master, textvariable=self.intra_user, width=26).grid(row=2, column=1)
        Entry(master, textvariable=self.intra_pwd, width=26, show="*").grid(row=3, column=1)
        Entry(master, textvariable=self.exp, width=26).grid(row=4, column=1)

        self.btn = Button(master, text="Descargar expediente", command=self.run)
        self.btn.grid(row=5, column=0, columnspan=2, pady=10)

        self._log_queue = None
        self._ui_handler = None
        self._progress_win = None

    def run(self):
        if not all([
            self.intra_user.get().strip(),
            self.intra_pwd.get().strip(),
            self.exp.get().strip(),
        ]):
            messagebox.showerror(
                "Faltan datos",
                "Completá usuario/clave de Intranet y Nº de expediente. "
                "Los de Teletrabajo son opcionales (solo si estás por VPN).",
            )
            return

        carpeta = filedialog.askdirectory(title="Carpeta destino")
        if not carpeta:
            return

        self.btn.config(state="disabled")

        # Ventana de progreso + handler de logging hacia la ventana
        self._log_queue = queue.Queue()
        self._progress_win = ProgressWin(
            self.btn.master, self._log_queue, title=f"Progreso – Exp. {self.exp.get().strip()}"
        )

        # Si hubiera un handler viejo, lo saco
        if self._ui_handler:
            logging.getLogger().removeHandler(self._ui_handler)

        self._ui_handler = TkQueueHandler(self._log_queue)
        self._ui_handler.setFormatter(
            logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S")
        )
        logging.getLogger().addHandler(self._ui_handler)

        threading.Thread(
            target=lambda: self._ejecutar(Path(carpeta)),
            daemon=True,
        ).start()

    def _ejecutar(self, carpeta: Path):
        try:
            descargar_expediente(
                self.tele_user.get().strip(),
                self.tele_pwd.get().strip(),
                self.intra_user.get().strip(),
                self.intra_pwd.get().strip(),
                self.exp.get().strip(),
                carpeta,
            )
        except Exception as e:
            messagebox.showerror("Error", str(e))
        finally:
            self.btn.config(state="normal")
            try:
                if self._ui_handler:
                    logging.getLogger().removeHandler(self._ui_handler)
                self._ui_handler = None
            except Exception:
                pass


# ---------------------------- MAIN -------------------------------------
LOG = BASE_PATH / "debug.log"
logging.basicConfig(
    filename=str(LOG),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)

# Filtro para silenciar logs de diagnóstico detallado de Informes Técnicos.
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
            "[INF] Abriendo sección",
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
    """Establece el AppUserModelID en Windows para usar el ícono de la app."""
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
    # Si el método anterior falla, no hacemos nada más.


if __name__ == "__main__":
    # Inicializa la aplicación de escritorio.
    _set_win_appusermodelid("SACDownloader.CBA")
    root = Tk()
    _set_tk_icon(root)  # usa icono3.ico desde BASE_PATH si está disponible
    App(root)
    root.mainloop()
# Nota: Al ejecutar con OCR_MODE=force, los adjuntos siempre salen con capa de texto.


