#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga un expediente del SAC (vía Teletrabajo -> Portal de Aplicaciones -> Intranet),
adjuntos incluidos, y arma un único PDF.
"""

import os, sys, tempfile, shutil, datetime, threading, re, logging
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

# ─────────────────────────── RUTAS Y RECURSOS ──────────────────────────
if getattr(sys, "frozen", False):   # ejecutable .exe
    BASE_PATH = Path(sys._MEIPASS)
else:                                # .py suelto
    BASE_PATH = Path(__file__).parent

# Playwright buscará el navegador empaquetado aquí (portabiliza el .exe)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BASE_PATH / "ms-playwright")
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _caratula_url(libro) -> str:
    S = _libro_scope(libro)
    exp_id = S.evaluate("() => document.getElementById('hdExpedienteId')?.value") or ""
    if not exp_id:
        raise RuntimeError("No encontré hdExpedienteId en el Libro.")
    proxy_prefix = _get_proxy_prefix(libro)
    return proxy_prefix + f"https://www.tribunales.gov.ar/SacInterior/_Expedientes/ImprimirCaratula.aspx?idExpediente={exp_id}&CuerpoDesde=1&CuerpoHasta=1"

def _requests_session_from_context(context):
    s = requests.Session()
    for c in context.cookies():
        if ".tribunales.gov.ar" in c.get("domain","") or "teletrabajo.justiciacordoba.gob.ar" in c.get("domain",""):
            s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path","/"))
    s.headers.update({"User-Agent":"Mozilla/5.0", "Referer":"https://www.tribunales.gov.ar/"})
    s.trust_env = True  # usa HTTP(S)_PROXY si el entorno los define

    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=Retry(total=3, backoff_factor=0.3, status_forcelist=[502,503,504])
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# --- URLs base ---------------------------------------------------------
TELETRABAJO_URL = "https://teletrabajo.justiciacordoba.gob.ar/remote/login?lang=sp"
URL_BASE        = "https://www.tribunales.gov.ar"
URL_LOGIN       = f"{URL_BASE}/SacInterior/Login.aspx"
URL_RADIOGRAFIA = f"{URL_BASE}/SacInterior/_Expedientes/Radiografia.aspx"
INTRANET_DESKTOP_URL     = "https://www.tribunales.gov.ar/PortalWeb/Privates/MyDesktop.aspx"
INTRANET_PUBLIC_APPS_URL = "https://www.tribunales.gov.ar/PortalWeb/PublicApps.aspx"
SAC_MENU_DEFAULT_URL     = "https://www.tribunales.gov.ar/SacInterior/Menu/Default.aspx"

def _is_teletrabajo(u: str) -> bool:
    return "teletrabajo.justiciacordoba.gob.ar" in (u or "")

def _is_tribunales(u: str) -> bool:
    import re
    return bool(re.search(r"https?://(www\.)?tribunales\.gov\.ar", u or "", re.I))

from io import BytesIO
import subprocess, shutil as _shutil

def _ensure_pdf(path: Path) -> Path:
    """
    Si path ya es PDF → lo devuelve.
    Si es imagen → convierte con PIL.
    Si es doc/xls/ppt (y hay LibreOffice) → convierte con soffice.
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
    soffice = _shutil.which("soffice") or _shutil.which("soffice.exe") \
              or r"C:\Program Files\LibreOffice\program\soffice.exe"
    if Path(str(soffice)).exists():
        outdir = path.parent
        try:
            subprocess.run(
                [soffice, "--headless", "--convert-to", "pdf", "--outdir", str(outdir), str(path)],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            pdf = path.with_suffix(".pdf")
            if pdf.exists():
                return pdf
        except Exception:
            pass

    # si no pudimos convertir, devolvemos el original (se omitirá en la fusión si no es PDF)
    return path

def _overlay_page(w, h, texto: str):
    """Crea un overlay PDF (marco + cabecera) en memoria del tamaño (w,h)."""
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=(w, h))
    margin = 18
    c.setLineWidth(1)
    c.rect(margin, margin, w - 2*margin, h - 2*margin)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin + 10, h - margin + 2, texto[:180])
    c.save()
    buf.seek(0)
    return PdfReader(buf).pages[0]


from PyPDF2 import PdfReader, PdfWriter, PdfMerger  # ← sumá PdfMerger

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
                    page.draw_rect(fitz.Rect(margin, margin, rect.width - margin, rect.height - margin), width=1)
                    try:
                        page.insert_text((margin + 10, rect.height - margin + 2), title,
                                         fontname="helv", fontsize=12)
                    except Exception:
                        page.insert_text((margin + 10, rect.height - margin + 2), title, fontsize=12)
            logging.info(f"[MERGE:+FITZ] {Path(pdf_path).name} · páginas={end-start} · header={'sí' if header_text else 'no'}")
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

            # bloque con header → estampar a archivo temporal
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
            try: t.unlink()
            except Exception: pass


# ───────────────────────── UTILIDADES PDF ──────────────────────────────
def _estampar_header(origen: Path, destino: Path, texto="ADJUNTO"):
    """
    Dibuja un marco en todo el borde y un texto (e.g. 'ADJUNTO – archivo.pdf')
    en la parte superior de CADA página del PDF 'origen', y lo guarda en 'destino'.
    """
    r = PdfReader(str(origen))
    w = PdfWriter()

    for i, p in enumerate(r.pages):
        # tamaño real de la página
        pw = float(p.mediabox.width)
        ph = float(p.mediabox.height)

        # overlay temporal de igual tamaño
        tmp = origen.with_suffix(f".overlay_{i}.pdf")
        c = canvas.Canvas(str(tmp), pagesize=(pw, ph))

        # marco
        margin = 18
        c.setLineWidth(1)
        c.rect(margin, margin, pw - 2*margin, ph - 2*margin)

        # cabecera
        try:
            title = str(texto)
        except Exception:
            title = texto
        c.setFont("Helvetica-Bold", 12)
        c.drawString(margin + 10, ph - margin + 2, title[:150])  # por si es largo
        c.save()

        # fusionar overlay con la página original
        overlay = PdfReader(str(tmp)).pages[0]
        p.merge_page(overlay)
        w.add_page(p)
        try: tmp.unlink()
        except Exception: pass

    with open(destino, "wb") as f:
        w.write(f)
def _libro_scope(libro):
    """Devuelve la página/frame que realmente contiene el índice y las operaciones."""
    try:
        if libro.locator("a[onclick^='onItemClick']").first.count():
            return libro
    except Exception:
        pass
    for fr in libro.frames:
        try:
            if fr.locator("a[onclick^='onItemClick']").first.count():
                return fr
        except Exception:
            continue
    return libro

def _listar_operaciones_rapido(libro):
    import re
    S = _libro_scope(libro)
    try:
        S.wait_for_load_state("domcontentloaded")
    except Exception:
        pass

    cont = S.locator("#indice, .nav-container").first
    if not cont.count():
        try:
            S.get_by_text(re.compile(r"índice", re.I)).first.click()
            S.wait_for_timeout(200)
            cont = S.locator("#indice, .nav-container").first
        except Exception:
            pass
    if not cont.count():
        raise RuntimeError("No encontré el panel del Índice en 'Expediente como Libro'.")

    # expandir grupos colapsados (sin abrir cada operación)
    for _ in range(20):
        t = cont.locator("a.nav-link.dropdown-toggle[aria-expanded='false']").first
        if not t.count():
            break
        try:
            t.click()
        except Exception:
            try: t.evaluate("el => el.click()")
            except Exception: pass
        S.wait_for_timeout(50)

    anchors = cont.locator("a[onclick^='onItemClick']")
    n = anchors.count()
    items = []
    for i in range(n):
        a = anchors.nth(i)
        oc = a.get_attribute("onclick") or ""
        m = re.search(r"onItemClick\('([^']+)'\s*,\s*'([^']+)'", oc)
        if not m:
            continue
        t = (a.inner_text() or "").strip()
        items.append({"id": m.group(1), "tipo": m.group(2), "titulo": t})
    return items


def _ocultar_indice_libro(libro):
    """Inyecta CSS para ocultar índice/menus sin tapar el visor de fojas."""
    css = """
    #indice, .indice, .nav-container, .menuLateral { display:none !important; }
    a[href*="Imprimir"], [onclick*="Imprimir"], .goup, .go-top, .scrollup { display:none !important; }
    """
    try:
        _libro_scope(libro).add_style_tag(content=css)
    except Exception:
        pass

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

# ─────────────────────────── Helpers UI/DOM ────────────────────────────
def _pick_selector(page, candidates):
    for s in candidates:
        try:
            if page.query_selector(s): return s
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

    # Sin proxy → Intranet directa
    return ""


def _handle_loginconfirm(page):
    """Si aparece 'Already Logged In', clic en 'Log in Anyway'."""
    if re.search(r"/remote/loginconfirm", page.url, re.I):
        for sel in ["text=Log in Anyway",
                    "a:has-text('Log in Anyway')",
                    "button:has-text('Log in Anyway')"]:
            try:
                page.locator(sel).first.click()
                page.wait_for_load_state("networkidle")
                break
            except Exception:
                pass

def _goto_portal_grid(page):
    # Aseguramos la grilla del portal
    page.goto("https://teletrabajo.justiciacordoba.gob.ar/static/sslvpn/portal/",
              wait_until="domcontentloaded")
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
    if not js: return None
    m = re.search(r"https?://[^\s'\"()]+", js)
    if m: return m.group(0)
    m = re.search(r"/proxy/[^'\"()]+", js)
    if m: return "https://teletrabajo.justiciacordoba.gob.ar" + m.group(0)
    return None
def _fill_radiografia_y_buscar(page, nro_exp):
    """Completa el Nº de Expediente en Radiografía y ejecuta la búsqueda (Enter o botón)."""
    def _first_visible(selectors):
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if loc.count():
                    try: loc.wait_for(state="visible", timeout=1500)
                    except Exception: pass
                    if loc.is_visible():
                        return loc
            except Exception:
                pass
        return None

    # 1) textbox (ids pueden cambiar: usamos 'termina con' y varios fallbacks)
    txt = _first_visible([
        "#txtNroExpediente",
        "input[id$='txtNroExpediente']",
        "input[name$='txtNroExpediente']",
        "xpath=//label[normalize-space()='Número de Expediente:']/following::input[1]",
        "xpath=//td[contains(normalize-space(.),'Número de Expediente')]/following::input[1]",
        "xpath=//input[@type='text' and (contains(@id,'Expediente') or contains(@name,'Expediente'))]"
    ])
    if not txt:
        # último recurso: primer textbox visible del panel central
        txt = page.get_by_role("textbox").first
        if not txt or not txt.count():
            _debug_dump(page, "no_txt_expediente")
            raise RuntimeError("No pude ubicar el campo 'Número de Expediente'.")

    try: txt.scroll_into_view_if_needed()
    except Exception: pass

    txt.click()
    txt.fill(str(nro_exp))

    # 2) Enter y, si no dispara, probamos el botón
    try:
        txt.press("Enter")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    # botón “Buscar” (la lupita) – varios posibles selectores
    if "Radiografia.aspx" in page.url:  # seguimos en la vista → quizá no buscó
        btn = _first_visible([
            "#btnBuscarExp",
            "input[id$='btnBuscarExp']",
            "xpath=//input[@type='image' or @type='submit'][contains(@id,'Buscar') or contains(@value,'Buscar')]",
            "xpath=//a[.//img[contains(@src,'buscar') or contains(@alt,'Buscar')]]",
        ])
        if btn:
            try:
                btn.click()
                page.wait_for_load_state("networkidle")
            except Exception:
                pass
        else:
            # click al primer botón vecino del input (por si es una imagen)
            try:
                txt.evaluate("""
                    el => {
                        const c = el.parentElement;
                        const b = c && (c.querySelector("input[type=image],input[type=submit],button,a"));
                        if (b) b.click();
                    }
                """)
                page.wait_for_load_state("networkidle")
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
        if nro_exp:                              # <- re-busca el expediente
            _fill_radiografia_y_buscar(sac, nro_exp)

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
                # si cayó al login → volver a Radiografía y reintentar una vez
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
                        if v: return (v or "").strip()
                except Exception:
                    pass
        return None

    exp_id = _read_hidden_generic(sac, ["hdIdExpediente", "hdExpedienteId"])
    if not exp_id:
        _debug_dump(sac, "no_hdIdExpediente")
        raise RuntimeError("No encontré el id del expediente (hdIdExpediente/hdExpedienteId).")

    key  = _read_hidden_generic(sac, ["hdIdExpedienteKey"]) or ""
    lvl  = _read_hidden_generic(sac, ["hdNivelAcceso"]) or ""

    base = "https://www.tribunales.gov.ar/SacInterior/_Expedientes/ExpedienteLibro.aspx"
    proxy_prefix = _get_proxy_prefix(sac)
    qs = f"idExpediente={exp_id}" + (f"&key={key}" if key else "") + (f"&nivelAcceso={lvl}" if lvl else "")
    url = proxy_prefix + base + "?" + qs

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
        return _abrir_libro_legacy(sac)                 # Teletrabajo intacto
    return _abrir_libro_intranet(sac, intra_user, intra_pass, nro_exp)


def _recorrer_indice_libro(libro):
    """
    Clickea cada entrada del índice del Libro para forzar la carga de
    todas las fojas en el visor. Tolera índice colapsado, ajax y re-render.
    """
    # Asegurar que se vea el índice (algunas skins lo colapsan bajo una pestaña "Índice")
    try:
        if libro.locator("#indice").count() == 0:
            libro.get_by_text(re.compile(r"índice", re.I)).first.click()
            libro.wait_for_timeout(200)
    except Exception:
        pass

    # Selector robusto de links del índice (según tu HTML: onclick=onItemClick(...))
    sel_links = "a[onclick*='onItemClick'], #indice a, .nav a"

    visitados = set()
    orden = [] 
    max_pasadas = 50  # por si hay re-render/virtualización

    for _ in range(max_pasadas):
        loc = libro.locator(sel_links)
        n = loc.count()
        nuevos = []

        for i in range(n):
            a = loc.nth(i)
            try:
                txt  = (a.inner_text() or "").strip()
            except Exception:
                txt = ""
            href = a.get_attribute("href") or ""
            oc   = a.get_attribute("onclick") or ""
            key  = (txt, href, oc)
            if key not in visitados:
                nuevos.append((i, key))

        if not nuevos:
            break

        for i, key in nuevos:
            a = loc.nth(i)
            try:
                a.scroll_into_view_if_needed()
            except Exception:
                pass
            # click normal → fallback a click JS
            try:
                a.click(timeout=1500)
            except Exception:
                try:
                    a.evaluate("el => el.click()")
                except Exception:
                    pass

            visitados.add(key)
            # pequeño respiro para que la foja cargue en el panel derecho
            libro.wait_for_timeout(120)

        # scrollear el contenedor del índice para revelar más elementos
        try:
            if libro.locator("#indice").count():
                libro.eval_on_selector("#indice", "(el)=>el.scrollBy(0, el.clientHeight)")
            else:
                libro.mouse.wheel(0, 900)
        except Exception:
            pass

    # un último respiro antes del PDF
    libro.wait_for_timeout(300)

# ───────────────── Capturar UNA operación a PDF ─────────────────
from PIL import Image
def _capturar_operacion_a_pdf(libro, op_id: str, tmp_dir: Path) -> Path | None:
    S = _libro_scope(libro)
    _cerrar_indice_libro(libro)

    cont = S.locator(f"[id='{op_id}'], [data-codigo='{op_id}']").first
    try:
        cont.wait_for(state="visible", timeout=5000)
    except Exception:
        return None

    # Normalización básica para que no haya sticky/overflow raros
    try:
        S.evaluate("""(id) => {
            const el = document.querySelector(`[id='${id}'], [data-codigo='${id}']`);
            if (!el) return;
            el.style.overflow = 'visible'; el.style.maxHeight = 'unset'; el.style.height = 'auto';
            el.style.transform = 'none'; el.style.zoom = 'unset';
            el.querySelectorAll('*').forEach(n => {
                const cs = getComputedStyle(n);
                if (/(sticky|fixed)/.test(cs.position)) n.style.position = 'static';
                if (/(auto|scroll|hidden)/.test(cs.overflowY)) n.style.overflow = 'visible';
                if (n.style.maxHeight && n.style.maxHeight !== 'none') n.style.maxHeight = 'unset';
            });
        }""", op_id)
    except Exception:
        pass

    # → Captura directa del elemento (rápida) con timeout largo
    elem_png = tmp_dir / f"op_{op_id}.png"
    try:
        cont.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        cont.screenshot(
            path=str(elem_png),
            animations="disabled",
            caret="hide",
            timeout=120_000
        )
        return _imagen_a_pdf(elem_png)
    except Exception:
        # Fallback: clip al bounding box (sin full_page)
        bb = cont.bounding_box()
        if not bb:
            return None
        clip_png = tmp_dir / f"op_{op_id}_clip.png"
        libro.screenshot(
            path=str(clip_png),
            clip={"x": bb["x"], "y": bb["y"], "width": bb["width"], "height": bb["height"]},
            animations="disabled",
            caret="hide",
            timeout=120_000
        )
        return _imagen_a_pdf(clip_png)



def _descargar_adjuntos_de_operacion(libro, op_id: str, carpeta: Path) -> list[Path]:
    """
    Encuentra y descarga los adjuntos que cuelgan de UNA operación dentro del Libro.
    Convierte a PDF si hace falta. Devuelve lista de Paths a PDFs.
    """
    pdfs: list[Path] = []
    scope = libro.locator(f"[id='{op_id}'], [data-codigo='{op_id}']")
    if not scope.count():
        return pdfs

    triggers = scope.locator(
        "[onclick*='VerAdjuntoFichero'], a[href*='Fichero.aspx'], a:has-text('Adjunto'), a[href*='VerAdjunto']"
    )
    n = 0
    try: n = triggers.count()
    except Exception: n = 0

    for i in range(n):
        link = triggers.nth(i)
        try:
            with libro.expect_download() as dl:
                try: link.click()
                except Exception:
                    try: link.evaluate("el => el.click()")
                    except Exception: continue
            d = dl.value
            destino = carpeta / d.suggested_filename
            d.save_as(destino)

            pdf = _ensure_pdf(destino)
            if pdf.suffix.lower() == ".pdf" and pdf.exists():
                pdfs.append(pdf)
        except Exception:
            # Si algo abre otra pestaña y falla, seguimos con el resto
            continue
    return pdfs


def _descargar_adjuntos_grid_mapeado(sac, carpeta: Path) -> dict[str, list[Path]]:
    """
    Devuelve { op_id: [PDFs...] } leyendo la grilla “Adjuntos” de Radiografía.
    Convierte a PDF cuando corresponde.
    """
    mapeo: dict[str, list[Path]] = {}

    # asegurar que la sección esté visible
    try:
        toggle = sac.locator("a[href*=\"Seccion('Adjuntos')\"]").first
        cont   = sac.locator("#divAdjuntos").first
        oculto = False
        if cont.count():
            try: oculto = cont.evaluate("el => getComputedStyle(el).display === 'none'")
            except Exception: pass
            if oculto and toggle.count(): toggle.click(); sac.wait_for_timeout(250)
        elif toggle.count(): toggle.click(); sac.wait_for_timeout(250)
    except Exception:
        pass

    filas = sac.locator("#cphDetalle_gvAdjuntos tr")
    total = filas.count() if filas else 0

    for i in range(1, total):
        fila = filas.nth(i)

        # op_id en la col. “Operación – Tipo de Operación”
        op_link = fila.locator("a[href*='VerDecretoHtml'], a[onclick*='VerDecretoHtml']").first
        op_id = None
        if op_link.count():
            href = op_link.get_attribute("href") or ""
            oc   = op_link.get_attribute("onclick") or ""
            m = re.search(r"VerDecretoHtml\('([^']+)'\)", href or oc)
            if m: op_id = m.group(1)

        file_link = fila.locator("a[href*='VerAdjuntoFichero'], a[href*='Fichero.aspx']").first
        if not file_link.count():
            continue

        try:
            with sac.expect_download() as dl:
                try: file_link.click()
                except Exception: file_link.evaluate("el => el.click()")
            d = dl.value
            destino = carpeta / d.suggested_filename
            d.save_as(destino)

            pdf = _ensure_pdf(destino)
            if pdf.suffix.lower() == ".pdf" and pdf.exists():
                mapeo.setdefault(op_id or "__SIN_OP__", []).append(pdf)
        except Exception:
            continue

    return mapeo



# ───────────────────── Portal → “Portal de Aplicaciones PJ” ────────────
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
        href, onclick = target.evaluate("""el=>{
          const a = el.querySelector('a[href]');
          return [a ? a.getAttribute('href') : null,
                  (a && a.getAttribute('onclick')) || el.getAttribute('onclick') || ""];
        }""")
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
    page.goto(proxy_prefix + "https://www.tribunales.gov.ar/PortalWeb/LogIn.aspx",
              wait_until="domcontentloaded")
    return page

# ───────────────────────── Intranet helpers ────────────────────────────
def _login_intranet(page, intra_user, intra_pass):
    """
    Login en la PÁGINA o en el FRAME que contenga el formulario.
    Si ya ve “Aplicaciones” / “Mi Escritorio” / “Desconectarse”, asume sesión activa.
    """
    import re
    page.wait_for_load_state("domcontentloaded")

    scopes = [page] + list(page.frames)

    # ¿Ya estamos adentro?
    for sc in scopes:
        try:
            if sc.get_by_text(re.compile(r"\b(Aplicaciones|Mi\s*Escritorio|Desconectarse)\b", re.I)).first.count():
                return
        except Exception:
            pass

    # Helpers
    def _first_visible(sc, selectors):
        for sel in selectors:
            try:
                loc = sc.locator(sel).first
                if loc.count():
                    try: loc.wait_for(state="visible", timeout=1500)
                    except Exception: pass
                    if loc.is_visible():
                        return loc
            except Exception:
                pass
        return None

    # Intentar encontrar usuario + password en algún scope (página o frame)
    user_sels = [
        "#txtUserName", "#txtUsuario",
        "input[id$='UserName']", "input[name$='UserName']",
        "input[id*='UserLogin'][type='text']", "input[name*='UserLogin'][type='text']",
        "input[type='text'][name*='Usuario']",
        "input[type='text'][aria-label*='Usuario']",
    ]
    pass_sels = [
        "#txtUserPassword", "#txtContrasena",
        "input[id$='Password']", "input[name$='Password']",
        "input[type='password']",
    ]
    btn_sels = [
        "#btnLogIn", "#btnIngresar",
        "button[type='submit']", "input[type='submit']",
        "xpath=//button[contains(.,'Ingresar') or contains(.,'Login') or contains(.,'Entrar')]",
        "xpath=//input[@type='submit' and (contains(@value,'Ingresar') or contains(@value,'Login') or contains(@value,'Entrar'))]"
    ]

    target_scope = None
    user_box = None
    pass_box = None

    for sc in scopes:
        u = _first_visible(sc, user_sels)
        p = _first_visible(sc, pass_sels)
        if u and p:
            target_scope, user_box, pass_box = sc, u, p
            break

    # Heurístico: si no encontramos ambos, tomamos el primer password visible y el textbox más cercano en ese mismo scope
    if not (target_scope and user_box and pass_box):
        for sc in scopes:
            p = _first_visible(sc, ["input[type='password']"])
            if not p:
                continue
            # username candidate: el primer input de texto visible en el mismo contenedor
            u = _first_visible(sc, ["input[type='text']"])
            if u:
                target_scope, user_box, pass_box = sc, u, p
                break

    if not (target_scope and user_box and pass_box):
        # No hay formulario visible: nada que hacer
        return

    # Limpiar overlays que interceptan clicks
    _kill_overlays(target_scope)

    # Completar credenciales (fill limpia el contenido previo)
    try:
        user_box.click()
    except Exception:
        pass
    user_box.fill(intra_user)

    try:
        pass_box.click()
    except Exception:
        pass
    pass_box.fill(intra_pass)

    # Click al botón dentro del MISMO scope
    btn = _first_visible(target_scope, btn_sels)
    if btn:
        try:
            btn.click(timeout=3000)
        except Exception:
            _kill_overlays(target_scope)
            try:
                btn.click(force=True, timeout=2000)
            except Exception:
                # Último recurso: postback típico
                try:
                    target_scope.evaluate(
                        "() => { try { __doPostBack('ctl00$ctl00$Sheader$body$UserLogin$btnLogIn','') } catch(e) {} }"
                    )
                except Exception:
                    pass
    else:
        # Sin botón visible, intentamos Enter en el password
        try:
            pass_box.press("Enter")
        except Exception:
            pass

    # Esperar a que cargue (si el login fue correcto cambiamos de vista)
    try:
        target_scope.wait_for_load_state("networkidle")
    except Exception:
        pass


def _kill_overlays(page):
    """Oculta/remueve cortinas/overlays que interceptan el click (jQuery UI / modales)."""
    try:
        page.evaluate("""
        () => {
            const sels = [
              '#divDialogCourtian_0', '.divDialogCourtian', '.divDialogCortina',
              '.ui-widget-overlay', '.ui-widget-shadow',
              '.modal-backdrop', '.modal[role=dialog]'
            ];
            for (const s of sels) {
                document.querySelectorAll(s).forEach(el => {
                    el.style.pointerEvents = 'none';
                    el.style.display = 'none';
                    el.remove();
                });
            }
        }
        """)
    except Exception:
        pass



def _ensure_public_apps(page):
    """
    Nos posiciona en PublicApps.aspx (listado 'Aplicaciones') con el mismo /proxy/.
    Evita errores “Access Denied” por deep-link sin contexto.
    """
    proxy_prefix = _get_proxy_prefix(page)
    page.goto(proxy_prefix + "https://www.tribunales.gov.ar/PortalWeb/PublicApps.aspx",
              wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

# ───────────────────────── CARGA DEL LIBRO ─────────────────────────────
def _expandir_y_cargar_todo_el_libro(libro):
    """
    Devuelve lista de dicts [{'id':op_id, 'tipo':tipo, 'titulo':texto}], en orden.
    Hace clic en cada item para precargarlo.
    """
    import re
    S = _libro_scope(libro)

    try:
        S.wait_for_load_state("domcontentloaded")
        S.wait_for_load_state("networkidle")
    except Exception:
        pass

    cont = S.locator("#indice, .nav-container").first
    if not cont.count():
        try:
            S.get_by_text(re.compile(r"índice", re.I)).first.click()
            S.wait_for_timeout(200)
            cont = S.locator("#indice, .nav-container").first
        except Exception:
            pass
    if not cont.count():
        raise RuntimeError("No encontré el panel del Índice en 'Expediente como Libro'.")

    # expandir grupos colapsados
    for _ in range(20):
        t = cont.locator("a.nav-link.dropdown-toggle[aria-expanded='false']").first
        if not t.count(): break
        try: t.scroll_into_view_if_needed()
        except Exception: pass
        try: t.click()
        except Exception:
            try: t.evaluate("el => el.click()")
            except Exception: pass
        S.wait_for_timeout(80)

    anchors = cont.locator("a[onclick^='onItemClick']")
    n = anchors.count()
    items = []
    for i in range(n):
        a = anchors.nth(i)
        oc = a.get_attribute("onclick") or ""
        m = re.search(r"onItemClick\('([^']+)'\s*,\s*'([^']+)'", oc)
        if not m:
            continue
        t = (a.inner_text() or "").strip()
        items.append({"id": m.group(1), "tipo": m.group(2), "titulo": t})

    # precargar todas
    orden = []
    for it in items:
        try:
            a = cont.locator(f"a[onclick*=\"onItemClick('{it['id']}'\"]").first
            try: a.click(timeout=700)
            except Exception:
                try: S.evaluate("([id,t]) => onItemClick && onItemClick(id,t)", [it["id"], it["tipo"]])
                except Exception: pass
            S.wait_for_selector(f"[id='{it['id']}'], [data-codigo='{it['id']}']", timeout=1200)
        except Exception:
            pass
        orden.append(it)

    return orden


def _mostrar_operacion(libro, op_id: str, tipo: str):
    S = _libro_scope(libro)
    # Intento clic al link visible del índice
    link = S.locator(f"a[onclick*=\"onItemClick('{op_id}'\"]").first
    if link.count():
        try: link.click()
        except Exception:
            try: link.evaluate("el => el.click()")
            except Exception: pass
    else:
        # Fallback: invocar la función JS
        try:
            S.evaluate("""([id,t]) => { if (window.onItemClick) onItemClick(id, t); }""", [op_id, tipo])
        except Exception:
            pass
    try:
        S.wait_for_selector(f"[id='{op_id}'], [data-codigo='{op_id}']", timeout=3000)
    except Exception:
        S.wait_for_timeout(200)

def _extraer_url_de_link(link, proxy_prefix: str) -> str | None:
    href = link.get_attribute("href") or ""
    oc   = link.get_attribute("onclick") or ""

    # 1) Caso clásico: URL absoluta o /proxy/ relativo
    url  = _extract_url_from_js(href or oc)
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

def _listar_adjuntos_urls_de_operacion(libro, op_id:str) -> list[tuple[str,str]]:
    """
    Devuelve [(url, nombre_sugerido), ...] de adjuntos que cuelgan de UNA operación del Libro.
    """
    urls = []
    scope = libro.locator(f"[id='{op_id}'], [data-codigo='{op_id}']")
    if not scope.count():
        return urls
    proxy_prefix = _get_proxy_prefix(libro)
    triggers = scope.locator("[onclick*='VerAdjuntoFichero'], a[href*='Fichero.aspx'], a:has-text('Adjunto'), a[href*='VerAdjunto']")
    n = triggers.count() if triggers else 0
    vistos = set()
    # si no encuentra triggers visibles, buscá por texto 'ADJUNTOS' y luego dentro de ese bloque
    if not triggers.count():
        alert = scope.locator(":text('ADJUNTOS')").first
        if alert.count():
            triggers = alert.locator("a[href], a[onclick]")

    for i in range(n):
        link = triggers.nth(i)
        try:
            u = _extraer_url_de_link(link, proxy_prefix)
            if not u or u in vistos: 
                continue
            vistos.add(u)
            # nombre por defecto: último segmento de la URL
            name = Path(urlparse(u).path).name or f"adj_{i}"
            urls.append((u, name))
        except Exception:
            continue
    return urls

def _listar_adjuntos_urls_grid_mapeado(sac) -> dict[str, list[tuple[str,str]]]:
    """
    Igual que _descargar_adjuntos_grid_mapeado pero NO descarga: devuelve {op_id:[(url,name),...]}
    """
    mapeo: dict[str,list[tuple[str,str]]] = {}
    proxy_prefix = _get_proxy_prefix(sac)
    try:
        toggle = sac.locator("a[href*=\"Seccion('Adjuntos')\"]").first
        cont   = sac.locator("#divAdjuntos").first
        oculto = False
        if cont.count():
            try: oculto = cont.evaluate("el => getComputedStyle(el).display === 'none'")
            except Exception: pass
            if oculto and toggle.count(): toggle.click(); sac.wait_for_timeout(250)
        elif toggle.count(): toggle.click(); sac.wait_for_timeout(250)
    except Exception:
        pass

    filas = sac.locator("#cphDetalle_gvAdjuntos tr")
    total = filas.count() if filas else 0
    for i in range(1, total):
        fila = filas.nth(i)
        op_link = fila.locator("a[href*='VerDecretoHtml'], a[onclick*='VerDecretoHtml']").first
        op_id = None
        if op_link.count():
            href = op_link.get_attribute("href") or ""
            oc   = op_link.get_attribute("onclick") or ""
            m = re.search(r"VerDecretoHtml\('([^']+)'\)", href or oc)
            if m: op_id = m.group(1)

        file_link = fila.locator("a[href*='VerAdjuntoFichero'], a[href*='Fichero.aspx']").first
        if not file_link.count(): 
            continue

        u = _extraer_url_de_link(file_link, proxy_prefix)
        if not u: 
            continue

        name = Path(urlparse(u).path).name or f"adj_{i}"
        mapeo.setdefault(op_id or "__SIN_OP__", []).append((u, name))
    return mapeo


def _descargar_archivo(session: requests.Session, url: str, destino: Path) -> Path | None:
    from requests.exceptions import SSLError
    from urllib.parse import urlparse
    import urllib3

    nombre = Path(urlparse(url).path).name or destino.name
    host = (urlparse(url).hostname or "").lower()
    logging.info(f"[DL:START] {nombre}  →  {destino.name}")

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
        logging.info(f"[DL:OK]    {destino.name} ({sz} bytes)")
        return destino

    except SSLError as e:
        msg = str(e).lower()
        # fallback SOLO si es el host de tribunales y el problema es verificación de cert
        if host.endswith("tribunales.gov.ar") and ("self-signed" in msg or "certificate verify failed" in msg):
            logging.info(f"[DL:WARN] SSL en {host} (self-signed). Reintento sin verificación TLS.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            try:
                with session.get(url, stream=True, timeout=60, verify=False) as r:
                    r.raise_for_status()
                    _stream_to_file(r)
                sz = destino.stat().st_size if destino.exists() else 0
                logging.info(f"[DL:OK/INSECURE] {destino.name} ({sz} bytes)")
                return destino
            except Exception as e2:
                logging.info(f"[DL:ERR]   {destino.name} · {e2}")
                return None
        # cualquier otro SSLError
        logging.info(f"[DL:ERR]   {destino.name} · {e}")
        return None

    except Exception as e:
        logging.info(f"[DL:ERR]   {destino.name} · {e}")
        return None



def _imagen_a_pdf_fast(img: Path) -> Path:
    pdf = img.with_suffix(".pdf")
    logging.info(f"[CNV:IMG]  {img.name}  →  {pdf.name}")
    try:
        import img2pdf
        with open(pdf, "wb") as f:
            f.write(img2pdf.convert(str(img)))
        logging.info(f"[CNV:OK ]  {pdf.name}")
        return pdf
    except Exception:
        Image.open(img).save(pdf, "PDF", resolution=144.0)
        logging.info(f"[CNV:OK ]  {pdf.name}")
        return pdf


def _ensure_pdf_fast(path: Path) -> Path:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return path
    if ext in {".jpg",".jpeg",".png",".tif",".tiff",".bmp"}:
        return _imagen_a_pdf_fast(path)

    soffice = _shutil.which("soffice") or _shutil.which("soffice.exe") or r"C:\Program Files\LibreOffice\program\soffice.exe"
    if soffice and Path(str(soffice)).exists():
        outdir = path.parent
        dst = path.with_suffix(".pdf")
        logging.info(f"[CNV:OFF] {path.name}  →  {dst.name}")
        try:
            subprocess.run([soffice, "--headless", "--convert-to", "pdf", "--outdir", str(outdir), str(path)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            pdf = path.with_suffix(".pdf")
            if pdf.exists():
                logging.info(f"[CNV:OK ]  {pdf.name}")
                return pdf
        except Exception as e:
            logging.info(f"[CNV:ERR] {path.name} · {e}")
    return path


def _bajar_urls_en_paralelo(urls:list[tuple[str,str]], carpeta:Path, session:requests.Session, max_workers=6, prefix:str|None=None) -> list[Path]:
    destinos = []
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for url, name in urls:
            base = (prefix or "") + name
            dst = carpeta / base
            k = 1
            while dst.exists():
                dst = carpeta / (f"{Path(base).stem}_{k}{Path(base).suffix}")
                k += 1
            futures.append(ex.submit(_descargar_archivo, session, url, dst))
        for fut in as_completed(futures):
            p = fut.result()
            if p: destinos.append(p)
    return destinos


def _convertir_a_pdf_en_paralelo(paths: list[Path], temp_dir: Path | None = None) -> list[Path]:
    img_ext = {".jpg",".jpeg",".png",".tif",".tiff",".bmp"}
    office_ext = {".doc",".docx",".xls",".xlsx",".ppt",".pptx",".odt",".ods",".odp",".rtf"}

    pdfs: list[Path] = []
    for p in paths:
        if p.suffix.lower() == ".pdf" and p.exists():
            logging.info(f"[CNV:SKP] {p.name} (ya es PDF)")
            pdfs.append(p)

    imgs = [p for p in paths if p.suffix.lower() in img_ext]
    offices = [p for p in paths if p.suffix.lower() in office_ext]

    if imgs:
        logging.info(f"[CNV:IMG]  {len(imgs)} imagen(es) a convertir")
    if offices:
        logging.info(f"[CNV:OFF]  {len(offices)} archivo(s) Office a convertir")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=os.cpu_count()*2 or 4) as ex:
        futures = [ex.submit(_imagen_a_pdf_fast, p) for p in imgs]
        for f in as_completed(futures):
            out = f.result()
            if out and out.exists():
                pdfs.append(out)

    for p in offices:
        out = _ensure_pdf_fast(p)
        if out.suffix.lower() == ".pdf" and out.exists():
            pdfs.append(out)

    return pdfs

def _open_sac_desde_portal_teletrabajo(page):
    """
    *** SOLO Teletrabajo ***
    Abre el menú 'Aplicaciones' (img#imgMenuServiciosPrivadas) y entra a 'SAC Multifuero'.
    Es el flujo que ya te funcionaba y NO usa navegación directa sin proxy.
    """
    import re
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
        raise RuntimeError("No encontré el botón 'Aplicaciones' (id imgMenuServiciosPrivadas).")

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
            try: trigger.evaluate("el => el.click()")
            except Exception: pass
        scope.wait_for_timeout(250)

        link = scope.get_by_role("link", name=matcher)
        if not link.count():
            link = scope.locator("a", has_text=matcher)
        if link.first.count():
            link = link.first
            break

    if not link or not link.count():
        _debug_dump(page, "apps_menu_sin_sac")
        raise RuntimeError("No encontré el enlace a 'SAC Multifuero' dentro de Aplicaciones.")

    # Puede ser popup o misma pestaña
    try:
        with page.context.expect_page() as pop:
            link.click()
        sac = pop.value
        sac.wait_for_load_state("domcontentloaded")
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
        href, onclick = link.evaluate("el => [el.getAttribute('href'), el.getAttribute('onclick') || '']")
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
    raise RuntimeError("No pude abrir 'SAC Multifuero' pese a desplegar el menú (ver click_sac_fail.*).")


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
            except Exception:
                continue

    if link.count():
        try:
            with (page.context if hasattr(page, "context") else page).expect_page(timeout=7000) as pop:
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
        raise RuntimeError("No hallé link a SAC y no hay proxy activo; evito navegación directa en Teletrabajo.")

    dest = (proxy_prefix or "") + "https://www.tribunales.gov.ar/SacInterior/Menu/Default.aspx"
    page.goto(dest, wait_until="domcontentloaded")
    return page


def _open_sac_desde_portal(page):
    """
    Dispatcher:
    - Si estamos en Teletrabajo (URL del portal o proxificada) → versión Teletrabajo (menú Aplicaciones).
    - Si estamos en tribunales.gov.ar o ya proxificados → versión Intranet.
    """
    u = page.url or ""
    if "teletrabajo.justiciacordoba.gob.ar" in u or "/proxy/" in u:
        return _open_sac_desde_portal_teletrabajo(page)
    return _open_sac_desde_portal_intranet(page)



def _ir_a_radiografia(sac):
    """
    Preferir el menú de SAC → “Radiografía”. Si no aparece, usar URL con el mismo /proxy/.
    """
    try:
        sac.get_by_role("link", name=re.compile(r"Radiograf[íi]a", re.I)).first.click()
        sac.wait_for_load_state("networkidle")
        if "Radiografia.aspx" in sac.url:
            return sac
    except Exception:
        pass

    proxy_prefix = _get_proxy_prefix(sac)
    sac.goto(proxy_prefix + URL_RADIOGRAFIA, wait_until="domcontentloaded")
    return sac

# ─────────────────────── Flujo principal de login ──────────────────────
def abrir_sac_via_teletrabajo(context, tele_user, tele_pass, intra_user, intra_pass):
    page = context.new_page()
    page.set_default_timeout(30000)
    page.set_default_navigation_timeout(45000)

    # 1) Login Teletrabajo
    page.goto(TELETRABAJO_URL, wait_until="domcontentloaded")
    _fill_first(page, ['#username','input[name="username"]','input[name="UserName"]','input[type="text"]'], tele_user)
    _fill_first(page, ['#password','input[name="password"]','input[type="password"]'], tele_pass)
    if not _click_first(page, ['text=Continuar','button[type="submit"]','input[type="submit"]']):
        page.keyboard.press("Enter")
    page.wait_for_load_state("networkidle")
    _handle_loginconfirm(page)
    _goto_portal_grid(page)

    # 2) Portal de Aplicaciones PJ
    page = _open_portal_aplicaciones_pj(page)

    # 3) Login en Intranet en la página actual
    _login_intranet(page, intra_user, intra_pass)

    # 4) Ir a Aplicaciones y abrir SAC
    sac = _open_sac_desde_portal_teletrabajo(page)

    # 5) Si el proxy niega el acceso, reintentamos 1 vez desde Aplicaciones
    if _is_proxy_error(sac):
        _goto_portal_grid(page)
        page = _open_portal_aplicaciones_pj(page)
        _login_intranet(page, intra_user, intra_pass)
        sac = _open_sac_desde_portal_teletrabajo(page)

    # 6) Radiografía
    sac = _ir_a_radiografia(sac)
    return sac

def abrir_sac(context, tele_user, tele_pass, intra_user, intra_pass):
    """
    Modo UNIVERSAL:
    - Si estás en Teletrabajo → hace login en Teletrabajo, abre 'Portal de Aplicaciones PJ', luego SAC.
    - Si estás en Intranet directa → salta Teletrabajo y entra por MyDesktop.aspx a SAC.
    """
    page = context.new_page()
    page.set_default_timeout(12000)
    page.set_default_navigation_timeout(15000)

    use_tele = bool(tele_user and tele_pass)

    # 1) Intento Teletrabajo (si no aplica o falla, caemos a Intranet directa)
    if use_tele:
        try:
            page.goto(TELETRABAJO_URL, wait_until="domcontentloaded")
            if _is_teletrabajo(page.url) or page.locator("text=Portal de Aplicaciones PJ").first.count():
                # Flujo original Teletrabajo
                _fill_first(page, ['#username','input[name="username"]','input[name="UserName"]','input[type="text"]'], tele_user)
                _fill_first(page, ['#password','input[name="password"]','input[type="password"]'], tele_pass)
                if not _click_first(page, ['text=Continuar','button[type="submit"]','input[type="submit"]']):
                    page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle")
                _handle_loginconfirm(page)
                _goto_portal_grid(page)  # grilla angular
                page = _open_portal_aplicaciones_pj(page)  # Tile "Portal de Aplicaciones PJ"
                _login_intranet(page, intra_user, intra_pass)  # por si pide credenciales ahí
                sac = _open_sac_desde_portal(page)
                if _is_proxy_error(sac):
                    _goto_portal_grid(page)
                    page = _open_portal_aplicaciones_pj(page)
                    _login_intranet(page, intra_user, intra_pass)
                    sac = _open_sac_desde_portal(page)
                return _ir_a_radiografia(sac)
        except Exception:
            pass  # seguimos a Intranet directa

    # 2) Intranet directa
    try:
        page.goto(INTRANET_DESKTOP_URL, wait_until="domcontentloaded")
    except Exception:
        # Último recurso: PublicApps
        try:
            page.goto(INTRANET_PUBLIC_APPS_URL, wait_until="domcontentloaded")
        except Exception:
            pass

    _login_intranet(page, intra_user, intra_pass)

    # Garantizar estar en 'Aplicaciones' (PublicApps.aspx) aunque vengamos de MyDesktop
    try:
        _ensure_public_apps(page)
    except Exception:
        pass

    # Abrir SAC desde el menú de Aplicaciones
    try:
        sac = _open_sac_desde_portal(page)
    except Exception:
        # Fallback duro: ir directo al menú del SAC
        proxy_prefix = _get_proxy_prefix(page)  # "" si no hay proxy
        page.goto(proxy_prefix + SAC_MENU_DEFAULT_URL, wait_until="domcontentloaded")
        sac = page

    return _ir_a_radiografia(sac)


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
        "button:has-text('Índice')", "a:has-text('Índice')",
        ".indice-toggle, .indice .toggle, .indice [role=button]",
        ".nav-container .navbar-toggler",
        ".nav-container .fa-chevron-left, .nav-container .fa-angle-left, .nav-container .fa-angle-double-left",
        ".btn-indice, #btnIndice, #indiceTab, #indice-tab",
        "xpath=//*[contains(translate(normalize-space(.),'ÍNDICE','índice'),'índice')]"
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
                try: t.scroll_into_view_if_needed()
                except Exception: pass
                try: t.click()
                except Exception:
                    try: t.evaluate("el => el.click()")
                    except Exception: continue
                S.wait_for_timeout(200)
                if not visible():
                    break
            except Exception:
                continue

def _imprimir_libro_a_pdf(libro, context, tmp_dir: Path, p) -> Path | None:
    """
    Intenta obtener el PDF del 'Expediente como Libro'.
    1) Click en 'Imprimir / Imprimir Selección' y captura download si el sitio genera PDF.
    2) Si abre el diálogo del navegador (no automatable), fallback: PDF por CDP
       en un Chromium HEADLESS con el mismo estado de sesión.
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
        "a[onclick*='Imprimir']", "button[onclick*='Imprimir']",
        "a[href*='Imprimir']",
    ]
    for sel in btn_selectors:
        try:
            loc = S.locator(sel).last
            if not loc.count():
                continue
            try: loc.scroll_into_view_if_needed()
            except Exception: pass
            try:
                with libro.expect_download(timeout=20000) as dl:
                    try: loc.click(force=True)
                    except Exception: loc.evaluate("el => el.click()")
                d = dl.value
                d.save_as(out)
                if out.exists() and out.stat().st_size > 1024:
                    logging.info(f"[PRINT:DL] PDF libro guardado: {out.name}")
                    return out
            except Exception:
                # Si abrió el diálogo del navegador, no habrá download → seguimos al plan B
                pass
        except Exception:
            continue

    # 2) Fallback HEADLESS: mismo estado de sesión + Page.pdf()
    try:
        state_file = tmp_dir / "state.json"
        context.storage_state(path=str(state_file))

        hbrowser = p.chromium.launch(
            headless=True,
            args=["--disable-gpu","--no-sandbox","--disable-dev-shm-usage"]
        )
        hctx = hbrowser.new_context(
            storage_state=str(state_file),
            viewport={"width": 1366, "height": 900}
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
            hctx.close(); hbrowser.close()
        except Exception:
            pass

        if out.exists() and out.stat().st_size > 1024:
            logging.info(f"[PRINT:HEADLESS] PDF libro guardado: {out.name}")
            return out
    except Exception as e:
        logging.info(f"[PRINT:HEADLESS-ERR] {e}")

    logging.info("[PRINT] No pude obtener el PDF del Libro ni por botón ni por fallback headless.")
    return None

def _guardar_libro_html_a_pdf(libro, context, tmp_dir: Path, p) -> Path | None:
    """
    Toma un snapshot HTML del 'Expediente como Libro' (del frame real),
    le inyecta <base> + CSS de impresión y lo convierte a PDF en headless.
    Evita por completo el diálogo 'Guardar como...'.
    """
    S = _libro_scope(libro)
    _cerrar_indice_libro(libro)

    out_html = tmp_dir / "libro_snapshot.html"
    out_pdf  = tmp_dir / "libro_from_html.pdf"

    try:
        # 1) HTML actual del frame (con todas las fojas ya precargadas)
        html = S.content()

        # 2) Base para que TODOS los src/href relativos apunten vía /proxy/ al dominio real
        proxy_prefix = _get_proxy_prefix(libro)
        base_href = proxy_prefix + "https://www.tribunales.gov.ar/"

        # 3) CSS para vista de impresión (oculta índice/menús, bordes, etc.)
        extra_css = """
        @page { size: A4; margin: 12mm; }
        html, body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        #indice, .indice, .nav-container, .menuLateral, .navbar,
        .goup, .go-top, .scrollup { display: none !important; }
        """

        # 4) Inyectar <base> + <style> dentro de <head>
        try:
            html = re.sub(
                r"(?i)<head([^>]*)>",
                lambda m: f"<head{m.group(1)}><base href=\"{base_href}\"><style>{extra_css}</style>",
                html, count=1
            )
            if "<base " not in html.lower():
                html = html.replace("<head>", f"<head><base href=\"{base_href}\"><style>{extra_css}</style>", 1)
        except Exception:
            # Último recurso: prepend
            html = f"<base href=\"{base_href}\"><style>{extra_css}</style>" + html

        # 5) Guardar el HTML snapshot (útil si activás KEEP_WORK)
        with open(out_html, "w", encoding="utf-8") as f:
            f.write(html)

        # 6) Convertir a PDF con Chromium headless reusando cookies/sesión
        state_file = tmp_dir / "state.json"
        context.storage_state(path=str(state_file))

        hbrowser = p.chromium.launch(headless=True, args=["--disable-gpu","--no-sandbox","--disable-dev-shm-usage"])
        hctx = hbrowser.new_context(storage_state=str(state_file), viewport={"width": 1366, "height": 900})
        hp = hctx.new_page()

        # Cargamos el HTML con base ya inyectada (los recursos se resuelven por el proxy)
        hp.set_content(html, wait_until="domcontentloaded")
        try: hp.emulate_media(media="print")
        except Exception: pass

        hp.pdf(path=str(out_pdf), format="A4", print_background=True, prefer_css_page_size=True)

        try:
            hctx.close(); hbrowser.close()
        except Exception:
            pass

        if out_pdf.exists() and out_pdf.stat().st_size > 1024:
            logging.info(f"[HTML→PDF] PDF libro guardado: {out_pdf.name}")
            return out_pdf
    except Exception as e:
        logging.info(f"[HTML→PDF:ERR] {e}")

    return None


def _inyectar_cortes_por_operacion(libro, ops):
    """Inserta CSS para paginar: cada operación nueva comienza en página nueva."""
    S = _libro_scope(libro)
    ids = [op["id"] for op in ops if op.get("id")]
    try:
        S.evaluate("""
            (ids) => {
                const s = document.createElement('style');
                // 1) Evitar que se parta el contenido de una operación
                let css = `
                    [data-codigo], [id] { page-break-inside: avoid; break-inside: avoid-page; }
                    @page { size: A4; margin: 12mm; }
                `;
                // 2) Cada operación (menos la 1ª) empieza en página nueva
                ids.slice(1).forEach(id => {
                    css += `
                        [id='${id}'], [data-codigo='${id}']{
                            page-break-before: always; break-before: page;
                        }
                    `;
                });
                s.textContent = css;
                document.head.appendChild(s);
            }
        """, ids)
    except Exception:
        pass


def _guardar_libro_como_html(libro, tmp_dir: Path) -> Path | None:
    """
    Snapshot del 'Expediente como Libro' a un .html en disco, parecido a
    'Guardar como… / Página web completa'. Inyecta <base> (para recursos
    relativos vía /proxy/) y CSS de impresión para ocultar el índice/menus.
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
        #indice, .indice, .nav-container, .menuLateral, .navbar,
        .goup, .go-top, .scrollup, [onclick*='Imprimir'] { display: none !important; }
        """

        # Inyectar <base> + <style> al <head>
        try:
            html = re.sub(
                r"(?i)<head([^>]*)>",
                lambda m: f"<head{m.group(1)}><base href=\"{base_href}\"><style>{extra_css}</style>",
                html, count=1
            )
            if "<base " not in html.lower():
                html = html.replace("<head>", f"<head><base href=\"{base_href}\"><style>{extra_css}</style>", 1)
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
            headless=True,
            args=["--disable-gpu","--no-sandbox","--disable-dev-shm-usage"]
        )
        hctx = hbrowser.new_context(
            storage_state=str(state_file),
            viewport={"width": 1366, "height": 900}
        )
        hp = hctx.new_page()

        # Cargar el archivo local; los recursos relativos se resuelven con el <base> inyectado
        hp.goto(f"file:///{html_path.as_posix()}", wait_until="domcontentloaded")
        try: hp.emulate_media(media="print")
        except Exception: pass

        hp.pdf(path=str(out_pdf), format="A4", print_background=True, prefer_css_page_size=True)

        try:
            hctx.close(); hbrowser.close()
        except Exception:
            pass

        if out_pdf.exists() and out_pdf.stat().st_size > 1024:
            logging.info(f"[HTML→PDF] {out_pdf.name}")
            return out_pdf
    except Exception as e:
        logging.info(f"[HTML→PDF:ERR] {e}")
    return None
def _render_operacion_a_pdf_paginas(libro, op_id: str, context, p, tmp_dir: Path) -> Path | None:
    S = _libro_scope(libro)
    cont = S.locator(f"[id='{op_id}'], [data-codigo='{op_id}']").first
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
        outer
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
      /* ↑ mantiene junto el cuadro de 'Protocolo…' con el primer bloque siguiente si hay espacio */
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
    hbrowser = p.chromium.launch(headless=True, args=["--disable-gpu","--no-sandbox","--disable-dev-shm-usage"])
    try:
        hctx = hbrowser.new_context(storage_state=str(state_file), viewport={"width":1366,"height":900})
        hp = hctx.new_page()
        hp.set_content(html, wait_until="domcontentloaded")
        try: hp.emulate_media(media="print")
        except Exception: pass
        hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
    finally:
        try: hctx.close()
        except Exception: pass
        try: hbrowser.close()
        except Exception: pass

    return out if out.exists() and out.stat().st_size > 500 else None


def _render_caratula_a_pdf(libro, context, p, tmp_dir: Path) -> Path | None:
    """
    Nueva forma: NO navega a ImprimirCaratula.aspx.
    Toma el HTML del bloque #caratula dentro del Libro, lo aísla en una
    página en blanco con <base> al proxy y lo exporta a PDF en headless.
    Así no aparece el índice ni overlays y se evita el proxy error.
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

    hbrowser = p.chromium.launch(
        headless=True,
        args=["--disable-gpu","--no-sandbox","--disable-dev-shm-usage"]
    )
    try:
        hctx = hbrowser.new_context(
            storage_state=str(state_file),
            viewport={"width": 900, "height": 1200}
        )
        hp = hctx.new_page()
        hp.set_content(html_doc, wait_until="domcontentloaded")
        try:
            hp.emulate_media(media="print")
        except Exception:
            pass
        hp.pdf(path=str(out), format="A4", print_background=True, prefer_css_page_size=True)
    finally:
        try: hctx.close()
        except Exception: pass
        try: hbrowser.close()
        except Exception: pass

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
        draws = pg.get_drawings()  # líneas/rectángulos, etc.

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
        doc.close(); out.close()
        return pdf_path
    cleaned = pdf_path.with_suffix(".clean.pdf")
    out.save(str(cleaned), deflate=True, garbage=3)
    doc.close(); out.close()
    try: pdf_path.unlink(missing_ok=True)
    except Exception: pass
    return cleaned



def _estampar_si_corresponde(pdf: Path, texto: str | None) -> Path:
    if os.getenv("STAMP_HEADERS", "0").lower() not in ("1","true","t","yes","y","si","sí"):
        return pdf
    try:
        stamped = pdf.with_suffix(".stamped.pdf")
        _estampar_header(pdf, stamped, texto=texto or "ADJUNTO")
        pdf.unlink(missing_ok=True)
        return stamped
    except Exception:
        return pdf


def _obtener_libro_pdf(libro, context, tmp_dir: Path, p) -> Path | None:
    modo = os.getenv("BOOK_CAPTURE_MODE", "html").lower()  # 'html' | 'print' | 'auto'
    if modo == "print":
        return _imprimir_libro_a_pdf(libro, context, tmp_dir, p)
    if modo == "auto":
        pdf = _imprimir_libro_a_pdf(libro, context, tmp_dir, p)
        if pdf: return pdf
    # 'html' o fallback de 'auto'
    html = _guardar_libro_como_html(libro, tmp_dir)
    return _convertir_html_a_pdf(html, context, p, tmp_dir) if html else None

# ─────────────────────── DESCARGA PRINCIPAL ────────────────────────────
def _env_true(name: str, default="0"):
    return os.getenv(name, default).lower() in ("1","true","t","yes","y","si","sí")
# ─────────────────────── DESCARGA PRINCIPAL ────────────────────────────

def descargar_expediente(tele_user, tele_pass, intra_user, intra_pass, nro_exp, carpeta_salida):
    SHOW_BROWSER = _env_true("SHOW_BROWSER", "0")
    CHROMIUM_ARGS = ["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
    KEEP_WORK = _env_true("KEEP_WORK", "0")
    STAMP = _env_true("STAMP_HEADERS", "1")

    work_dir = Path(carpeta_salida) / f"Exp_{nro_exp}_work"
    if KEEP_WORK:
        temp_dir = work_dir
        temp_dir.mkdir(parents=True, exist_ok=True)
    else:
        temp_dir = Path(tempfile.mkdtemp())

    def _mf(line: str):
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not SHOW_BROWSER,
            args=CHROMIUM_ARGS,
            slow_mo=0
        )
        if SHOW_BROWSER:
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1366, "height": 900}
            )
        else:
            vid_dir = temp_dir / "video"
            vid_dir.mkdir(parents=True, exist_ok=True)
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1366, "height": 900},
                record_video_dir=str(vid_dir)
            )

        try:
            # 1) Login → Radiografía
            sac = abrir_sac(context, tele_user, tele_pass, intra_user, intra_pass)


            # 2) Buscar expediente
            _fill_radiografia_y_buscar(sac, nro_exp)
            if "SacInterior/Login.aspx" in sac.url:
                messagebox.showerror("Error de sesión", "El SAC pidió re-login. Probá nuevamente.")
                return
            if "PortalWeb/LogIn/Login.aspx" in (sac.url or ""):
                _login_intranet(sac, intra_user, intra_pass)
                sac = _ir_a_radiografia(sac)
                _fill_radiografia_y_buscar(sac, nro_exp)

            # 3) Abrir Libro y precargar TODAS las operaciones
            libro = _abrir_libro(sac, intra_user, intra_pass, nro_exp)
            ops = _expandir_y_cargar_todo_el_libro(libro)
            if not ops:
                _recorrer_indice_libro(libro)
                ops = _listar_operaciones_rapido(libro)
            logging.info(f"[OPS] Encontradas {len(ops)} operaciones en el índice.")



            # 4) Volver a Radiografía y DESCARGAR adjuntos de la grilla (mapeados por operación)
            try:
                sac.bring_to_front()
            except Exception:
                pass
            pdfs_grid = _descargar_adjuntos_grid_mapeado(sac, temp_dir)  # {op_id: [Path(pdf), ...]}
            # 4 bis) CARÁTULA (primera página del PDF final)
            bloques: list[tuple[Path, str | None]] = []  # inicializá acá para poder anteponer la carátula
            try:
                caratula_pdf = _render_caratula_a_pdf(libro, context, p, temp_dir)
                if caratula_pdf and caratula_pdf.exists():
                    caratula_pdf = _pdf_sin_blancos(caratula_pdf)
                    _mf(f"CARATULA · {caratula_pdf.name}")
                    bloques.append((caratula_pdf, None))  # ← va primera
                    logging.info("[CARATULA] agregada al inicio")
                else:
                    logging.info("[CARATULA] no se pudo capturar (se continúa)")
            except Exception as e:
                logging.info(f"[CARATULA:ERR] {e}")

            # 5) Volver al Libro para iterar operación por operación e INTERCALAR adjuntos
            try:
                libro.bring_to_front()
            except Exception:
                pass


            ya_agregados: set[tuple[str, int]] = set()  # evita duplicados exactos

            for op in ops:
                op_id   = op["id"]
                op_tipo = op["tipo"]
                titulo  = (op.get("titulo") or "").strip() or f"Operación {op_id}"

                # Mostrar la operación para asegurar que su HTML esté completo
                _mostrar_operacion(libro, op_id, op_tipo)

                # 5.1) PDF paginado de ESTA operación (sin índice, sin basura)
                pdf_op = _render_operacion_a_pdf_paginas(libro, op_id, context, p, temp_dir)
                if pdf_op and pdf_op.exists():
                    pdf_op = _pdf_sin_blancos(pdf_op)
                    _mf(f"OPERACION · {titulo} · {pdf_op.name}")
                    bloques.append((pdf_op, None))
                    logging.info(f"[OP] {op_id}: agregado {pdf_op.name}")
                else:
                    logging.info(f"[OP:SKIP] {op_id}: no se pudo renderizar en PDF")

                # 5.2) ADJUNTOS de esta operación (Libro + grilla)
                pdfs_op: list[Path] = []
                try:
                    # Descargas "in-situ" dentro del Libro (Playwright expect_download)
                    pdfs_op.extend(_descargar_adjuntos_de_operacion(libro, op_id, temp_dir))
                except Exception:
                    pass
                # Más lo que vino desde la grilla (ya descargado)
                pdfs_op.extend(pdfs_grid.get(op_id, []))

                # 5.3) Normalizar cada adjunto: convertir si hace falta, deduplicar, estampar y limpiar blancos
                for ap in pdfs_op:
                    pth = ap
                    if pth.suffix.lower() != ".pdf":
                        pth = _ensure_pdf_fast(pth)

                    try:
                        key = (pth.name, pth.stat().st_size)
                    except Exception:
                        key = (pth.name, 0)

                    if key in ya_agregados:
                        continue
                    ya_agregados.add(key)

                    pth = _pdf_sin_blancos(pth)
                    _mf(f"ADJUNTO · {titulo} · {pth.name}")
                    hdr = (f"ADJUNTO · {titulo}") if STAMP else None
                    bloques.append((pth, hdr))
                    logging.info(f"[MERGE] ADJ · {pth.name} (op {op_id})")

            # 6) Adjuntos sin operación mapeada → al final
            adj_sin = pdfs_grid.get("__SIN_OP__", [])
            if adj_sin:
                logging.info(f"[ADJ] SIN_OP · {len(adj_sin)} archivo(s)")
                for pdf in adj_sin:
                    pth = pdf
                    if pth.suffix.lower() != ".pdf":
                        pth = _ensure_pdf_fast(pth)

                    try:
                        key = (pth.name, pth.stat().st_size)
                    except Exception:
                        key = (pth.name, 0)

                    if key in ya_agregados:
                        continue
                    ya_agregados.add(key)

                    pth = _pdf_sin_blancos(pth)
                    _mf(f"ADJUNTO · (sin operación) · {pth.name}")
                    hdr = ("ADJUNTO · (sin operación)") if STAMP else None
                    bloques.append((pth, hdr))

            if not bloques:
                raise RuntimeError("No hubo nada para fusionar (no se pudo capturar operaciones ni adjuntos).")

            # 7) Fusión final en el orden deseado (op -> adjuntos)
            out = Path(carpeta_salida) / f"Exp_{nro_exp}.pdf"
            fusionar_bloques_inline(bloques, out)
            _mf(f"==> PDF FINAL: {out.name}  (total bloques={len(bloques)})")
            logging.info(f"[OK] PDF final creado: {out} · bloques={len(bloques)}")
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
            if not KEEP_WORK:
                shutil.rmtree(temp_dir, ignore_errors=True)



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
                self.text.insert("end", msg + "\n")
                self.text.see("end")
        except queue.Empty:
            pass
        self.after(100, self._poll)

    def _on_close(self):
        # Solo oculta la ventana; los logs siguen en debug.log
        self.withdraw()

# ───────────────────────── INTERFAZ Tkinter ────────────────────────────
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
        self.tele_pwd  = StringVar(value=os.getenv("TELE_PASS", ""))
        self.intra_user = StringVar(value=os.getenv("INTRA_USER", os.getenv("SAC_USER","")))
        self.intra_pwd  = StringVar(value=os.getenv("INTRA_PASS", os.getenv("SAC_PASS","")))
        self.exp        = StringVar()

        Entry(master, textvariable=self.tele_user, width=26).grid(row=0, column=1)
        Entry(master, textvariable=self.tele_pwd,  width=26, show="*").grid(row=1, column=1)
        Entry(master, textvariable=self.intra_user, width=26).grid(row=2, column=1)
        Entry(master, textvariable=self.intra_pwd,  width=26, show="*").grid(row=3, column=1)
        Entry(master, textvariable=self.exp,        width=26).grid(row=4, column=1)

        self.btn = Button(master, text="Descargar expediente", command=self.run)
        self.btn.grid(row=5, column=0, columnspan=2, pady=10)
        self._log_queue = None
        self._ui_handler = None
        self._progress_win = None

    def run(self):
        if not all([
            self.intra_user.get().strip(),
            self.intra_pwd.get().strip(),
            self.exp.get().strip()
        ]):
            messagebox.showerror("Faltan datos",
                "Completá usuario/clave de Intranet y Nº de expediente. "
                "Los de Teletrabajo son opcionales (solo si estás por VPN).")
            return

        carpeta = filedialog.askdirectory(title="Carpeta destino")
        if not carpeta:
            return

        self.btn.config(state="disabled")
        # Ventana de progreso + handler de logging hacia la ventana
        self._log_queue = queue.Queue()
        self._progress_win = ProgressWin(self.btn.master, self._log_queue, title=f"Progreso – Exp. {self.exp.get().strip()}")
        # Si hubiera un handler viejo, lo saco
        if self._ui_handler:
            logging.getLogger().removeHandler(self._ui_handler)
        self._ui_handler = TkQueueHandler(self._log_queue)
        self._ui_handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%H:%M:%S"))
        logging.getLogger().addHandler(self._ui_handler)

        threading.Thread(
            target=lambda: self._ejecutar(Path(carpeta)),
            daemon=True
        ).start()

    def _ejecutar(self, carpeta: Path):
        try:
            descargar_expediente(
                self.tele_user.get().strip(),
                self.tele_pwd.get().strip(),
                self.intra_user.get().strip(),
                self.intra_pwd.get().strip(),
                self.exp.get().strip(),
                carpeta
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

# ──────────────────────────── MAIN ─────────────────────────────────────
LOG = BASE_PATH / "debug.log"
logging.basicConfig(filename=LOG, level=logging.INFO,
                    format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

def _set_win_appusermodelid(appid="SACDownloader.CBA"):
    """Para que Windows agrupe en la barra de tareas con el ícono del exe."""
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(appid)
    except Exception:
        pass

def _set_tk_icon(root):
    """Intenta usar .ico; si falla, hace fallback a iconphoto."""
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


if __name__ == "__main__":
    _set_win_appusermodelid("SACDownloader.CBA")
    root = Tk()
    _set_tk_icon(root)  # ⟵ usa icono3.ico desde BASE_PATH (soporta modo frozen)
    App(root)
    root.mainloop()

