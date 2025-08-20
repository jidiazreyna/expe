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
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from PIL import Image

# ─────────────────────────── RUTAS Y RECURSOS ──────────────────────────
if getattr(sys, "frozen", False):   # ejecutable .exe
    BASE_PATH = Path(sys._MEIPASS)
else:                                # .py suelto
    BASE_PATH = Path(__file__).parent

# Playwright buscará el navegador empaquetado aquí (portabiliza el .exe)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BASE_PATH / "ms-playwright")

# --- URLs base ---------------------------------------------------------
TELETRABAJO_URL = "https://teletrabajo.justiciacordoba.gob.ar/remote/login?lang=sp"
URL_BASE        = "https://www.tribunales.gov.ar"
URL_LOGIN       = f"{URL_BASE}/SacInterior/Login.aspx"
URL_RADIOGRAFIA = f"{URL_BASE}/SacInterior/_Expedientes/Radiografia.aspx"
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


def fusionar_bloques_inline(bloques, destino: Path):
    """
    bloques: lista de tuplas (pdf_path: Path, header_text: str|None).
    Si header_text NO es None, se estampa en TODAS las páginas de ese PDF.
    """
    w = PdfWriter()
    overlay_cache: dict[tuple[float, float, str], object] = {}

    for pdf_path, header_text in bloques:
        r = PdfReader(str(pdf_path))
        if len(r.pages) == 0:
            continue

        for p in r.pages:
            if header_text:
                pw, ph = float(p.mediabox.width), float(p.mediabox.height)
                key = (pw, ph, header_text)
                ov = overlay_cache.get(key)
                if ov is None:
                    ov = _overlay_page(pw, ph, header_text)  # crea el overlay en memoria
                    overlay_cache[key] = ov
                # estampa el overlay sobre la página real (sin alterar tamaño/mediabox)
                p.merge_page(ov)

            w.add_page(p)

    with open(destino, "wb") as f:
        w.write(f)



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
    Devuelve 'https://teletrabajo.justiciacordoba.gob.ar/proxy/<token>/' a partir
    de la URL actual o cualquier href de la página/iframes.
    """
    def _scan_url(u: str):
        if not u: return None
        m = re.search(r"https://teletrabajo\.justiciacordoba\.gob\.ar/proxy/[^/]+/", u)
        if m: return m.group(0)
        m = re.search(r"^/proxy/[^/]+/", u)
        if m: return "https://teletrabajo.justiciacordoba.gob.ar" + m.group(0)
        return None

    p = _scan_url(page.url)
    if p: return p
    try:
        for a in page.query_selector_all("a[href]"):
            p = _scan_url(a.get_attribute("href") or "")
            if p: return p
    except Exception:
        pass
    # frames
    for fr in page.frames:
        try:
            p = _scan_url(fr.url)
            if p: return p
        except Exception:
            pass
    raise RuntimeError("No pude detectar el prefijo del proxy de Teletrabajo.")

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

def _abrir_libro(sac):
    """Abre '* Ver Expediente como Libro' y devuelve la Page del libro."""
    # 1) abrir el menú
    try:
        sac.locator("text=¿Qué puedo hacer?").first.click()
    except Exception:
        pass
    sac.wait_for_timeout(200)

    # 2) intentar con el link accesible (tolera '* ' antes del texto)
    link = sac.get_by_role("link", name=re.compile(r"Expediente\s+como\s+Libro", re.I)).first
    if link.count():
        # primero probamos popup
        try:
            with sac.context.expect_page() as pop:
                link.click()
            libro = pop.value
            libro.wait_for_load_state("domcontentloaded")
            return libro
        except Exception:
            # si no hubo popup, probamos navegación inline
            try:
                with sac.expect_navigation(timeout=4000):
                    link.click()
                return sac
            except Exception:
                pass

    # 3) fallback: ejecutar la función JS que usa el sistema
    try:
        with sac.context.expect_page() as pop:
            sac.evaluate("() => window.ExpedienteLibro && window.ExpedienteLibro()")
        libro = pop.value
        libro.wait_for_load_state("domcontentloaded")
        return libro
    except Exception:
        pass

    # 4) a veces el popup aparece un poquito después
    try:
        libro = sac.wait_for_event("popup", timeout=5000)
        libro.wait_for_load_state("domcontentloaded")
        return libro
    except Exception:
        pass

    raise RuntimeError("No pude abrir 'Ver Expediente como Libro'.")

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

    # Normalizar estilos (como arriba)
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

    # Bounding box del elemento + DPR
    bb = cont.bounding_box()
    if not bb:
        return None
    dpr = S.evaluate("() => window.devicePixelRatio") or 1

    # Screenshot de TODA la página (stitch completo)
    full_png = tmp_dir / f"op_{op_id}_full.png"
    libro.screenshot(path=str(full_png), full_page=True)

    # Recorte al área exacta del elemento
    left   = int(bb["x"] * dpr)
    top    = int(bb["y"] * dpr)
    right  = int((bb["x"] + bb["width"]) * dpr)
    bottom = int((bb["y"] + bb["height"]) * dpr)

    crop_png = tmp_dir / f"op_{op_id}.png"
    with Image.open(full_png) as im:
        im.crop((left, top, right, bottom)).save(crop_png)

    # Convertir a PDF
    return _imagen_a_pdf(crop_png)



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
    Login en la PÁGINA que esté (LogIn / MyDesktop / PublicApps).
    Si ya ve “Aplicaciones” o “Mi Escritorio”, asume sesión activa.
    """
    page.wait_for_load_state("domcontentloaded")
    if page.locator("text=Aplicaciones, text=Mi Escritorio, text=Desconectarse").first.count():
        return

    # Nuevo
    if page.locator("#txtUserName").first.count():
        page.fill("#txtUserName", intra_user)
        page.fill("#txtUserPassword", intra_pass)
        page.click("#btnLogIn")
        page.wait_for_load_state("networkidle")
        return

    # Viejo
    if page.locator("#txtUsuario").first.count():
        page.fill("#txtUsuario", intra_user)
        page.fill("#txtContrasena", intra_pass)
        page.click("#btnIngresar")
        page.wait_for_load_state("networkidle")
        return

    # Heurístico
    user = page.locator("input[type='text'], input[name*='User'], input[id*='User']").first
    pwd  = page.locator("input[type='password']").first
    if user.count() and pwd.count():
        user.fill(intra_user); pwd.fill(intra_pass)
        page.locator("button[type=submit], input[type=submit], #btnLogIn, #btnIngresar").first.click()
        page.wait_for_load_state("networkidle")

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


def _open_sac_desde_portal(page):
    """
    En el Portal Intranet: abrir el menú 'Aplicaciones' (img#imgMenuServiciosPrivadas)
    y luego hacer click en 'SAC Multifuero' (o 'SAC Multifueros').
    Devuelve la pestaña/ventana del SAC.
    """
    try:
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
    except Exception:
        pass

    scopes = [page] + page.frames
    trigger = None
    scope = page

    # 1) disparador del menú "Aplicaciones"
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

    # 2) abrir el menú (reintenta un par de veces)
    matcher = re.compile(r"SAC\s*Multifueros?", re.I)
    link = None
    for _ in range(3):
        try:
            trigger.click(force=True)
        except Exception:
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
        raise RuntimeError("No encontré el enlace a 'SAC Multifuero' dentro de Aplicaciones.")

    # 3) abrir SAC (popup o misma pestaña)
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

    # 4) último recurso: extraer URL real
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
    page.set_default_timeout(12000)
    page.set_default_navigation_timeout(15000)

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
    sac = _open_sac_desde_portal(page)

    # 5) Si el proxy niega el acceso, reintentamos 1 vez desde Aplicaciones
    if _is_proxy_error(sac):
        _goto_portal_grid(page)
        page = _open_portal_aplicaciones_pj(page)
        _login_intranet(page, intra_user, intra_pass)
        sac = _open_sac_desde_portal(page)

    # 6) Radiografía
    sac = _ir_a_radiografia(sac)
    return sac

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


# ─────────────────────── DESCARGA PRINCIPAL ────────────────────────────
def _env_true(name: str, default="0"):
    return os.getenv(name, default).lower() in ("1","true","t","yes","y","si","sí")
# ─────────────────────── DESCARGA PRINCIPAL ────────────────────────────
def descargar_expediente(tele_user, tele_pass, intra_user, intra_pass, nro_exp, carpeta_salida):
    temp_dir = Path(tempfile.mkdtemp())

    SHOW_BROWSER = _env_true("SHOW_BROWSER", "1")
    CHROMIUM_ARGS = ["--disable-gpu","--no-sandbox","--disable-dev-shm-usage"]

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not SHOW_BROWSER,
            args=CHROMIUM_ARGS,
            slow_mo=0 if SHOW_BROWSER else 0
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
            sac = abrir_sac_via_teletrabajo(context, tele_user, tele_pass, intra_user, intra_pass)

            # 2) Buscar expediente
            _fill_radiografia_y_buscar(sac, nro_exp)
            if "SacInterior/Login.aspx" in sac.url:
                messagebox.showerror("Error de sesión", "El SAC pidió re-login. Probá nuevamente.")
                return

            # 3) Abrir Libro y precargar TODAS las operaciones
            libro = _abrir_libro(sac)
            ops = _expandir_y_cargar_todo_el_libro(libro)   # [{'id','tipo','titulo'}, ...]
            if not ops:
                raise RuntimeError("No pude detectar operaciones en el Libro.")

            # 4) Volver a Radiografía y bajar adjuntos mapeados por operación
            try: sac.bring_to_front()
            except Exception: pass
            adj_grid = _descargar_adjuntos_grid_mapeado(sac, temp_dir)  # {op_id: [Path(pdf), ...]}

            # 5) Volver al Libro y armar bloques: operación → adjuntos
            try: libro.bring_to_front()
            except Exception: pass

            bloques: list[tuple[Path, str | None]] = []
            ya_agregados: set[tuple[str,int]] = set()  # (nombre, tamaño) para deduplicar

            for op in ops:
                op_id   = op["id"]
                op_tipo = op["tipo"]
                titulo  = (op.get("titulo") or "").strip() or f"Operación {op_id}"

                _mostrar_operacion(libro, op_id, op_tipo)

                # 5.1) Captura completa de la operación (SIN header)
                op_pdf = _capturar_operacion_a_pdf(libro, op_id, temp_dir)
                if op_pdf:
                    bloques.append((op_pdf, None))

                # 5.2) Adjuntos de esa operación: primero los del Libro, luego los de la grilla
                pdfs_op: list[Path] = []
                try:
                    pdfs_op.extend(_descargar_adjuntos_de_operacion(libro, op_id, temp_dir))
                except Exception:
                    pass
                pdfs_op.extend(adj_grid.get(op_id, []))

                # 5.3) Insertar cada adjunto con hoja-encabezado antes
                for pdf in pdfs_op:
                    try:
                        key = (pdf.name, pdf.stat().st_size)
                        if key in ya_agregados:
                            continue
                        ya_agregados.add(key)
                    except Exception:
                        pass
                    header = f"ADJUNTO · {titulo} · {pdf.name}"
                    bloques.append((pdf, header))

            # 5.4) Adjuntos no mapeados → al final
            for pdf in adj_grid.get("__SIN_OP__", []):
                try:
                    key = (pdf.name, pdf.stat().st_size)
                    if key in ya_agregados:
                        continue
                    ya_agregados.add(key)
                except Exception:
                    pass
                bloques.append((pdf, f"ADJUNTO · (sin operación) · {pdf.name}"))

            if not bloques:
                raise RuntimeError("No hubo nada para fusionar (operaciones/adjuntos no capturados).")

            # 6) Fusión (inserta hoja-encabezado antes de cada adjunto)
            out = Path(carpeta_salida) / f"Exp_{nro_exp}.pdf"
            fusionar_bloques_inline(bloques, out)
            messagebox.showinfo("Éxito", f"PDF creado en:\n{out}")

        finally:
            try: context.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass
            shutil.rmtree(temp_dir, ignore_errors=True)



# ───────────────────────── INTERFAZ Tkinter ────────────────────────────
class App:
    def __init__(self, master):
        master.title("Descargar expediente SAC")
        load_dotenv()

        Label(master, text="Usuario Teletrabajo:").grid(row=0, column=0, sticky="e")
        Label(master, text="Clave Teletrabajo:").grid(row=1, column=0, sticky="e")
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

    def run(self):
        if not all([
            self.tele_user.get().strip(),
            self.tele_pwd.get().strip(),
            self.intra_user.get().strip(),
            self.intra_pwd.get().strip(),
            self.exp.get().strip()
        ]):
            messagebox.showerror("Faltan datos",
                "Completá usuario/clave de Teletrabajo, usuario/clave de Intranet y Nº de expediente.")
            return

        carpeta = filedialog.askdirectory(title="Carpeta destino")
        if not carpeta:
            return

        self.btn.config(state="disabled")
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

