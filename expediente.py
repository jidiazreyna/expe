#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga un expediente del SAC, adjuntos incluidos, y arma un único PDF.
"""

import os, sys, tempfile, shutil, datetime, threading, pathlib
from pathlib import Path
from tkinter import Tk, Label, Entry, Button, StringVar, filedialog, messagebox
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from PIL import Image
import logging


# ─────────────────────────── RUTAS Y RECURSOS ──────────────────────────
if getattr(sys, "frozen", False):                     # ejecutable .exe
    BASE_PATH = Path(sys._MEIPASS)
else:                                                 # .py suelto
    BASE_PATH = Path(__file__).parent

# Playwright buscará el navegador empaquetado aquí:
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(BASE_PATH / "ms-playwright")

# ───────────────────────── UTILIDADES PDF ──────────────────────────────
def _estampar_header(origen: Path, destino: Path, texto="ADJUNTO"):
    tmp = origen.with_suffix(".stamp.pdf")
    r, w = PdfReader(str(origen)), PdfWriter()
    for p in r.pages:
        w.add_page(p)
        w.pages[-1].merge_page(
            PdfReader(canvas.Canvas(str(tmp), pagesize=p.mediabox[-2:])._doc).pages[0]
        )
    with open(destino, "wb") as f:
        w.write(f)
    tmp.unlink(missing_ok=True)

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

# --- CONST -------------------------------------------------------------
URL_BASE        = "https://www.tribunales.gov.ar"
URL_LOGIN       = f"{URL_BASE}/SacInterior/Login.aspx"
URL_RADIOGRAFIA = f"{URL_BASE}/SacInterior/_Expedientes/Radiografia.aspx"

# ---------- LOGIN ROBUSTO ---------------------------------------------
LOGIN_USER = '#txtUserName'
LOGIN_PASS = '#txtUserPassword'
LOGIN_BTN  = '#btnLogIn'
FORM_POST  = 'form#frmPost'                # por si mañana se vuelve a usar

def hacer_login(portal, usuario, clave):
    """Devuelve la pestaña SAC Interior ya autenticada."""
    portal.set_default_timeout(60000)
    portal.goto(URL_LOGIN, wait_until="domcontentloaded")

    # 1. completar login (pantalla nueva o vieja)
    if "PortalWeb/LogIn" in portal.url:          # login nuevo
        portal.fill(LOGIN_USER, usuario)
        portal.fill(LOGIN_PASS, clave)
        portal.click(LOGIN_BTN)
        portal.wait_for_load_state("networkidle")
    else:                                        # login viejo
        portal.fill("#txtUsuario", usuario)
        portal.fill("#txtContrasena", clave)
        portal.click("#btnIngresar")
        portal.wait_for_load_state("networkidle")

    # 2. menú Aplicaciones → SAC Multifuero  (si ya estuviera desplegado no pasa nada)
    portal.click('text="Aplicaciones"')
    with portal.expect_popup() as sac_popup:
        portal.click('text="SAC Multifuero"')
    sac = sac_popup.value                        # pestaña SAC
    sac.wait_for_load_state("domcontentloaded")
    return sac

# ─────────────────────── DESCARGA PRINCIPAL ────────────────────────────
def descargar_expediente(usuario, clave, nro_exp, carpeta_salida):
    temp_dir = Path(tempfile.mkdtemp())

    with sync_playwright() as p:
        browser  = p.chromium.launch(headless=False)
        context  = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            

            # 1. Login      -> ahora recibimos la pestaña SAC interior
            sac = hacer_login(page, usuario, clave)
            logging.info(f"Tras login – Portal URL: {page.url}")
            logging.info(f"SAC URL: {sac.url}")
            # 2. Radiografía
            sac.goto(URL_RADIOGRAFIA, wait_until="domcontentloaded")
            sac.fill("#txtNroExpediente", nro_exp)
            sac.click("#btnBuscarExp")
            sac.wait_for_load_state("networkidle")
            logging.info(f"Radiografía URL: {sac.url}")
            if "SacInterior/Login.aspx" in sac.url:
                logging.error("Redirigió al 404 de Login.aspx")
                messagebox.showerror("Error de sesión",
                    "El SAC devolvió 404 después del login. "
                    "Revisá debug.log y avisanos.")
                return
            # 3. Adjuntos
            adjuntos = []
            try:
                filas = sac.query_selector_all("table#gvAdjuntos tr")[1:]
                for f in filas:
                    enlace = f.query_selector("a")
                    if not enlace: continue
                    fecha = datetime.datetime.strptime(
                        f.query_selector("td").inner_text().strip(), "%d/%m/%Y")
                    adjuntos.append((fecha, enlace))
            except Exception:
                pass

            # 4. Libro
            sac.click("text='¿Qué puedo hacer?'")
            with sac.expect_popup() as pop:
                sac.click("text='Ver Expediente como Libro'")
            libro = pop.value
            libro.wait_for_load_state("domcontentloaded")

            # expandir índice
            libro.wait_for_selector("div#indice")
            vistos = set()
            while True:
                anchors = libro.query_selector_all("div#indice a")
                nuevos  = [a for a in anchors if a not in vistos]
                if not nuevos: break
                for a in nuevos:
                    a.click()
                    vistos.add(a)
                    libro.wait_for_timeout(80)
                libro.eval_on_selector("div#indice", "(el)=>el.scrollBy(0, el.clientHeight)")

            # 5. PDF del libro
            pdf_libro = temp_dir / f"Libro_{nro_exp}.pdf"
            libro.emulate_media(media="print")
            libro.pdf(path=str(pdf_libro), print_background=True, scale=0.9)

            # 6. Descarga de adjuntos
            adj_pdfs = []
            for fecha, link in sorted(adjuntos, key=lambda x: x[0]):
                with sac.expect_download() as dl: link.click()
                d = dl.value
                destino = temp_dir / d.suggested_filename
                d.save_as(destino)

                if destino.suffix.lower() in {".jpg", ".jpeg", ".png"}:
                    destino = _imagen_a_pdf(destino)

                marcado = destino.with_stem(destino.stem + "_ADJUNTO")
                _estampar_header(destino, marcado, "ADJUNTO")
                adj_pdfs.append(marcado)

            # 7. Fusión
            out = carpeta_salida / f"Exp_{nro_exp}.pdf"
            fusionar_pdfs([pdf_libro] + adj_pdfs, out)
            messagebox.showinfo("Éxito", f"PDF creado en:\n{out}")

        finally:
            context.close()
            browser.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

# ───────────────────────── INTERFAZ Tkinter ────────────────────────────
class App:
    def __init__(self, master):
        master.title("Descargar expediente SAC")
        load_dotenv()

        Label(master, text="Usuario:").grid(row=0, column=0, sticky="e")
        Label(master, text="Clave:").grid(row=1, column=0, sticky="e")
        Label(master, text="Nº Expediente:").grid(row=2, column=0, sticky="e")

        self.user = StringVar(value=os.getenv("SAC_USER", ""))
        self.pwd  = StringVar(value=os.getenv("SAC_PASS", ""))
        self.exp  = StringVar()

        Entry(master, textvariable=self.user, width=26).grid(row=0, column=1)
        Entry(master, textvariable=self.pwd,  width=26, show="*").grid(row=1, column=1)
        Entry(master, textvariable=self.exp,  width=26).grid(row=2, column=1)

        self.btn = Button(master, text="Descargar expediente", command=self.run)
        self.btn.grid(row=3, column=0, columnspan=2, pady=10)

    def run(self):
        if not all([self.user.get().strip(), self.pwd.get().strip(), self.exp.get().strip()]):
            messagebox.showerror("Faltan datos", "Completá usuario, clave y expediente.")
            return
        carpeta = filedialog.askdirectory(title="Carpeta destino")
        if not carpeta: return

        self.btn.config(state="disabled")
        threading.Thread(
            target=lambda: self._ejecutar(Path(carpeta)),
            daemon=True).start()

    def _ejecutar(self, carpeta: Path):
        try:
            descargar_expediente(self.user.get().strip(),
                                  self.pwd.get().strip(),
                                  self.exp.get().strip(),
                                  carpeta)
        except Exception as e:
            messagebox.showerror("Error", str(e))
        finally:
            self.btn.config(state="normal")



LOG = BASE_PATH / "debug.log"
logging.basicConfig(filename=LOG, level=logging.INFO,
                    format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
# ──────────────────────────── MAIN ─────────────────────────────────────
if __name__ == "__main__":
    root = Tk()
    # Icono
    ico = BASE_PATH / "icono3.ico"
    if ico.exists():
        try: root.iconbitmap(default=str(ico))
        except Exception: pass
    App(root)
    root.mainloop()
