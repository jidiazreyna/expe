"""Herramientas para convertir PDFs en imágenes."""
from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
from typing import List


def convertir_pdf_a_imagenes(pdf_path: str | Path, out_dir: str | Path, formato: str = "png") -> List[str]:
    """Convierte cada página de un PDF en un archivo de imagen independiente.

    Se intentará usar ``pdfimages`` (Poppler) si está disponible en el sistema.
    Si no se encuentra, se probará ``pdftoppm``. Como último recurso, se
    utilizará `PyMuPDF <https://pymupdf.readthedocs.io/>`_ (``fitz``).

    Los archivos resultantes se nombran ``page_001.png``, ``page_002.png``,
    etc. y se guardan en ``out_dir``.

    Parameters
    ----------
    pdf_path:
        Ruta al archivo PDF de origen.
    out_dir:
        Directorio donde se guardarán las imágenes.
    formato:
        Formato de salida: ``"png"`` (por defecto) o ``"tiff"``.

    Returns
    -------
    list[str]
        Lista con las rutas de las imágenes generadas.

    Raises
    ------
    FileNotFoundError
        Si ``pdf_path`` no existe.
    ValueError
        Si ``formato`` no es ``"png"`` ni ``"tiff"``.
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

    def _renombrar_salida() -> List[str]:
        generados = sorted(out_dir.glob(f"{tmp_base.name}*"))
        imagenes: List[str] = []
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
        cmd = ["pdftoppm", f"-{formato}", str(pdf_path), str(tmp_base)]
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
    imagenes: List[str] = []
    for i, pagina in enumerate(doc, 1):
        pix = pagina.get_pixmap(dpi=300)
        dst = out_dir / f"page_{i:03d}.{ext}"
        pix.save(str(dst))
        imagenes.append(str(dst))
    return imagenes


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convierte un PDF en imágenes")
    parser.add_argument("pdf", help="Ruta al PDF de entrada")
    parser.add_argument("out", help="Directorio de salida")
    parser.add_argument("--formato", choices=["png", "tiff"], default="png")
    args = parser.parse_args()

    rutas = convertir_pdf_a_imagenes(args.pdf, args.out, args.formato)
    for r in rutas:
        print(r)
