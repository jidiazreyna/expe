# expe

## Empaquetado

Ejemplo con PyInstaller:

```
pyinstaller --noconfirm --onefile expediente.py ^
  --add-data "ms-playwright;ms-playwright" ^
  --hidden-import=winrt.windows.media.ocr ^
  --hidden-import=winrt.windows.graphics.imaging ^
  --hidden-import=winrt.windows.storage.streams ^
  --hidden-import=winrt.windows.globalization
```

## Dependencias

- [ocrmypdf](https://ocrmypdf.readthedocs.io/) (requiere Tesseract)

## Variables de entorno

- `OCR_FINAL_FORCE`: si se establece en `1`/`true`, ejecuta un OCR final sobre el PDF generado usando `ocrmypdf` (300 DPI, `--deskew`, `--rotate-pages`).
