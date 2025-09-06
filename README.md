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
