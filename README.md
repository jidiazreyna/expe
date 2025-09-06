# expe

## OCR portátil

Incluir un Tesseract portátil con la siguiente estructura:

```
tesseract/
  tesseract.exe
  *.dll
  tessdata/
    spa.traineddata
    eng.traineddata
```

### Empaquetado

Ejemplo con PyInstaller:

```
pyinstaller --noconfirm --onefile expediente.py ^
  --add-data "tesseract;tesseract" ^
  --add-data "ms-playwright;ms-playwright"
```