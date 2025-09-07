# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['expediente.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        # winsdk (Py 3.12+)
        "winsdk.windows.media.ocr",
        "winsdk.windows.globalization",
        "winsdk.windows.storage.streams",
        "winsdk.windows.graphics.imaging",
        # winrt (Py 3.8–3.11)
        "winrt.windows.media.ocr",
        "winrt.windows.globalization",
        "winrt.windows.storage.streams",
        "winrt.windows.graphics.imaging",
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='app',
    debug=False,
    strip=False,
    upx=True,
    console=True,
)
