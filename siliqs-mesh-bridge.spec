# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec — builds a single-file siliqs-mesh-bridge desktop app for the host OS.
#   pyinstaller siliqs-mesh-bridge.spec           # -> dist/siliqs-mesh-bridge[.exe]
# Cross-platform: run this on each OS (macOS/Windows/Linux) — see .github/workflows.
from PyInstaller.utils.hooks import collect_all

# meshtastic ships generated protobufs + data; bleak/pyserial/paho/pubsub have submodules
# PyInstaller's static analysis misses. collect_all pulls their datas + hidden imports.
datas, binaries, hiddenimports = [], [], []
for pkg in ("meshtastic", "bleak", "serial", "paho", "pubsub", "google.protobuf",
            "dotmap", "print_color", "tabulate", "pyqrcode", "bleak_winrt"):
    try:
        d, b, h = collect_all(pkg)
        datas += d; binaries += b; hiddenimports += h
    except Exception:
        pass  # optional/platform-specific packages (e.g. bleak_winrt only on Windows)

a = Analysis(
    ["app.py"],
    pathex=["."],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports + ["siliqs_mesh_bridge", "siliqs_mesh_bridge_web"],
    hookspath=[],
    runtime_hooks=[],
    excludes=["tkinter", "PyQt5", "PySide2", "matplotlib", "numpy.tests"],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="siliqs-mesh-bridge",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=True,          # the app IS a small local server — the console shows its log; close to quit
    icon=None,
)
