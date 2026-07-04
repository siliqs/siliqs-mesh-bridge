# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec — builds a single-file siliqs-mesh-bridge desktop app for the host OS.
#   pyinstaller siliqs-mesh-bridge.spec  # -> dist/siliqs-mesh-bridge[.exe] (+ .app on macOS)
# Cross-platform: run this on each OS (macOS/Windows/Linux) — see .github/workflows.
import sys
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

if sys.platform == "darwin":
    # macOS: onedir → .app bundle. A bare no-extension binary opens in TextEdit when
    # double-clicked; a .app double-clicks and runs. onedir (not onefile) is the robust
    # form for a downloaded/quarantined .app under Gatekeeper. The browser control panel
    # is the UI — quit it with the panel's "Quit app" button.
    exe = EXE(
        pyz, a.scripts, [], exclude_binaries=True,
        name="siliqs-mesh-bridge",
        debug=False, bootloader_ignore_signals=False, strip=False, upx=False,
        console=True, icon=None,
    )
    coll = COLLECT(exe, a.binaries, a.datas, strip=False, upx=False, name="siliqs-mesh-bridge")
    app = BUNDLE(
        coll,
        name="siliqs-mesh-bridge.app",
        icon=None,
        bundle_identifier="net.siliqs.mesh-bridge",
        info_plist={
            "CFBundleName": "siliqs-mesh-bridge",
            "CFBundleDisplayName": "Siliqs Mesh Bridge",
            "CFBundleShortVersionString": "0.3.1",
            "NSHighResolutionCapable": True,
        },
    )
else:
    # Windows / Linux: a single self-contained file. On Windows the .exe double-clicks;
    # on Linux `chmod +x` then run. The console shows the log (close it to quit).
    exe = EXE(
        pyz, a.scripts, a.binaries, a.datas, [],
        name="siliqs-mesh-bridge",
        debug=False, bootloader_ignore_signals=False, strip=False, upx=False,
        runtime_tmpdir=None, console=True, icon=None,
    )
