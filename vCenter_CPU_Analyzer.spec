# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['c:\\Github\\CPUReadyTime\\vcenter_cpu_analyzer.py'],
    pathex=[],
    binaries=[],
    datas=[('README.txt', '.')],
    hiddenimports=['pandas', 'numpy', 'matplotlib', 'seaborn', 'openpyxl', 'xlrd', 'pyvmomi', 'pyVim', 'pyVmomi', 'requests'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pytest', 'IPython'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='vCenter_CPU_Analyzer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['app_icon.ico'],
)
