# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['c:\\Github\\CPUReadyTime\\vcenter_cpu_analyzerv.py'],
    pathex=[],
    binaries=[],
    datas=[('README.txt', '.')],
    hiddenimports=['pandas', 'numpy', 'matplotlib', 'matplotlib.backends.backend_tkagg', 'matplotlib.colors', 'seaborn', 'openpyxl', 'xlrd', 'pyvmomi', 'pyVim', 'pyVim.connect', 'pyVmomi.vim', 'requests', 'ssl', 'reportlab', 'reportlab.lib', 'reportlab.platypus', 'reportlab.pdfgen', 'realtime_dashboard', 'threading', 'queue', 'collections', 'sqlite3', 'tkinter.ttk', 'datetime', 'calendar', 're', 'pathlib', 'base64', 'io', 'tempfile', 'matplotlib.patches', 'matplotlib.figure', 'pandas.core.algorithms', 'pandas.core.arrays', 'pandas.io.formats.style', 'pandas._libs.tslibs', 'pandas._libs.hashtable', 'pandas._libs.lib', 'pandas._libs.missing', 'pytz', 'dateutil', 'dateutil.parser', 'dateutil.tz', 'sqlite3.dbapi2', 'matplotlib.animation', 'matplotlib.dates', 'matplotlib.ticker', 'collections.deque', 'json', 'threading.Timer', 'math', 'statistics', 'warnings', 'socket', 'ipaddress', 'traceback', 'time', 'copy', 'uuid', 'pandas.plotting', 'pandas.core.dtypes'],
    hookspath=['.'],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pkg_resources', 'setuptools', 'jaraco', 'more_itertools', 'pytest', 'IPython', 'jupyter', 'notebook', 'sphinx', 'pytest-cov', 'coverage', 'black', 'flake8'],
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
    name='vCenter_CPU_Analyzer_v2',
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
