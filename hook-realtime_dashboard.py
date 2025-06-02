
# PyInstaller hook for realtime_dashboard module
from PyInstaller.utils.hooks import collect_all

datas, binaries, hiddenimports = collect_all('realtime_dashboard')

# Additional hidden imports for real-time functionality
hiddenimports += [
    'sqlite3',
    'threading',
    'queue',
    'collections.deque',
    'datetime',
    'pandas',
    'numpy',
    'matplotlib',
    're',
]
