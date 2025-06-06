import PyInstaller.__main__
import os
import sys

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
main_script = os.path.join(script_dir, 'vcenter_cpu_analyser.py')

PyInstaller.__main__.run([
    main_script,
    '--onefile',                    # Create a single executable file
    '--windowed',                   # Don't show console window (GUI app)
    '--name=vCenter_CPU_Analyser_v2',  # Updated name for version 2
    '--icon=app_icon.ico',          # Optional: add an icon file
    '--add-data=README.txt;.',      # Optional: include additional files
    
    # Core data analysis libraries
    '--hidden-import=pandas',
    '--hidden-import=numpy', 
    '--hidden-import=matplotlib',
    '--hidden-import=matplotlib.backends.backend_tkagg',
    '--hidden-import=matplotlib.colors',
    '--hidden-import=seaborn',
    
    # Excel/CSV support
    '--hidden-import=openpyxl',
    '--hidden-import=xlrd',
    
    # vCenter integration (only if available)
    '--hidden-import=pyvmomi',
    '--hidden-import=pyVim',
    '--hidden-import=pyVim.connect',
    '--hidden-import=pyVmomi.vim',
    '--hidden-import=requests',
    '--hidden-import=ssl',
    
    # PDF export support
    '--hidden-import=reportlab',
    '--hidden-import=reportlab.lib',
    '--hidden-import=reportlab.platypus',
    '--hidden-import=reportlab.pdfgen',
    
    # Real-time dashboard module
    '--hidden-import=realtime_dashboard',
    
    # Threading and queue support
    '--hidden-import=threading',
    '--hidden-import=queue',
    '--hidden-import=collections',
    
    # SQLite for real-time monitoring database
    '--hidden-import=sqlite3',
    
    # Tkinter styling
    '--hidden-import=tkinter.ttk',
    
    # Date/time handling
    '--hidden-import=datetime',
    '--hidden-import=calendar',
    
    # Regular expressions
    '--hidden-import=re',
    
    # Pathlib for file handling
    '--hidden-import=pathlib',
    
    # Base64 encoding (used in PDF generation)
    '--hidden-import=base64',
    '--hidden-import=io',
    
    # Temporary file handling
    '--hidden-import=tempfile',
    
    # Additional matplotlib components
    '--hidden-import=matplotlib.patches',
    '--hidden-import=matplotlib.figure',
    
    # NEW ADDITIONS for real-time integration:
    '--hidden-import=pandas.core.algorithms',
    '--hidden-import=pandas.core.arrays',
    '--hidden-import=pandas.io.formats.style',
    '--hidden-import=pandas._libs.tslibs',
    '--hidden-import=pandas._libs.hashtable',
    '--hidden-import=pandas._libs.lib',
    '--hidden-import=pandas._libs.missing',
    
    # Time zone handling for real-time timestamps
    '--hidden-import=pytz',
    '--hidden-import=dateutil',
    '--hidden-import=dateutil.parser',
    '--hidden-import=dateutil.tz',
    
    # Additional SQLite3 dependencies
    '--hidden-import=sqlite3.dbapi2',
    
    # Enhanced matplotlib for real-time charts
    '--hidden-import=matplotlib.animation',
    '--hidden-import=matplotlib.dates',
    '--hidden-import=matplotlib.ticker',
    
    # Additional collections for real-time data structures
    '--hidden-import=collections.deque',
    
    # JSON handling for configuration
    '--hidden-import=json',
    
    # Enhanced threading for real-time collection
    '--hidden-import=threading.Timer',
    
    # Mathematical operations for CPU Ready calculations
    '--hidden-import=math',
    '--hidden-import=statistics',
    
    # Warning handling
    '--hidden-import=warnings',
    
    # Additional imports for hostname/IP handling
    '--hidden-import=socket',
    '--hidden-import=ipaddress',
    
    # Traceback for better error handling
    '--hidden-import=traceback',
    
    # Time handling for intervals
    '--hidden-import=time',
    
    # Copy operations for data processing
    '--hidden-import=copy',
    
    # UUID for unique identifiers (if needed)
    '--hidden-import=uuid',
    
    # Additional pandas components for real-time data
    '--hidden-import=pandas.plotting',
    '--hidden-import=pandas.core.dtypes',
    
    # SIMPLE FIX: Exclude problematic modules and use alternatives
    '--exclude-module=pkg_resources',     # Exclude the problematic module
    '--exclude-module=setuptools',        # Exclude setuptools to avoid pkg_resources
    '--exclude-module=jaraco',            # Exclude jaraco to avoid dependency
    '--exclude-module=more_itertools',    # Exclude this too
    '--exclude-module=pytest',      
    '--exclude-module=IPython',
    '--exclude-module=jupyter',
    '--exclude-module=notebook',
    '--exclude-module=sphinx',
    
    # Exclude additional development tools
    '--exclude-module=pytest-cov',
    '--exclude-module=coverage',
    '--exclude-module=black',
    '--exclude-module=flake8',
    
    # Build options
    '--clean',                      # Clean cache before building
    '--noconfirm',                  # Overwrite output directory without confirmation
    
    # Increase recursion limit for complex imports
    '--additional-hooks-dir=.',
    
    # Optional: Include version info (Windows only)
    # '--version-file=version_info.txt',
])

print("Build complete! Check the 'dist' folder for your executable.")
print("Real-time dashboard integration included in build.")
print("Note: If you encounter any import errors, the application will still work for core functionality.")

# ALTERNATIVE: Create a hook file for better dependency management
# Create a file called 'hook-realtime_dashboard.py' in the same directory:

hook_content = '''
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
'''

# Write the hook file
hook_file = os.path.join(script_dir, 'hook-realtime_dashboard.py')
with open(hook_file, 'w') as f:
    f.write(hook_content)

print(f"Hook file created: {hook_file}")
print("This will help PyInstaller better detect real-time dashboard dependencies.")