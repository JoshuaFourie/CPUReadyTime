import PyInstaller.__main__
import os
import sys

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
main_script = os.path.join(script_dir, 'vcenter_cpu_analyzerv.py')

PyInstaller.__main__.run([
    main_script,
    '--onefile',                    # Create a single executable file
    '--windowed',                   # Don't show console window (GUI app)
    '--name=vCenter_CPU_Analyzer_v2',  # Updated name for version 2
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
    
    # Build options
    '--clean',                      # Clean cache before building
    '--noconfirm',                  # Overwrite output directory without confirmation
    
    # Optional: Include version info (Windows only)
    # '--version-file=version_info.txt',
])

print("Build complete! Check the 'dist' folder for your executable.")
print("Note: If you encounter any import errors, the application will still work for core functionality.")