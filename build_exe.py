import PyInstaller.__main__
import os
import sys

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
main_script = os.path.join(script_dir, 'vcenter_cpu_analyzer.py')  # Your main Python file

PyInstaller.__main__.run([
    main_script,
    '--onefile',                    # Create a single executable file
    '--windowed',                   # Don't show console window (GUI app)
    '--name=vCenter_CPU_Analyzer',  # Name of the executable
    '--icon=app_icon.ico',          # Optional: add an icon file
    '--add-data=README.txt;.',      # Optional: include additional files
    '--hidden-import=pandas',
    '--hidden-import=numpy', 
    '--hidden-import=matplotlib',
    '--hidden-import=seaborn',
    '--hidden-import=openpyxl',
    '--hidden-import=xlrd',
    '--hidden-import=pyvmomi',
    '--hidden-import=pyVim',
    '--hidden-import=pyVmomi',
    '--hidden-import=requests',
    '--exclude-module=pytest',      # Exclude unnecessary modules
    '--exclude-module=IPython',
    '--clean',                      # Clean cache before building
])