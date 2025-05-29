import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
import calendar
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.patches as patches
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns
from pathlib import Path
import re
import threading

# vCenter integration imports
try:
    from pyVim.connect import SmartConnect, Disconnect
    from pyVmomi import vim
    import ssl
    import requests
    from requests.auth import HTTPBasicAuth
    VCENTER_AVAILABLE = True
except ImportError:
    VCENTER_AVAILABLE = False

class ModernCPUAnalyzer:
    def __init__(self, root):
        self.root = root
        self.root.title("vCenter CPU Ready Analysis Tool")
        
        # Configure dark theme first
        self.setup_theme()
        
        # Setup matplotlib dark theme
        self.setup_matplotlib_dark_theme()
        
        # Responsive window sizing
        self.setup_window_geometry()
        
        # Data storage
        self.data_frames = []
        self.processed_data = None
        self.current_interval = "Last Day"
        self.vcenter_connection = None
        
        # Update intervals
        self.intervals = {
            "Real-Time": 20,
            "Last Day": 300,
            "Last Week": 1800,
            "Last Month": 7200,
            "Last Year": 86400
        }
        
        self.vcenter_intervals = {
            "Real-Time": 20,
            "Last Day": 300,
            "Last Week": 1800,
            "Last Month": 7200,
            "Last Year": 86400
        }
        
        self.setup_modern_ui()
        
    def setup_theme(self):
        """Configure modern dark theme with clean, minimal borders"""
        style = ttk.Style()
        
        # Use the best available theme as base
        available_themes = style.theme_names()
        if 'clam' in available_themes:
            style.theme_use('clam')
        elif 'vista' in available_themes:
            style.theme_use('vista')
        else:
            style.theme_use('default')
        
        # Modern dark color scheme (Visio-inspired)
        self.colors = {
            'bg_primary': '#1e1e1e',      # Dark background (main)
            'bg_secondary': '#2d2d30',    # Slightly lighter dark
            'bg_tertiary': '#3e3e42',     # Cards/panels
            'bg_accent': '#404040',       # Hover states
            'text_primary': '#f0f0f0',    # Main text (light)
            'text_secondary': '#cccccc',  # Secondary text
            'text_muted': '#999999',      # Muted text
            'accent_blue': '#0078d4',     # Microsoft blue
            'accent_hover': '#106ebe',    # Blue hover
            'success': '#107c10',         # Success green
            'warning': '#ff8c00',         # Warning orange
            'error': '#d13438',           # Error red
            'border': '#464647',          # Subtle borders
            'input_bg': '#333337',        # Input backgrounds
            'tab_active': '#007acc',      # Active tab
            'selection': '#094771'        # Selection color
        }
        
        # Configure root window
        self.root.configure(bg=self.colors['bg_primary'])
        
        # Configure all major styles with MINIMAL borders
        
        # Labels
        style.configure('Title.TLabel', 
                    font=('Segoe UI', 14, 'bold'), 
                    foreground=self.colors['text_primary'],
                    background=self.colors['bg_primary'])
        
        style.configure('Subtitle.TLabel', 
                    font=('Segoe UI', 10), 
                    foreground=self.colors['text_secondary'],
                    background=self.colors['bg_primary'])
        
        style.configure('Header.TLabel', 
                    font=('Segoe UI', 11, 'bold'), 
                    foreground=self.colors['accent_blue'],
                    background=self.colors['bg_primary'])
        
        style.configure('Success.TLabel', 
                    foreground=self.colors['success'],
                    background=self.colors['bg_primary'])
        
        style.configure('Warning.TLabel', 
                    foreground=self.colors['warning'],
                    background=self.colors['bg_primary'])
        
        style.configure('Error.TLabel', 
                    foreground=self.colors['error'],
                    background=self.colors['bg_primary'])
        
        # Default label
        style.configure('TLabel',
                    foreground=self.colors['text_primary'],
                    background=self.colors['bg_primary'])
        
        # Frames - NO BORDERS
        style.configure('TFrame',
                    background=self.colors['bg_primary'],
                    borderwidth=0,
                    relief='flat')
        
        # LabelFrames (Cards) - THIN BORDER ONLY
        style.configure('TLabelframe',
                    background=self.colors['bg_primary'],
                    borderwidth=1,
                    relief='solid')
        
        style.configure('TLabelframe.Label',
                    background=self.colors['bg_primary'],
                    foreground=self.colors['accent_blue'],
                    font=('Segoe UI', 10, 'bold'))
        
        # Override any padding/margins that create thick borders
        style.layout('TLabelframe', [
            ('Labelframe.border', {'sticky': 'nswe', 'children': [
                ('Labelframe.padding', {'sticky': 'nswe', 'children': [
                    ('Labelframe.label', {'side': 'top', 'sticky': 'w'}),
                    ('Labelframe.body', {'sticky': 'nswe'})
                ]})
            ]})
        ])
        
        # Buttons
        style.configure('TButton',
                    background=self.colors['bg_secondary'],
                    foreground=self.colors['text_primary'],
                    borderwidth=1,
                    relief='solid',
                    focuscolor='none',
                    font=('Segoe UI', 9))
        
        style.map('TButton',
                background=[('active', self.colors['bg_accent']),
                            ('pressed', self.colors['selection'])])
        
        style.configure('Primary.TButton',
                    background=self.colors['accent_blue'],
                    foreground='white',
                    borderwidth=1,
                    relief='solid',
                    focuscolor='none',
                    font=('Segoe UI', 9, 'bold'))
        
        style.map('Primary.TButton',
                background=[('active', self.colors['accent_hover']),
                            ('pressed', '#005a9e')])
        
        # Entries
        style.configure('TEntry',
                    fieldbackground=self.colors['input_bg'],
                    background=self.colors['input_bg'],
                    foreground=self.colors['text_primary'],
                    borderwidth=1,
                    relief='solid',
                    insertcolor=self.colors['text_primary'])
        
        style.map('TEntry',
                focuscolor=[('focus', self.colors['accent_blue'])])
        
        # Combobox
        style.configure('TCombobox',
                    fieldbackground=self.colors['input_bg'],
                    background=self.colors['input_bg'],
                    foreground=self.colors['text_primary'],
                    borderwidth=1,
                    relief='solid',
                    arrowcolor=self.colors['text_secondary'])
        
        style.map('TCombobox',
                fieldbackground=[('readonly', self.colors['input_bg'])])
        
        # Notebook (Tabs) - CLEAN tabs
        style.configure('TNotebook',
                    background=self.colors['bg_primary'],
                    borderwidth=0,
                    tabmargins=[0, 0, 0, 0])
        
        style.configure('TNotebook.Tab',
                            background=self.colors['bg_secondary'],
                            foreground=self.colors['text_secondary'],
                            padding=[20, 12, 20, 12],  # Increased padding: left, top, right, bottom
                            borderwidth=0,
                            relief='flat',
                            focuscolor='none',
                            font=('Segoe UI', 10, 'bold'),  # Made font bold for better visibility
                            width=15)  # Set minimum width for consistency
        
        style.map('TNotebook.Tab',
                background=[('selected', self.colors['bg_primary']),
                            ('active', self.colors['bg_accent']),
                            ('!active', self.colors['bg_secondary'])],
                foreground=[('selected', self.colors['tab_active']),
                            ('active', self.colors['text_primary']),
                            ('!active', self.colors['text_secondary'])],
                padding=[('selected', [20, 12, 20, 12]),  # Keep same padding when selected
                        ('!selected', [20, 12, 20, 12])])  # And when not selected
        
        # Treeview
        style.configure('Treeview',
                    background=self.colors['bg_secondary'],
                    foreground=self.colors['text_primary'],
                    fieldbackground=self.colors['bg_secondary'],
                    borderwidth=1,
                    relief='solid')
        
        style.configure('Treeview.Heading',
                    background=self.colors['bg_tertiary'],
                    foreground=self.colors['text_primary'],
                    font=('Segoe UI', 9, 'bold'),
                    borderwidth=1,
                    relief='solid')
        
        style.map('Treeview',
                background=[('selected', self.colors['selection'])],
                foreground=[('selected', self.colors['text_primary'])])
        
        style.map('Treeview.Heading',
                background=[('active', self.colors['bg_accent'])])
        
        # Scrollbars
        style.configure('Vertical.TScrollbar',
                    background=self.colors['bg_secondary'],
                    troughcolor=self.colors['bg_primary'],
                    borderwidth=0,
                    arrowcolor=self.colors['text_secondary'])
        
        style.configure('Horizontal.TScrollbar',
                    background=self.colors['bg_secondary'],
                    troughcolor=self.colors['bg_primary'],
                    borderwidth=0,
                    arrowcolor=self.colors['text_secondary'])
        
        # Progressbar
        style.configure('TProgressbar',
                    background=self.colors['accent_blue'],
                    troughcolor=self.colors['bg_secondary'],
                    borderwidth=0)
        
        # Spinbox
        style.configure('TSpinbox',
                    fieldbackground=self.colors['input_bg'],
                    background=self.colors['input_bg'],
                    foreground=self.colors['text_primary'],
                    borderwidth=1,
                    relief='solid',
                    arrowcolor=self.colors['text_secondary'])
    
    def setup_window_geometry(self):
        """Setup responsive window sizing with clean borders"""
        # Get screen dimensions
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        # Calculate appropriate size for different screen resolutions
        if screen_width <= 1920:  # 1080p and below
            window_width = min(1400, int(screen_width * 0.9))
            window_height = min(800, int(screen_height * 0.85))
        else:  # Higher resolutions
            window_width = min(1600, int(screen_width * 0.8))
            window_height = min(1000, int(screen_height * 0.8))
        
        # Center the window
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
        self.root.minsize(1200, 700)  # Minimum size for usability
        
        # Remove window border effects
        self.root.configure(bg=self.colors['bg_primary'])
        
        # Configure grid weights for responsiveness
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

    def setup_matplotlib_dark_theme(self):
        """Configure matplotlib for dark theme"""
        plt.style.use('dark_background')
        
        # Set custom colors for better integration
        plt.rcParams.update({
            'figure.facecolor': self.colors['bg_primary'],
            'axes.facecolor': self.colors['bg_secondary'],
            'axes.edgecolor': self.colors['border'],
            'axes.labelcolor': self.colors['text_primary'],
            'text.color': self.colors['text_primary'],
            'xtick.color': self.colors['text_secondary'],
            'ytick.color': self.colors['text_secondary'],
            'grid.color': self.colors['border'],
            'legend.facecolor': self.colors['bg_tertiary'],
            'legend.edgecolor': self.colors['border'],
            'legend.framealpha': 0.9
        })

    def create_dark_text_widget(self, parent, **kwargs):
        """Create a text widget with dark theme styling"""
        text_widget = tk.Text(parent, 
                            bg=self.colors['bg_secondary'],
                            fg=self.colors['text_primary'],
                            insertbackground=self.colors['text_primary'],
                            selectbackground=self.colors['selection'],
                            selectforeground=self.colors['text_primary'],
                            relief='solid',
                            borderwidth=1,
                            highlightcolor=self.colors['accent_blue'],
                            highlightbackground=self.colors['border'],
                            highlightthickness=1,
                            font=('Consolas', 10),
                            **kwargs)
        return text_widget

    def create_dark_listbox(self, parent, **kwargs):
        """Create a listbox with dark theme styling"""
        listbox = tk.Listbox(parent,
                            bg=self.colors['bg_secondary'],
                            fg=self.colors['text_primary'],
                            selectbackground=self.colors['selection'],
                            selectforeground=self.colors['text_primary'],
                            relief='solid',
                            borderwidth=1,
                            highlightcolor=self.colors['accent_blue'],
                            highlightbackground=self.colors['border'],
                            highlightthickness=1,
                            font=('Segoe UI', 10),
                            **kwargs)
        return listbox

    def create_card(self, parent, title):
        """Create a clean card using tk.LabelFrame instead of ttk"""
        card_frame = tk.LabelFrame(parent, text=f"  {title}  ",
                                bg=self.colors['bg_primary'],
                                fg=self.colors['accent_blue'],
                                font=('Segoe UI', 10, 'bold'),
                                borderwidth=1,
                                relief='solid')
        return card_frame

    def setup_modern_ui(self):
        """Create modern UI with proper expansion settings"""
        # Main container - ensure it expands properly
        main_container = tk.Frame(self.root, bg=self.colors['bg_primary'])
        main_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        
        # Configure main container to expand
        main_container.columnconfigure(0, weight=1)
        main_container.rowconfigure(1, weight=1)  # Notebook gets the space
        
        # Configure root window expansion
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Header (fixed height)
        self.create_header(main_container)
        
        # Create notebook (expandable)
        self.create_notebook(main_container)
        
        # Status bar (fixed height)
        self.create_status_bar(main_container)
       
    def create_header(self, parent):
        """Create modern header section"""
        header_frame = ttk.Frame(parent)
        header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 20))
        header_frame.columnconfigure(1, weight=1)
        
        # App title and description
        title_frame = ttk.Frame(header_frame)
        title_frame.grid(row=0, column=0, sticky=(tk.W))
        
        ttk.Label(title_frame, text="🖥️ vCenter CPU Ready Analyzer", 
                 style='Title.TLabel').grid(row=0, column=0, sticky=tk.W)
        ttk.Label(title_frame, text="Analyze CPU Ready metrics and optimize host consolidation", 
                 style='Subtitle.TLabel').grid(row=1, column=0, sticky=tk.W, pady=(2, 0))
        
        # Connection status
        self.status_frame = ttk.Frame(header_frame)
        self.status_frame.grid(row=0, column=1, sticky=(tk.E))
        
        self.connection_status = ttk.Label(self.status_frame, text="⚫ Disconnected", 
                                         style='Error.TLabel')
        self.connection_status.grid(row=0, column=0, sticky=tk.E)
        
    def create_notebook(self, parent):
        """Create tabbed interface with proper sizing"""
        self.notebook = ttk.Notebook(parent)
        self.notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        
        # Ensure notebook expands properly
        parent.rowconfigure(1, weight=1)
        parent.columnconfigure(0, weight=1)
        
        # Data Source Tab
        self.create_data_source_tab()
        
        # Analysis Tab
        self.create_analysis_tab()
        
        # Visualization Tab
        self.create_visualization_tab()
        
        # Host Management Tab
        self.create_host_management_tab()
        
        # Advanced Tab
        self.create_advanced_tab()
        
    def create_data_source_tab(self):
        """Create data source tab with complete vCenter integration"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="📁 Data Source")
        
        # File Import Section
        file_section = tk.LabelFrame(tab_frame, text="  📂 File Import  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        file_section.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        file_content = tk.Frame(file_section, bg=self.colors['bg_primary'])
        file_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # File import controls
        import_btn = tk.Button(file_content, text="📤 Import CSV/Excel Files",
                            command=self.import_files,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=5)
        import_btn.grid(row=0, column=0, padx=(0, 15))
        
        self.file_count_label = tk.Label(file_content, text="No files imported",
                                        bg=self.colors['bg_primary'], 
                                        fg=self.colors['text_primary'],
                                        font=('Segoe UI', 10))
        self.file_count_label.grid(row=0, column=1, sticky=tk.W)
        
        clear_btn = tk.Button(file_content, text="🗑️ Clear Files",
                            command=self.clear_files,
                            bg=self.colors['error'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=5)
        clear_btn.grid(row=0, column=2, padx=(15, 0))
        
        # Configure grid weights
        file_content.columnconfigure(1, weight=1)
        
        # vCenter Integration Section
        self.create_complete_vcenter_section(tab_frame)
        
        # Data Preview Section
        preview_section = tk.LabelFrame(tab_frame, text="  👁️ Data Preview  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        preview_section.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        preview_content = tk.Frame(preview_section, bg=self.colors['bg_primary'])
        preview_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Preview treeview
        columns = ('Source', 'Hosts', 'Records', 'Date Range')
        self.preview_tree = ttk.Treeview(preview_content, columns=columns, show='headings', height=6)
        
        for col in columns:
            self.preview_tree.heading(col, text=col)
            self.preview_tree.column(col, width=150)
        
        # Scrollbars for preview
        v_scrollbar = ttk.Scrollbar(preview_content, orient=tk.VERTICAL, command=self.preview_tree.yview)
        h_scrollbar = ttk.Scrollbar(preview_content, orient=tk.HORIZONTAL, command=self.preview_tree.xview)
        
        self.preview_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        self.preview_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        preview_content.columnconfigure(0, weight=1)
        preview_content.rowconfigure(0, weight=1)
      
    def create_vcenter_section(self, parent):
        """Create vCenter integration section"""
        vcenter_section = self.create_card(parent, "🔗 vCenter Integration")
        vcenter_section.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        content = ttk.Frame(vcenter_section)
        content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        content.columnconfigure(1, weight=1)
        content.columnconfigure(3, weight=1)
        
        # Connection fields
        ttk.Label(content, text="vCenter Server:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.vcenter_host = ttk.Entry(content, width=25)
        self.vcenter_host.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 20))
        
        ttk.Label(content, text="Username:").grid(row=0, column=2, sticky=tk.W, padx=(0, 10))
        self.vcenter_user = ttk.Entry(content, width=20)
        self.vcenter_user.grid(row=0, column=3, sticky=(tk.W, tk.E), padx=(0, 20))
        
        ttk.Label(content, text="Password:").grid(row=0, column=4, sticky=tk.W, padx=(0, 10))
        self.vcenter_pass = ttk.Entry(content, show="*", width=20)
        self.vcenter_pass.grid(row=0, column=5, sticky=(tk.W, tk.E))
        
        # Time period selection
        period_frame = ttk.Frame(content)
        period_frame.grid(row=1, column=0, columnspan=6, sticky=(tk.W, tk.E), pady=(15, 0))
        
        ttk.Label(period_frame, text="Time Period:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.vcenter_period_var = tk.StringVar(value="Last Day")
        period_combo = ttk.Combobox(period_frame, textvariable=self.vcenter_period_var,
                                  values=list(self.intervals.keys()), state="readonly", width=15)
        period_combo.grid(row=0, column=1, padx=(0, 20))
        period_combo.bind('<<ComboboxSelected>>', self.update_date_range_display)
        
        self.date_range_label = ttk.Label(period_frame, text="", style='Subtitle.TLabel')
        self.date_range_label.grid(row=0, column=2, sticky=tk.W)
        
        # Action buttons
        button_frame = ttk.Frame(content)
        button_frame.grid(row=2, column=0, columnspan=6, sticky=(tk.W, tk.E), pady=(15, 0))
        
        self.connect_btn = ttk.Button(button_frame, text="🔌 Connect", 
                                    command=self.connect_vcenter, style='Primary.TButton')
        self.connect_btn.grid(row=0, column=0, padx=(0, 10))
        
        self.fetch_btn = ttk.Button(button_frame, text="📊 Fetch Data", 
                                  command=self.fetch_vcenter_data, state='disabled')
        self.fetch_btn.grid(row=0, column=1, padx=(0, 10))
        
        self.vcenter_status = ttk.Label(button_frame, text="Not connected", style='Error.TLabel')
        self.vcenter_status.grid(row=0, column=2, padx=(15, 0))
        
        self.update_date_range_display()
        
    def create_vcenter_unavailable_section(self, parent):
        """Create section for when vCenter libraries are not available"""
        vcenter_section = self.create_card(parent, "⚠️ vCenter Integration")
        vcenter_section.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        content = ttk.Frame(vcenter_section)
        content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        ttk.Label(content, text="vCenter integration requires additional packages:", 
                 style='Warning.TLabel').pack(anchor=tk.W)
        ttk.Label(content, text="pip install pyvmomi requests", 
                 font=('Consolas', 9), foreground='#666').pack(anchor=tk.W, pady=(5, 0))
        
    def create_data_preview_section(self, parent):
        """Create data preview section"""
        preview_section = self.create_card(parent, "👁️ Data Preview")
        preview_section.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        content = ttk.Frame(preview_section)
        content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        content.columnconfigure(0, weight=1)
        content.rowconfigure(0, weight=1)
        
        # Create treeview for data preview
        columns = ('Source', 'Hosts', 'Records', 'Date Range')
        self.preview_tree = ttk.Treeview(content, columns=columns, show='headings', height=6)
        
        for col in columns:
            self.preview_tree.heading(col, text=col)
            self.preview_tree.column(col, width=150)
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(content, orient=tk.VERTICAL, command=self.preview_tree.yview)
        h_scrollbar = ttk.Scrollbar(content, orient=tk.HORIZONTAL, command=self.preview_tree.xview)
        
        self.preview_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        self.preview_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
    def create_analysis_tab(self):
        """Create analysis tab with tk widgets instead of ttk"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="📊 Analysis")
        
        # Configuration Section - use tk.LabelFrame
        config_section = tk.LabelFrame(tab_frame, text="  ⚙️ Analysis Configuration  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        config_section.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        config_content = tk.Frame(config_section, bg=self.colors['bg_primary'])
        config_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Controls frame
        controls_frame = tk.Frame(config_content, bg=self.colors['bg_primary'])
        controls_frame.pack(fill=tk.X)
        
        tk.Label(controls_frame, text="Update Interval:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        self.interval_var = tk.StringVar(value="Last Day")
        interval_combo = ttk.Combobox(controls_frame, textvariable=self.interval_var,
                                    values=list(self.intervals.keys()), state="readonly", width=15)
        interval_combo.grid(row=0, column=1, padx=(0, 20))
        interval_combo.bind('<<ComboboxSelected>>', self.on_interval_change)
        
        # Use tk.Button instead of ttk.Button for cleaner look
        calc_btn = tk.Button(controls_frame, text="🔍 Calculate CPU Ready %",
                            command=self.calculate_cpu_ready,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=5)
        calc_btn.grid(row=0, column=2, padx=(20, 0))
        
        # Threshold controls with tk widgets
        threshold_frame = tk.Frame(config_content, bg=self.colors['bg_primary'])
        threshold_frame.pack(fill=tk.X, pady=(10, 0))
        
        tk.Label(threshold_frame, text="Warning Threshold:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        
        self.warning_threshold = tk.DoubleVar(value=5.0)
        warning_spin = tk.Spinbox(threshold_frame, from_=1.0, to=50.0, width=8,
                                textvariable=self.warning_threshold, increment=1.0,
                                bg=self.colors['input_bg'], fg=self.colors['text_primary'],
                                insertbackground=self.colors['text_primary'],
                                relief='flat', borderwidth=1)
        warning_spin.grid(row=0, column=1, padx=(0, 5))
        
        tk.Label(threshold_frame, text="%",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary']).grid(row=0, column=2, padx=(0, 15))
        
        tk.Label(threshold_frame, text="Critical Threshold:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=3, sticky=tk.W, padx=(0, 5))
        
        self.critical_threshold = tk.DoubleVar(value=15.0)
        critical_spin = tk.Spinbox(threshold_frame, from_=5.0, to=100.0, width=8,
                                textvariable=self.critical_threshold, increment=5.0,
                                bg=self.colors['input_bg'], fg=self.colors['text_primary'],
                                insertbackground=self.colors['text_primary'],
                                relief='flat', borderwidth=1)
        critical_spin.grid(row=0, column=4, padx=(0, 5))
        
        tk.Label(threshold_frame, text="%",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary']).grid(row=0, column=5)
        
        # Results Section - clean tk.LabelFrame
        results_section = tk.LabelFrame(tab_frame, text="  📈 Analysis Results  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        results_section.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        results_content = tk.Frame(results_section, bg=self.colors['bg_primary'])
        results_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Keep ttk.Treeview but style it better
        columns = ('Host', 'Avg CPU Ready %', 'Max CPU Ready %', 'Health Score', 'Status', 'Records')
        self.results_tree = ttk.Treeview(results_content, columns=columns, show='headings')
        
        for col in columns:
            self.results_tree.heading(col, text=col)
            if col == 'Host':
                self.results_tree.column(col, width=150)
            elif 'CPU Ready' in col:
                self.results_tree.column(col, width=120)
            else:
                self.results_tree.column(col, width=100)
        
        # Results scrollbar
        results_scrollbar = ttk.Scrollbar(results_content, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=results_scrollbar.set)
        
        self.results_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        results_content.columnconfigure(0, weight=1)
        results_content.rowconfigure(0, weight=1)
        
    def create_visualization_tab(self):
        """Create visualization tab"""
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text="📊 Visualization")
        
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(0, weight=1)
        
        # Chart container
        chart_section = self.create_card(tab_frame, "📈 CPU Ready Timeline")
        chart_section.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        chart_content = ttk.Frame(chart_section)
        chart_content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        chart_content.columnconfigure(0, weight=1)
        chart_content.rowconfigure(0, weight=1)
        
        # Create matplotlib figure with modern styling
        self.fig, self.ax = plt.subplots(figsize=(12, 6))
        self.fig.patch.set_facecolor(self.colors['bg_primary'])
        self.ax.set_facecolor(self.colors['bg_secondary'])
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_content)
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
    def create_host_management_tab(self):
        """Create host management tab"""
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text="🖥️ Host Management")
        
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(1, weight=1)
        
        # Host Selection Section
        selection_section = self.create_card(tab_frame, "🎯 Host Consolidation Analysis")
        selection_section.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        
        selection_content = ttk.Frame(selection_section)
        selection_content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        selection_content.columnconfigure(0, weight=1)
        
        # Instructions
        ttk.Label(selection_content, text="Select hosts to analyze removal impact:", 
                 style='Header.TLabel').pack(anchor=tk.W, pady=(0, 10))
        
        # Host list frame
        list_frame = ttk.Frame(selection_content)
        list_frame.pack(fill=tk.BOTH, expand=True)
        list_frame.columnconfigure(0, weight=1)
        
        # Listbox with modern styling
        self.hosts_listbox = self.create_dark_listbox(list_frame, selectmode=tk.MULTIPLE, height=8)
        self.hosts_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        hosts_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.hosts_listbox.yview)
        self.hosts_listbox.configure(yscrollcommand=hosts_scrollbar.set)
        hosts_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Control buttons
        button_frame = ttk.Frame(list_frame)
        button_frame.grid(row=0, column=2, padx=(15, 0), sticky=(tk.N))
        
        ttk.Button(button_frame, text="✓ Select All", 
                  command=self.select_all_hosts).pack(pady=(0, 5), fill=tk.X)
        ttk.Button(button_frame, text="✗ Clear All", 
                  command=self.clear_all_hosts).pack(pady=(0, 15), fill=tk.X)
        ttk.Button(button_frame, text="🔍 Analyze Impact", 
                  command=self.analyze_multiple_removal_impact, 
                  style='Primary.TButton').pack(fill=tk.X)
        
        # Results Section
        results_section = self.create_card(tab_frame, "📊 Impact Analysis Results")
        results_section.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        results_content = ttk.Frame(results_section)
        results_content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        results_content.columnconfigure(0, weight=1)
        results_content.rowconfigure(0, weight=1)
        
        # Results text with modern styling
        self.impact_text = self.create_dark_text_widget(results_content, wrap=tk.WORD, padx=10, pady=10)
        
        impact_scrollbar = ttk.Scrollbar(results_content, orient=tk.VERTICAL, command=self.impact_text.yview)
        self.impact_text.configure(yscrollcommand=impact_scrollbar.set)
        
        self.impact_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        impact_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
    def create_advanced_tab(self):
        """Create advanced analysis tab using full available space"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="🔬 Advanced")
        
        # Configure the main frame to expand properly
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(0, weight=1)
        
        # Main container that fills the entire tab
        main_container = tk.Frame(tab_frame, bg=self.colors['bg_primary'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        main_container.columnconfigure(0, weight=1)
        main_container.rowconfigure(1, weight=1)  # Health dashboard gets most space
        
        # Advanced Analysis Options Section (top section - fixed height)
        analysis_section = tk.LabelFrame(main_container, text="  🔬 Advanced Analysis Options  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        analysis_section.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        
        analysis_content = tk.Frame(analysis_section, bg=self.colors['bg_primary'])
        analysis_content.pack(fill=tk.X, padx=15, pady=15)
        
        # Analysis buttons in a responsive grid
        button_grid = tk.Frame(analysis_content, bg=self.colors['bg_primary'])
        button_grid.pack(fill=tk.X)
        
        # Configure grid weights for even distribution
        for i in range(2):
            button_grid.columnconfigure(i, weight=1)
        
        # Button styling
        btn_style = {
            'bg': self.colors['bg_secondary'],
            'fg': self.colors['text_primary'],
            'font': ('Segoe UI', 10, 'bold'),
            'relief': 'flat',
            'borderwidth': 0,
            'padx': 20,
            'pady': 10,
            'cursor': 'hand2'
        }
        
        # Create buttons with proper spacing
        heatmap_btn = tk.Button(button_grid, text="📅 Heat Map Calendar", 
                            command=self.show_heatmap_calendar, **btn_style)
        heatmap_btn.grid(row=0, column=0, padx=(0, 5), pady=(0, 8), sticky="ew")
        
        trends_btn = tk.Button(button_grid, text="📈 Performance Trends", 
                            command=self.show_performance_trends, **btn_style)
        trends_btn.grid(row=0, column=1, padx=(5, 0), pady=(0, 8), sticky="ew")
        
        comparison_btn = tk.Button(button_grid, text="🎯 Host Comparison", 
                                command=self.show_host_comparison, **btn_style)
        comparison_btn.grid(row=1, column=0, padx=(0, 5), pady=0, sticky="ew")
        
        # Export button with accent color
        export_btn = tk.Button(button_grid, text="📋 Export Report", 
                            command=self.export_analysis_report,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 10, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=20, pady=10, cursor='hand2')
        export_btn.grid(row=1, column=1, padx=(5, 0), pady=0, sticky="ew")
        
        # Add hover effects
        def on_enter(event):
            if event.widget.cget('bg') != self.colors['accent_blue']:
                event.widget.config(bg=self.colors['bg_accent'])
        
        def on_leave(event):
            if event.widget.cget('bg') != self.colors['accent_blue']:
                event.widget.config(bg=self.colors['bg_secondary'])
        
        # Bind hover effects to regular buttons
        for widget in [heatmap_btn, trends_btn, comparison_btn]:
            widget.bind("<Enter>", on_enter)
            widget.bind("<Leave>", on_leave)
        
        # Host Health Dashboard Section (bottom section - expandable)
        health_section = tk.LabelFrame(main_container, text="  🏥 Host Health Dashboard  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        health_section.grid(row=1, column=0, sticky="nsew")  # This will expand to fill remaining space
        
        # Health content frame that expands
        health_content = tk.Frame(health_section, bg=self.colors['bg_primary'])
        health_content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        health_content.columnconfigure(0, weight=1)
        health_content.rowconfigure(0, weight=1)
        
        # Health dashboard text widget that fills available space
        self.health_text = tk.Text(health_content, 
                                wrap=tk.WORD,
                                bg=self.colors['bg_secondary'],
                                fg=self.colors['text_primary'],
                                insertbackground=self.colors['text_primary'],
                                selectbackground=self.colors['selection'],
                                selectforeground=self.colors['text_primary'],
                                relief='solid',
                                borderwidth=1,
                                highlightcolor=self.colors['accent_blue'],
                                highlightbackground=self.colors['border'],
                                highlightthickness=1,
                                font=('Consolas', 11),
                                padx=15,
                                pady=15)
        
        # Scrollbar for health text
        health_scrollbar = ttk.Scrollbar(health_content, orient=tk.VERTICAL, command=self.health_text.yview)
        self.health_text.configure(yscrollcommand=health_scrollbar.set)
        
        # Grid the text widget and scrollbar to fill space
        self.health_text.grid(row=0, column=0, sticky="nsew")
        health_scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Add enhanced initial content
        initial_content = """🏥 HOST HEALTH DASHBOARD
    ════════════════════════════════════════════════════════════════════════════════════

    💡 Welcome to the Advanced Analysis Center!

    This comprehensive dashboard provides deep insights into your vCenter infrastructure
    performance and helps optimize your host consolidation strategy.

    🚀 GETTING STARTED:
    ═══════════════════
    1. 📁 Import CPU Ready data files OR connect to vCenter (Data Source tab)
    2. 📊 Calculate CPU Ready percentages (Analysis tab)  
    3. 🔍 Run advanced analysis using the buttons above

    🔬 ADVANCED FEATURES AVAILABLE:
    ═══════════════════════════════════

    📅 HEAT MAP CALENDAR
    • Visual calendar showing daily CPU Ready patterns
    • Color-coded performance indicators
    • Identify problematic time periods at a glance
    • Perfect for capacity planning and maintenance scheduling

    📈 PERFORMANCE TRENDS  
    • Moving average analysis with trend lines
    • Peak performance identification
    • Distribution analysis with box plots
    • Hourly performance patterns (when sufficient data available)

    🎯 HOST COMPARISON
    • Side-by-side performance ranking
    • Health score calculations (0-100 scale)
    • Performance recommendations for each host
    • Consolidation candidate identification

    📋 EXPORT REPORTS
    • Comprehensive CSV reports with all metrics
    • Executive summaries with key findings
    • Timestamp tracking for audit trails

    🏥 AUTOMATED HEALTH ANALYSIS:
    ═══════════════════════════════
    Once you process your data, this dashboard will automatically display:

    • 🔴 Critical hosts requiring immediate attention
    • 🟡 Warning hosts needing monitoring  
    • 🟢 Healthy hosts suitable for consolidation
    • 📊 Performance statistics and trends
    • 💡 Optimization recommendations
    • ⏱️ Time-based performance patterns

    🎯 CONSOLIDATION INSIGHTS:
    ═════════════════════════
    • Identify over-provisioned hosts
    • Calculate consolidation impact
    • Risk assessment for host removal
    • Workload redistribution analysis
    • Infrastructure cost optimization

    Ready to optimize your infrastructure? Start by importing your data! 🚀

    ═══════════════════════════════════════════════════════════════════════════════════"""
        
        self.health_text.insert(1.0, initial_content)
        
        # Make it editable so users can scroll and select text
        self.health_text.config(state='normal')
        
        # Add mouse wheel scrolling
        def _on_mousewheel(event):
            self.health_text.yview_scroll(int(-1*(event.delta/120)), "units")
        
        self.health_text.bind("<MouseWheel>", _on_mousewheel)
        
        # Auto-populate health data if available
        self.root.after(100, lambda: self.apply_thresholds() if hasattr(self, 'processed_data') and self.processed_data is not None else None)
      
    def create_status_bar(self, parent):
        """Create modern status bar"""
        status_frame = ttk.Frame(parent)
        status_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(15, 0))
        status_frame.columnconfigure(1, weight=1)
        
        # Status information
        self.status_label = ttk.Label(status_frame, text="Ready", style='Subtitle.TLabel')
        self.status_label.grid(row=0, column=0, sticky=tk.W)
        
        # Progress bar (hidden by default)
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, 
                                          mode='determinate', length=200)
        
        # Add window close handling
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def show_progress(self, message="Processing..."):
        """Show progress bar with message"""
        self.status_label.config(text=message)
        self.progress_bar.grid(row=0, column=2, sticky=tk.E, padx=(10, 0))
        self.progress_bar.start()
        self.root.update()
    
    def hide_progress(self, message="Ready"):
        """Hide progress bar"""
        self.progress_bar.stop()
        self.progress_bar.grid_remove()
        self.status_label.config(text=message)
        self.root.update()
    
    # Data Import Methods
    def import_files(self):
        """Import CSV/Excel files with modern progress indication"""
        file_paths = filedialog.askopenfilenames(
            title="Select CPU Ready Data Files",
            filetypes=[
                ("CSV files", "*.csv"),
                ("Excel files", "*.xlsx *.xls"),
                ("All supported", "*.csv *.xlsx *.xls"),
                ("All files", "*.*")
            ]
        )
        
        if not file_paths:
            return
        
        self.show_progress(f"Importing {len(file_paths)} files...")
        successful_imports = 0
        
        try:
            for file_path in file_paths:
                try:
                    if file_path.lower().endswith('.csv'):
                        df = pd.read_csv(file_path)
                    else:
                        df = pd.read_excel(file_path)
                    
                    if not self.validate_dataframe(df):
                        messagebox.showwarning("Invalid File", 
                                             f"File {Path(file_path).name} does not have required columns")
                        continue
                    
                    df['source_file'] = Path(file_path).name
                    self.data_frames.append(df)
                    successful_imports += 1
                    
                except Exception as e:
                    messagebox.showerror("Import Error", 
                                       f"Error importing {Path(file_path).name}:\n{str(e)}")
            
            if successful_imports > 0:
                self.update_file_status()
                self.update_data_preview()
                messagebox.showinfo("Import Complete", 
                                  f"Successfully imported {successful_imports} files")
            
        finally:
            self.hide_progress()
    
    def validate_dataframe(self, df):
        try:
            time_cols = [col for col in df.columns if 'time' in col.lower()]
            ready_cols = [col for col in df.columns if 'ready for' in col.lower()]
            return bool(time_cols) and bool(ready_cols)
        except AttributeError:
            return False
    
    def clear_files(self):
        """Clear all imported data"""
        self.data_frames = []
        self.processed_data = None
        self.update_file_status()
        self.update_data_preview()
        self.clear_results()
        self.status_label.config(text="All data cleared")
    
    def update_file_status(self):
        """Update file count display"""
        if self.data_frames:
            self.file_count_label.config(text=f"{len(self.data_frames)} files imported")
        else:
            self.file_count_label.config(text="No files imported")
    
    def update_data_preview(self):
        """Update data preview table"""
        # Clear existing items
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)
        
        if not self.data_frames:
            return
        
        for df in self.data_frames:
            try:
                # Extract info from dataframe
                source = df['source_file'].iloc[0] if 'source_file' in df.columns else "Unknown"
                
                # Count hosts
                ready_cols = [col for col in df.columns if 'ready for' in col.lower()]
                host_count = len(ready_cols)
                
                # Get record count
                record_count = len(df)
                
                # Get date range if available
                time_cols = [col for col in df.columns if 'time' in col.lower()]
                if time_cols:
                    try:
                        time_data = pd.to_datetime(df[time_cols[0]])
                        date_range = f"{time_data.min().strftime('%Y-%m-%d')} to {time_data.max().strftime('%Y-%m-%d')}"
                    except:
                        date_range = "Unknown"
                else:
                    date_range = "No time data"
                
                self.preview_tree.insert('', 'end', values=(
                    source, f"{host_count} hosts", f"{record_count:,} records", date_range
                ))
                
            except Exception as e:
                self.preview_tree.insert('', 'end', values=(
                    "Error", str(e), "", ""
                ))
    
    # vCenter Integration Methods
    def update_date_range_display(self, event=None):
        """Update date range label based on selected period"""
        period = self.vcenter_period_var.get()
        now = datetime.now()
        
        ranges = {
            "Real-Time": (now - timedelta(hours=1), now),
            "Last Day": (now - timedelta(days=1), now),
            "Last Week": (now - timedelta(weeks=1), now),
            "Last Month": (now - timedelta(days=30), now),
            "Last Year": (now - timedelta(days=365), now)
        }
        
        if period in ranges:
            start_time, end_time = ranges[period]
            if period == "Real-Time":
                range_text = f"({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')} today)"
            elif period in ["Last Day"]:
                range_text = f"({start_time.strftime('%m/%d %H:%M')} - {end_time.strftime('%m/%d %H:%M')})"
            else:
                range_text = f"({start_time.strftime('%m/%d')} - {end_time.strftime('%m/%d')})"
        else:
            range_text = ""
        
        self.date_range_label.config(text=range_text)

    def connect_vcenter(self):
        """Connect to vCenter server with proper error handling"""
        if not VCENTER_AVAILABLE:
            messagebox.showerror("Error", "vCenter integration not available. Install required packages.")
            return
        
        # Get connection details
        vcenter_host = self.vcenter_host.get().strip()
        username = self.vcenter_user.get().strip()
        password = self.vcenter_pass.get()
        
        if not all([vcenter_host, username, password]):
            messagebox.showwarning("Missing Information", 
                                "Please fill in all vCenter connection fields")
            return
        
        # Show progress
        self.show_progress("Connecting to vCenter...")
        self.connect_btn.config(text="Connecting...", state='disabled')
        
        def connect_thread():
            try:
                # Disable SSL verification for self-signed certificates
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                
                # Attempt connection
                self.vcenter_connection = SmartConnect(
                    host=vcenter_host,
                    user=username,
                    pwd=password,
                    sslContext=context
                )
                
                if self.vcenter_connection:
                    # Success - update UI on main thread
                    self.root.after(0, self.on_vcenter_connected, vcenter_host)
                else:
                    self.root.after(0, self.on_vcenter_connect_failed, "Connection failed")
                    
            except Exception as e:
                error_msg = f"Failed to connect to vCenter:\n{str(e)}"
                self.root.after(0, self.on_vcenter_connect_failed, error_msg)
        
        # Run connection in separate thread
        threading.Thread(target=connect_thread, daemon=True).start()

    def on_vcenter_connected(self, vcenter_host):
        """Handle successful vCenter connection"""
        self.vcenter_status.config(text="🟢 Connected", fg=self.colors['success'])
        self.connection_status.config(text="🟢 vCenter Connected", 
                                    style='Success.TLabel')
        self.fetch_btn.config(state='normal')
        self.connect_btn.config(text="🔌 Disconnect", 
                            command=self.disconnect_vcenter,
                            state='normal')
        self.hide_progress()
        messagebox.showinfo("Success", f"Connected to vCenter: {vcenter_host}")

    def create_complete_vcenter_section(self, parent):
        """Create complete vCenter integration section"""
        if not VCENTER_AVAILABLE:
            # Show unavailable message
            vcenter_section = tk.LabelFrame(parent, text="  ⚠️ vCenter Integration  ",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['warning'],
                                        font=('Segoe UI', 10, 'bold'),
                                        borderwidth=1,
                                        relief='solid')
            vcenter_section.pack(fill=tk.X, padx=10, pady=5)
            
            warning_frame = tk.Frame(vcenter_section, bg=self.colors['bg_primary'])
            warning_frame.pack(fill=tk.X, padx=10, pady=10)
            
            tk.Label(warning_frame, 
                    text="⚠️ vCenter integration requires additional packages:",
                    bg=self.colors['bg_primary'], 
                    fg=self.colors['warning'],
                    font=('Segoe UI', 10, 'bold')).pack(anchor=tk.W)
            
            tk.Label(warning_frame,
                    text="pip install pyvmomi requests",
                    bg=self.colors['bg_primary'],
                    fg=self.colors['text_secondary'],
                    font=('Consolas', 9)).pack(anchor=tk.W, pady=(5, 0))
            return

        # vCenter Available - Create full integration
        vcenter_section = tk.LabelFrame(parent, text="  🔗 vCenter Integration  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        vcenter_section.pack(fill=tk.X, padx=10, pady=5)
        
        vcenter_content = tk.Frame(vcenter_section, bg=self.colors['bg_primary'])
        vcenter_content.pack(fill=tk.X, padx=10, pady=10)
        
        # Connection fields frame
        conn_frame = tk.Frame(vcenter_content, bg=self.colors['bg_primary'])
        conn_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Row 1: Server and Username
        row1_frame = tk.Frame(conn_frame, bg=self.colors['bg_primary'])
        row1_frame.pack(fill=tk.X, pady=(0, 5))
        
        tk.Label(row1_frame, text="vCenter Server:", 
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        
        self.vcenter_host = tk.Entry(row1_frame, width=25,
                                    bg=self.colors['input_bg'], 
                                    fg=self.colors['text_primary'],
                                    insertbackground=self.colors['text_primary'],
                                    relief='flat', borderwidth=1)
        self.vcenter_host.grid(row=0, column=1, padx=(0, 20), sticky=tk.W)
        
        tk.Label(row1_frame, text="Username:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        
        self.vcenter_user = tk.Entry(row1_frame, width=20,
                                    bg=self.colors['input_bg'],
                                    fg=self.colors['text_primary'],
                                    insertbackground=self.colors['text_primary'],
                                    relief='flat', borderwidth=1)
        self.vcenter_user.grid(row=0, column=3, sticky=tk.W)
        
        # Row 2: Password and Connect button
        row2_frame = tk.Frame(conn_frame, bg=self.colors['bg_primary'])
        row2_frame.pack(fill=tk.X)
        
        tk.Label(row2_frame, text="Password:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        
        self.vcenter_pass = tk.Entry(row2_frame, show="*", width=25,
                                    bg=self.colors['input_bg'],
                                    fg=self.colors['text_primary'],
                                    insertbackground=self.colors['text_primary'],
                                    relief='flat', borderwidth=1)
        self.vcenter_pass.grid(row=0, column=1, padx=(0, 20), sticky=tk.W)
        
        # Connect button
        self.connect_btn = tk.Button(row2_frame, text="🔌 Connect",
                                    command=self.connect_vcenter,
                                    bg=self.colors['accent_blue'], fg='white',
                                    font=('Segoe UI', 9, 'bold'),
                                    relief='flat', borderwidth=0,
                                    padx=15, pady=5)
        self.connect_btn.grid(row=0, column=2, padx=(0, 10))
        
        # Status label
        self.vcenter_status = tk.Label(row2_frame, text="⚫ Disconnected",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['error'],
                                    font=('Segoe UI', 10))
        self.vcenter_status.grid(row=0, column=3, sticky=tk.W)
        
        # Data fetch controls frame
        fetch_frame = tk.Frame(vcenter_content, bg=self.colors['bg_primary'])
        fetch_frame.pack(fill=tk.X, pady=(10, 0))
        
        tk.Label(fetch_frame, text="Time Period:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        
        self.vcenter_period_var = tk.StringVar(value="Last Day")
        period_values = list(self.vcenter_intervals.keys())
        
        # Create a custom combobox-like widget using tk
        self.period_dropdown = tk.OptionMenu(fetch_frame, self.vcenter_period_var, *period_values)
        self.period_dropdown.config(bg=self.colors['input_bg'],
                                fg=self.colors['text_primary'],
                                activebackground=self.colors['bg_accent'],
                                activeforeground=self.colors['text_primary'],
                                relief='flat', borderwidth=1)
        self.period_dropdown.grid(row=0, column=1, padx=(0, 20))
        
        # Fetch button  
        self.fetch_btn = tk.Button(fetch_frame, text="📊 Fetch Data",
                                command=self.fetch_vcenter_data,
                                bg=self.colors['bg_secondary'], fg=self.colors['text_primary'],
                                font=('Segoe UI', 9, 'bold'),
                                relief='flat', borderwidth=0,
                                padx=15, pady=5,
                                state='disabled')  # Disabled until connected
        self.fetch_btn.grid(row=0, column=2)
   
    def on_vcenter_connect_failed(self, error_msg):
        """Handle failed vCenter connection"""
        self.vcenter_status.config(text="🔴 Failed", fg=self.colors['error'])
        self.connect_btn.config(text="🔌 Connect", state='normal')
        self.hide_progress()
        messagebox.showerror("Connection Error", error_msg)

    def disconnect_vcenter(self):
        """Disconnect from vCenter"""
        try:
            if self.vcenter_connection:
                Disconnect(self.vcenter_connection)
                self.vcenter_connection = None
            
            self.vcenter_status.config(text="⚫ Disconnected", fg=self.colors['error'])
            self.connection_status.config(text="⚫ Disconnected", style='Error.TLabel')
            self.fetch_btn.config(state='disabled')
            self.connect_btn.config(text="🔌 Connect", command=self.connect_vcenter)
            
            messagebox.showinfo("Disconnected", "Successfully disconnected from vCenter")
            
        except Exception as e:
            messagebox.showerror("Disconnect Error", f"Error disconnecting: {str(e)}")
    
    def fetch_vcenter_data(self):
        """Fetch CPU Ready data from vCenter for all hosts"""
        if not self.vcenter_connection:
            messagebox.showerror("No Connection", "Please connect to vCenter first")
            return
        
        # Get date range based on selected vCenter period
        start_date, end_date = self.get_vcenter_date_range()
        selected_period = self.vcenter_period_var.get()
        perf_interval = self.vcenter_intervals[selected_period]
        
        # Create progress dialog
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Fetching vCenter Data")
        progress_window.geometry("400x150")
        progress_window.transient(self.root)
        progress_window.grab_set()
        
        # Center the progress window
        progress_window.geometry("+%d+%d" % (
            self.root.winfo_rootx() + 50,
            self.root.winfo_rooty() + 50
        ))
        
        ttk.Label(progress_window, text="🔄 Fetching CPU Ready data from vCenter...", 
                 font=('Segoe UI', 11, 'bold')).pack(pady=15)
        
        info_frame = ttk.Frame(progress_window)
        info_frame.pack(pady=10)
        
        ttk.Label(info_frame, text=f"Period: {selected_period}").pack()
        ttk.Label(info_frame, text=f"Range: {start_date} to {end_date}").pack()
        ttk.Label(info_frame, text=f"Interval: {perf_interval} seconds").pack()
        
        progress_bar = ttk.Progressbar(progress_window, mode='indeterminate', length=300)
        progress_bar.pack(pady=15)
        progress_bar.start()
        
        # Run fetch in separate thread
        def fetch_thread():
            try:
                content = self.vcenter_connection.RetrieveContent()
                hosts = self.get_all_hosts(content)
                
                if not hosts:
                    messagebox.showwarning("No Hosts", "No ESXi hosts found in vCenter")
                    return
                
                cpu_ready_data = self.fetch_cpu_ready_metrics(content, hosts, start_date, end_date, perf_interval)
                
                if cpu_ready_data:
                    df = pd.DataFrame(cpu_ready_data)
                    df['source_file'] = f'vCenter_{selected_period}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}'
                    
                    self.data_frames.append(df)
                    self.update_file_status()
                    self.update_data_preview()
                    
                    # Auto-set the interval
                    self.interval_var.set(selected_period)
                    self.current_interval = selected_period
                    
                    messagebox.showinfo("Success", 
                                      f"✅ Successfully fetched vCenter data!\n\n"
                                      f"📊 Period: {selected_period}\n"
                                      f"🖥️  Hosts: {len(hosts)}\n"
                                      f"📅 Date Range: {start_date} to {end_date}\n"
                                      f"📈 Total Records: {len(cpu_ready_data)}")
                else:
                    messagebox.showwarning("No Data", f"No CPU Ready data found for {selected_period}")
                    
            except Exception as e:
                messagebox.showerror("Fetch Error", f"Error fetching data from vCenter:\n{str(e)}")
            finally:
                progress_window.destroy()
        
        threading.Thread(target=fetch_thread, daemon=True).start()
    
    def fetch_cpu_ready_metrics(self, content, hosts, start_date, end_date, interval_seconds):
        """Fetch CPU Ready metrics for all hosts with proper interval handling"""
        perf_manager = content.perfManager
        cpu_ready_data = []
        
        print(f"DEBUG: Requesting data from {start_date} to {end_date}")
        print(f"DEBUG: Looking for interval: {interval_seconds} seconds")
        
        # Find CPU Ready metric
        counter_info = None
        for counter in perf_manager.perfCounter:
            if (counter.groupInfo.key == 'cpu' and 
                counter.nameInfo.key == 'ready' and 
                counter.unitInfo.key == 'millisecond'):
                counter_info = counter
                print(f"DEBUG: Found CPU Ready counter ID: {counter.key}")
                break
        
        if not counter_info:
            raise Exception("CPU Ready metric not found in vCenter")
        
        # Get available performance intervals from vCenter
        print("DEBUG: Checking available performance intervals...")
        
        # Query available intervals for historical data
        available_intervals = perf_manager.historicalInterval
        print("DEBUG: Available historical intervals:")
        for interval in available_intervals:
            print(f"  - Key: {interval.key}, Name: {interval.name}, Period: {interval.samplingPeriod}s, Level: {interval.level}")
        
        # Convert dates to vCenter format
        start_time = datetime.combine(start_date, datetime.min.time())
        end_time = datetime.combine(end_date, datetime.max.time())
        time_diff = (end_time - start_time).total_seconds()
        
        print(f"DEBUG: Time difference: {time_diff} seconds ({time_diff/3600:.1f} hours)")
        
        # Determine the best interval to use based on available intervals and time range
        selected_interval = None
        
        # For very recent data (last 1 hour), try real-time first
        if time_diff <= 3600:
            print("DEBUG: Using real-time data approach")
            selected_interval = None  # Real-time uses no intervalId
            use_realtime = True
        else:
            use_realtime = False
            # Find the best matching interval from available historical intervals
            best_interval = None
            best_diff = float('inf')
            
            for interval in available_intervals:
                # Calculate how close this interval's period is to what we want
                period_diff = abs(interval.samplingPeriod - interval_seconds)
                if period_diff < best_diff:
                    best_diff = period_diff
                    best_interval = interval
            
            if best_interval:
                selected_interval = best_interval.key
                print(f"DEBUG: Selected historical interval: Key={selected_interval}, Period={best_interval.samplingPeriod}s, Name={best_interval.name}")
            else:
                print("DEBUG: No suitable historical interval found, trying real-time")
                selected_interval = None
                use_realtime = True
        
        # Fetch data for each host
        print(f"DEBUG: Fetching data for {len(hosts)} hosts...")
        for host_info in hosts:
            try:
                host = host_info['object']
                hostname = host_info['name']
                print(f"DEBUG: Processing host: {hostname}")
                
                # Create metric specification
                metric_spec = vim.PerformanceManager.MetricId(
                    counterId=counter_info.key,
                    instance=""  # Empty instance for aggregate data
                )
                
                # Try to query available metrics for this specific host first
                try:
                    available_metrics = perf_manager.QueryAvailablePerfMetric(entity=host)
                    cpu_ready_metrics = [m for m in available_metrics if m.counterId == counter_info.key]
                    print(f"DEBUG: Found {len(cpu_ready_metrics)} CPU Ready metrics available for {hostname}")
                    
                    # If we have specific instances, use them
                    if cpu_ready_metrics:
                        metric_specs = []
                        for metric in cpu_ready_metrics:
                            metric_specs.append(vim.PerformanceManager.MetricId(
                                counterId=counter_info.key,
                                instance=metric.instance
                            ))
                    else:
                        metric_specs = [metric_spec]
                        
                except Exception as e:
                    print(f"DEBUG: Could not query available metrics for {hostname}: {e}")
                    metric_specs = [metric_spec]
                
                # Create the query specification
                if use_realtime:
                    # Real-time query - no intervalId specified
                    query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=metric_specs,
                        maxSample=20,  # Limit samples for real-time
                        startTime=start_time,
                        endTime=end_time
                    )
                    print(f"DEBUG: Using real-time query for {hostname}")
                else:
                    # Historical query with proper intervalId
                    query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=metric_specs,
                        intervalId=selected_interval,
                        maxSample=100,  # Limit samples to avoid timeouts
                        startTime=start_time,
                        endTime=end_time
                    )
                    print(f"DEBUG: Using historical query for {hostname} with interval {selected_interval}")
                
                print(f"DEBUG: Executing query for {hostname}...")
                
                # Execute query
                perf_data = perf_manager.QueryPerf(querySpec=[query_spec])
                
                if perf_data and len(perf_data) > 0:
                    print(f"DEBUG: Query successful for {hostname}")
                    
                    if perf_data[0].value and len(perf_data[0].value) > 0:
                        print(f"DEBUG: Found {len(perf_data[0].sampleInfo)} samples for {hostname}")
                        
                        # Process the performance data
                        for i, sample_info in enumerate(perf_data[0].sampleInfo):
                            timestamp = sample_info.timestamp
                            
                            # Get CPU Ready value for this timestamp
                            total_ready = 0
                            for value_info in perf_data[0].value:
                                if i < len(value_info.value) and value_info.value[i] is not None and value_info.value[i] >= 0:
                                    total_ready += value_info.value[i]
                            
                            # Only add if we have actual data
                            if total_ready > 0:
                                cpu_ready_data.append({
                                    'Time': timestamp.isoformat() + 'Z',
                                    f'Ready for {hostname}': total_ready,
                                    'Hostname': hostname.split('.')[0]  # Short hostname
                                })
                    else:
                        print(f"DEBUG: No values in performance data for {hostname}")
                else:
                    print(f"DEBUG: No performance data returned for {hostname}")
                    
            except Exception as e:
                print(f"DEBUG: Error fetching data for host {hostname}: {e}")
                
                # If the query failed, try a simpler approach
                try:
                    print(f"DEBUG: Trying fallback query approach for {hostname}...")
                    
                    # Simplest possible query - no time range, no interval
                    simple_query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=[vim.PerformanceManager.MetricId(counterId=counter_info.key, instance="")],
                        maxSample=10
                    )
                    
                    perf_data = perf_manager.QueryPerf(querySpec=[simple_query_spec])
                    
                    if perf_data and perf_data[0].value:
                        print(f"DEBUG: Fallback query successful for {hostname}")
                        for i, sample_info in enumerate(perf_data[0].sampleInfo):
                            timestamp = sample_info.timestamp
                            total_ready = 0
                            for value_info in perf_data[0].value:
                                if i < len(value_info.value) and value_info.value[i] is not None and value_info.value[i] >= 0:
                                    total_ready += value_info.value[i]
                            
                            if total_ready > 0:
                                cpu_ready_data.append({
                                    'Time': timestamp.isoformat() + 'Z',
                                    f'Ready for {hostname}': total_ready,
                                    'Hostname': hostname.split('.')[0]
                                })
                    else:
                        print(f"DEBUG: Fallback query also failed for {hostname}")
                        
                except Exception as e2:
                    print(f"DEBUG: Fallback query also failed for {hostname}: {e2}")
                
                continue
        
        print(f"DEBUG: Total records collected: {len(cpu_ready_data)}")
        return cpu_ready_data

    def get_vcenter_date_range(self):
        """Calculate start and end dates based on selected vCenter period"""
        period = self.vcenter_period_var.get()
        now = datetime.now()
        
        ranges = {
            "Real-Time": (now - timedelta(hours=1), now),
            "Last Day": (now - timedelta(days=1), now),
            "Last Week": (now - timedelta(weeks=1), now),
            "Last Month": (now - timedelta(days=30), now),
            "Last Year": (now - timedelta(days=365), now)
        }
        
        start_time, end_time = ranges.get(period, ranges["Last Day"])
        return start_time.date(), end_time.date()

    def get_all_hosts(self, content):
        """Get all ESXi hosts from vCenter"""
        hosts = []
        container = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.HostSystem], True)
        
        for host in container.view:
            if host.runtime.connectionState == vim.HostSystemConnectionState.connected:
                hosts.append({
                    'name': host.name,
                    'object': host
                })
            else:
                print(f"DEBUG: Skipping disconnected host: {host.name}")
        
        container.Destroy()
        print(f"DEBUG: Found {len(hosts)} connected hosts")
        return hosts
      
    # Analysis Methods
    def on_interval_change(self, event):
        """Handle interval change"""
        self.current_interval = self.interval_var.get()
        if self.processed_data is not None:
            self.calculate_cpu_ready()
    
    def calculate_cpu_ready(self):
        """Calculate CPU Ready percentages"""
        if not self.data_frames:
            messagebox.showwarning("No Data", "Please import files or fetch data from vCenter first")
            return
        
        self.show_progress("Calculating CPU Ready percentages...")
        
        try:
            combined_data = []
            
            for df in self.data_frames:
                print(f"DEBUG: Processing dataframe with columns: {list(df.columns)}")
                
                # Find time and ready columns
                time_col = next((col for col in df.columns if 'time' in col.lower()), None)
                ready_cols = [col for col in df.columns if 'ready for' in col.lower()]
                
                print(f"DEBUG: Found time column: {time_col}")
                print(f"DEBUG: Found ready columns: {ready_cols}")
                
                if not time_col or not ready_cols:
                    print("DEBUG: Missing required columns, skipping this dataframe")
                    continue
                
                # Process each ready column (each represents a different host)
                for ready_col in ready_cols:
                    print(f"DEBUG: Processing ready column: {ready_col}")
                    
                    # Extract hostname from column name
                    if '$' in ready_col:
                        # Format: "Ready for $hostname"
                        hostname_match = re.search(r'\$(\w+)', ready_col)
                        hostname = hostname_match.group(1) if hostname_match else "Unknown"
                    else:
                        # Format: "Ready for full.hostname.domain" or "Ready for IP"
                        hostname_match = re.search(r'Ready for (.+)', ready_col)
                        if hostname_match:
                            full_hostname = hostname_match.group(1).strip()
                            
                            # Check if it's an IP address
                            if re.match(r'^\d+\.\d+\.\d+\.\d+$', full_hostname):
                                # Use the full IP as hostname for clarity
                                hostname = f"Host-{full_hostname.replace('.', '-')}"
                                print(f"DEBUG: IP address detected, using hostname: {hostname}")
                            else:
                                # Extract just the first part of the hostname
                                hostname = full_hostname.split('.')[0]
                                print(f"DEBUG: Extracted hostname '{hostname}' from '{full_hostname}'")
                        else:
                            hostname = "Unknown"
                            print("DEBUG: Could not extract hostname, using 'Unknown'")
                    
                    print(f"DEBUG: Final hostname: {hostname}")
                    
                    # Check if we already have data for this hostname (avoid duplicates)
                    existing_hostnames = [data['Hostname'].iloc[0] for data in combined_data if len(data) > 0]
                    if hostname in existing_hostnames:
                        print(f"DEBUG: Hostname {hostname} already processed, skipping duplicate")
                        continue
                    
                    # Process data for this host
                    subset = df[[time_col, ready_col, 'source_file']].copy()
                    subset.columns = ['Time', 'CPU_Ready_Sum', 'Source_File']
                    subset['Hostname'] = hostname
                    
                    # Remove rows with null/zero values
                    subset = subset.dropna()
                    subset = subset[subset['CPU_Ready_Sum'] != 0]
                    
                    print(f"DEBUG: Created subset with {len(subset)} rows for host {hostname}")
                    
                    if len(subset) == 0:
                        print(f"DEBUG: No valid data for host {hostname}, skipping")
                        continue
                    
                    # Fix timestamp format issues from vCenter
                    def clean_timestamp(ts):
                        if isinstance(ts, str):
                            # Remove trailing Z if there's already timezone info
                            if '+' in ts and ts.endswith('Z'):
                                ts = ts[:-1]
                            # Handle various vCenter timestamp formats
                            try:
                                return pd.to_datetime(ts, utc=True)
                            except:
                                # Try removing timezone info and adding Z
                                if '+' in ts:
                                    ts = ts.split('+')[0] + 'Z'
                                return pd.to_datetime(ts, utc=True)
                        return pd.to_datetime(ts, utc=True)
                    
                    # Convert time to datetime with proper handling
                    subset['Time'] = subset['Time'].apply(clean_timestamp)
                    
                    # Calculate CPU Ready %
                    interval_seconds = self.intervals[self.current_interval]
                    subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 1000)) * 100
                    
                    print(f"DEBUG: Sample CPU Ready values for {hostname}: {subset['CPU_Ready_Percent'].head().tolist()}")
                    
                    combined_data.append(subset)
            
            if not combined_data:
                messagebox.showerror("Processing Error", "No valid data found in imported files")
                return
                
            self.processed_data = pd.concat(combined_data, ignore_index=True)
            
            print(f"DEBUG: Final combined data has {len(self.processed_data)} rows")
            print(f"DEBUG: Unique hostnames found: {self.processed_data['Hostname'].unique().tolist()}")
            print(f"DEBUG: Records per hostname:")
            for hostname in self.processed_data['Hostname'].unique():
                count = len(self.processed_data[self.processed_data['Hostname'] == hostname])
                print(f"  - {hostname}: {count} records")
            
            # Update all displays
            self.update_results_display()
            self.update_chart()
            self.update_host_list()
            self.apply_thresholds()
            
            self.status_label.config(text=f"Analysis complete - {len(self.processed_data)} records processed")
            
        except Exception as e:
            print(f"DEBUG: Full error details: {e}")
            import traceback
            traceback.print_exc()
            messagebox.showerror("Calculation Error", f"Error calculating CPU Ready %:\n{str(e)}")
        finally:
            self.hide_progress()
    
    def update_results_display(self):
        """Update analysis results table"""
        # Clear existing results
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        
        if self.processed_data is None:
            return
        
        # Calculate statistics for each host
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            
            avg_cpu = host_data['CPU_Ready_Percent'].mean()
            max_cpu = host_data['CPU_Ready_Percent'].max()
            std_cpu = host_data['CPU_Ready_Percent'].std()
            record_count = len(host_data)
            
            # Calculate health score
            health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu)
            
            # Determine status
            if avg_cpu >= self.critical_threshold.get():
                status = "🔴 Critical"
            elif avg_cpu >= self.warning_threshold.get():
                status = "🟡 Warning"
            else:
                status = "🟢 Healthy"
            
            self.results_tree.insert('', 'end', values=(
                hostname,
                f"{avg_cpu:.2f}%",
                f"{max_cpu:.2f}%",
                f"{health_score:.0f}/100",
                status,
                f"{record_count:,}"
            ))
    
    def calculate_health_score(self, avg_cpu_ready, max_cpu_ready, std_cpu_ready):
        """Calculate health score (0-100)"""
        score = 100
        warning_level = self.warning_threshold.get()
        critical_level = self.critical_threshold.get()
        
        # Penalize based on average
        if avg_cpu_ready >= critical_level:
            score -= 50
        elif avg_cpu_ready >= warning_level:
            score -= 25
        else:
            score -= (avg_cpu_ready / warning_level) * 10
        
        # Penalize based on max (spikes)
        if max_cpu_ready >= critical_level * 2:
            score -= 30
        elif max_cpu_ready >= critical_level:
            score -= 15
        
        # Penalize based on variability
        if std_cpu_ready > warning_level:
            score -= 15
        
        return max(0, min(100, score))
    
    def update_chart(self):
        """Update visualization chart with dark theme styling"""
        if self.processed_data is None:
            return
        
        self.ax.clear()
        
        # Modern dark color palette for lines
        colors = ['#00d4ff', '#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#feca57', '#ff9ff3', '#54a0ff']
        
        hostnames = sorted(self.processed_data['Hostname'].unique())
        
        for i, hostname in enumerate(hostnames):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            host_data = host_data.sort_values('Time')
            
            color = colors[i % len(colors)]
            self.ax.plot(host_data['Time'], host_data['CPU_Ready_Percent'],
                        marker='o', markersize=3, linewidth=2.5, label=hostname,
                        color=color, alpha=0.9)
        
        # Add threshold lines with dark theme colors
        warning_line = self.warning_threshold.get()
        critical_line = self.critical_threshold.get()
        
        self.ax.axhline(y=warning_line, color='#ff8c00', linestyle='--', 
                    alpha=0.8, linewidth=2, label=f'Warning ({warning_line}%)')
        self.ax.axhline(y=critical_line, color='#ff4757', linestyle='--', 
                    alpha=0.8, linewidth=2, label=f'Critical ({critical_line}%)')
        
        # Dark theme styling
        self.ax.set_facecolor(self.colors['bg_secondary'])
        self.ax.set_title(f'CPU Ready % Timeline ({self.current_interval} Interval)', 
                        fontsize=14, fontweight='bold', color=self.colors['text_primary'], pad=20)
        self.ax.set_xlabel('Time', fontsize=12, color=self.colors['text_primary'])
        self.ax.set_ylabel('CPU Ready %', fontsize=12, color=self.colors['text_primary'])
        
        # Legend with dark styling
        legend = self.ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1),
                            frameon=True, fancybox=True, shadow=False,
                            facecolor=self.colors['bg_tertiary'],
                            edgecolor=self.colors['border'],
                            labelcolor=self.colors['text_primary'])
        
        # Grid styling
        self.ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5, color=self.colors['border'])
        
        # Axis colors
        self.ax.tick_params(colors=self.colors['text_secondary'])
        self.ax.spines['bottom'].set_color(self.colors['border'])
        self.ax.spines['top'].set_color(self.colors['border'])
        self.ax.spines['left'].set_color(self.colors['border'])
        self.ax.spines['right'].set_color(self.colors['border'])
        
        # Format dates on x-axis
        self.fig.autofmt_xdate()
        self.fig.patch.set_facecolor(self.colors['bg_primary'])
        self.fig.tight_layout()
        self.canvas.draw()
  
    def clear_results(self):
        """Clear all results displays"""
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        
        if hasattr(self, 'impact_text'):
            self.impact_text.delete(1.0, tk.END)
        
        self.ax.clear()
        self.canvas.draw()
    
    # Host Management Methods
    def update_host_list(self):
        """Update host selection listbox"""
        if self.processed_data is None:
            return
        
        self.hosts_listbox.delete(0, tk.END)
        
        hostnames = sorted(self.processed_data['Hostname'].unique())
        for hostname in hostnames:
            self.hosts_listbox.insert(tk.END, hostname)
    
    def select_all_hosts(self):
        """Select all hosts"""
        self.hosts_listbox.select_set(0, tk.END)
    
    def clear_all_hosts(self):
        """Clear host selection"""
        self.hosts_listbox.selection_clear(0, tk.END)
    
    def get_selected_hosts(self):
        """Get selected hosts list"""
        selected_indices = self.hosts_listbox.curselection()
        return [self.hosts_listbox.get(i) for i in selected_indices]
    
    def analyze_multiple_removal_impact(self):
        """Analyze impact of removing multiple hosts"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        selected_hosts = self.get_selected_hosts()
        
        if not selected_hosts:
            messagebox.showwarning("No Selection", "Please select at least one host")
            return
        
        self.show_progress("Analyzing removal impact...")
        
        try:
            # Perform analysis (simplified version)
            total_hosts = len(self.processed_data['Hostname'].unique())
            if len(selected_hosts) >= total_hosts:
                self.impact_text.delete(1.0, tk.END)
                self.impact_text.insert(1.0, "❌ Cannot remove all hosts - no remaining infrastructure!")
                return
            
            # Calculate impact metrics
            selected_data = self.processed_data[self.processed_data['Hostname'].isin(selected_hosts)]
            remaining_data = self.processed_data[~self.processed_data['Hostname'].isin(selected_hosts)]
            
            workload_to_redistribute = selected_data['CPU_Ready_Sum'].sum()
            total_workload = self.processed_data['CPU_Ready_Sum'].sum()
            workload_percentage = (workload_to_redistribute / total_workload) * 100
            
            current_avg = self.processed_data['CPU_Ready_Percent'].mean()
            
            # Simple redistribution calculation
            remaining_hosts = len(remaining_data['Hostname'].unique())
            additional_per_host = workload_to_redistribute / remaining_hosts
            
            # Create analysis report
            report = f"""📊 HOST REMOVAL IMPACT ANALYSIS
{'='*50}

🗑️  Hosts to Remove: {len(selected_hosts)}
{chr(10).join([f"   • {host}" for host in selected_hosts])}

📈 Workload Impact:
   • Total workload to redistribute: {workload_percentage:.1f}%
   • Current average CPU Ready: {current_avg:.2f}%
   • Remaining hosts: {remaining_hosts}

⚠️  Estimated Impact:
   • Additional workload per remaining host: +{(workload_percentage/remaining_hosts):.1f}%
   • Infrastructure reduction: {(len(selected_hosts)/total_hosts*100):.1f}%

💡 Recommendations:
"""
            
            if workload_percentage > 20:
                report += "   🔴 HIGH RISK: Significant workload redistribution required\n"
            elif workload_percentage > 10:
                report += "   🟡 MODERATE RISK: Monitor performance after consolidation\n"
            else:
                report += "   🟢 LOW RISK: Safe for consolidation\n"
            
            self.impact_text.delete(1.0, tk.END)
            self.impact_text.insert(1.0, report)
            
        except Exception as e:
            messagebox.showerror("Analysis Error", f"Error analyzing removal impact:\n{str(e)}")
        finally:
            self.hide_progress()
    
    # Advanced Analysis Methods
    def apply_thresholds(self):
        """Apply threshold analysis"""
        if self.processed_data is None:
            return
        
        warning_level = self.warning_threshold.get()
        critical_level = self.critical_threshold.get()
        
        # Generate health report
        report = f"""🏥 HOST HEALTH ANALYSIS
{'='*50}

Thresholds: Warning {warning_level}% | Critical {critical_level}%

"""
        
        hosts_summary = []
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            
            avg_cpu = host_data['CPU_Ready_Percent'].mean()
            max_cpu = host_data['CPU_Ready_Percent'].max()
            std_cpu = host_data['CPU_Ready_Percent'].std()
            
            health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu)
            
            if avg_cpu >= critical_level:
                status = "🔴 CRITICAL"
            elif avg_cpu >= warning_level:
                status = "🟡 WARNING"
            else:
                status = "🟢 HEALTHY"
            
            # Time above thresholds
            warning_time = len(host_data[host_data['CPU_Ready_Percent'] >= warning_level])
            critical_time = len(host_data[host_data['CPU_Ready_Percent'] >= critical_level])
            total_time = len(host_data)
            
            hosts_summary.append({
                'hostname': hostname,
                'health_score': health_score,
                'status': status,
                'avg_cpu': avg_cpu,
                'max_cpu': max_cpu,
                'warning_pct': (warning_time / total_time) * 100,
                'critical_pct': (critical_time / total_time) * 100
            })
        
        # Sort by health score (worst first)
        hosts_summary.sort(key=lambda x: x['health_score'])
        
        for i, host in enumerate(hosts_summary, 1):
            report += f"{i}. {host['hostname']} - {host['status']}\n"
            report += f"   Health Score: {host['health_score']:.0f}/100\n"
            report += f"   Avg: {host['avg_cpu']:.2f}% | Max: {host['max_cpu']:.2f}%\n"
            report += f"   Time > Warning: {host['warning_pct']:.1f}%\n"
            report += f"   Time > Critical: {host['critical_pct']:.1f}%\n\n"
        
        # Summary
        critical_hosts = [h for h in hosts_summary if h['avg_cpu'] >= critical_level]
        warning_hosts = [h for h in hosts_summary if warning_level <= h['avg_cpu'] < critical_level]
        healthy_hosts = [h for h in hosts_summary if h['avg_cpu'] < warning_level]
        
        report += f"📊 SUMMARY:\n"
        report += f"🔴 Critical: {len(critical_hosts)} hosts\n"
        report += f"🟡 Warning: {len(warning_hosts)} hosts\n"
        report += f"🟢 Healthy: {len(healthy_hosts)} hosts\n"
        
        self.health_text.delete(1.0, tk.END)
        self.health_text.insert(1.0, report)
    
    def show_heatmap_calendar(self):
        """Display CPU Ready data as a heat map calendar"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create new window for heatmap
        heatmap_window = tk.Toplevel(self.root)
        heatmap_window.title("📅 CPU Ready Heat Map Calendar")
        heatmap_window.geometry("1200x800")
        heatmap_window.transient(self.root)
        
        # Create matplotlib figure with modern styling
        num_hosts = len(self.processed_data['Hostname'].unique())
        fig_height = max(8, num_hosts * 2)
        fig, axes = plt.subplots(nrows=num_hosts, ncols=1, figsize=(14, fig_height))
        
        if num_hosts == 1:
            axes = [axes]
        
        # Get date range
        start_date = self.processed_data['Time'].min().date()
        end_date = self.processed_data['Time'].max().date()
        
        # Modern colormap
        colors = ['#10b981', '#84cc16', '#eab308', '#f59e0b', '#ef4444', '#dc2626']
        custom_cmap = LinearSegmentedColormap.from_list('modern_cpu_ready', colors, N=256)
        
        for idx, hostname in enumerate(sorted(self.processed_data['Hostname'].unique())):
            ax = axes[idx] if num_hosts > 1 else axes[0]
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            
            # Group by date and calculate daily average
            host_data['Date'] = host_data['Time'].dt.date
            daily_avg = host_data.groupby('Date')['CPU_Ready_Percent'].mean().reset_index()
            
            # Create calendar grid
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            weeks = []
            current_week = []
            
            for single_date in date_range:
                date_val = single_date.date()
                cpu_ready_val = daily_avg[daily_avg['Date'] == date_val]['CPU_Ready_Percent']
                
                value = cpu_ready_val.iloc[0] if len(cpu_ready_val) > 0 else 0
                current_week.append(value)
                
                if date_val.weekday() == 6 or single_date == date_range[-1]:
                    while len(current_week) < 7:
                        current_week.append(0)
                    weeks.append(current_week)
                    current_week = []
            
            calendar_array = np.array(weeks)
            max_val = max(20, calendar_array.max())
            
            # Create heatmap
            im = ax.imshow(calendar_array, cmap=custom_cmap, aspect='auto', 
                          vmin=0, vmax=max_val, interpolation='nearest')
            
            # Styling
            ax.set_title(f'{hostname} - Daily CPU Ready %', 
                        fontsize=12, fontweight='bold', pad=10)
            ax.set_xlabel('Day of Week')
            ax.set_ylabel('Week')
            
            # Day labels
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            ax.set_xticks(range(7))
            ax.set_xticklabels(days)
            
            # Add values for significant readings
            warning_threshold = self.warning_threshold.get()
            for week_idx, week in enumerate(weeks):
                for day_idx, value in enumerate(week):
                    if value >= warning_threshold:
                        text_color = 'white' if value > max_val * 0.6 else 'black'
                        ax.text(day_idx, week_idx, f'{value:.1f}', 
                               ha='center', va='center', fontsize=8, 
                               color=text_color, fontweight='bold')
            
            # Colorbar
            cbar = plt.colorbar(im, ax=ax, shrink=0.8)
            cbar.set_label('CPU Ready %', rotation=270, labelpad=15)
        
        plt.tight_layout(pad=2.0)
        
        # Embed in window
        canvas = FigureCanvasTkAgg(fig, master=heatmap_window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Add legend
        legend_frame = ttk.Frame(heatmap_window)
        legend_frame.pack(fill=tk.X, padx=15, pady=(0, 10))
        
        ttk.Label(legend_frame, text="🌡️ Heat Map Legend:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor=tk.W)
        ttk.Label(legend_frame, text="🟢 Green: Excellent (0-2%) | 🟡 Yellow: Good (2-5%) | 🟠 Orange: Warning (5-15%) | 🔴 Red: Critical (>15%)", 
                 style='Subtitle.TLabel').pack(anchor=tk.W, pady=(2, 0))
    
    def show_performance_trends(self):
        """Show advanced performance trend analysis"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create trends window
        trends_window = tk.Toplevel(self.root)
        trends_window.title("📈 Performance Trends Analysis")  
        trends_window.geometry("1400x900")
        trends_window.transient(self.root)
        
        # Create figure with subplots
        fig = plt.figure(figsize=(16, 12))
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
        
        # 1. Moving Average Trends
        ax1 = fig.add_subplot(gs[0, :])
        colors = plt.cm.Set3(np.linspace(0, 1, len(self.processed_data['Hostname'].unique())))
        
        for i, hostname in enumerate(sorted(self.processed_data['Hostname'].unique())):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            host_data = host_data.sort_values('Time')
            
            # Calculate moving averages
            host_data['MA_5'] = host_data['CPU_Ready_Percent'].rolling(window=5, center=True).mean()
            host_data['MA_10'] = host_data['CPU_Ready_Percent'].rolling(window=10, center=True).mean()
            
            color = colors[i]
            ax1.plot(host_data['Time'], host_data['CPU_Ready_Percent'], 
                    alpha=0.3, color=color, linewidth=1)
            ax1.plot(host_data['Time'], host_data['MA_10'], 
                    linewidth=2.5, label=f'{hostname}', color=color)
        
        ax1.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                   linestyle='--', alpha=0.8, label='Warning')
        ax1.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                   linestyle='--', alpha=0.8, label='Critical')
        
        ax1.set_title('CPU Ready % Trends with Moving Averages', fontsize=14, fontweight='bold')
        ax1.set_ylabel('CPU Ready %')
        ax1.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # 2. Distribution Analysis
        ax2 = fig.add_subplot(gs[1, 0])
        all_values = []
        labels = []
        
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            all_values.append(host_data['CPU_Ready_Percent'].values)
            labels.append(hostname)
        
        bp = ax2.boxplot(all_values, labels=labels, patch_artist=True)
        
        # Color the boxes
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax2.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                   linestyle='--', alpha=0.8)
        ax2.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                   linestyle='--', alpha=0.8)
        
        ax2.set_title('CPU Ready % Distribution by Host', fontsize=12, fontweight='bold')
        ax2.set_ylabel('CPU Ready %')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
        
        # 3. Peak Analysis
        ax3 = fig.add_subplot(gs[1, 1])
        peak_data = []
        peak_hosts = []
        
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            top_peaks = host_data.nlargest(3, 'CPU_Ready_Percent')['CPU_Ready_Percent'].values
            
            for peak in top_peaks:
                peak_data.append(peak)
                peak_hosts.append(hostname)
        
        scatter_colors = [colors[list(sorted(self.processed_data['Hostname'].unique())).index(host)] 
                         for host in peak_hosts]
        
        ax3.scatter(range(len(peak_data)), peak_data, c=scatter_colors, 
                   s=100, alpha=0.7, edgecolors='black', linewidth=1)
        
        ax3.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                   linestyle='--', alpha=0.8, label='Warning')
        ax3.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                   linestyle='--', alpha=0.8, label='Critical')
        
        ax3.set_title('Performance Peaks Analysis', fontsize=12, fontweight='bold')
        ax3.set_ylabel('CPU Ready %')
        ax3.set_xlabel('Peak Instance')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # 4. Time Pattern Analysis
        ax4 = fig.add_subplot(gs[2, :])
        
        if len(self.processed_data) > 24:
            self.processed_data['Hour'] = self.processed_data['Time'].dt.hour
            hourly_stats = self.processed_data.groupby(['Hour', 'Hostname'])['CPU_Ready_Percent'].agg(['mean', 'std']).reset_index()
            
            for i, hostname in enumerate(sorted(self.processed_data['Hostname'].unique())):
                host_hourly = hourly_stats[hourly_stats['Hostname'] == hostname]
                color = colors[i]
                
                ax4.plot(host_hourly['Hour'], host_hourly['mean'], 
                        marker='o', linewidth=2, label=hostname, color=color)
                ax4.fill_between(host_hourly['Hour'], 
                               host_hourly['mean'] - host_hourly['std'],
                               host_hourly['mean'] + host_hourly['std'],
                               alpha=0.2, color=color)
            
            ax4.set_title('Average CPU Ready % by Hour (with Standard Deviation)', 
                         fontsize=12, fontweight='bold')
            ax4.set_xlabel('Hour of Day')
            ax4.set_ylabel('CPU Ready %')
            ax4.legend()
            ax4.grid(True, alpha=0.3)
            ax4.set_xticks(range(0, 24, 2))
        else:
            ax4.text(0.5, 0.5, '📊 Insufficient data for hourly analysis\n\nNeed more data points to show patterns', 
                    ha='center', va='center', transform=ax4.transAxes, 
                    fontsize=14, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            ax4.set_title('Hourly Pattern Analysis', fontsize=12, fontweight='bold')
        
        # Embed in window
        canvas = FigureCanvasTkAgg(fig, master=trends_window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
    
    def show_host_comparison(self):
        """Show detailed host-by-host comparison"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create comparison window
        comparison_window = tk.Toplevel(self.root)
        comparison_window.title("🎯 Host Performance Comparison")
        comparison_window.geometry("1200x700")
        comparison_window.transient(self.root)
        
        # Create main frame
        main_frame = ttk.Frame(comparison_window)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Title
        ttk.Label(main_frame, text="📊 Comprehensive Host Performance Analysis", 
                 font=('Segoe UI', 14, 'bold')).pack(pady=(0, 15))
        
        # Create comparison table with modern styling
        columns = ('Rank', 'Host', 'Avg %', 'Max %', 'Min %', 'Std Dev', 'Health Score', 'Status', 'Recommendation')
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=15)
        
        # Configure columns
        tree.column('Rank', width=50)
        tree.column('Host', width=120)
        tree.column('Avg %', width=80)
        tree.column('Max %', width=80)
        tree.column('Min %', width=80)
        tree.column('Std Dev', width=80)
        tree.column('Health Score', width=100)
        tree.column('Status', width=100)
        tree.column('Recommendation', width=150)
        
        for col in columns:
            tree.heading(col, text=col)
        
        # Calculate comprehensive stats
        comparison_data = []
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            
            avg_cpu = host_data['CPU_Ready_Percent'].mean()
            max_cpu = host_data['CPU_Ready_Percent'].max()
            min_cpu = host_data['CPU_Ready_Percent'].min()
            std_cpu = host_data['CPU_Ready_Percent'].std()
            
            health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu)
            
            # Determine status and recommendation
            if avg_cpu >= self.critical_threshold.get():
                status = "🔴 Critical"
                recommendation = "Immediate attention needed"
            elif avg_cpu >= self.warning_threshold.get():
                status = "🟡 Warning"
                recommendation = "Monitor and investigate"
            elif avg_cpu < 2:
                status = "🟢 Excellent"
                recommendation = "Great consolidation candidate"
            else:
                status = "🟢 Good"
                recommendation = "Performing well"
            
            comparison_data.append({
                'hostname': hostname,
                'avg': avg_cpu,
                'max': max_cpu,
                'min': min_cpu,
                'std': std_cpu,
                'health': health_score,
                'status': status,
                'recommendation': recommendation
            })
        
        # Sort by health score (best first for ranking)
        comparison_data.sort(key=lambda x: -x['health'])
        
        # Populate table
        for rank, data in enumerate(comparison_data, 1):
            # Color coding based on status
            tree.insert('', 'end', values=(
                f"#{rank}",
                data['hostname'],
                f"{data['avg']:.2f}%",
                f"{data['max']:.2f}%", 
                f"{data['min']:.2f}%",
                f"{data['std']:.2f}%",
                f"{data['health']:.0f}/100",
                data['status'],
                data['recommendation']
            ))
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        
        # Summary statistics
        summary_frame = ttk.LabelFrame(main_frame, text="📈 Summary Statistics", padding=10)
        summary_frame.pack(fill=tk.X, pady=(15, 0))
        
        critical_count = len([d for d in comparison_data if d['avg'] >= self.critical_threshold.get()])
        warning_count = len([d for d in comparison_data if self.warning_threshold.get() <= d['avg'] < self.critical_threshold.get()])
        healthy_count = len([d for d in comparison_data if d['avg'] < self.warning_threshold.get()])
        
        summary_text = (f"🔴 Critical Hosts: {critical_count} | "
                       f"🟡 Warning Hosts: {warning_count} | "
                       f"🟢 Healthy Hosts: {healthy_count} | "
                       f"📊 Total Hosts: {len(comparison_data)}")
        
        ttk.Label(summary_frame, text=summary_text, font=('Segoe UI', 10)).pack()
        
        # Export button
        ttk.Button(main_frame, text="📋 Export Comparison Report", 
                  command=lambda: self.export_comparison_report(comparison_data),
                  style='Primary.TButton').pack(pady=(10, 0))
    
    def export_comparison_report(self, comparison_data):
        """Export detailed comparison report"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Host Comparison Report"
        )
        
        if filename:
            try:
                df_data = []
                for rank, data in enumerate(comparison_data, 1):
                    df_data.append({
                        'Rank': rank,
                        'Hostname': data['hostname'],
                        'Average_CPU_Ready_Percent': round(data['avg'], 3),
                        'Maximum_CPU_Ready_Percent': round(data['max'], 3),
                        'Minimum_CPU_Ready_Percent': round(data['min'], 3),
                        'Standard_Deviation': round(data['std'], 3),
                        'Health_Score': round(data['health'], 1),
                        'Status': data['status'].replace('🔴 ', '').replace('🟡 ', '').replace('🟢 ', ''),
                        'Recommendation': data['recommendation'],
                        'Export_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                df = pd.DataFrame(df_data)
                df.to_csv(filename, index=False)
                messagebox.showinfo("Export Complete", f"Comparison report exported to:\n{filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export report:\n{str(e)}")
    
    def export_analysis_report(self):
        """Export analysis report"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                # Create comprehensive report
                report_data = []
                
                for hostname in sorted(self.processed_data['Hostname'].unique()):
                    host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
                    
                    avg_cpu = host_data['CPU_Ready_Percent'].mean()
                    max_cpu = host_data['CPU_Ready_Percent'].max()
                    min_cpu = host_data['CPU_Ready_Percent'].min()
                    std_cpu = host_data['CPU_Ready_Percent'].std()
                    
                    health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu)
                    
                    report_data.append({
                        'Hostname': hostname,
                        'Average_CPU_Ready_Percent': round(avg_cpu, 2),
                        'Maximum_CPU_Ready_Percent': round(max_cpu, 2),
                        'Minimum_CPU_Ready_Percent': round(min_cpu, 2),
                        'Standard_Deviation': round(std_cpu, 2),
                        'Health_Score': round(health_score, 0),
                        'Total_Records': len(host_data),
                        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Warning_Threshold': self.warning_threshold.get(),
                        'Critical_Threshold': self.critical_threshold.get()
                    })
                
                df = pd.DataFrame(report_data)
                df.to_csv(filename, index=False)
                
                messagebox.showinfo("Export Complete", f"Report exported to:\n{filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export report:\n{str(e)}")
    
    def on_closing(self):
        """Handle application closing"""
        try:
            if hasattr(self, 'vcenter_connection') and self.vcenter_connection:
                Disconnect(self.vcenter_connection)
            
            if hasattr(self, 'fig'):
                plt.close(self.fig)
            
            plt.close('all')
            self.root.quit()
            self.root.destroy()
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
        finally:
            import sys
            sys.exit(0)


def main():
    """Main application entry point"""
    try:
        root = tk.Tk()
        app = ModernCPUAnalyzer(root)
        root.mainloop()
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
    finally:
        try:
            plt.close('all')
        except:
            pass
        print("Application closed successfully")


if __name__ == "__main__":
    main()