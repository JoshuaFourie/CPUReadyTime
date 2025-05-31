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
        
        self.auto_analyze = tk.BooleanVar(value=True)  # Default: auto-analyze ON
        self.auto_switch_tabs = tk.BooleanVar(value=True)  # Default: auto-switch ON
        
        # Add workflow state tracking
        self.workflow_state = {
            'data_imported': False,
            'analysis_complete': False,
            'last_action': None
        }
        
        # NOW setup the UI (this will access the auto-flow variables)
        self.setup_modern_ui()

    def create_auto_flow_controls(self, parent):
        """Add auto-flow controls to Data Source tab"""
        # Add this to your Data Source tab creation
        
        auto_flow_frame = tk.LabelFrame(parent, text="  ⚙️ Workflow Settings  ",
                                       bg=self.colors['bg_primary'],
                                       fg=self.colors['accent_blue'],
                                       font=('Segoe UI', 10, 'bold'),
                                       borderwidth=1,
                                       relief='solid')
        auto_flow_frame.pack(fill=tk.X, padx=10, pady=5)
        
        controls_content = tk.Frame(auto_flow_frame, bg=self.colors['bg_primary'])
        controls_content.pack(fill=tk.X, padx=10, pady=8)
        
        # Auto-analyze checkbox
        auto_analyze_cb = tk.Checkbutton(controls_content,
                                        text="🔄 Auto-analyze after import",
                                        variable=self.auto_analyze,
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['text_primary'],
                                        selectcolor=self.colors['input_bg'],
                                        activebackground=self.colors['bg_primary'],
                                        activeforeground=self.colors['text_primary'],
                                        font=('Segoe UI', 9))
        auto_analyze_cb.pack(side=tk.LEFT, padx=(0, 20))
        
        # Auto-switch tabs checkbox  
        auto_switch_cb = tk.Checkbutton(controls_content,
                                       text="📊 Auto-switch to results",
                                       variable=self.auto_switch_tabs,
                                       bg=self.colors['bg_primary'],
                                       fg=self.colors['text_primary'],
                                       selectcolor=self.colors['input_bg'],
                                       activebackground=self.colors['bg_primary'],
                                       activeforeground=self.colors['text_primary'],
                                       font=('Segoe UI', 9))
        auto_switch_cb.pack(side=tk.LEFT)

    def fetch_vcenter_data(self):
        """Fetch CPU Ready data from vCenter for all hosts with styled progress dialog and auto-flow"""
        if not self.vcenter_connection:
            messagebox.showerror("No Connection", "Please connect to vCenter first")
            return
        
        # Get date range based on selected vCenter period
        start_date, end_date = self.get_vcenter_date_range()
        selected_period = self.vcenter_period_var.get()
        perf_interval = self.vcenter_intervals[selected_period]
        
        # Create styled progress dialog
        progress_window = self.create_styled_popup_window("Fetching vCenter Data", 400, 150)
        progress_window.resizable(False, False)
        
        # Progress content with dark theme
        progress_content = tk.Frame(progress_window, bg=self.colors['bg_primary'])
        progress_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = tk.Label(progress_content, 
                            text="🔄 Fetching CPU Ready data from vCenter...",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 11, 'bold'))
        title_label.pack(pady=(0, 15))
        
        # Info frame
        info_frame = tk.Frame(progress_content, bg=self.colors['bg_primary'])
        info_frame.pack(pady=(0, 10))
        
        tk.Label(info_frame, text=f"Period: {selected_period}",
                bg=self.colors['bg_primary'],
                fg=self.colors['text_secondary'],
                font=('Segoe UI', 9)).pack()
        tk.Label(info_frame, text=f"Range: {start_date} to {end_date}",
                bg=self.colors['bg_primary'],
                fg=self.colors['text_secondary'],
                font=('Segoe UI', 9)).pack()
        tk.Label(info_frame, text=f"Interval: {perf_interval} seconds",
                bg=self.colors['bg_primary'],
                fg=self.colors['text_secondary'],
                font=('Segoe UI', 9)).pack()
        
        # Progress bar
        progress_bar = ttk.Progressbar(progress_content, mode='indeterminate', length=300)
        progress_bar.pack(pady=15)
        progress_bar.start()
        
        # Track success for auto-flow logic
        fetch_successful = False
        
        # Run fetch in separate thread
        def fetch_thread():
            nonlocal fetch_successful
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
                    
                    # Mark as successful for auto-flow
                    fetch_successful = True
                    
                    # AUTO-FLOW INTEGRATION - Mark workflow state
                    self.workflow_state['data_imported'] = True
                    self.workflow_state['last_action'] = 'vcenter_fetch'
                    
                    # Show traditional success message first
                    messagebox.showinfo("Success", 
                                    f"✅ Successfully fetched vCenter data!\n\n"
                                    f"📊 Period: {selected_period}\n"
                                    f"🖥️  Hosts: {len(hosts)}\n"
                                    f"📅 Date Range: {start_date} to {end_date}\n"
                                    f"📈 Total Records: {len(cpu_ready_data)}")
                    
                    # AUTO-FLOW LOGIC - Execute after success message
                    if self.auto_analyze.get():
                        self.show_smart_notification("vCenter data fetched! Auto-analyzing...", 2000)
                        self.root.after(1500, self.auto_calculate_and_switch)
                    else:
                        self.show_action_prompt("vCenter data ready! Analyze now?", 
                                            "🔍 Analyze", 
                                            self.manual_calculate_and_switch)
                else:
                    messagebox.showwarning("No Data", f"No CPU Ready data found for {selected_period}")
                    
            except Exception as e:
                messagebox.showerror("Fetch Error", f"Error fetching data from vCenter:\n{str(e)}")
            finally:
                progress_window.destroy()
        
        threading.Thread(target=fetch_thread, daemon=True).start()
 
    def manual_calculate_and_switch(self):
        """Manual trigger for calculate and switch"""
        self.auto_calculate_and_switch()

    def switch_to_analysis_with_highlight(self):
        """Switch to Analysis tab with visual feedback"""
        # Switch to Analysis tab (tab index 1)
        self.notebook.select(1)
        
        # Show success notification
        self.show_smart_notification("✅ Analysis complete! Showing results...", 2000)
        
        # Highlight the results briefly
        self.root.after(500, self.highlight_results_table)

    def highlight_analysis_tab(self):
        """Add visual indicator to Analysis tab"""
        # Add a temporary visual indicator (could be color change, icon, etc.)
        current_text = self.notebook.tab(1, "text")
        self.notebook.tab(1, text="📊 Analysis ⚡")  # Add lightning bolt
        
        # Reset after 5 seconds
        self.root.after(5000, lambda: self.notebook.tab(1, text=current_text))

    def highlight_results_table(self):
        """Briefly highlight the results table"""
        # Flash the results tree background
        original_style = self.results_tree.cget('style') if hasattr(self.results_tree, 'cget') else None
        
        # Create a highlight effect (you can customize this)
        def flash_table(count=0):
            if count < 3:  # Flash 3 times
                bg_color = self.colors['accent_blue'] if count % 2 == 0 else self.colors['bg_secondary']
                # Apply highlighting logic here
                self.root.after(300, lambda: flash_table(count + 1))
        
        flash_table()

    def show_smart_notification(self, message, duration=3000):
        """Show non-blocking notification"""
        # Create a temporary notification that doesn't block workflow
        notification_frame = tk.Frame(self.root, 
                                    bg=self.colors['accent_blue'], 
                                    relief='solid', 
                                    borderwidth=1)
        
        notification_label = tk.Label(notification_frame,
                                    text=f"ℹ️ {message}",
                                    bg=self.colors['accent_blue'],
                                    fg='white',
                                    font=('Segoe UI', 10, 'bold'),
                                    padx=15, pady=8)
        notification_label.pack()
        
        # Position at top center
        notification_frame.place(relx=0.5, y=10, anchor='n')
        
        # Auto-hide after duration
        self.root.after(duration, notification_frame.destroy)

    def show_action_prompt(self, message, button_text, callback):
        """Show action prompt with single button"""
        prompt_frame = tk.Frame(self.root,
                              bg=self.colors['bg_tertiary'],
                              relief='solid',
                              borderwidth=1)
        
        content_frame = tk.Frame(prompt_frame, bg=self.colors['bg_tertiary'])
        content_frame.pack(padx=15, pady=10)
        
        message_label = tk.Label(content_frame,
                               text=message,
                               bg=self.colors['bg_tertiary'],
                               fg=self.colors['text_primary'],
                               font=('Segoe UI', 10))
        message_label.pack(pady=(0, 10))
        
        button_frame = tk.Frame(content_frame, bg=self.colors['bg_tertiary'])
        button_frame.pack()
        
        action_btn = tk.Button(button_frame,
                             text=button_text,
                             command=lambda: [callback(), prompt_frame.destroy()],
                             bg=self.colors['accent_blue'], fg='white',
                             font=('Segoe UI', 9, 'bold'),
                             relief='flat', borderwidth=0,
                             padx=15, pady=5)
        action_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        dismiss_btn = tk.Button(button_frame,
                              text="Later",
                              command=prompt_frame.destroy,
                              bg=self.colors['bg_secondary'], 
                              fg=self.colors['text_primary'],
                              font=('Segoe UI', 9),
                              relief='flat', borderwidth=0,
                              padx=15, pady=5)
        dismiss_btn.pack(side=tk.LEFT)
        
        # Position and auto-hide
        prompt_frame.place(relx=0.5, rely=0.1, anchor='n')
        self.root.after(10000, prompt_frame.destroy)  # Auto-hide after 10s

    def setup_modern_ui(self):
        """Create modern UI with CONSISTENT geometry management"""
        # Main container - ensure it expands properly
        main_container = tk.Frame(self.root, bg=self.colors['bg_primary'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Header (fixed height) - USE PACK CONSISTENTLY
        self.create_header(main_container)
        
        # Create notebook (expandable) - USE PACK CONSISTENTLY  
        self.create_notebook(main_container)
        
        # Status bar (fixed height) - USE PACK CONSISTENTLY
        self.create_status_bar(main_container)

    def create_header(self, parent):
        """Create modern header section - FIXED widget types"""
        header_frame = tk.Frame(parent, bg=self.colors['bg_primary'])
        header_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Title container
        title_container = tk.Frame(header_frame, bg=self.colors['bg_primary'])
        title_container.pack(fill=tk.X)
        
        # Left side - title
        title_frame = tk.Frame(title_container, bg=self.colors['bg_primary'])
        title_frame.pack(side=tk.LEFT)
        
        title_label = tk.Label(title_frame, text="🖥️ vCenter CPU Ready Analyzer",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 14, 'bold'))
        title_label.pack(anchor=tk.W)
        
        subtitle_label = tk.Label(title_frame, text="Analyse CPU Ready metrics and optimize host consolidation",
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_secondary'],
                                font=('Segoe UI', 11))
        subtitle_label.pack(anchor=tk.W, pady=(2, 0))
        
        # Right side - connection status - USE tk.Label (not ttk.Label)
        status_frame = tk.Frame(title_container, bg=self.colors['bg_primary'])
        status_frame.pack(side=tk.RIGHT)
        
        self.connection_status = tk.Label(status_frame, text="⚫ Disconnected",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['error'],
                                        font=('Segoe UI', 10))
        self.connection_status.pack(side=tk.RIGHT)

    def create_notebook(self, parent):
        """Create tabbed interface with pack geometry"""
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Data Source Tab
        self.create_data_source_tab()
        
        # Analysis Tab
        self.create_analysis_tab()
        
        # Visualisation Tab
        self.create_visualization_tab()
        
        # Host Management Tab
        self.create_host_management_tab()
        
        # Advanced Tab
        self.create_advanced_tab()
        
        # About Tab
        self.create_about_tab()

    def create_status_bar(self, parent):
        """Create modern status bar - FIXED for pack"""
        status_frame = tk.Frame(parent, bg=self.colors['bg_primary'])
        status_frame.pack(fill=tk.X, pady=(15, 0))
        
        # Left side - status label
        status_left = tk.Frame(status_frame, bg=self.colors['bg_primary'])
        status_left.pack(side=tk.LEFT)
        
        self.status_label = tk.Label(status_left, text="Ready",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_secondary'],
                                    font=('Segoe UI', 10))
        self.status_label.pack(side=tk.LEFT)
        
        # Right side - progress bar (hidden by default)
        status_right = tk.Frame(status_frame, bg=self.colors['bg_primary'])
        status_right.pack(side=tk.RIGHT)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(status_right, variable=self.progress_var,
                                        mode='determinate', length=200)
        
        # Store reference for workflow indicators
        self.status_frame = status_frame
        self.add_workflow_status_bar()

    def create_data_source_tab(self):
        """Create data source tab with CONSISTENT pack management"""
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
        file_content.pack(fill=tk.X, padx=10, pady=10)
        
        # File import controls frame
        controls_frame = tk.Frame(file_content, bg=self.colors['bg_primary'])
        controls_frame.pack(fill=tk.X)
        
        import_btn = tk.Button(controls_frame, text="📤 Import CSV/Excel Files",
                            command=self.import_files,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=5)
        import_btn.pack(side=tk.LEFT)
        
        self.file_count_label = tk.Label(controls_frame, text="No files imported",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['text_primary'],
                                        font=('Segoe UI', 10))
        self.file_count_label.pack(side=tk.LEFT, padx=(15, 0), expand=True, anchor=tk.W)
        
        clear_btn = tk.Button(controls_frame, text="🗑️ Clear Files",
                            command=self.clear_files,
                            bg=self.colors['error'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=5)
        clear_btn.pack(side=tk.RIGHT)
        
        # Auto-flow controls section
        self.create_auto_flow_controls(tab_frame)
        
        # vCenter Integration Section
        self.create_complete_vcenter_section(tab_frame)
        
        # Data Preview Section - COMPLETELY PACK-BASED
        preview_section = tk.LabelFrame(tab_frame, text="  👁️ Data Preview  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        preview_section.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Preview content frame
        preview_content = tk.Frame(preview_section, bg=self.colors['bg_primary'])
        preview_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create treeview
        columns = ('Source', 'Hosts', 'Records', 'Date Range')
        self.preview_tree = ttk.Treeview(preview_content, columns=columns, show='headings', height=6)
        
        for col in columns:
            self.preview_tree.heading(col, text=col)
            self.preview_tree.column(col, width=150)
        
        # Scrollbars - USE PACK ONLY
        # Horizontal scrollbar at bottom
        h_scrollbar = ttk.Scrollbar(preview_content, orient=tk.HORIZONTAL, command=self.preview_tree.xview)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Vertical scrollbar at right
        v_scrollbar = ttk.Scrollbar(preview_content, orient=tk.VERTICAL, command=self.preview_tree.yview)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Treeview fills remaining space
        self.preview_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Configure scrollbars
        self.preview_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

    def create_analysis_tab(self):
        """Create analysis tab with PACK management"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="📊 Analysis")
        
        # Configuration Section
        config_section = tk.LabelFrame(tab_frame, text="  ⚙️ Analysis Configuration  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        config_section.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        config_content = tk.Frame(config_section, bg=self.colors['bg_primary'])
        config_content.pack(fill=tk.X, padx=10, pady=10)
        
        # Controls frame - USE PACK
        controls_frame = tk.Frame(config_content, bg=self.colors['bg_primary'])
        controls_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Interval selection
        tk.Label(controls_frame, text="Update Interval:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).pack(side=tk.LEFT)
        
        self.interval_var = tk.StringVar(value="Last Day")
        interval_combo = ttk.Combobox(controls_frame, textvariable=self.interval_var,
                                    values=list(self.intervals.keys()), state="readonly", width=15)
        interval_combo.pack(side=tk.LEFT, padx=(10, 20))
        interval_combo.bind('<<ComboboxSelected>>', self.on_interval_change)
        
        # Calculate button
        calc_btn = tk.Button(controls_frame, text="🔍 Calculate CPU Ready %",
                            command=lambda: self.calculate_cpu_ready(auto_triggered=False),
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=5)
        calc_btn.pack(side=tk.LEFT)
        
        # Threshold controls frame
        threshold_frame = tk.Frame(config_content, bg=self.colors['bg_primary'])
        threshold_frame.pack(fill=tk.X)
        
        # Warning threshold
        tk.Label(threshold_frame, text="Warning Threshold:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).pack(side=tk.LEFT)
        
        self.warning_threshold = tk.DoubleVar(value=5.0)
        warning_spin = tk.Spinbox(threshold_frame, from_=1.0, to=50.0, width=8,
                                textvariable=self.warning_threshold, increment=1.0,
                                bg=self.colors['input_bg'], fg=self.colors['text_primary'],
                                insertbackground=self.colors['text_primary'],
                                relief='flat', borderwidth=1)
        warning_spin.pack(side=tk.LEFT, padx=(5, 2))
        
        tk.Label(threshold_frame, text="%",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary']).pack(side=tk.LEFT, padx=(0, 15))
        
        # Critical threshold
        tk.Label(threshold_frame, text="Critical Threshold:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).pack(side=tk.LEFT)
        
        self.critical_threshold = tk.DoubleVar(value=15.0)
        critical_spin = tk.Spinbox(threshold_frame, from_=5.0, to=100.0, width=8,
                                textvariable=self.critical_threshold, increment=5.0,
                                bg=self.colors['input_bg'], fg=self.colors['text_primary'],
                                insertbackground=self.colors['text_primary'],
                                relief='flat', borderwidth=1)
        critical_spin.pack(side=tk.LEFT, padx=(5, 2))
        
        tk.Label(threshold_frame, text="%",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary']).pack(side=tk.LEFT)
        
        # Results Section
        results_section = tk.LabelFrame(tab_frame, text="  📈 Analysis Results  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        results_section.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        results_content = tk.Frame(results_section, bg=self.colors['bg_primary'])
        results_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Results table with pack layout
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
        
        # Results scrollbar with pack
        results_scrollbar = ttk.Scrollbar(results_content, orient=tk.VERTICAL, command=self.results_tree.yview)
        results_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.results_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.results_tree.configure(yscrollcommand=results_scrollbar.set)

    def add_workflow_status_bar(self):
        """Add workflow progress indicator - FIXED for pack"""
        workflow_frame = tk.Frame(self.status_frame, bg=self.colors['bg_primary'])
        workflow_frame.pack(side=tk.RIGHT, padx=(10, 0))
        
        # Workflow status indicators
        self.workflow_indicators = {
            'import': tk.Label(workflow_frame, text="📁", bg=self.colors['bg_primary']),
            'analyze': tk.Label(workflow_frame, text="📊", bg=self.colors['bg_primary']),
            'visualize': tk.Label(workflow_frame, text="📈", bg=self.colors['bg_primary'])
        }
        
        for key, label in self.workflow_indicators.items():
            label.pack(side=tk.LEFT, padx=2)
            self.update_workflow_indicator(key, 'pending')

    def update_workflow_indicator(self, step, status):
        """Update workflow step indicator"""
        colors = {
            'pending': self.colors['text_muted'],
            'active': self.colors['warning'], 
            'complete': self.colors['success']
        }
        
        if step in self.workflow_indicators:
            self.workflow_indicators[step].config(fg=colors.get(status, self.colors['text_muted'])) 
        
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

    def create_styled_popup_window(self, title, width=1200, height=800):
        """Create a popup window with consistent dark theme styling"""
        popup = tk.Toplevel(self.root)
        popup.title(title)
        popup.geometry(f"{width}x{height}")
        popup.transient(self.root)
        popup.configure(bg=self.colors['bg_primary'])
        
        # Center the window
        popup.geometry(f"+{self.root.winfo_rootx() + 50}+{self.root.winfo_rooty() + 50}")
        
        return popup
    
    def create_styled_frame(self, parent, text=""):
        """Create a styled frame with consistent dark theme"""
        frame = tk.LabelFrame(parent, text=f"  {text}  " if text else "",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['accent_blue'],
                            font=('Segoe UI', 10, 'bold'),
                            borderwidth=1,
                            relief='solid')
        return frame
                        
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
               
    def create_visualization_tab(self):
        """Create Visualisation tab"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="📊 Visualisation")
        
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(0, weight=1)
        
        # Chart container
        chart_section = tk.LabelFrame(tab_frame, text="  📈 CPU Ready Timeline  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        chart_section.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        chart_content = tk.Frame(chart_section, bg=self.colors['bg_primary'])
        chart_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
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
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="🖥️ Hosts")
        
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(1, weight=1)
        
        # Host Selection Section
        selection_section = tk.LabelFrame(tab_frame, text="  🎯 Host Consolidation Analysis  ",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['accent_blue'],
                                        font=('Segoe UI', 10, 'bold'),
                                        borderwidth=1,
                                        relief='solid')
        selection_section.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        selection_content = tk.Frame(selection_section, bg=self.colors['bg_primary'])
        selection_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        selection_content.columnconfigure(0, weight=1)
        
        # Instructions
        tk.Label(selection_content, text="Select hosts to Analyse removal impact:",
                bg=self.colors['bg_primary'],
                fg=self.colors['accent_blue'],
                font=('Segoe UI', 11, 'bold')).pack(anchor=tk.W, pady=(0, 10))
        
        # Host list frame
        list_frame = tk.Frame(selection_content, bg=self.colors['bg_primary'])
        list_frame.pack(fill=tk.BOTH, expand=True)
        list_frame.columnconfigure(0, weight=1)
        
        # Listbox with modern styling
        self.hosts_listbox = self.create_dark_listbox(list_frame, selectmode=tk.MULTIPLE, height=8)
        self.hosts_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        hosts_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.hosts_listbox.yview)
        self.hosts_listbox.configure(yscrollcommand=hosts_scrollbar.set)
        hosts_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Control buttons frame
        button_frame = tk.Frame(list_frame, bg=self.colors['bg_primary'])
        button_frame.grid(row=0, column=2, padx=(15, 0), sticky=(tk.N))
        
        # Control buttons with consistent styling
        select_all_btn = tk.Button(button_frame, text="✓ Select All",
                                command=self.select_all_hosts,
                                bg=self.colors['bg_secondary'], fg=self.colors['text_primary'],
                                font=('Segoe UI', 9, 'bold'),
                                relief='flat', borderwidth=0,
                                padx=10, pady=5)
        select_all_btn.pack(pady=(0, 5), fill=tk.X)
        
        clear_all_btn = tk.Button(button_frame, text="✗ Clear All",
                                command=self.clear_all_hosts,
                                bg=self.colors['bg_secondary'], fg=self.colors['text_primary'],
                                font=('Segoe UI', 9, 'bold'),
                                relief='flat', borderwidth=0,
                                padx=10, pady=5)
        clear_all_btn.pack(pady=(0, 15), fill=tk.X)
        
        analyze_btn = tk.Button(button_frame, text="🔍 Analyse Impact",
                            command=self.analyze_multiple_removal_impact,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=10, pady=5)
        analyze_btn.pack(fill=tk.X)
        
        # Results Section
        results_section = tk.LabelFrame(tab_frame, text="  📊 Impact Analysis Results  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        results_section.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        results_content = tk.Frame(results_section, bg=self.colors['bg_primary'])
        results_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
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
        return self.enhanced_import_files()

    def auto_calculate_and_switch(self):
        """Auto-calculate CPU Ready and optionally switch tabs"""
        try:
            # Run the analysis
            success = self.calculate_cpu_ready(auto_triggered=True)
            
            if success:
                # Mark analysis complete
                self.workflow_state['analysis_complete'] = True
                
                # Smart tab switching
                if self.auto_switch_tabs.get():
                    self.switch_to_analysis_with_highlight()
                else:
                    self.show_smart_notification("Analysis complete! Check the Analysis tab.", 3000)
                    self.highlight_analysis_tab()
                    
        except Exception as e:
            messagebox.showerror("Auto-Analysis Error", f"Error during auto-analysis:\n{str(e)}")
    
    def validate_dataframe(self, df):
        """Enhanced dataframe validation"""
        try:
            # Check if dataframe is valid
            if df is None or df.empty:
                return False
            
            # Look for time columns
            time_cols = [col for col in df.columns if 'time' in col.lower()]
            
            # Look for CPU Ready columns (various possible formats)
            ready_cols = [col for col in df.columns if any([
                'ready for' in col.lower(),
                'cpu ready' in col.lower(),
                'cpuready' in col.lower()
            ])]
            
            # Must have at least one time column and one ready column
            has_time = bool(time_cols)
            has_ready = bool(ready_cols)
            
            print(f"DEBUG: Validation - Time columns: {time_cols}")
            print(f"DEBUG: Validation - Ready columns: {ready_cols}")
            print(f"DEBUG: Validation result: Time={has_time}, Ready={has_ready}")
            
            return has_time and has_ready
            
        except Exception as e:
            print(f"DEBUG: Validation error: {e}")
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

    def detect_interval_from_data(self, df, filename=""):
        """
        Automatically detect the correct interval based on data analysis
        Returns the detected interval key that matches self.intervals
        """
        try:
            # Find time column
            time_col = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['time', 'timestamp', 'date']):
                    time_col = col
                    break
            
            if not time_col:
                print(f"DEBUG: No time column found for interval detection")
                return "Last Day"  # Default fallback
            
            # Convert to datetime and sort
            df_copy = df.copy()
            df_copy[time_col] = pd.to_datetime(df_copy[time_col])
            df_copy = df_copy.sort_values(time_col)
            
            # Calculate time span and interval
            time_span = df_copy[time_col].max() - df_copy[time_col].min()
            num_records = len(df_copy)
            
            # Calculate average interval between samples
            if num_records > 1:
                total_seconds = time_span.total_seconds()
                avg_interval_seconds = total_seconds / (num_records - 1)
            else:
                avg_interval_seconds = 300  # Default 5 minutes
            
            print(f"DEBUG: Interval detection for {filename}")
            print(f"  Time span: {time_span}")
            print(f"  Records: {num_records}")
            print(f"  Average interval: {avg_interval_seconds:.1f} seconds")
            
            # Method 1: Filename-based detection (most reliable)
            filename_lower = filename.lower()
            if any(keyword in filename_lower for keyword in ['real', 'realtime', 'real-time', 'live']):
                detected_interval = "Real-Time"
                print(f"  Filename suggests: Real-Time")
            elif any(keyword in filename_lower for keyword in ['day', 'daily', '24h', '1day']):
                detected_interval = "Last Day"
                print(f"  Filename suggests: Last Day")
            elif any(keyword in filename_lower for keyword in ['week', 'weekly', '7day', '1week']):
                detected_interval = "Last Week"
                print(f"  Filename suggests: Last Week")
            elif any(keyword in filename_lower for keyword in ['month', 'monthly', '30day', '1month']):
                detected_interval = "Last Month"
                print(f"  Filename suggests: Last Month")
            elif any(keyword in filename_lower for keyword in ['year', 'yearly', 'annual', '365day', '1year']):
                detected_interval = "Last Year"
                print(f"  Filename suggests: Last Year")
            else:
                # Method 2: Time span analysis
                days = time_span.days
                hours = time_span.total_seconds() / 3600
                
                if hours <= 1.5:
                    detected_interval = "Real-Time"
                    print(f"  Time span suggests: Real-Time ({hours:.1f} hours)")
                elif days <= 1.5:
                    detected_interval = "Last Day"
                    print(f"  Time span suggests: Last Day ({days:.1f} days)")
                elif days <= 8:
                    detected_interval = "Last Week"
                    print(f"  Time span suggests: Last Week ({days:.1f} days)")
                elif days <= 35:
                    detected_interval = "Last Month"
                    print(f"  Time span suggests: Last Month ({days:.1f} days)")
                else:
                    detected_interval = "Last Year"
                    print(f"  Time span suggests: Last Year ({days:.1f} days)")
            
            # Method 3: Validation against expected intervals
            expected_intervals = {
                "Real-Time": 20,      # 20 seconds
                "Last Day": 300,      # 5 minutes
                "Last Week": 1800,    # 30 minutes
                "Last Month": 7200,   # 2 hours
                "Last Year": 86400    # 1 day
            }
            
            # Check if detected interval makes sense with the data
            expected_seconds = expected_intervals.get(detected_interval, 300)
            interval_ratio = avg_interval_seconds / expected_seconds
            
            print(f"  Expected interval for {detected_interval}: {expected_seconds}s")
            print(f"  Actual vs Expected ratio: {interval_ratio:.2f}")
            
            # If ratio is way off, try to find a better match
            if interval_ratio > 3 or interval_ratio < 0.3:
                print(f"  Interval mismatch detected, searching for better match...")
                
                best_match = "Last Day"
                best_ratio = float('inf')
                
                for interval_name, expected_sec in expected_intervals.items():
                    ratio = abs(1 - (avg_interval_seconds / expected_sec))
                    print(f"    {interval_name}: ratio {ratio:.2f}")
                    if ratio < best_ratio:
                        best_ratio = ratio
                        best_match = interval_name
                
                if best_ratio < 2:  # Accept if within reasonable range
                    detected_interval = best_match
                    print(f"  Auto-corrected to: {detected_interval} (ratio: {best_ratio:.2f})")
            
            # Method 4: Special case handling for your specific files
            # Based on the CSV info you provided
            special_cases = {
                # Real-Time: 180 records, ~1 hour, 20-second intervals
                (180, "realtime"): "Real-Time",
                (180, "real"): "Real-Time",
                
                # Last Day: 288 records, ~24 hours, 5-minute intervals  
                (288, "day"): "Last Day",
                (288, "daily"): "Last Day",
                
                # Last Week: 336 records, ~7 days, 30-minute intervals
                (336, "week"): "Last Week",
                (336, "weekly"): "Last Week",
                
                # Last Month: 360 records, ~30 days, 2-hour intervals
                (360, "month"): "Last Month",
                (360, "monthly"): "Last Month",
            }
            
            for (record_count, keyword), suggested_interval in special_cases.items():
                if (abs(num_records - record_count) <= 20 and 
                    keyword in filename_lower):
                    detected_interval = suggested_interval
                    print(f"  Special case match: {detected_interval} (records: {num_records}, keyword: {keyword})")
                    break
            
            print(f"  FINAL DETECTION: {detected_interval}")
            return detected_interval
            
        except Exception as e:
            print(f"DEBUG: Error in interval detection: {e}")
            return "Last Day"  # Safe fallback

    def enhanced_import_files(self):
        """Enhanced file import with auto-interval detection and auto-flow"""
        # File selection dialog
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
        failed_imports = []
        detected_intervals = []
        
        try:
            for file_path in file_paths:
                try:
                    filename = Path(file_path).name
                    print(f"DEBUG: Processing file: {filename}")
                    
                    # Determine file type and read accordingly
                    if file_path.lower().endswith('.csv'):
                        df = pd.read_csv(file_path)
                    else:
                        df = pd.read_excel(file_path)
                    
                    # Validate the dataframe structure
                    if not self.validate_dataframe(df):
                        failed_imports.append(f"{filename} - Invalid columns")
                        continue
                    
                    # AUTO-DETECT INTERVAL based on data characteristics
                    detected_interval = self.detect_interval_from_data(df, filename)
                    detected_intervals.append((filename, detected_interval))
                    
                    # Add metadata to dataframe
                    df['source_file'] = filename
                    df['detected_interval'] = detected_interval
                    
                    self.data_frames.append(df)
                    successful_imports += 1
                    
                    print(f"DEBUG: Successfully imported {filename} with interval: {detected_interval}")
                    
                except Exception as e:
                    error_msg = f"{Path(file_path).name} - {str(e)}"
                    failed_imports.append(error_msg)
                    print(f"DEBUG: Import error: {error_msg}")
                    continue
            
            # Update UI components
            if successful_imports > 0:
                self.update_file_status()
                self.update_data_preview()
                
                # AUTO-SET INTERVAL based on most common detection or first file
                if detected_intervals:
                    # Use the most commonly detected interval, or first file's interval
                    interval_counts = {}
                    for filename, interval in detected_intervals:
                        interval_counts[interval] = interval_counts.get(interval, 0) + 1
                    
                    # Choose most frequent interval
                    most_common_interval = max(interval_counts, key=interval_counts.get)
                    
                    print(f"DEBUG: Detected intervals: {detected_intervals}")
                    print(f"DEBUG: Auto-setting interval to: {most_common_interval}")
                    
                    # Update the interval dropdown
                    self.interval_var.set(most_common_interval)
                    self.current_interval = most_common_interval
                
                # Mark workflow state
                self.workflow_state['data_imported'] = True
                self.workflow_state['last_action'] = 'import'
                
                # Build result message with interval detection info
                result_msg = f"Successfully imported {successful_imports} files"
                if failed_imports:
                    result_msg += f"\n{len(failed_imports)} files failed to import"
                
                # Add interval detection summary
                if detected_intervals:
                    unique_intervals = set(interval for _, interval in detected_intervals)
                    if len(unique_intervals) == 1:
                        result_msg += f"\nAuto-detected interval: {most_common_interval}"
                    else:
                        result_msg += f"\nDetected intervals: {', '.join(unique_intervals)}"
                        result_msg += f"\nUsing: {most_common_interval}"
                
                # AUTO-FLOW LOGIC
                if hasattr(self, 'auto_analyze') and self.auto_analyze.get():
                    self.show_smart_notification(f"{result_msg}. Auto-analyzing...", 4000)
                    # Small delay for user feedback, then auto-analyze
                    self.root.after(2000, self.auto_calculate_and_switch)
                else:
                    # Show traditional success message with interval info
                    messagebox.showinfo("Import Complete", result_msg)
                    # Show manual prompt
                    if hasattr(self, 'show_action_prompt'):
                        self.show_action_prompt("Data imported with auto-detected intervals! Ready to analyze?", 
                                            "🔍 Analyze Now", 
                                            self.manual_calculate_and_switch)
            else:
                # No successful imports
                error_details = "\n".join(failed_imports) if failed_imports else "Unknown error"
                messagebox.showerror("Import Failed", f"No files were imported successfully.\n\nErrors:\n{error_details}")
                    
        except Exception as e:
            messagebox.showerror("Import Error", f"Unexpected error during import process:\n{str(e)}")
        finally:
            self.hide_progress()

    def update_data_preview(self):
        """Update data preview table with interval detection info"""
        # Clear existing items
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)
        
        if not self.data_frames:
            return
        
        for df in self.data_frames:
            try:
                # Extract info from dataframe
                source = df['source_file'].iloc[0] if 'source_file' in df.columns else "Unknown"
                detected_interval = df['detected_interval'].iloc[0] if 'detected_interval' in df.columns else "Not detected"
                
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
                        start_date = time_data.min().strftime('%Y-%m-%d %H:%M')
                        end_date = time_data.max().strftime('%Y-%m-%d %H:%M')
                        date_range = f"{start_date} to {end_date}"
                    except:
                        date_range = "Invalid dates"
                else:
                    date_range = "No time data"
                
                # Enhanced display with interval info
                display_source = f"{source} [{detected_interval}]"
                
                self.preview_tree.insert('', 'end', values=(
                    display_source, 
                    f"{host_count} hosts", 
                    f"{record_count:,} records", 
                    date_range
                ))
                
            except Exception as e:
                self.preview_tree.insert('', 'end', values=(
                    "Error", str(e), "", ""
                ))

    # Add this method to support interval-specific CPU Ready calculations
    def get_interval_for_data(self, df):
        """Get the detected interval for a specific dataframe"""
        if 'detected_interval' in df.columns:
            return df['detected_interval'].iloc[0]
        return self.current_interval  # Fallback to current selection
       
    def create_complete_vcenter_section(self, parent):
        """Create complete vCenter integration section with improved formatting"""
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
        
        # Connection fields frame - all in one row
        conn_frame = tk.Frame(vcenter_content, bg=self.colors['bg_primary'])
        conn_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Configure grid weights for proper resizing
        conn_frame.columnconfigure(1, weight=2)  # vCenter Server gets more space
        conn_frame.columnconfigure(3, weight=1)  # Username gets normal space
        conn_frame.columnconfigure(5, weight=1)  # Password gets normal space
        
        # vCenter Server
        tk.Label(conn_frame, text="vCenter Server:", 
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 8))
        
        self.vcenter_host = tk.Entry(conn_frame, width=30,
                                    bg=self.colors['input_bg'], 
                                    fg=self.colors['text_primary'],
                                    insertbackground=self.colors['text_primary'],
                                    relief='flat', borderwidth=1,
                                    font=('Segoe UI', 9))
        self.vcenter_host.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 15))
        
        # Username
        tk.Label(conn_frame, text="Username:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=2, sticky=tk.W, padx=(0, 8))
        
        self.vcenter_user = tk.Entry(conn_frame, width=25,
                                    bg=self.colors['input_bg'],
                                    fg=self.colors['text_primary'],
                                    insertbackground=self.colors['text_primary'],
                                    relief='flat', borderwidth=1,
                                    font=('Segoe UI', 9))
        self.vcenter_user.grid(row=0, column=3, sticky=(tk.W, tk.E), padx=(0, 15))
        
        # Password
        tk.Label(conn_frame, text="Password:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=4, sticky=tk.W, padx=(0, 8))
        
        self.vcenter_pass = tk.Entry(conn_frame, show="*", width=20,
                                    bg=self.colors['input_bg'],
                                    fg=self.colors['text_primary'],
                                    insertbackground=self.colors['text_primary'],
                                    relief='flat', borderwidth=1,
                                    font=('Segoe UI', 9))
        self.vcenter_pass.grid(row=0, column=5, sticky=(tk.W, tk.E), padx=(0, 15))
        
        # Connect button
        self.connect_btn = tk.Button(conn_frame, text="🔌 Connect",
                                    command=self.connect_vcenter,
                                    bg=self.colors['accent_blue'], fg='white',
                                    font=('Segoe UI', 9, 'bold'),
                                    relief='flat', borderwidth=0,
                                    padx=15, pady=6)
        self.connect_btn.grid(row=0, column=6, padx=(0, 15))
        
        # Status label
        self.vcenter_status = tk.Label(conn_frame, text="⚫ Disconnected",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['error'],
                                    font=('Segoe UI', 10))
        self.vcenter_status.grid(row=0, column=7, sticky=tk.W)
        
        # Data fetch controls frame - second row
        fetch_frame = tk.Frame(vcenter_content, bg=self.colors['bg_primary'])
        fetch_frame.pack(fill=tk.X, pady=(5, 0))
        
        # Configure grid weights
        fetch_frame.columnconfigure(1, weight=1)
        
        tk.Label(fetch_frame, text="Time Period:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).grid(row=0, column=0, sticky=tk.W, padx=(0, 8))
        
        self.vcenter_period_var = tk.StringVar(value="Last Day")
        period_values = list(self.vcenter_intervals.keys())
        
        # Create a styled dropdown using tk.OptionMenu
        self.period_dropdown = tk.OptionMenu(fetch_frame, self.vcenter_period_var, *period_values)
        self.period_dropdown.config(bg=self.colors['input_bg'],
                                fg=self.colors['text_primary'],
                                activebackground=self.colors['bg_accent'],
                                activeforeground=self.colors['text_primary'],
                                relief='flat', borderwidth=1,
                                font=('Segoe UI', 9),
                                width=12)
        
        # Style the dropdown menu
        dropdown_menu = self.period_dropdown['menu']
        dropdown_menu.config(bg=self.colors['bg_secondary'],
                            fg=self.colors['text_primary'],
                            activebackground=self.colors['accent_blue'],
                            activeforeground='white',
                            relief='flat',
                            borderwidth=1)
        
        self.period_dropdown.grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        
        # Fetch button  
        self.fetch_btn = tk.Button(fetch_frame, text="📊 Fetch Data",
                                command=self.fetch_vcenter_data,
                                bg=self.colors['bg_secondary'], fg=self.colors['text_primary'],
                                font=('Segoe UI', 9, 'bold'),
                                relief='flat', borderwidth=0,
                                padx=15, pady=6,
                                state='disabled')  # Disabled until connected
        self.fetch_btn.grid(row=0, column=2, padx=(0, 15))
        
        # Date range display label
        self.date_range_label = tk.Label(fetch_frame, text="",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['text_secondary'],
                                        font=('Segoe UI', 9, 'italic'))
        self.date_range_label.grid(row=0, column=3, sticky=tk.W)
        
        # Update date range display initially
        self.update_date_range_display()
        
        # Bind the period dropdown change event
        self.vcenter_period_var.trace('w', lambda *args: self.update_date_range_display())

    def update_date_range_display(self, event=None):
        """Update date range label based on selected period"""
        if not hasattr(self, 'date_range_label'):
            return
            
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
        """Handle successful vCenter connection - FIXED widget usage"""
        self.vcenter_status.config(text="🟢 Connected", fg=self.colors['success'])
        
        # FIXED: Use tk.Label config instead of ttk style
        self.connection_status.config(text="🟢 vCenter Connected", 
                                    fg=self.colors['success'])
        
        self.fetch_btn.config(state='normal')
        self.connect_btn.config(text="🔌 Disconnect", 
                            command=self.disconnect_vcenter,
                            state='normal')
        self.hide_progress()
        messagebox.showinfo("Success", f"Connected to vCenter: {vcenter_host}")

    def on_vcenter_connect_failed(self, error_msg):
        """Handle failed vCenter connection - FIXED widget usage"""
        self.vcenter_status.config(text="🔴 Failed", fg=self.colors['error'])
        
        # FIXED: Use tk.Label config instead of ttk style
        self.connection_status.config(text="🔴 Connection Failed", 
                                    fg=self.colors['error'])
        
        self.connect_btn.config(text="🔌 Connect", state='normal')
        self.hide_progress()
        messagebox.showerror("Connection Error", error_msg)

    def disconnect_vcenter(self):
        """Disconnect from vCenter - FIXED widget usage"""
        try:
            if self.vcenter_connection:
                Disconnect(self.vcenter_connection)
                self.vcenter_connection = None
            
            self.vcenter_status.config(text="⚫ Disconnected", fg=self.colors['error'])
            
            # FIXED: Use tk.Label config instead of ttk style
            self.connection_status.config(text="⚫ Disconnected", 
                                        fg=self.colors['error'])
            
            self.fetch_btn.config(state='disabled')
            self.connect_btn.config(text="🔌 Connect", command=self.connect_vcenter)
            
            messagebox.showinfo("Disconnected", "Successfully disconnected from vCenter")
            
        except Exception as e:
            messagebox.showerror("Disconnect Error", f"Error disconnecting: {str(e)}")
          
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

    def show_analysis_summary_dialog(self, processed_hosts, total_records, warnings):
        """Show detailed analysis summary for manual triggers"""
        summary_window = self.create_styled_popup_window("📊 Analysis Summary", 500, 350)
        
        main_frame = tk.Frame(summary_window, bg=self.colors['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = tk.Label(main_frame, 
                            text="📊 Analysis Complete",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 14, 'bold'))
        title_label.pack(pady=(0, 20))
        
        # Summary stats
        stats_frame = tk.Frame(main_frame, bg=self.colors['bg_primary'])
        stats_frame.pack(fill=tk.X, pady=(0, 15))
        
        stats_text = f"""✅ Successfully processed {processed_hosts} hosts
    📊 Total records analyzed: {total_records:,}
    ⚙️ Analysis interval: {self.current_interval}
    📅 Analysis time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        if warnings:
            stats_text += f"\n⚠️ Processing warnings: {len(warnings)}"
        
        stats_label = tk.Label(stats_frame, text=stats_text,
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 10),
                            justify=tk.LEFT)
        stats_label.pack(anchor=tk.W)
        
        # Host summary if available
        if hasattr(self, 'processed_data') and self.processed_data is not None:
            host_summary_frame = tk.Frame(main_frame, bg=self.colors['bg_primary'])
            host_summary_frame.pack(fill=tk.X, pady=(0, 15))
            
            # Calculate quick stats
            critical_hosts = 0
            warning_hosts = 0
            healthy_hosts = 0
            
            for hostname in self.processed_data['Hostname'].unique():
                avg_cpu = self.processed_data[self.processed_data['Hostname'] == hostname]['CPU_Ready_Percent'].mean()
                if avg_cpu >= self.critical_threshold.get():
                    critical_hosts += 1
                elif avg_cpu >= self.warning_threshold.get():
                    warning_hosts += 1
                else:
                    healthy_hosts += 1
            
            host_summary = f"""🔴 Critical hosts: {critical_hosts}
    🟡 Warning hosts: {warning_hosts}
    🟢 Healthy hosts: {healthy_hosts}"""
            
            host_label = tk.Label(host_summary_frame, text=host_summary,
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_primary'],
                                font=('Segoe UI', 10),
                                justify=tk.LEFT)
            host_label.pack(anchor=tk.W)
        
        # Action buttons
        button_frame = tk.Frame(main_frame, bg=self.colors['bg_primary'])
        button_frame.pack(fill=tk.X, pady=(15, 0))
        
        if warnings:
            warnings_btn = tk.Button(button_frame, text="⚠️ View Warnings",
                                command=lambda: [summary_window.destroy(), self.show_processing_warnings(warnings)],
                                bg=self.colors['warning'], fg='white',
                                font=('Segoe UI', 9, 'bold'),
                                relief='flat', borderwidth=0,
                                padx=15, pady=6)
            warnings_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        results_btn = tk.Button(button_frame, text="📊 View Results",
                            command=lambda: [summary_window.destroy(), self.notebook.select(1)],
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=6)
        results_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        close_btn = tk.Button(button_frame, text="✓ Close",
                            command=summary_window.destroy,
                            bg=self.colors['bg_secondary'], fg=self.colors['text_primary'],
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=6)
        close_btn.pack(side=tk.RIGHT)

    def show_processing_warnings(self, warnings):
        """Show processing warnings in a non-blocking way"""
        if not warnings:
            return
        
        # Create warning popup
        warning_window = self.create_styled_popup_window("⚠️ Processing Warnings", 600, 400)
        
        main_frame = tk.Frame(warning_window, bg=self.colors['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = tk.Label(main_frame, 
                            text=f"⚠️ {len(warnings)} Processing Warnings",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['warning'],
                            font=('Segoe UI', 12, 'bold'))
        title_label.pack(pady=(0, 15))
        
        # Warning list
        warning_frame = tk.Frame(main_frame, bg=self.colors['bg_primary'])
        warning_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create scrollable text widget
        warning_text = self.create_dark_text_widget(warning_frame, wrap=tk.WORD, height=12)
        warning_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Scrollbar
        warning_scrollbar = ttk.Scrollbar(warning_frame, orient=tk.VERTICAL, command=warning_text.yview)
        warning_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        warning_text.configure(yscrollcommand=warning_scrollbar.set)
        
        # Populate warnings
        warning_content = "The following issues were encountered during processing:\n\n"
        for i, warning in enumerate(warnings[:20], 1):  # Limit to first 20 warnings
            warning_content += f"{i}. {warning}\n\n"
        
        if len(warnings) > 20:
            warning_content += f"... and {len(warnings) - 20} more warnings.\n"
        
        warning_content += "\nNote: These warnings don't prevent analysis but may indicate data quality issues."
        
        warning_text.insert(1.0, warning_content)
        warning_text.config(state='disabled')  # Make read-only
        
        # Close button
        close_btn = tk.Button(main_frame, text="✓ Continue",
                            command=warning_window.destroy,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=20, pady=8)
        close_btn.pack(pady=(15, 0))

      
    # Analysis Methods
    def on_interval_change(self, event):
        """Handle interval change"""
        self.current_interval = self.interval_var.get()
        if self.processed_data is not None:
            self.calculate_cpu_ready()
    
    def calculate_cpu_ready(self, auto_triggered=False):
        """Calculate CPU Ready percentages with enhanced data processing and validation - COMPLETE UPDATED VERSION"""
        if not self.data_frames:
            if not auto_triggered:
                messagebox.showwarning("No Data", "Please import files or fetch data from vCenter first")
            else:
                self.show_smart_notification("⚠️ No data available for analysis", 3000)
            return False
        
        # Mark analysis as active in workflow
        if hasattr(self, 'update_workflow_indicator'):
            self.update_workflow_indicator('analyze', 'active')
        
        # Show progress with context-aware message
        progress_msg = "Auto-analyzing CPU Ready data..." if auto_triggered else "Calculating CPU Ready percentages..."
        self.show_progress(progress_msg)
        
        try:
            combined_data = []
            processed_hosts = 0
            total_records = 0
            processing_warnings = []
            
            print(f"DEBUG: Starting analysis with {len(self.data_frames)} dataframes")
            
            # Process each dataframe
            for df_index, df in enumerate(self.data_frames):
                print(f"DEBUG: Processing dataframe {df_index + 1}/{len(self.data_frames)} with {len(df)} rows")
                print(f"DEBUG: Columns: {list(df.columns)}")
                
                # Find time and ready columns with improved detection
                time_col = None
                for col in df.columns:
                    if any(keyword in col.lower() for keyword in ['time', 'timestamp', 'date']):
                        time_col = col
                        break
                
                ready_cols = []
                for col in df.columns:
                    if any(keyword in col.lower() for keyword in ['ready for', 'cpu ready', 'cpuready']):
                        ready_cols.append(col)
                
                print(f"DEBUG: Found time column: {time_col}")
                print(f"DEBUG: Found ready columns: {ready_cols}")
                
                if not time_col:
                    warning_msg = f"No time column found in dataframe {df_index + 1}"
                    processing_warnings.append(warning_msg)
                    print(f"DEBUG: {warning_msg}")
                    continue
                    
                if not ready_cols:
                    warning_msg = f"No CPU Ready columns found in dataframe {df_index + 1}"
                    processing_warnings.append(warning_msg)
                    print(f"DEBUG: {warning_msg}")
                    continue
                
                # Process each ready column (each represents a different host)
                for ready_col in ready_cols:
                    print(f"DEBUG: Processing ready column: {ready_col}")
                    
                    # Extract hostname with enhanced logic
                    hostname = self.extract_hostname_from_column(ready_col)
                    print(f"DEBUG: Extracted hostname: {hostname}")
                    
                    # Check for duplicates across all dataframes
                    existing_hostnames = [data['Hostname'].iloc[0] for data in combined_data if len(data) > 0]
                    if hostname in existing_hostnames:
                        print(f"DEBUG: Hostname {hostname} already processed, skipping duplicate")
                        continue
                    
                    # Process data for this host
                    try:
                        # Create subset with required columns
                        required_cols = [time_col, ready_col]
                        if 'source_file' in df.columns:
                            required_cols.append('source_file')
                            subset = df[required_cols].copy()
                            subset.columns = ['Time', 'CPU_Ready_Sum', 'Source_File']
                        else:
                            subset = df[required_cols].copy()
                            subset.columns = ['Time', 'CPU_Ready_Sum']
                            subset['Source_File'] = f'dataframe_{df_index + 1}'
                        
                        subset['Hostname'] = hostname
                        
                        # Data cleaning and validation
                        initial_rows = len(subset)
                        
                        # Remove rows with missing data
                        subset = subset.dropna(subset=['Time', 'CPU_Ready_Sum'])
                        after_dropna = len(subset)
                        
                        # Remove zero values (typically indicates no CPU Ready data)
                        subset = subset[subset['CPU_Ready_Sum'] != 0]
                        valid_rows = len(subset)
                        
                        print(f"DEBUG: Host {hostname}: {initial_rows} initial → {after_dropna} after dropna → {valid_rows} valid rows")
                        
                        if valid_rows == 0:
                            warning_msg = f"No valid CPU Ready data for host {hostname}"
                            processing_warnings.append(warning_msg)
                            print(f"DEBUG: {warning_msg}")
                            continue
                        
                        if valid_rows < initial_rows * 0.5:
                            warning_msg = f"Host {hostname}: Lost {initial_rows - valid_rows} of {initial_rows} rows during cleaning"
                            processing_warnings.append(warning_msg)
                            print(f"DEBUG: {warning_msg}")
                        
                        # Enhanced timestamp processing
                        try:
                            subset['Time'] = self.clean_timestamps(subset['Time'])
                        except Exception as time_error:
                            warning_msg = f"Timestamp processing error for {hostname}: {time_error}"
                            processing_warnings.append(warning_msg)
                            print(f"DEBUG: {warning_msg}")
                            continue
                        
                        # ENHANCED CPU Ready calculation with intelligent data format detection
                        interval_seconds = self.intervals[self.current_interval]
                        print(f"DEBUG: Using interval: {self.current_interval} ({interval_seconds} seconds)")
                        
                        # Analyze sample values to determine data format
                        sample_values = subset['CPU_Ready_Sum'].head(10)
                        avg_sample = sample_values.mean()
                        max_sample = sample_values.max()
                        min_sample = sample_values.min()
                        
                        print(f"DEBUG: Sample statistics - Min: {min_sample:.2f}, Max: {max_sample:.2f}, Avg: {avg_sample:.2f}")
                        
                        # Data format detection and conversion
                        if avg_sample > 10000:
                            # Data is likely in microseconds or very high milliseconds
                            subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 10000)) * 100
                            print(f"DEBUG: Data appears to be in microseconds, applying conversion factor")
                            conversion_applied = "microseconds"
                        elif avg_sample > 1000:
                            # Data is likely in milliseconds (standard vCenter format)
                            subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 1000)) * 100
                            print(f"DEBUG: Data appears to be in milliseconds (standard format)")
                            conversion_applied = "milliseconds"
                        elif avg_sample > 100:
                            # Data might be in centipercent or wrong units
                            subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 100
                            print(f"DEBUG: Data appears to be in centipercent, dividing by 100")
                            conversion_applied = "centipercent"
                        elif avg_sample > 10:
                            # Data might be in permille (per thousand)
                            subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 10
                            print(f"DEBUG: Data appears to be in permille, dividing by 10")
                            conversion_applied = "permille"
                        else:
                            # Data appears to be already in percentage or reasonable range
                            subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum']
                            print(f"DEBUG: Data appears to be in percentage format already")
                            conversion_applied = "percentage"
                        
                        # Post-conversion validation and outlier handling
                        post_avg = subset['CPU_Ready_Percent'].mean()
                        post_max = subset['CPU_Ready_Percent'].max()
                        post_min = subset['CPU_Ready_Percent'].min()
                        
                        print(f"DEBUG: After conversion - Min: {post_min:.2f}%, Max: {post_max:.2f}%, Avg: {post_avg:.2f}%")
                        
                        # Handle extreme outliers (>100% CPU Ready is theoretically impossible but can happen due to measurement issues)
                        extreme_outliers = subset[subset['CPU_Ready_Percent'] > 100]
                        if len(extreme_outliers) > 0:
                            outlier_pct = (len(extreme_outliers) / len(subset)) * 100
                            print(f"DEBUG: Found {len(extreme_outliers)} extreme outliers for {hostname} ({outlier_pct:.1f}% of data)")
                            
                            if outlier_pct > 50:
                                # If more than 50% are outliers, the conversion is probably wrong
                                warning_msg = f"Host {hostname}: {outlier_pct:.1f}% of values >100% - possible incorrect data format detection"
                                processing_warnings.append(warning_msg)
                                
                                # Try alternative conversion
                                if conversion_applied == "milliseconds":
                                    subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 1000
                                    print(f"DEBUG: Retrying with different conversion factor")
                                    post_avg = subset['CPU_Ready_Percent'].mean()
                                    post_max = subset['CPU_Ready_Percent'].max()
                            
                            # Cap values at reasonable maximum (200% to allow for some measurement variance)
                            reasonable_max = 200
                            subset.loc[subset['CPU_Ready_Percent'] > reasonable_max, 'CPU_Ready_Percent'] = reasonable_max
                            
                            capped_count = len(subset[subset['CPU_Ready_Percent'] == reasonable_max])
                            if capped_count > 0:
                                print(f"DEBUG: Capped {capped_count} values at {reasonable_max}%")
                        
                        # Final statistics
                        final_avg = subset['CPU_Ready_Percent'].mean()
                        final_max = subset['CPU_Ready_Percent'].max()
                        final_min = subset['CPU_Ready_Percent'].min()
                        final_std = subset['CPU_Ready_Percent'].std()
                        
                        print(f"DEBUG: Host {hostname} - FINAL stats:")
                        print(f"  Min: {final_min:.2f}%, Max: {final_max:.2f}%, Avg: {final_avg:.2f}%, Std: {final_std:.2f}%")
                        print(f"  Conversion applied: {conversion_applied}")
                        print(f"  Sample values: {subset['CPU_Ready_Percent'].head().tolist()}")
                        
                        # Quality check - warn if data seems unusual
                        if final_avg > 50:
                            warning_msg = f"Host {hostname}: Very high average CPU Ready ({final_avg:.1f}%) - verify data accuracy"
                            processing_warnings.append(warning_msg)
                        elif final_max < 1 and final_avg < 0.1:
                            warning_msg = f"Host {hostname}: Very low CPU Ready values ({final_avg:.3f}%) - may indicate low load or incorrect scaling"
                            processing_warnings.append(warning_msg)
                        
                        combined_data.append(subset)
                        processed_hosts += 1
                        total_records += valid_rows
                        
                    except Exception as host_error:
                        error_msg = f"Error processing host {hostname}: {str(host_error)}"
                        processing_warnings.append(error_msg)
                        print(f"DEBUG: {error_msg}")
                        import traceback
                        traceback.print_exc()
                        continue
            
            # Final validation check
            if not combined_data:
                error_msg = "No valid CPU Ready data found in any imported files"
                detailed_msg = error_msg
                if processing_warnings:
                    detailed_msg += f"\n\nIssues encountered:\n" + "\n".join(processing_warnings[:5])
                
                if auto_triggered:
                    self.show_smart_notification(f"❌ {error_msg}", 4000)
                else:
                    messagebox.showerror("Processing Error", detailed_msg)
                return False
            
            # Combine all processed data
            self.processed_data = pd.concat(combined_data, ignore_index=True)
            
            # Final data summary
            unique_hosts = self.processed_data['Hostname'].unique()
            date_range_start = self.processed_data['Time'].min()
            date_range_end = self.processed_data['Time'].max()
            
            print(f"DEBUG: FINAL ANALYSIS SUMMARY:")
            print(f"  Total records: {len(self.processed_data):,}")
            print(f"  Unique hosts: {len(unique_hosts)}")
            print(f"  Date range: {date_range_start} to {date_range_end}")
            print(f"  Processing warnings: {len(processing_warnings)}")
            
            # Per-host final summary
            for hostname in sorted(unique_hosts):
                host_final = self.processed_data[self.processed_data['Hostname'] == hostname]
                print(f"  {hostname}: {len(host_final)} records, avg {host_final['CPU_Ready_Percent'].mean():.2f}% CPU Ready")
            
            # Mark analysis complete in workflow
            if hasattr(self, 'workflow_state'):
                self.workflow_state['analysis_complete'] = True
                self.workflow_state['last_action'] = 'analyze'
            
            if hasattr(self, 'update_workflow_indicator'):
                self.update_workflow_indicator('analyze', 'complete')
                self.update_workflow_indicator('visualize', 'active')
            
            # Update all displays
            try:
                self.update_results_display()
                self.update_chart()
                self.update_host_list()
                self.apply_thresholds()
                
                # Mark visualization complete
                if hasattr(self, 'update_workflow_indicator'):
                    self.update_workflow_indicator('visualize', 'complete')
            except Exception as display_error:
                print(f"DEBUG: Error updating displays: {display_error}")
                # Continue anyway, data processing was successful
            
            # Enhanced user feedback with summary
            if auto_triggered:
                # Auto-triggered - show smart notification with summary
                summary_msg = f"✅ Analysis complete! {processed_hosts} hosts, {total_records:,} records"
                
                # Add health insights to notification
                critical_hosts = len([h for h in unique_hosts 
                                    if self.processed_data[self.processed_data['Hostname'] == h]['CPU_Ready_Percent'].mean() >= self.critical_threshold.get()])
                warning_hosts = len([h for h in unique_hosts 
                                if self.warning_threshold.get() <= self.processed_data[self.processed_data['Hostname'] == h]['CPU_Ready_Percent'].mean() < self.critical_threshold.get()])
                
                if critical_hosts > 0:
                    summary_msg += f" | ⚠️ {critical_hosts} critical hosts"
                elif warning_hosts > 0:
                    summary_msg += f" | 🟡 {warning_hosts} warning hosts"
                else:
                    summary_msg += " | 🟢 All hosts healthy"
                
                # Show warnings if any
                if processing_warnings:
                    summary_msg += f" | ⚠️ {len(processing_warnings)} warnings"
                
                self.show_smart_notification(summary_msg, 5000)
                
                # If there were significant warnings, offer details
                if len(processing_warnings) > 0:
                    self.root.after(3000, lambda: self.show_processing_warnings(processing_warnings))
                
            else:
                # Manual trigger - use traditional status update with detailed feedback
                status_msg = f"Analysis complete - {processed_hosts} hosts analyzed, {total_records:,} records processed"
                
                if processing_warnings:
                    status_msg += f" ({len(processing_warnings)} warnings)"
                
                self.status_label.config(text=status_msg)
                
                # Show detailed results dialog for manual triggers
                if processing_warnings:
                    self.show_analysis_summary_dialog(processed_hosts, total_records, processing_warnings)
            
            # Auto-switch logic for auto-triggered analysis
            if auto_triggered and hasattr(self, 'auto_switch_tabs') and self.auto_switch_tabs.get():
                self.root.after(2000, self.switch_to_analysis_with_highlight)
            
            # Update analysis timestamp
            if hasattr(self, 'last_analysis_time'):
                self.last_analysis_time = datetime.now()
            
            return True  # Indicate successful analysis
            
        except Exception as e:
            print(f"DEBUG: Full error details: {e}")
            import traceback
            traceback.print_exc()
            
            # Mark analysis as failed
            if hasattr(self, 'update_workflow_indicator'):
                self.update_workflow_indicator('analyze', 'pending')
            
            error_msg = f"Error calculating CPU Ready percentages:\n{str(e)}"
            if auto_triggered:
                self.show_smart_notification("❌ Analysis failed - check data format", 4000)
            else:
                messagebox.showerror("Calculation Error", error_msg)
            
            return False  # Indicate failed analysis
            
        finally:
            self.hide_progress()

    def extract_hostname_from_column(self, ready_col):
        """Extract hostname from CPU Ready column name with enhanced logic"""
        try:
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
                        # Use a cleaner IP-based hostname
                        hostname = f"Host-{full_hostname.replace('.', '-')}"
                        print(f"DEBUG: IP address detected, using hostname: {hostname}")
                    else:
                        # Extract just the first part of the hostname for cleaner display
                        hostname = full_hostname.split('.')[0]
                        print(f"DEBUG: Extracted hostname '{hostname}' from '{full_hostname}'")
                else:
                    hostname = "Unknown-Host"
                    print("DEBUG: Could not extract hostname, using 'Unknown-Host'")
            
            return hostname
            
        except Exception as e:
            print(f"DEBUG: Error extracting hostname from '{ready_col}': {e}")
            return "Error-Host"

    def clean_timestamps(self, timestamp_series):
        """Clean and standardize timestamps with enhanced error handling"""
        def clean_single_timestamp(ts):
            try:
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
            except Exception as e:
                print(f"DEBUG: Timestamp parsing error for '{ts}': {e}")
                # Return current time as fallback
                return pd.to_datetime(datetime.now(), utc=True)
        
        return timestamp_series.apply(clean_single_timestamp)

    def generate_analysis_summary(self):
        """Generate comprehensive analysis summary"""
        if self.processed_data is None:
            return {}
        
        try:
            unique_hosts = self.processed_data['Hostname'].unique()
            total_hosts = len(unique_hosts)
            total_records = len(self.processed_data)
            
            # Calculate health statistics
            critical_count = 0
            warning_count = 0
            healthy_count = 0
            
            host_stats = []
            
            for hostname in unique_hosts:
                host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
                avg_cpu = host_data['CPU_Ready_Percent'].mean()
                max_cpu = host_data['CPU_Ready_Percent'].max()
                
                if avg_cpu >= self.critical_threshold.get():
                    critical_count += 1
                    status = 'critical'
                elif avg_cpu >= self.warning_threshold.get():
                    warning_count += 1
                    status = 'warning'
                else:
                    healthy_count += 1
                    status = 'healthy'
                
                host_stats.append({
                    'hostname': hostname,
                    'avg_cpu': avg_cpu,
                    'max_cpu': max_cpu,
                    'status': status,
                    'records': len(host_data)
                })
            
            # Overall statistics
            overall_avg = self.processed_data['CPU_Ready_Percent'].mean()
            overall_max = self.processed_data['CPU_Ready_Percent'].max()
            
            # Time range
            time_range = {
                'start': self.processed_data['Time'].min(),
                'end': self.processed_data['Time'].max(),
                'duration': self.processed_data['Time'].max() - self.processed_data['Time'].min()
            }
            
            summary = {
                'total_hosts': total_hosts,
                'total_records': total_records,
                'critical_hosts': critical_count,
                'warning_hosts': warning_count,
                'healthy_hosts': healthy_count,
                'overall_avg_cpu': overall_avg,
                'overall_max_cpu': overall_max,
                'time_range': time_range,
                'host_stats': host_stats,
                'analysis_time': datetime.now()
            }
            
            return summary
            
        except Exception as e:
            print(f"DEBUG: Error generating analysis summary: {e}")
            return {}

    def show_analysis_ready_prompt(self, summary):
        """Show analysis completion prompt with summary"""
        if not summary:
            return
        
        # Create a rich prompt with analysis insights
        critical_hosts = summary.get('critical_hosts', 0)
        warning_hosts = summary.get('warning_hosts', 0)
        total_hosts = summary.get('total_hosts', 0)
        
        # Build message with insights
        message_parts = [f"✅ Analysis complete! Processed {total_hosts} hosts"]
        
        if critical_hosts > 0:
            message_parts.append(f"🔴 {critical_hosts} hosts need immediate attention")
        elif warning_hosts > 0:
            message_parts.append(f"🟡 {warning_hosts} hosts need monitoring")
        else:
            message_parts.append("🟢 All hosts performing well")
        
        message = "\n".join(message_parts)
        
        # Show prompt with multiple action options
        self.show_enhanced_action_prompt(
            message=message,
            actions=[
                ("📊 View Analysis", lambda: self.notebook.select(1)),
                ("📈 Show Charts", lambda: self.notebook.select(2)),
                ("🖥️ Host Analysis", lambda: self.notebook.select(3))
            ]
        )

    def show_enhanced_action_prompt(self, message, actions):
        """Show enhanced action prompt with multiple options"""
        prompt_frame = tk.Frame(self.root,
                            bg=self.colors['bg_tertiary'],
                            relief='solid',
                            borderwidth=1)
        
        content_frame = tk.Frame(prompt_frame, bg=self.colors['bg_tertiary'])
        content_frame.pack(padx=20, pady=15)
        
        # Message
        message_label = tk.Label(content_frame,
                            text=message,
                            bg=self.colors['bg_tertiary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 11),
                            justify=tk.LEFT)
        message_label.pack(pady=(0, 15))
        
        # Action buttons
        button_frame = tk.Frame(content_frame, bg=self.colors['bg_tertiary'])
        button_frame.pack()
        
        for i, (text, callback) in enumerate(actions):
            btn = tk.Button(button_frame,
                        text=text,
                        command=lambda cb=callback: [cb(), prompt_frame.destroy()],
                        bg=self.colors['accent_blue'] if i == 0 else self.colors['bg_secondary'],
                        fg='white' if i == 0 else self.colors['text_primary'],
                        font=('Segoe UI', 9, 'bold' if i == 0 else 'normal'),
                        relief='flat', borderwidth=0,
                        padx=12, pady=6)
            btn.pack(side=tk.LEFT, padx=(0, 8) if i < len(actions)-1 else 0)
        
        # Dismiss button
        dismiss_btn = tk.Button(button_frame,
                            text="Later",
                            command=prompt_frame.destroy,
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_secondary'],
                            font=('Segoe UI', 9),
                            relief='flat', borderwidth=0,
                            padx=12, pady=6)
        dismiss_btn.pack(side=tk.LEFT, padx=(15, 0))
        
        # Position and auto-hide
        prompt_frame.place(relx=0.5, rely=0.15, anchor='n')
        self.root.after(15000, prompt_frame.destroy)  # Auto-hide after 15s
    
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
        """Update Visualisation chart with dark theme styling"""
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
        """Analyse impact of removing multiple hosts"""
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
        """Display CPU Ready data as a heat map calendar with consistent styling"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create styled popup window
        heatmap_window = self.create_styled_popup_window("📅 CPU Ready Heat Map Calendar", 1200, 800)
        
        # Main container with dark background
        main_container = tk.Frame(heatmap_window, bg=self.colors['bg_primary'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create matplotlib figure with modern styling
        num_hosts = len(self.processed_data['Hostname'].unique())
        fig_height = max(8, num_hosts * 2)
        fig, axes = plt.subplots(nrows=num_hosts, ncols=1, figsize=(14, fig_height))
        
        # Set dark theme for the figure
        fig.patch.set_facecolor(self.colors['bg_primary'])
        
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
            ax.set_facecolor(self.colors['bg_secondary'])
            
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
            
            # Dark theme styling
            ax.set_title(f'{hostname} - Daily CPU Ready %', 
                        fontsize=12, fontweight='bold', pad=10,
                        color=self.colors['text_primary'])
            ax.set_xlabel('Day of Week', color=self.colors['text_primary'])
            ax.set_ylabel('Week', color=self.colors['text_primary'])
            
            # Style the axes
            ax.tick_params(colors=self.colors['text_secondary'])
            ax.spines['bottom'].set_color(self.colors['border'])
            ax.spines['top'].set_color(self.colors['border'])
            ax.spines['left'].set_color(self.colors['border'])
            ax.spines['right'].set_color(self.colors['border'])
            
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
            
            # Colorbar with dark styling
            cbar = plt.colorbar(im, ax=ax, shrink=0.8)
            cbar.set_label('CPU Ready %', rotation=270, labelpad=15, 
                        color=self.colors['text_primary'])
            cbar.ax.tick_params(colors=self.colors['text_secondary'])
        
        plt.tight_layout(pad=2.0)
        
        # Embed in window with dark background
        canvas_frame = tk.Frame(main_container, bg=self.colors['bg_primary'])
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        canvas = FigureCanvasTkAgg(fig, master=canvas_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        canvas.get_tk_widget().configure(bg=self.colors['bg_primary'])
        
        # Add styled legend frame
        legend_frame = self.create_styled_frame(main_container, "🌡️ Heat Map Legend")
        legend_frame.pack(fill=tk.X, pady=(10, 0))
        
        legend_content = tk.Frame(legend_frame, bg=self.colors['bg_primary'])
        legend_content.pack(fill=tk.X, padx=10, pady=5)
        
        tk.Label(legend_content, 
                text="🟢 Green: Excellent (0-2%) | 🟡 Yellow: Good (2-5%) | 🟠 Orange: Warning (5-15%) | 🔴 Red: Critical (>15%)", 
                bg=self.colors['bg_primary'],
                fg=self.colors['text_secondary'],
                font=('Segoe UI', 10)).pack(anchor=tk.W)
   
    def show_performance_trends(self):
        """Show advanced performance trend analysis with consistent styling - COMPLETE UPDATED VERSION"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create styled trends window
        trends_window = self.create_styled_popup_window("📈 Performance Trends Analysis", 1400, 900)
        
        # Main container
        main_container = tk.Frame(trends_window, bg=self.colors['bg_primary'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create figure with subplots and dark theme
        fig = plt.figure(figsize=(16, 12))
        fig.patch.set_facecolor(self.colors['bg_primary'])
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
        
        # Color palette
        colors = plt.cm.Set3(np.linspace(0, 1, len(self.processed_data['Hostname'].unique())))
        
        # 1. Moving Average Trends
        ax1 = fig.add_subplot(gs[0, :])
        ax1.set_facecolor(self.colors['bg_secondary'])
        
        for i, hostname in enumerate(sorted(self.processed_data['Hostname'].unique())):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            host_data = host_data.sort_values('Time')
            
            # Calculate moving averages with minimum window check
            window_size = min(5, len(host_data))
            if window_size >= 3:
                host_data['MA_5'] = host_data['CPU_Ready_Percent'].rolling(window=window_size, center=True, min_periods=1).mean()
            
            window_size_10 = min(10, len(host_data))
            if window_size_10 >= 3:
                host_data['MA_10'] = host_data['CPU_Ready_Percent'].rolling(window=window_size_10, center=True, min_periods=1).mean()
            
            color = colors[i]
            
            # Plot raw data with transparency
            ax1.plot(host_data['Time'], host_data['CPU_Ready_Percent'], 
                    alpha=0.3, color=color, linewidth=1)
            
            # Plot moving average if available
            if 'MA_10' in host_data.columns:
                ax1.plot(host_data['Time'], host_data['MA_10'], 
                        linewidth=2.5, label=f'{hostname}', color=color)
            else:
                ax1.plot(host_data['Time'], host_data['CPU_Ready_Percent'], 
                        linewidth=2.5, label=f'{hostname}', color=color)
        
        # Add threshold lines
        ax1.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                linestyle='--', alpha=0.8, label='Warning', linewidth=2)
        ax1.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                linestyle='--', alpha=0.8, label='Critical', linewidth=2)
        
        # Style ax1
        ax1.set_title('CPU Ready % Trends with Moving Averages', 
                    fontsize=14, fontweight='bold', color=self.colors['text_primary'], pad=15)
        ax1.set_ylabel('CPU Ready %', color=self.colors['text_primary'], fontsize=12)
        ax1.tick_params(colors=self.colors['text_secondary'])
        ax1.grid(True, alpha=0.3, color=self.colors['border'])
        
        # Format x-axis dates
        ax1.tick_params(axis='x', rotation=45)
        
        # Legend
        legend1 = ax1.legend(bbox_to_anchor=(1.02, 1), loc='upper left',
                            frameon=True, fancybox=True, shadow=False,
                            facecolor=self.colors['bg_tertiary'],
                            edgecolor=self.colors['border'],
                            labelcolor=self.colors['text_primary'])
        
        # Style spines
        for spine in ax1.spines.values():
            spine.set_color(self.colors['border'])
        
        # 2. Distribution Analysis - FIXED matplotlib deprecation
        ax2 = fig.add_subplot(gs[1, 0])
        ax2.set_facecolor(self.colors['bg_secondary'])
        
        all_values = []
        labels = []
        
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            all_values.append(host_data['CPU_Ready_Percent'].values)
            labels.append(hostname[:10])  # Truncate long hostnames for display
        
        # FIXED: Use tick_labels instead of labels for matplotlib 3.9+
        try:
            bp = ax2.boxplot(all_values, tick_labels=labels, patch_artist=True)
        except TypeError:
            # Fallback for older matplotlib versions
            bp = ax2.boxplot(all_values, labels=labels, patch_artist=True)
        
        # Color the boxes
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
            patch.set_edgecolor(self.colors['text_secondary'])
        
        # Style whiskers, caps, medians, fliers
        elements_to_style = ['whiskers', 'caps', 'medians', 'fliers']
        for element in elements_to_style:
            if element in bp:
                for item in bp[element]:
                    item.set_color(self.colors['text_secondary'])
                    if element == 'medians':
                        item.set_linewidth(2)
        
        # Add threshold lines
        ax2.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                linestyle='--', alpha=0.8, linewidth=2)
        ax2.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                linestyle='--', alpha=0.8, linewidth=2)
        
        ax2.set_title('CPU Ready % Distribution by Host', 
                    fontsize=12, fontweight='bold', color=self.colors['text_primary'], pad=15)
        ax2.set_ylabel('CPU Ready %', color=self.colors['text_primary'])
        ax2.tick_params(axis='x', rotation=45, colors=self.colors['text_secondary'])
        ax2.tick_params(axis='y', colors=self.colors['text_secondary'])
        ax2.grid(True, alpha=0.3, color=self.colors['border'], axis='y')
        
        for spine in ax2.spines.values():
            spine.set_color(self.colors['border'])
        
        # 3. Peak Analysis
        ax3 = fig.add_subplot(gs[1, 1])
        ax3.set_facecolor(self.colors['bg_secondary'])
        
        peak_data = []
        peak_hosts = []
        peak_times = []
        
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            # Get top 3 peaks for each host
            top_peaks = host_data.nlargest(3, 'CPU_Ready_Percent')
            
            for _, peak_row in top_peaks.iterrows():
                peak_data.append(peak_row['CPU_Ready_Percent'])
                peak_hosts.append(hostname)
                peak_times.append(peak_row['Time'])
        
        # Create scatter plot
        if peak_data:
            scatter_colors = [colors[list(sorted(self.processed_data['Hostname'].unique())).index(host)] 
                            for host in peak_hosts]
            
            scatter = ax3.scatter(range(len(peak_data)), peak_data, 
                                c=scatter_colors, s=80, alpha=0.7, 
                                edgecolors=self.colors['border'], linewidth=1)
            
            # Add threshold lines
            ax3.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                    linestyle='--', alpha=0.8, label='Warning', linewidth=2)
            ax3.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                    linestyle='--', alpha=0.8, label='Critical', linewidth=2)
            
            ax3.set_title('Performance Peaks Analysis (Top 3 per Host)', 
                        fontsize=12, fontweight='bold', color=self.colors['text_primary'], pad=15)
            ax3.set_ylabel('CPU Ready %', color=self.colors['text_primary'])
            ax3.set_xlabel('Peak Instance', color=self.colors['text_primary'])
            ax3.tick_params(colors=self.colors['text_secondary'])
            ax3.grid(True, alpha=0.3, color=self.colors['border'])
            
            # Add legend for thresholds only
            legend3 = ax3.legend(loc='upper left',
                                frameon=True, fancybox=True, shadow=False,
                                facecolor=self.colors['bg_tertiary'],
                                edgecolor=self.colors['border'],
                                labelcolor=self.colors['text_primary'])
        else:
            ax3.text(0.5, 0.5, 'No peak data available', 
                    ha='center', va='center', transform=ax3.transAxes,
                    color=self.colors['text_primary'], fontsize=12)
        
        for spine in ax3.spines.values():
            spine.set_color(self.colors['border'])
        
        # 4. Time Pattern Analysis
        ax4 = fig.add_subplot(gs[2, :])
        ax4.set_facecolor(self.colors['bg_secondary'])
        
        # Check if we have enough data for hourly analysis
        if len(self.processed_data) > 24:
            try:
                # Add hour column
                self.processed_data['Hour'] = self.processed_data['Time'].dt.hour
                
                # Group by hour and hostname
                hourly_stats = self.processed_data.groupby(['Hour', 'Hostname'])['CPU_Ready_Percent'].agg(['mean', 'std']).reset_index()
                
                # Plot for each hostname
                for i, hostname in enumerate(sorted(self.processed_data['Hostname'].unique())):
                    host_hourly = hourly_stats[hourly_stats['Hostname'] == hostname]
                    
                    if len(host_hourly) > 0:
                        color = colors[i]
                        
                        # Plot mean line
                        ax4.plot(host_hourly['Hour'], host_hourly['mean'], 
                                marker='o', linewidth=2.5, markersize=4,
                                label=hostname, color=color)
                        
                        # Add standard deviation as fill_between (if std exists and is not NaN)
                        if 'std' in host_hourly.columns and not host_hourly['std'].isna().all():
                            std_values = host_hourly['std'].fillna(0)
                            ax4.fill_between(host_hourly['Hour'], 
                                            host_hourly['mean'] - std_values,
                                            host_hourly['mean'] + std_values,
                                            alpha=0.2, color=color)
                
                ax4.set_title('Average CPU Ready % by Hour of Day (with Standard Deviation)', 
                            fontsize=12, fontweight='bold', color=self.colors['text_primary'], pad=15)
                ax4.set_xlabel('Hour of Day (0-23)', color=self.colors['text_primary'])
                ax4.set_ylabel('CPU Ready %', color=self.colors['text_primary'])
                ax4.tick_params(colors=self.colors['text_secondary'])
                ax4.grid(True, alpha=0.3, color=self.colors['border'])
                ax4.set_xticks(range(0, 24, 2))
                ax4.set_xlim(-0.5, 23.5)
                
                # Add threshold reference lines
                ax4.axhline(y=self.warning_threshold.get(), color='#f59e0b', 
                        linestyle='--', alpha=0.6, linewidth=1)
                ax4.axhline(y=self.critical_threshold.get(), color='#ef4444', 
                        linestyle='--', alpha=0.6, linewidth=1)
                
                legend4 = ax4.legend(loc='upper left',
                                    frameon=True, fancybox=True, shadow=False,
                                    facecolor=self.colors['bg_tertiary'],
                                    edgecolor=self.colors['border'],
                                    labelcolor=self.colors['text_primary'])
                
            except Exception as e:
                print(f"DEBUG: Error in hourly analysis: {e}")
                ax4.text(0.5, 0.5, f'Error creating hourly analysis:\n{str(e)}', 
                        ha='center', va='center', transform=ax4.transAxes,
                        color=self.colors['text_primary'], fontsize=12,
                        bbox=dict(boxstyle='round', facecolor=self.colors['bg_tertiary'], 
                                edgecolor=self.colors['border'], alpha=0.8))
        else:
            ax4.text(0.5, 0.5, '📊 Insufficient data for hourly pattern analysis\n\n'
                            f'Current data points: {len(self.processed_data)}\n'
                            'Need at least 24 data points to show hourly patterns', 
                    ha='center', va='center', transform=ax4.transAxes, 
                    fontsize=12, color=self.colors['text_primary'],
                    bbox=dict(boxstyle='round', facecolor=self.colors['bg_tertiary'], 
                            edgecolor=self.colors['border'], alpha=0.8))
            ax4.set_title('Hourly Pattern Analysis', 
                        fontsize=12, fontweight='bold', color=self.colors['text_primary'], pad=15)
        
        for spine in ax4.spines.values():
            spine.set_color(self.colors['border'])
        
        # Adjust layout to prevent overlapping
        plt.tight_layout(pad=2.0)
        
        # Embed in window with scrollable canvas if needed
        canvas_frame = tk.Frame(main_container, bg=self.colors['bg_primary'])
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        canvas = FigureCanvasTkAgg(fig, master=canvas_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        canvas.get_tk_widget().configure(bg=self.colors['bg_primary'])
        
        # Add summary statistics at the bottom
        stats_frame = self.create_styled_frame(main_container, "📊 Trend Analysis Summary")
        stats_frame.pack(fill=tk.X, pady=(10, 0))
        
        stats_content = tk.Frame(stats_frame, bg=self.colors['bg_primary'])
        stats_content.pack(fill=tk.X, padx=10, pady=10)
        
        try:
            # Calculate summary statistics
            overall_mean = self.processed_data['CPU_Ready_Percent'].mean()
            overall_std = self.processed_data['CPU_Ready_Percent'].std()
            overall_max = self.processed_data['CPU_Ready_Percent'].max()
            
            # Calculate volatility (coefficient of variation)
            volatility = (overall_std / overall_mean) * 100 if overall_mean > 0 else 0
            
            summary_text = (f"📈 Overall Average: {overall_mean:.2f}% | "
                        f"📊 Standard Deviation: {overall_std:.2f}% | "
                        f"⚡ Peak Value: {overall_max:.2f}% | "
                        f"📉 Volatility: {volatility:.1f}%")
            
            summary_label = tk.Label(stats_content, text=summary_text,
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_primary'],
                                    font=('Segoe UI', 10))
            summary_label.pack(pady=5)
            
        except Exception as e:
            print(f"DEBUG: Error calculating summary stats: {e}")

    def show_host_comparison(self):
        """Show detailed host-by-host comparison with consistent styling"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create styled comparison window
        comparison_window = self.create_styled_popup_window("🎯 Host Performance Comparison", 1200, 700)
        
        # Main container with dark background
        main_container = tk.Frame(comparison_window, bg=self.colors['bg_primary'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Title with dark theme
        title_label = tk.Label(main_container, 
                            text="📊 Comprehensive Host Performance Analysis",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 14, 'bold'))
        title_label.pack(pady=(0, 15))
        
        # Create comparison table frame
        table_frame = self.create_styled_frame(main_container, "Performance Comparison")
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        table_content = tk.Frame(table_frame, bg=self.colors['bg_primary'])
        table_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create comparison table with modern styling
        columns = ('Rank', 'Host', 'Avg %', 'Max %', 'Min %', 'Std Dev', 'Health Score', 'Status', 'Recommendation')
        tree = ttk.Treeview(table_content, columns=columns, show='headings', height=15)
        
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
        v_scrollbar = ttk.Scrollbar(table_content, orient=tk.VERTICAL, command=tree.yview)
        h_scrollbar = ttk.Scrollbar(table_content, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        table_content.columnconfigure(0, weight=1)
        table_content.rowconfigure(0, weight=1)
        
        # Summary statistics with styled frame
        summary_frame = self.create_styled_frame(main_container, "📈 Summary Statistics")
        summary_frame.pack(fill=tk.X, pady=(10, 0))
        
        summary_content = tk.Frame(summary_frame, bg=self.colors['bg_primary'])
        summary_content.pack(fill=tk.X, padx=10, pady=10)
        
        critical_count = len([d for d in comparison_data if d['avg'] >= self.critical_threshold.get()])
        warning_count = len([d for d in comparison_data if self.warning_threshold.get() <= d['avg'] < self.critical_threshold.get()])
        healthy_count = len([d for d in comparison_data if d['avg'] < self.warning_threshold.get()])
        
        summary_text = (f"🔴 Critical Hosts: {critical_count} | "
                    f"🟡 Warning Hosts: {warning_count} | "
                    f"🟢 Healthy Hosts: {healthy_count} | "
                    f"📊 Total Hosts: {len(comparison_data)}")
        
        summary_label = tk.Label(summary_content, text=summary_text,
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_primary'],
                                font=('Segoe UI', 10))
        summary_label.pack()
        
        # Export button with consistent styling
        export_btn = tk.Button(main_container, text="📋 Export Comparison Report",
                            command=lambda: self.export_comparison_report(comparison_data),
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=15, pady=8)
        export_btn.pack(pady=(10, 0))
  
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

    def create_about_tab(self):
        """Create scrollable about tab with application and developer information"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="ℹ️ About")
        
        # Configure the main frame to expand properly
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(0, weight=1)
        
        # Create canvas and scrollbar for scrolling
        canvas = tk.Canvas(tab_frame, bg=self.colors['bg_primary'], highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=self.colors['bg_primary'])
        
        # Configure scrolling
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Main content container
        main_container = tk.Frame(scrollable_frame, bg=self.colors['bg_primary'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        main_container.columnconfigure(0, weight=1)
        
        # Header Section
        header_frame = tk.Frame(main_container, bg=self.colors['bg_primary'])
        header_frame.pack(fill=tk.X, pady=(0, 20))
        
        # App icon and title
        title_label = tk.Label(header_frame, 
                            text="🖥️ vCenter CPU Ready Analyzer",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 20, 'bold'))
        title_label.pack(pady=(0, 5))
        
        version_label = tk.Label(header_frame,
                            text="Version 2.0",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_secondary'],
                            font=('Segoe UI', 12))
        version_label.pack(pady=(0, 10))
        
        description_label = tk.Label(header_frame,
                                    text="Advanced CPU Ready metrics analysis and host consolidation optimization tool",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_secondary'],
                                    font=('Segoe UI', 11),
                                    wraplength=600)
        description_label.pack()
        
        # Developer Information Card
        dev_section = tk.LabelFrame(main_container, text="  👨‍💻 Development Team  ",
                                bg=self.colors['bg_primary'],
                                fg=self.colors['accent_blue'],
                                font=('Segoe UI', 12, 'bold'),
                                borderwidth=1,
                                relief='solid')
        dev_section.pack(fill=tk.X, pady=(0, 15))
        
        dev_content = tk.Frame(dev_section, bg=self.colors['bg_primary'])
        dev_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Developer details
        chief_label = tk.Label(dev_content,
                            text="Chief Architect & Developer",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['accent_blue'],
                            font=('Segoe UI', 11, 'bold'))
        chief_label.pack(anchor=tk.W, pady=(0, 5))
        
        name_label = tk.Label(dev_content,
                            text="Joshua Fourie",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 14, 'bold'))
        name_label.pack(anchor=tk.W, pady=(0, 10))
        
        # Contact info
        contact_frame = tk.Frame(dev_content, bg=self.colors['bg_primary'])
        contact_frame.pack(fill=tk.X, pady=(0, 15))
        
        email_icon = tk.Label(contact_frame,
                            text="📧",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 12))
        email_icon.pack(side=tk.LEFT, padx=(0, 8))
        
        email_label = tk.Label(contact_frame,
                            text="joshua.fourie@outlook.com",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['accent_blue'],
                            font=('Segoe UI', 11),
                            cursor='hand2')
        email_label.pack(side=tk.LEFT)
        
        # Make email clickable
        def open_email(event):
            import webbrowser
            webbrowser.open(f"mailto:joshua.fourie@outlook.com")
        
        email_label.bind("<Button-1>", open_email)
        email_label.bind("<Enter>", lambda e: email_label.config(fg=self.colors['accent_hover']))
        email_label.bind("<Leave>", lambda e: email_label.config(fg=self.colors['accent_blue']))
        
        # Expertise
        expertise_label = tk.Label(dev_content,
                                text="Expertise:",
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_secondary'],
                                font=('Segoe UI', 10, 'bold'))
        expertise_label.pack(anchor=tk.W, pady=(10, 5))
        
        expertise_text = """• VMware vCenter & vSphere Infrastructure
    • Performance Analytics & Monitoring
    • Host Consolidation & Capacity Planning
    • Python Development & Data Analysis
    • Enterprise Virtualization Solutions"""
        
        expertise_content = tk.Label(dev_content,
                                    text=expertise_text,
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_primary'],
                                    font=('Segoe UI', 10),
                                    justify=tk.LEFT)
        expertise_content.pack(anchor=tk.W)
        
        # Application Features Card
        features_section = tk.LabelFrame(main_container, text="  ⭐ Key Features  ",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['accent_blue'],
                                        font=('Segoe UI', 12, 'bold'),
                                        borderwidth=1,
                                        relief='solid')
        features_section.pack(fill=tk.X, pady=(0, 15))
        
        features_content = tk.Frame(features_section, bg=self.colors['bg_primary'])
        features_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        features_text = """🔗 Direct vCenter Integration
    • Live performance data fetching
    • Real-time and historical analysis
    • Support for multiple time periods

    📊 Advanced Analytics
    • CPU Ready percentage calculations
    • Health scoring algorithms
    • Performance trend analysis
    • Statistical distribution analysis

    📈 Visual Reporting
    • Interactive timeline charts
    • Heat map calendar views
    • Host comparison matrices
    • Export capabilities

    🎯 Consolidation Analysis
    • Host removal impact assessment
    • Workload redistribution modeling
    • Risk analysis and recommendations
    • Infrastructure optimization

    🏥 Health Monitoring
    • Automated threshold detection
    • Performance alerting
    • Comprehensive dashboards
    • Executive reporting"""
        
        features_label = tk.Label(features_content,
                                text=features_text,
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_primary'],
                                font=('Segoe UI', 10),
                                justify=tk.LEFT)
        features_label.pack(anchor=tk.W)
        
        # Technology Stack Card
        tech_section = tk.LabelFrame(main_container, text="  🛠️ Technology Stack  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 12, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        tech_section.pack(fill=tk.X, pady=(0, 15))
        
        tech_content = tk.Frame(tech_section, bg=self.colors['bg_primary'])
        tech_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        tech_text = """🐍 Python 3.x
    📊 Pandas & NumPy (Data Analysis)
    📈 Matplotlib & Seaborn (Visualisation)
    🖥️ Tkinter (Modern UI Framework)
    🔗 PyVmomi (vCenter API Integration)
    📡 Requests (HTTP Communications)
    🎨 Custom Dark Theme Implementation"""
        
        tech_label = tk.Label(tech_content,
                            text=tech_text,
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 10),
                            justify=tk.LEFT)
        tech_label.pack(anchor=tk.W)
        
        # License & Copyright Card
        license_section = tk.LabelFrame(main_container, text="  📄 License & Copyright  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 12, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        license_section.pack(fill=tk.X, pady=(0, 15))
        
        license_content = tk.Frame(license_section, bg=self.colors['bg_primary'])
        license_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        copyright_text = f"""© {datetime.now().year} Joshua Fourie
    All Rights Reserved

    This application is proprietary software
    developed for enterprise infrastructure
    analysis and optimization.

    Built with ❤️ for the VMware community"""
        
        copyright_label = tk.Label(license_content,
                                text=copyright_text,
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_primary'],
                                font=('Segoe UI', 10),
                                justify=tk.CENTER)
        copyright_label.pack(expand=True)
        
        # Footer
        footer_frame = tk.Frame(main_container, bg=self.colors['bg_primary'])
        footer_frame.pack(fill=tk.X, pady=(20, 0))
        
        footer_label = tk.Label(footer_frame,
                            text="🚀 Empowering infrastructure teams with intelligent performance insights",
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_secondary'],
                            font=('Segoe UI', 11, 'italic'),
                            wraplength=600)
        footer_label.pack()
        
        # Mouse wheel scrolling support
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        # Bind mouse wheel to canvas and all child widgets
        def bind_to_mousewheel(widget):
            widget.bind("<MouseWheel>", _on_mousewheel)
            for child in widget.winfo_children():
                bind_to_mousewheel(child)
        
        bind_to_mousewheel(scrollable_frame)
        canvas.bind("<MouseWheel>", _on_mousewheel)
        
        # Update scroll region when window is resized
        def configure_scroll_region(event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))
            # Update the scrollable frame width to match canvas
            canvas_width = canvas.winfo_width()
            canvas.itemconfig(canvas.find_all()[0], width=canvas_width)
        
        canvas.bind('<Configure>', configure_scroll_region)

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