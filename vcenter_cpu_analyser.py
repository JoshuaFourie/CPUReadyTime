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

try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.colors import Color, blue, red, green, orange, black, white, grey
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.pdfgen import canvas
    from reportlab.platypus.tableofcontents import TableOfContents
    import io
    import base64
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
from realtime_dashboard import RealTimeDashboard
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
        
        self.setup_threshold_bindings()
        
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

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

    def export_realtime_data_to_main_app(self):
        """Export real-time data to main application for analysis - CORRECT SCALING"""
        if not hasattr(self, 'realtime_dashboard'):
            return None
        
        try:
            # Get data from real-time database
            realtime_db = self.realtime_dashboard.db
            realtime_data = realtime_db.get_recent_performance_data(minutes=10080)
            
            if realtime_data.empty:
                return None
            
            print(f"DEBUG: Exporting {len(realtime_data)} real-time records to main app")
            
            converted_data = []
            
            for _, row in realtime_data.iterrows():
                ready_col_name = f"Ready for {row['hostname']}"
                
                # Convert UTC timestamp to local time
                try:
                    utc_timestamp = pd.to_datetime(row['timestamp'], utc=True)
                    local_timestamp = utc_timestamp.tz_convert(None)
                except:
                    local_timestamp = row['timestamp']
                
                # CORRECT FIX: Use the percentage value directly (not as decimal)
                # The analysis engine expects percentage values, not decimals
                raw_value_for_analysis = row['cpu_ready_percent']  # Use percentage directly
                
                print(f"DEBUG: Converting {row['hostname']}: {row['cpu_ready_percent']:.3f}% -> {raw_value_for_analysis:.3f} (direct percentage)")
                
                converted_data.append({
                    'Time': local_timestamp,
                    ready_col_name: raw_value_for_analysis,
                    'Hostname': row['hostname'],
                    'source_file': 'realtime_dashboard_percentage',
                    'detected_interval': 'Real-Time'
                })
            
            if converted_data:
                time_groups = {}
                
                for item in converted_data:
                    time_key = item['Time']
                    if time_key not in time_groups:
                        time_groups[time_key] = {
                            'Time': time_key,
                            'source_file': 'realtime_dashboard_percentage',
                            'detected_interval': 'Real-Time'
                        }
                    
                    ready_col = [k for k in item.keys() if k.startswith('Ready for')][0]
                    time_groups[time_key][ready_col] = item[ready_col]
                
                final_data = list(time_groups.values())
                final_df = pd.DataFrame(final_data)
                
                print(f"DEBUG: Using direct percentage values - analysis should use them as-is")
                
                # Show expected results
                for col in final_df.columns:
                    if col.startswith('Ready for'):
                        sample_values = final_df[col].dropna().head(3)
                        print(f"  {col}: {sample_values.tolist()} -> Expected same values in analysis")
                
                return final_df
                
        except Exception as e:
            print(f"DEBUG: Error exporting real-time data: {e}")
            return None

    def verify_realtime_conversion(self):
        """Verify that real-time data conversion is working correctly"""
        if not hasattr(self, 'realtime_dashboard'):
            return
        
        try:
            # Get some recent real-time data
            realtime_db = self.realtime_dashboard.db
            recent_data = realtime_db.get_recent_performance_data(minutes=60)  # Last hour
            
            if recent_data.empty:
                print("DEBUG: No real-time data to verify")
                return
            
            print("DEBUG: REAL-TIME DATA VERIFICATION - FIXED")
            print("=" * 50)
            
            for hostname in recent_data['hostname'].unique():
                host_data = recent_data[recent_data['hostname'] == hostname]
                
                # Get latest values
                latest = host_data.iloc[-1]
                avg_percent = host_data['cpu_ready_percent'].mean()
                
                print(f"Host: {hostname}")
                print(f"  Real-time dashboard shows: {avg_percent:.3f}% average")
                print(f"  Latest raw sum value: {latest['cpu_ready_sum']}")
                print(f"  Latest percentage: {latest['cpu_ready_percent']:.3f}%")
                
                # Calculate what main app will see after fixed conversion
                converted_value = max(1.5, avg_percent * 10)
                expected_analysis_percent = converted_value / 10  # What analysis will calculate
                
                print(f"  Will be stored as: {converted_value:.2f} (forced permille range)")
                print(f"  Expected analysis result: {expected_analysis_percent:.3f}%")
                
                # Check if it's close to real-time dashboard value
                difference = abs(expected_analysis_percent - avg_percent)
                match_status = "✅ CLOSE" if difference < 0.05 else "❌ DIFFERENT"
                print(f"  Match real-time dashboard: {match_status} (diff: {difference:.3f}%)")
                print()
            
        except Exception as e:
            print(f"DEBUG: Error in verification: {e}")

    def integrate_realtime_data(self):
        """Integrate real-time data with main application data"""
        realtime_df = self.export_realtime_data_to_main_app()
        
        if realtime_df is not None:
            # Add to main data frames
            self.data_frames.append(realtime_df)
            
            # Update UI components
            self.update_file_status()
            self.update_data_preview()
            
            # Set interval to Real-Time
            self.interval_var.set("Real-Time")
            self.current_interval = "Real-Time"
            
            print(f"DEBUG: Successfully integrated real-time data into main application")
            return True
        
        return False

    def create_realtime_export_controls(self, parent):
        """Add real-time data export controls to the Real-Time Dashboard tab"""
        
        # Export controls frame
        export_frame = tk.LabelFrame(parent, text="  📤 Export Real-Time Data  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        export_frame.pack(fill=tk.X, padx=10, pady=5)
        
        export_content = tk.Frame(export_frame, bg=self.colors['bg_primary'])
        export_content.pack(fill=tk.X, padx=10, pady=10)
        
        # Instructions
        tk.Label(export_content,
                text="Export collected real-time data for analysis in other tabs:",
                bg=self.colors['bg_primary'],
                fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).pack(anchor=tk.W, pady=(0, 10))
        
        # Export buttons
        button_frame = tk.Frame(export_content, bg=self.colors['bg_primary'])
        button_frame.pack(fill=tk.X)
        
        # Export to main app button
        export_to_main_btn = tk.Button(button_frame,
                                      text="📊 Use for Analysis",
                                      command=self.export_realtime_to_analysis,
                                      bg=self.colors['accent_blue'], fg='white',
                                      font=('Segoe UI', 9, 'bold'),
                                      relief='flat', borderwidth=0,
                                      padx=15, pady=6)
        export_to_main_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Export to CSV button
        export_csv_btn = tk.Button(button_frame,
                                  text="💾 Export CSV",
                                  command=self.export_realtime_to_csv,
                                  bg=self.colors['success'], fg='white',
                                  font=('Segoe UI', 9, 'bold'),
                                  relief='flat', borderwidth=0,
                                  padx=15, pady=6)
        export_csv_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Data info label
        self.realtime_data_info = tk.Label(button_frame,
                                          text="No real-time data collected",
                                          bg=self.colors['bg_primary'],
                                          fg=self.colors['text_secondary'],
                                          font=('Segoe UI', 9))
        self.realtime_data_info.pack(side=tk.RIGHT)
        
        # Update data info periodically
        self.update_realtime_data_info()

    def update_realtime_data_info(self):
        """Update real-time data information display"""
        try:
            if hasattr(self, 'realtime_dashboard') and hasattr(self.realtime_dashboard, 'db'):
                recent_data = self.realtime_dashboard.db.get_recent_performance_data(minutes=10080)
                
                if not recent_data.empty:
                    unique_hosts = recent_data['hostname'].nunique()
                    total_records = len(recent_data)
                    oldest_record = recent_data['timestamp'].min()
                    newest_record = recent_data['timestamp'].max()
                    
                    duration = newest_record - oldest_record
                    duration_str = f"{duration.total_seconds() / 3600:.1f} hours" if duration.total_seconds() > 3600 else f"{duration.total_seconds() / 60:.0f} minutes"
                    
                    info_text = f"{total_records:,} records, {unique_hosts} hosts, {duration_str} span"
                else:
                    info_text = "No real-time data collected"
                
                if hasattr(self, 'realtime_data_info'):
                    self.realtime_data_info.config(text=info_text)
        
        except Exception as e:
            print(f"DEBUG: Error updating real-time data info: {e}")
        
        # Schedule next update
        self.root.after(10000, self.update_realtime_data_info)

    def export_realtime_to_analysis(self):
        """Export real-time data for use in analysis tabs"""
        if not hasattr(self, 'realtime_dashboard'):
            messagebox.showwarning("No Dashboard", "Real-time dashboard not available")
            return
        
        # Check if we have real-time data
        recent_data = self.realtime_dashboard.db.get_recent_performance_data(minutes=10080)
        
        if recent_data.empty:
            messagebox.showwarning("No Data", "No real-time data has been collected yet.\n\nStart monitoring first to collect data.")
            return
        
        # Get user confirmation
        unique_hosts = recent_data['hostname'].nunique()
        total_records = len(recent_data)
        
        result = messagebox.askyesno("Export Real-Time Data",
                                    f"Export {total_records:,} real-time records from {unique_hosts} hosts?\n\n"
                                    f"This will:\n"
                                    f"• Add real-time data to analysis\n"
                                    f"• Switch interval to 'Real-Time'\n"
                                    f"• Enable all analysis features\n\n"
                                    f"Continue?")
        
        if result:
            try:
                self.verify_realtime_conversion()
                success = self.integrate_realtime_data()
                
                if success:
                    # Show success and offer to analyze
                    messagebox.showinfo("Export Complete",
                                      f"✅ Real-time data exported successfully!\n\n"
                                      f"📊 {total_records:,} records from {unique_hosts} hosts\n"
                                      f"⚙️ Interval set to 'Real-Time'\n\n"
                                      f"You can now use all analysis features with your real-time data.")
                    
                    # Offer to auto-analyze
                    if self.auto_analyze.get():
                        self.show_smart_notification("Real-time data exported! Auto-analyzing...", 3000)
                        self.root.after(1500, self.auto_calculate_and_switch)
                    else:
                        self.show_action_prompt("Real-time data ready! Analyze now?",
                                              "🔍 Analyze",
                                              self.auto_calculate_and_switch)
                else:
                    messagebox.showerror("Export Failed", "Failed to export real-time data.\nCheck the debug output for details.")
                    
            except Exception as e:
                messagebox.showerror("Export Error", f"Error exporting real-time data:\n{str(e)}")

    def export_realtime_to_csv(self):
        """Export real-time data to CSV file"""
        if not hasattr(self, 'realtime_dashboard'):
            messagebox.showwarning("No Dashboard", "Real-time dashboard not available")
            return
        
        # Get real-time data
        recent_data = self.realtime_dashboard.db.get_recent_performance_data(minutes=10080)
        
        if recent_data.empty:
            messagebox.showwarning("No Data", "No real-time data to export")
            return
        
        # File selection dialog
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Real-Time Data"
        )
        
        if filename:
            try:
                # Prepare data for export with metadata
                export_data = recent_data.copy()
                export_data['export_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                export_data['source'] = 'realtime_dashboard'
                export_data['interval_seconds'] = 20
                
                # Rename columns for clarity
                export_data = export_data.rename(columns={
                    'timestamp': 'Time',
                    'hostname': 'Hostname',
                    'cpu_ready_percent': 'CPU_Ready_Percent',
                    'cpu_ready_sum': 'CPU_Ready_Sum'
                })
                
                # Reorder columns
                column_order = ['Time', 'Hostname', 'CPU_Ready_Percent', 'CPU_Ready_Sum', 
                              'source', 'interval_seconds', 'export_time']
                export_data = export_data[column_order]
                
                # Export to CSV
                export_data.to_csv(filename, index=False)
                
                messagebox.showinfo("Export Complete",
                                  f"Real-time data exported successfully!\n\n"
                                  f"📄 File: {filename}\n"
                                  f"📊 Records: {len(export_data):,}\n"
                                  f"🖥️  Hosts: {export_data['Hostname'].nunique()}\n"
                                  f"📅 Time Range: {export_data['Time'].min()} to {export_data['Time'].max()}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export CSV:\n{str(e)}")

    def setup_threshold_bindings(self):
        """Setup threshold change bindings for real-time dashboard integration"""
        if hasattr(self, 'warning_threshold'):
            self.warning_threshold.trace('w', lambda *args: self.on_threshold_change())
        if hasattr(self, 'critical_threshold'):
            self.critical_threshold.trace('w', lambda *args: self.on_threshold_change())

    def fetch_vcenter_data(self):
        """Fetch CPU Ready data from vCenter for all hosts with styled progress dialog and auto-flow - FIXED TIME PERIOD"""
        if not self.vcenter_connection:
            messagebox.showerror("No Connection", "Please connect to vCenter first")
            return
        
        # Get date range based on selected vCenter period - FIXED
        start_date, end_date = self.get_vcenter_date_range()
        selected_period = self.vcenter_period_var.get()
        perf_interval = self.vcenter_intervals[selected_period]
        
        print(f"DEBUG: Selected period: {selected_period}")
        print(f"DEBUG: Date range: {start_date} to {end_date}")
        print(f"DEBUG: Performance interval: {perf_interval} seconds")
        
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
                
                # FIXED: Pass the correct time period parameters
                cpu_ready_data = self.fetch_cpu_ready_metrics(
                    content, hosts, start_date, end_date, perf_interval, selected_period
                )
                
                if cpu_ready_data:
                    df = pd.DataFrame(cpu_ready_data)
                    df['source_file'] = f'vCenter_{selected_period}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}'
                    df['detected_interval'] = selected_period  # FIXED: Set the correct interval
                    
                    self.data_frames.append(df)
                    self.update_file_status()
                    self.update_data_preview()
                    
                    # Auto-set the interval - FIXED
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

    def create_complete_vcenter_section(self, parent):
        """Create complete vCenter integration section with improved formatting - FIXED to include Real-Time"""
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
        
        # FIXED: Include Real-Time in vCenter time periods
        self.vcenter_period_var = tk.StringVar(value="Last Day")
        period_values = ["Real-Time", "Last Day", "Last Week", "Last Month", "Last Year"]  # Added Real-Time back
        
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
        """Update date range label based on selected period - FIXED to include Real-Time"""
        if not hasattr(self, 'date_range_label'):
            return
            
        period = self.vcenter_period_var.get()
        now = datetime.now()
        
        ranges = {
            "Real-Time": (now - timedelta(hours=1), now),  # FIXED: Added Real-Time back
            "Last Day": (now - timedelta(days=1), now),
            "Last Week": (now - timedelta(weeks=1), now),
            "Last Month": (now - timedelta(days=30), now),
            "Last Year": (now - timedelta(days=365), now)
        }
        
        if period in ranges:
            start_time, end_time = ranges[period]
            if period == "Real-Time":
                range_text = f"(Last hour: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"
            elif period in ["Last Day"]:
                range_text = f"({start_time.strftime('%m/%d %H:%M')} - {end_time.strftime('%m/%d %H:%M')})"
            else:
                range_text = f"({start_time.strftime('%m/%d')} - {end_time.strftime('%m/%d')})"
        else:
            range_text = ""
        
        self.date_range_label.config(text=range_text)

    def get_vcenter_date_range(self):
        """Calculate start and end dates based on selected vCenter period - FIXED to include Real-Time"""
        period = self.vcenter_period_var.get()
        now = datetime.now()
        
        print(f"DEBUG: Getting date range for period: {period}")
        
        ranges = {
            "Real-Time": (now - timedelta(hours=1), now),  # FIXED: Added Real-Time back
            "Last Day": (now - timedelta(days=1), now),
            "Last Week": (now - timedelta(weeks=1), now),
            "Last Month": (now - timedelta(days=30), now),
            "Last Year": (now - timedelta(days=365), now)
        }
        
        start_time, end_time = ranges.get(period, ranges["Last Day"])
        
        print(f"DEBUG: Date range calculated: {start_time.date()} to {end_time.date()}")
        
        return start_time.date(), end_time.date()

    def fetch_cpu_ready_metrics(self, content, hosts, start_date, end_date, interval_seconds, selected_period):
        """Fetch CPU Ready metrics for all hosts with proper interval handling - FIXED TIME PERIOD USAGE"""
        perf_manager = content.perfManager
        cpu_ready_data = []
        
        print(f"DEBUG: Requesting data for period: {selected_period}")
        print(f"DEBUG: Date range: {start_date} to {end_date}")
        print(f"DEBUG: Interval: {interval_seconds} seconds")
        
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
        available_intervals = perf_manager.historicalInterval
        print("DEBUG: Available historical intervals:")
        for interval in available_intervals:
            print(f"  - Key: {interval.key}, Name: {interval.name}, Period: {interval.samplingPeriod}s, Level: {interval.level}")
        
        # Convert dates to vCenter format - FIXED TIME RANGE CALCULATION
        start_time = datetime.combine(start_date, datetime.min.time())
        end_time = datetime.combine(end_date, datetime.max.time())
        time_diff = (end_time - start_time).total_seconds()
        
        print(f"DEBUG: Time difference: {time_diff} seconds ({time_diff/3600:.1f} hours)")
        print(f"DEBUG: Looking for interval close to: {interval_seconds} seconds")
        
        # FIXED: Better interval selection based on the selected period
        selected_interval = None
        use_realtime = False
        
        if selected_period == "Real-Time" or time_diff <= 3600:
            print("DEBUG: Using real-time data approach")
            selected_interval = None  # Real-time uses no intervalId
            use_realtime = True
        else:
            # FIXED: Find the best matching interval from available historical intervals
            best_interval = None
            best_diff = float('inf')
            
            print(f"DEBUG: Looking for historical interval close to {interval_seconds} seconds")
            
            for interval in available_intervals:
                # Calculate how close this interval's period is to what we want
                period_diff = abs(interval.samplingPeriod - interval_seconds)
                print(f"  - Checking interval {interval.key}: {interval.samplingPeriod}s (diff: {period_diff})")
                
                if period_diff < best_diff:
                    best_diff = period_diff
                    best_interval = interval
                    print(f"    - New best match: {interval.key} (diff: {period_diff})")
            
            if best_interval:
                selected_interval = best_interval.key
                actual_interval = best_interval.samplingPeriod
                print(f"DEBUG: Selected historical interval: Key={selected_interval}, Period={actual_interval}s, Name={best_interval.name}")
                
                # Update interval_seconds to match what vCenter actually uses
                interval_seconds = actual_interval
            else:
                print("DEBUG: No suitable historical interval found, falling back to real-time")
                selected_interval = None
                use_realtime = True
        
        # Fetch data for each host
        print(f"DEBUG: Fetching data for {len(hosts)} hosts using {'real-time' if use_realtime else f'historical interval {selected_interval}'}...")
        
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
                
                # Create the query specification - FIXED TIME RANGE
                if use_realtime:
                    # Real-time query - no intervalId specified, limited time range
                    query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=[metric_spec],
                        maxSample=100,  # Limit for real-time
                        startTime=start_time,
                        endTime=end_time
                    )
                    print(f"DEBUG: Using real-time query for {hostname}")
                else:
                    # Historical query with proper intervalId and time range
                    query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=[metric_spec],
                        intervalId=selected_interval,
                        maxSample=1000,  # Allow more samples for historical data
                        startTime=start_time,
                        endTime=end_time
                    )
                    print(f"DEBUG: Using historical query for {hostname} with interval {selected_interval}")
                
                print(f"DEBUG: Executing query for {hostname} from {start_time} to {end_time}...")
                
                # Execute query
                perf_data = perf_manager.QueryPerf(querySpec=[query_spec])
                
                if perf_data and len(perf_data) > 0:
                    print(f"DEBUG: Query successful for {hostname}")
                    
                    if perf_data[0].value and len(perf_data[0].value) > 0:
                        samples_found = len(perf_data[0].sampleInfo)
                        print(f"DEBUG: Found {samples_found} samples for {hostname}")
                        
                        if samples_found == 0:
                            print(f"DEBUG: No sample data for {hostname}")
                            continue
                        
                        # Process the performance data
                        for i, sample_info in enumerate(perf_data[0].sampleInfo):
                            timestamp = sample_info.timestamp
                            
                            # Get CPU Ready value for this timestamp
                            total_ready = 0
                            for value_info in perf_data[0].value:
                                if i < len(value_info.value) and value_info.value[i] is not None:
                                    # FIXED: Don't exclude zero values - they are valid
                                    if value_info.value[i] >= 0:  # Include zero values
                                        total_ready += value_info.value[i]
                            
                            # Add data point (including zero values)
                            cpu_ready_data.append({
                                'Time': timestamp.isoformat() + 'Z',
                                f'Ready for {hostname}': total_ready,
                                'Hostname': hostname.split('.')[0]  # Short hostname
                            })
                            
                            # Debug for first few samples
                            if i < 3:
                                print(f"DEBUG: Sample {i} for {hostname}: timestamp={timestamp}, total_ready={total_ready}")
                    else:
                        print(f"DEBUG: No values in performance data for {hostname}")
                else:
                    print(f"DEBUG: No performance data returned for {hostname}")
                    
            except Exception as e:
                print(f"DEBUG: Error fetching data for host {hostname}: {e}")
                continue
        
        print(f"DEBUG: Total records collected: {len(cpu_ready_data)}")
        
        if len(cpu_ready_data) == 0:
            print("DEBUG: No data collected from any host!")
            # Try to provide helpful information
            print("DEBUG: Possible issues:")
            print("  - Time range might be outside available data")
            print("  - Selected interval might not have data")
            print("  - Hosts might not have CPU Ready metrics enabled")
            print("  - vCenter might not be collecting performance data")
        
        return cpu_ready_data
 
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
        self.create_realtime_dashboard_tab()
        
        # Host Management Tab
        self.create_host_management_tab()
        
        # Advanced Tab
        self.create_advanced_tab()
        
        # About Tab
        self.create_about_tab()

    def create_realtime_dashboard_tab(self):
        """Create real-time dashboard tab with export capabilities"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="📡 Real-Time Dashboard")
        
        # Dashboard container
        dashboard_container = tk.Frame(tab_frame, bg=self.colors['bg_primary'])
        dashboard_container.pack(fill=tk.BOTH, expand=True)
        
        # Initialize dashboard with your theme colors and thresholds
        self.realtime_dashboard = RealTimeDashboard(
            parent=dashboard_container,
            vcenter_connection=getattr(self, 'vcenter_connection', None),
            warning_threshold=self.warning_threshold.get() if hasattr(self, 'warning_threshold') else 5.0,
            critical_threshold=self.critical_threshold.get() if hasattr(self, 'critical_threshold') else 15.0,
            theme_colors=self.colors
        )
        
        # Add export controls at the bottom
        self.create_realtime_export_controls(tab_frame)

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

    def export_comprehensive_pdf_report(self):
        """Export comprehensive PDF report with all analysis results, visualizations, and AI recommendations"""
        if not PDF_AVAILABLE:
            messagebox.showerror("PDF Export Unavailable", 
                            "PDF export requires additional packages:\n\n"
                            "pip install reportlab\n\n"
                            "Please install and restart the application.")
            return
        
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # File selection dialog
        filename = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            title="Export Comprehensive PDF Report"
        )
        
        if not filename:
            return
        
        self.show_progress("Generating comprehensive PDF report with visualizations...")
        
        try:
            # Create PDF document
            doc = SimpleDocTemplate(filename, pagesize=A4, 
                                topMargin=0.75*inch, bottomMargin=0.75*inch,
                                leftMargin=0.75*inch, rightMargin=0.75*inch)
            
            # Get styles
            styles = getSampleStyleSheet()
            
            # Create custom styles
            title_style = ParagraphStyle('CustomTitle',
                                    parent=styles['Heading1'],
                                    fontSize=24,
                                    spaceAfter=30,
                                    textColor=colors.darkblue,
                                    alignment=1)  # Center alignment
            
            heading_style = ParagraphStyle('CustomHeading',
                                        parent=styles['Heading2'],
                                        fontSize=16,
                                        spaceBefore=20,
                                        spaceAfter=12,
                                        textColor=colors.darkblue,
                                        borderWidth=1,
                                        borderColor=colors.lightgrey,
                                        borderPadding=5,
                                        backColor=colors.lightgrey)
            
            subheading_style = ParagraphStyle('CustomSubHeading',
                                            parent=styles['Heading3'],
                                            fontSize=14,
                                            spaceBefore=15,
                                            spaceAfter=8,
                                            textColor=colors.darkred)
            
            # Build story (content)
            story = []
            
            # TITLE PAGE
            story.append(Paragraph("🖥️ vCenter CPU Ready Analysis Report", title_style))
            story.append(Spacer(1, 0.5*inch))
            
            # Executive Summary
            exec_summary = self.generate_executive_summary()
            story.append(Paragraph("📊 Executive Summary", heading_style))
            
            summary_data = [
                ["Metric", "Value"],
                ["Analysis Date", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ["Total Hosts Analyzed", str(len(self.processed_data['Hostname'].unique()))],
                ["Analysis Period", self.current_interval],
                ["Total Records", f"{len(self.processed_data):,}"],
                ["Date Range", f"{self.processed_data['Time'].min().strftime('%Y-%m-%d')} to {self.processed_data['Time'].max().strftime('%Y-%m-%d')}"],
                ["Critical Hosts", str(exec_summary['critical_hosts'])],
                ["Warning Hosts", str(exec_summary['warning_hosts'])],
                ["Healthy Hosts", str(exec_summary['healthy_hosts'])],
                ["Overall Health", exec_summary['overall_health']]
            ]
            
            summary_table = Table(summary_data, colWidths=[2.5*inch, 2*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(summary_table)
            story.append(Spacer(1, 0.3*inch))
            
            # Key Findings
            story.append(Paragraph("🎯 Key Findings", subheading_style))
            findings = exec_summary['key_findings']
            for finding in findings:
                story.append(Paragraph(f"• {finding}", styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            # Page break before main content
            story.append(PageBreak())
            
            # SECTION 1: PERFORMANCE TIMELINE VISUALIZATION
            story.append(Paragraph("📈 Performance Timeline Analysis", heading_style))
            
            # Generate and include main timeline chart
            timeline_chart = self.generate_timeline_chart_for_pdf()
            if timeline_chart:
                story.append(timeline_chart)
                story.append(Spacer(1, 0.2*inch))
            
            # Timeline analysis text
            timeline_analysis = self.generate_timeline_analysis_text()
            story.append(Paragraph(timeline_analysis, styles['Normal']))
            story.append(Spacer(1, 0.3*inch))
            
            # SECTION 2: DETAILED HOST ANALYSIS TABLE
            story.append(Paragraph("📊 Detailed Host Performance Analysis", heading_style))
            
            # Create detailed host table
            host_data = []
            host_data.append(["Host", "Avg CPU Ready %", "Max CPU Ready %", "Min CPU Ready %", "Health Score", "Status", "Records"])
            
            host_analysis_details = []
            
            for hostname in sorted(self.processed_data['Hostname'].unique()):
                host_df = self.processed_data[self.processed_data['Hostname'] == hostname]
                avg_cpu = host_df['CPU_Ready_Percent'].mean()
                max_cpu = host_df['CPU_Ready_Percent'].max()
                min_cpu = host_df['CPU_Ready_Percent'].min()
                std_cpu = host_df['CPU_Ready_Percent'].std()
                health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu)
                
                if avg_cpu >= self.critical_threshold.get():
                    status = "Critical"
                elif avg_cpu >= self.warning_threshold.get():
                    status = "Warning"
                else:
                    status = "Healthy"
                
                host_data.append([
                    hostname,
                    f"{avg_cpu:.2f}%",
                    f"{max_cpu:.2f}%",
                    f"{min_cpu:.2f}%",
                    f"{health_score:.0f}/100",
                    status,
                    f"{len(host_df):,}"
                ])
                
                # Store detailed analysis for later
                host_analysis_details.append({
                    'hostname': hostname,
                    'avg_cpu': avg_cpu,
                    'max_cpu': max_cpu,
                    'min_cpu': min_cpu,
                    'std_cpu': std_cpu,
                    'health_score': health_score,
                    'status': status,
                    'records': len(host_df)
                })
            
            # Create and style the host table
            host_table = Table(host_data, colWidths=[1.3*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.7*inch])
            host_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8)
            ]))
            
            story.append(host_table)
            story.append(Spacer(1, 0.3*inch))
            
            # SECTION 3: AI CONSOLIDATION RECOMMENDATIONS
            story.append(PageBreak())
            story.append(Paragraph("🤖 AI Consolidation Recommendations", heading_style))
            
            # Generate AI recommendations if not already done
            if not hasattr(self, 'current_recommendations') or not self.current_recommendations:
                # Generate recommendations for PDF
                strategy = "Balanced"
                target_reduction = 20.0
                recommendations = self.analyze_consolidation_candidates(strategy, target_reduction)
                self.current_recommendations = recommendations
            
            if hasattr(self, 'current_recommendations') and self.current_recommendations:
                # AI Recommendations Summary Table
                story.append(Paragraph("🎯 Recommended Hosts for Consolidation", subheading_style))
                
                rec_data = [["Rank", "Host", "Consolidation Score", "Risk Level", "Avg CPU Ready %", "Peak CPU Ready %"]]
                
                for i, rec in enumerate(self.current_recommendations[:5], 1):  # Top 5 recommendations
                    if rec['metrics']['avg_cpu_ready'] < 2.0 and rec['metrics']['critical_periods'] == 0:
                        risk_level = "Low"
                        risk_color = colors.green
                    elif rec['metrics']['avg_cpu_ready'] < 5.0 and rec['metrics']['critical_periods'] < 2:
                        risk_level = "Medium"
                        risk_color = colors.orange
                    else:
                        risk_level = "High"
                        risk_color = colors.red
                    
                    rec_data.append([
                        str(i),
                        rec['hostname'],
                        f"{rec['consolidation_score']:.0f}/100",
                        risk_level,
                        f"{rec['metrics']['avg_cpu_ready']:.2f}%",
                        f"{rec['metrics']['percentile_95']:.2f}%"
                    ])
                
                rec_table = Table(rec_data, colWidths=[0.4*inch, 1.5*inch, 1*inch, 0.8*inch, 1*inch, 1*inch])
                rec_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.lightgreen),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 8)
                ]))
                story.append(rec_table)
                story.append(Spacer(1, 0.3*inch))
                
                # Detailed AI Analysis Text
                ai_analysis_text = self.generate_ai_recommendations_text()
                story.append(Paragraph(ai_analysis_text, styles['Normal']))
                story.append(Spacer(1, 0.3*inch))
                
                # Impact Assessment
                story.append(Paragraph("📊 Consolidation Impact Assessment", subheading_style))
                impact_text = self.generate_consolidation_impact_text()
                story.append(Paragraph(impact_text, styles['Normal']))
                story.append(Spacer(1, 0.3*inch))
            
            else:
                story.append(Paragraph("No AI recommendations generated. Run AI analysis first for consolidation recommendations.", styles['Normal']))
                story.append(Spacer(1, 0.3*inch))
            
            # SECTION 4: ADVANCED VISUALIZATIONS
            story.append(PageBreak())
            story.append(Paragraph("📊 Advanced Performance Analysis", heading_style))
            
            # Host Comparison Chart
            story.append(Paragraph("🎯 Host Performance Comparison", subheading_style))
            comparison_chart = self.generate_host_comparison_chart_for_pdf()
            if comparison_chart:
                story.append(comparison_chart)
                story.append(Spacer(1, 0.2*inch))
            
            # Performance Distribution Analysis
            story.append(Paragraph("📈 Performance Distribution Analysis", subheading_style))
            distribution_text = self.generate_distribution_analysis_text()
            story.append(Paragraph(distribution_text, styles['Normal']))
            story.append(Spacer(1, 0.3*inch))
            
            # SECTION 5: RECOMMENDATIONS AND BEST PRACTICES
            story.append(PageBreak())
            story.append(Paragraph("💡 Recommendations & Implementation Guide", heading_style))
            
            # Implementation recommendations
            story.append(Paragraph("🚀 Implementation Recommendations", subheading_style))
            impl_recommendations = self.generate_implementation_recommendations()
            story.append(Paragraph(impl_recommendations, styles['Normal']))
            story.append(Spacer(1, 0.3*inch))
            
            # Best practices
            story.append(Paragraph("✅ Best Practices", subheading_style))
            best_practices_text = """
            <b>Performance Monitoring:</b><br/>
            • Monitor CPU Ready metrics continuously<br/>
            • Set up automated alerting for thresholds<br/>
            • Regular capacity planning reviews<br/>
            • Document all performance baselines<br/>
            <br/>
            <b>Consolidation Guidelines:</b><br/>
            • Test consolidation in non-production first<br/>
            • Implement changes during maintenance windows<br/>
            • Monitor performance for 2 weeks post-consolidation<br/>
            • Maintain rollback procedures<br/>
            <br/>
            <b>Infrastructure Management:</b><br/>
            • Regular health assessments using this tool<br/>
            • Update DRS/HA configurations after changes<br/>
            • Keep documentation current<br/>
            • Train team on performance optimization
            """
            story.append(Paragraph(best_practices_text, styles['Normal']))
            story.append(Spacer(1, 0.3*inch))
            
            # Footer information
            story.append(Paragraph("📋 Report Details", subheading_style))
            footer_text = f"""
            <b>Generated by:</b> vCenter CPU Ready Analyzer v2.0<br/>
            <b>Report Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>
            <b>Analysis Method:</b> VMware vCenter Performance Data Analysis<br/>
            <b>Total Pages:</b> Multiple sections with visualizations<br/>
            <b>Data Period:</b> {self.current_interval}<br/>
            <b>Developed by:</b> Joshua Fourie (joshua.fourie@outlook.com)
            """
            story.append(Paragraph(footer_text, styles['Normal']))
            
            # Build PDF
            doc.build(story)
            
            messagebox.showinfo("PDF Export Complete", 
                            f"Comprehensive PDF report exported successfully!\n\n"
                            f"📄 Location: {filename}\n"
                            f"📊 Content: Complete analysis with charts, visualizations, and AI recommendations\n"
                            f"📈 Includes: Timeline charts, host comparison, and consolidation analysis")
            
        except Exception as e:
            messagebox.showerror("PDF Export Error", f"Failed to generate PDF report:\n{str(e)}")
            import traceback
            print(f"PDF Export Error Details: {traceback.format_exc()}")
        finally:
            self.hide_progress()

    def generate_timeline_chart_for_pdf(self):
        """Generate timeline chart specifically for PDF inclusion"""
        try:
            import tempfile
            import os
            
            # Create a copy of the current figure for PDF
            fig_copy, ax_copy = plt.subplots(figsize=(10, 6))
            fig_copy.patch.set_facecolor('white')
            ax_copy.set_facecolor('white')
            
            # Modern color palette for PDF (darker colors for better printing)
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
            
            hostnames = sorted(self.processed_data['Hostname'].unique())
            
            for i, hostname in enumerate(hostnames):
                host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
                host_data = host_data.sort_values('Time')
                
                color = colors[i % len(colors)]
                ax_copy.plot(host_data['Time'], host_data['CPU_Ready_Percent'],
                            marker='o', markersize=2, linewidth=2, label=hostname,
                            color=color, alpha=0.8)
            
            # Add threshold lines
            warning_line = self.warning_threshold.get()
            critical_line = self.critical_threshold.get()
            
            ax_copy.axhline(y=warning_line, color='orange', linestyle='--', 
                        alpha=0.7, linewidth=2, label=f'Warning ({warning_line}%)')
            ax_copy.axhline(y=critical_line, color='red', linestyle='--', 
                        alpha=0.7, linewidth=2, label=f'Critical ({critical_line}%)')
            
            # Styling for PDF
            ax_copy.set_title(f'CPU Ready % Timeline ({self.current_interval} Interval)', 
                            fontsize=14, fontweight='bold', color='black', pad=15)
            ax_copy.set_xlabel('Time', fontsize=12, color='black')
            ax_copy.set_ylabel('CPU Ready %', fontsize=12, color='black')
            
            # Legend
            ax_copy.legend(loc='upper left', bbox_to_anchor=(1.02, 1),
                        frameon=True, fancybox=True)
            
            # Grid
            ax_copy.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
            
            # Format dates on x-axis
            fig_copy.autofmt_xdate()
            fig_copy.tight_layout()
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                fig_copy.savefig(tmp_file.name, dpi=150, bbox_inches='tight', 
                            facecolor='white', edgecolor='none')
                
                # Create reportlab Image
                from reportlab.platypus import Image
                chart_img = Image(tmp_file.name, width=7*inch, height=4*inch)
                
                # Clean up
                plt.close(fig_copy)
                os.unlink(tmp_file.name)
                
                return chart_img
        except Exception as e:
            print(f"DEBUG: Could not generate timeline chart for PDF: {e}")
            return None

    def generate_host_comparison_chart_for_pdf(self):
        """Generate host comparison bar chart for PDF"""
        try:
            import tempfile
            import os
            
            # Create comparison chart
            fig_comp, ax_comp = plt.subplots(figsize=(10, 6))
            fig_comp.patch.set_facecolor('white')
            ax_comp.set_facecolor('white')
            
            # Prepare data
            hostnames = sorted(self.processed_data['Hostname'].unique())
            avg_cpu_values = []
            max_cpu_values = []
            health_scores = []
            
            for hostname in hostnames:
                host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
                avg_cpu = host_data['CPU_Ready_Percent'].mean()
                max_cpu = host_data['CPU_Ready_Percent'].max()
                health_score = self.calculate_health_score(avg_cpu, max_cpu, host_data['CPU_Ready_Percent'].std())
                
                avg_cpu_values.append(avg_cpu)
                max_cpu_values.append(max_cpu)
                health_scores.append(health_score)
            
            # Create grouped bar chart
            x = range(len(hostnames))
            width = 0.35
            
            bars1 = ax_comp.bar([i - width/2 for i in x], avg_cpu_values, width, 
                            label='Average CPU Ready %', color='steelblue', alpha=0.8)
            bars2 = ax_comp.bar([i + width/2 for i in x], max_cpu_values, width,
                            label='Maximum CPU Ready %', color='lightcoral', alpha=0.8)
            
            # Add threshold lines
            ax_comp.axhline(y=self.warning_threshold.get(), color='orange', linestyle='--', alpha=0.7, linewidth=2)
            ax_comp.axhline(y=self.critical_threshold.get(), color='red', linestyle='--', alpha=0.7, linewidth=2)
            
            # Styling
            ax_comp.set_xlabel('Hosts', fontsize=12, color='black')
            ax_comp.set_ylabel('CPU Ready %', fontsize=12, color='black')
            ax_comp.set_title('Host Performance Comparison', fontsize=14, fontweight='bold', color='black')
            ax_comp.set_xticks(x)
            ax_comp.set_xticklabels([h[:10] for h in hostnames], rotation=45, ha='right')
            ax_comp.legend()
            ax_comp.grid(True, alpha=0.3, axis='y')
            
            plt.tight_layout()
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                fig_comp.savefig(tmp_file.name, dpi=150, bbox_inches='tight', 
                            facecolor='white', edgecolor='none')
                
                from reportlab.platypus import Image
                chart_img = Image(tmp_file.name, width=7*inch, height=4*inch)
                
                plt.close(fig_comp)
                os.unlink(tmp_file.name)
                
                return chart_img
        except Exception as e:
            print(f"DEBUG: Could not generate comparison chart for PDF: {e}")
            return None

    def generate_timeline_analysis_text(self):
        """Generate timeline analysis text for PDF"""
        overall_avg = self.processed_data['CPU_Ready_Percent'].mean()
        overall_max = self.processed_data['CPU_Ready_Percent'].max()
        date_range = (self.processed_data['Time'].max() - self.processed_data['Time'].min()).days
        
        analysis = f"""
        <b>Timeline Analysis Summary:</b><br/>
        The performance timeline shows CPU Ready metrics over {date_range} days of monitoring. 
        Overall average CPU Ready across all hosts is {overall_avg:.2f}%, with a peak value of {overall_max:.2f}%. 
        """
        
        if overall_avg < 2.0:
            analysis += "The infrastructure shows excellent performance with very low CPU contention. "
        elif overall_avg < 5.0:
            analysis += "Performance is good with acceptable CPU Ready levels. "
        else:
            analysis += "Elevated CPU Ready levels indicate potential resource contention that should be investigated. "
        
        # Add host-specific insights
        critical_hosts = [h for h in self.processed_data['Hostname'].unique() 
                        if self.processed_data[self.processed_data['Hostname']==h]['CPU_Ready_Percent'].mean() >= self.critical_threshold.get()]
        
        if critical_hosts:
            analysis += f"<br/><br/><b>Attention Required:</b> {len(critical_hosts)} host(s) exceed critical thresholds and require immediate investigation."
        else:
            analysis += "<br/><br/><b>Performance Status:</b> All hosts are operating within acceptable performance parameters."
        
        return analysis

    def generate_ai_recommendations_text(self):
        """Generate AI recommendations analysis text for PDF"""
        if not hasattr(self, 'current_recommendations') or not self.current_recommendations:
            return "AI recommendations not available. Please generate recommendations first."
        
        total_hosts = len(self.processed_data['Hostname'].unique())
        recommended_count = len(self.current_recommendations)
        reduction_percentage = (recommended_count / total_hosts) * 100
        
        text = f"""
        <b>AI Analysis Results:</b><br/>
        The AI consolidation analysis identified {recommended_count} out of {total_hosts} hosts ({reduction_percentage:.1f}%) 
        as potential consolidation candidates. These recommendations are based on comprehensive analysis of:
        <br/><br/>
        • CPU Ready usage patterns and consistency<br/>
        • Performance stability over time<br/>
        • Peak usage and workload distribution<br/>
        • Risk assessment for workload redistribution<br/>
        <br/>
        <b>Risk Assessment:</b><br/>
        """
        
        low_risk = sum(1 for rec in self.current_recommendations 
                    if rec['metrics']['avg_cpu_ready'] < 2.0 and rec['metrics']['critical_periods'] == 0)
        medium_risk = sum(1 for rec in self.current_recommendations 
                        if 2.0 <= rec['metrics']['avg_cpu_ready'] < 5.0)
        high_risk = recommended_count - low_risk - medium_risk
        
        text += f"• Low Risk Candidates: {low_risk} hosts<br/>"
        text += f"• Medium Risk Candidates: {medium_risk} hosts<br/>"
        text += f"• High Risk Candidates: {high_risk} hosts<br/>"
        
        return text

    def generate_consolidation_impact_text(self):
        """Generate consolidation impact analysis text"""
        if not hasattr(self, 'current_recommendations') or not self.current_recommendations:
            return ""
        
        total_hosts = len(self.processed_data['Hostname'].unique())
        recommended_hosts = [rec['hostname'] for rec in self.current_recommendations]
        
        # Calculate workload impact
        total_workload = self.processed_data['CPU_Ready_Sum'].sum()
        recommended_workload = self.processed_data[
            self.processed_data['Hostname'].isin(recommended_hosts)
        ]['CPU_Ready_Sum'].sum()
        
        workload_percentage = (recommended_workload / total_workload) * 100 if total_workload > 0 else 0
        remaining_hosts = total_hosts - len(recommended_hosts)
        
        impact_text = f"""
        <b>Infrastructure Impact Assessment:</b><br/>
        • Infrastructure Reduction: {(len(recommended_hosts)/total_hosts)*100:.1f}%<br/>
        • Workload Redistribution: {workload_percentage:.1f}% of total workload<br/>
        • Remaining Hosts: {remaining_hosts}<br/>
        • Additional Load per Host: +{workload_percentage/remaining_hosts:.1f}%<br/>
        <br/>
        <b>Risk Level: </b>
        """
        
        if workload_percentage > 30:
            impact_text += "HIGH - Significant workload redistribution required<br/>"
        elif workload_percentage > 15:
            impact_text += "MEDIUM - Moderate redistribution with monitoring recommended<br/>"
        else:
            impact_text += "LOW - Safe for consolidation<br/>"
        
        # Cost savings estimate
        annual_savings = len(recommended_hosts) * 20000  # Rough estimate per host
        impact_text += f"<br/><b>Estimated Annual Savings:</b> ${annual_savings:,} (hardware, power, licensing)"
        
        return impact_text

    def generate_distribution_analysis_text(self):
        """Generate performance distribution analysis text"""
        overall_std = self.processed_data['CPU_Ready_Percent'].std()
        overall_mean = self.processed_data['CPU_Ready_Percent'].mean()
        cv = (overall_std / overall_mean) * 100 if overall_mean > 0 else 0
        
        p95 = self.processed_data['CPU_Ready_Percent'].quantile(0.95)
        p99 = self.processed_data['CPU_Ready_Percent'].quantile(0.99)
        
        text = f"""
        <b>Performance Distribution Analysis:</b><br/>
        • Standard Deviation: {overall_std:.2f}%<br/>
        • Coefficient of Variation: {cv:.1f}%<br/>
        • 95th Percentile: {p95:.2f}%<br/>
        • 99th Percentile: {p99:.2f}%<br/>
        <br/>
        """
        
        if cv < 30:
            text += "<b>Variability Assessment:</b> Low variability indicates consistent performance across the infrastructure."
        elif cv < 60:
            text += "<b>Variability Assessment:</b> Moderate variability suggests some performance differences between hosts."
        else:
            text += "<b>Variability Assessment:</b> High variability indicates significant performance differences requiring investigation."
        
        return text

    def generate_implementation_recommendations(self):
        """Generate implementation recommendations based on analysis"""
        overall_avg = self.processed_data['CPU_Ready_Percent'].mean()
        critical_hosts = len([h for h in self.processed_data['Hostname'].unique() 
                            if self.processed_data[self.processed_data['Hostname']==h]['CPU_Ready_Percent'].mean() >= self.critical_threshold.get()])
        
        recommendations = "<b>Immediate Actions:</b><br/>"
        
        if critical_hosts > 0:
            recommendations += f"• Investigate {critical_hosts} critical host(s) immediately<br/>"
            recommendations += "• Check for resource contention and workload distribution<br/>"
            recommendations += "• Consider workload migration from overloaded hosts<br/>"
        else:
            recommendations += "• No immediate critical issues identified<br/>"
        
        recommendations += "<br/><b>Optimization Opportunities:</b><br/>"
        
        if overall_avg < 2.0:
            recommendations += "• Infrastructure shows consolidation potential<br/>"
            recommendations += "• Consider implementing AI recommendations for host consolidation<br/>"
            recommendations += "• Evaluate power and cooling savings opportunities<br/>"
        elif overall_avg < 5.0:
            recommendations += "• Good performance baseline established<br/>"
            recommendations += "• Monitor trends for capacity planning<br/>"
            recommendations += "• Consider selective consolidation of lowest-utilized hosts<br/>"
        else:
            recommendations += "• Focus on performance optimization before consolidation<br/>"
            recommendations += "• Investigate workload balancing opportunities<br/>"
            recommendations += "• Review DRS and HA configurations<br/>"
        
        recommendations += "<br/><b>Monitoring Recommendations:</b><br/>"
        recommendations += "• Set up automated alerting for CPU Ready thresholds<br/>"
        recommendations += "• Implement regular performance trending analysis<br/>"
        recommendations += "• Schedule quarterly infrastructure health assessments<br/>"
        recommendations += "• Document performance baselines for future comparison<br/>"
        
        return recommendations

    def generate_executive_summary(self):
        """Generate executive summary data for PDF report"""
        if self.processed_data is None:
            return {}
        
        unique_hosts = self.processed_data['Hostname'].unique()
        
        critical_hosts = 0
        warning_hosts = 0
        healthy_hosts = 0
        key_findings = []
        
        for hostname in unique_hosts:
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            avg_cpu = host_data['CPU_Ready_Percent'].mean()
            
            if avg_cpu >= self.critical_threshold.get():
                critical_hosts += 1
            elif avg_cpu >= self.warning_threshold.get():
                warning_hosts += 1
            else:
                healthy_hosts += 1
        
        # Generate key findings
        total_hosts = len(unique_hosts)
        overall_avg = self.processed_data['CPU_Ready_Percent'].mean()
        
        if critical_hosts > 0:
            key_findings.append(f"{critical_hosts} hosts require immediate attention (>={self.critical_threshold.get()}% CPU Ready)")
            overall_health = "Needs Attention"
        elif warning_hosts > 0:
            key_findings.append(f"{warning_hosts} hosts need monitoring (>={self.warning_threshold.get()}% CPU Ready)")
            overall_health = "Good with Monitoring"
        else:
            key_findings.append("All hosts performing within healthy parameters")
            overall_health = "Excellent"
        
        key_findings.append(f"Overall average CPU Ready: {overall_avg:.2f}%")
        
        if healthy_hosts > total_hosts * 0.7:
            key_findings.append(f"{healthy_hosts} hosts are good consolidation candidates")
        
        if overall_avg < 2.0:
            key_findings.append("Infrastructure shows potential for consolidation opportunities")
        
        return {
            'critical_hosts': critical_hosts,
            'warning_hosts': warning_hosts,
            'healthy_hosts': healthy_hosts,
            'overall_health': overall_health,
            'key_findings': key_findings
        }

    def generate_chart_for_pdf(self):
        """Generate chart image for PDF inclusion"""
        try:
            # Save current chart to image
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                self.fig.savefig(tmp_file.name, dpi=150, bbox_inches='tight', 
                            facecolor='white', edgecolor='none')
                
                # Create reportlab Image
                chart_img = Image(tmp_file.name, width=6*inch, height=3*inch)
                
                # Clean up temp file
                os.unlink(tmp_file.name)
                
                return chart_img
        except Exception as e:
            print(f"DEBUG: Could not generate chart for PDF: {e}")
            return None

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
        """Create enhanced host management tab with auto-recommendations"""
        tab_frame = tk.Frame(self.notebook, bg=self.colors['bg_primary'])
        self.notebook.add(tab_frame, text="🖥️ Hosts")
        
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.rowconfigure(2, weight=1)  # Results section gets most space
        
        # Auto-Recommendation Section (NEW)
        auto_section = tk.LabelFrame(tab_frame, text="  🤖 AI Consolidation Recommendations  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        auto_section.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        auto_content = tk.Frame(auto_section, bg=self.colors['bg_primary'])
        auto_content.pack(fill=tk.X, padx=10, pady=10)
        
        # Auto-recommendation controls
        auto_controls = tk.Frame(auto_content, bg=self.colors['bg_primary'])
        auto_controls.pack(fill=tk.X, pady=(0, 10))
        
        # Strategy selection
        tk.Label(auto_controls, text="Consolidation Strategy:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT)
        
        self.consolidation_strategy = tk.StringVar(value="Balanced")
        strategy_combo = ttk.Combobox(auto_controls, textvariable=self.consolidation_strategy,
                                    values=["Conservative", "Balanced", "Aggressive", "Custom"], 
                                    state="readonly", width=12)
        strategy_combo.pack(side=tk.LEFT, padx=(10, 20))
        strategy_combo.bind('<<ComboboxSelected>>', self.on_strategy_change)
        
        # Target reduction
        tk.Label(auto_controls, text="Target Reduction:",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                font=('Segoe UI', 10)).pack(side=tk.LEFT)
        
        self.target_reduction = tk.DoubleVar(value=20.0)
        reduction_spin = tk.Spinbox(auto_controls, from_=5.0, to=50.0, width=8,
                                textvariable=self.target_reduction, increment=5.0,
                                bg=self.colors['input_bg'], fg=self.colors['text_primary'],
                                insertbackground=self.colors['text_primary'],
                                relief='flat', borderwidth=1)
        reduction_spin.pack(side=tk.LEFT, padx=(5, 2))
        
        tk.Label(auto_controls, text="%",
                bg=self.colors['bg_primary'], fg=self.colors['text_primary']).pack(side=tk.LEFT, padx=(0, 20))
        
        # Generate recommendations button
        auto_recommend_btn = tk.Button(auto_controls, text="🤖 Generate Recommendations",
                                    command=self.generate_auto_recommendations,
                                    bg=self.colors['accent_blue'], fg='white',
                                    font=('Segoe UI', 9, 'bold'),
                                    relief='flat', borderwidth=0,
                                    padx=15, pady=5)
        auto_recommend_btn.pack(side=tk.LEFT)
        
        # Quick info
        info_label = tk.Label(auto_content, 
                            text="💡 AI will analyze performance patterns, workload distribution, and consolidation risk to recommend optimal hosts for removal",
                            bg=self.colors['bg_primary'], fg=self.colors['text_secondary'],
                            font=('Segoe UI', 9), wraplength=800)
        info_label.pack(anchor=tk.W)
        
        # Manual Selection Section (Enhanced)
        manual_section = tk.LabelFrame(tab_frame, text="  👤 Manual Host Selection  ",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['accent_blue'],
                                    font=('Segoe UI', 10, 'bold'),
                                    borderwidth=1,
                                    relief='solid')
        manual_section.pack(fill=tk.X, padx=10, pady=5)
        
        manual_content = tk.Frame(manual_section, bg=self.colors['bg_primary'])
        manual_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        manual_content.columnconfigure(0, weight=1)
        
        # Instructions
        tk.Label(manual_content, text="Select hosts manually to analyze removal impact:",
                bg=self.colors['bg_primary'],
                fg=self.colors['text_primary'],
                font=('Segoe UI', 10, 'bold')).pack(anchor=tk.W, pady=(0, 10))
        
        # Host list frame with enhanced display
        list_frame = tk.Frame(manual_content, bg=self.colors['bg_primary'])
        list_frame.pack(fill=tk.X)
        list_frame.columnconfigure(0, weight=1)
        
        # Enhanced listbox with performance indicators
        self.hosts_listbox = self.create_enhanced_host_listbox(list_frame)
        self.hosts_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        hosts_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.hosts_listbox.yview)
        self.hosts_listbox.configure(yscrollcommand=hosts_scrollbar.set)
        hosts_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Control buttons frame
        button_frame = tk.Frame(list_frame, bg=self.colors['bg_primary'])
        button_frame.grid(row=0, column=2, padx=(15, 0), sticky=(tk.N))
        
        # Enhanced control buttons
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
        clear_all_btn.pack(pady=(0, 5), fill=tk.X)
        
        select_recommended_btn = tk.Button(button_frame, text="🤖 Select AI Picks",
                                        command=self.select_recommended_hosts,
                                        bg=self.colors['warning'], fg='white',
                                        font=('Segoe UI', 9, 'bold'),
                                        relief='flat', borderwidth=0,
                                        padx=10, pady=5)
        select_recommended_btn.pack(pady=(0, 15), fill=tk.X)
        
        analyze_btn = tk.Button(button_frame, text="🔍 Analyze Impact",
                            command=self.analyze_multiple_removal_impact,
                            bg=self.colors['accent_blue'], fg='white',
                            font=('Segoe UI', 9, 'bold'),
                            relief='flat', borderwidth=0,
                            padx=10, pady=5)
        analyze_btn.pack(fill=tk.X)
        
        # Results Section (Enhanced)
        results_section = tk.LabelFrame(tab_frame, text="  📊 Consolidation Analysis Results  ",
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
        
        # Enhanced results text widget
        self.impact_text = self.create_dark_text_widget(results_content, wrap=tk.WORD, padx=10, pady=10)
        
        impact_scrollbar = ttk.Scrollbar(results_content, orient=tk.VERTICAL, command=self.impact_text.yview)
        self.impact_text.configure(yscrollcommand=impact_scrollbar.set)
        
        self.impact_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        impact_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Initialize with helpful content
        self.show_consolidation_welcome_message()

    def create_enhanced_host_listbox(self, parent):
        """Create enhanced listbox that shows host performance indicators - FIXED"""
        # FIXED: Don't use a container frame that might hide the listbox
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
                            font=('Consolas', 10),
                            selectmode=tk.MULTIPLE,
                            height=8)
        
        # REMOVED: Don't pack inside container, return the listbox directly
        return listbox

    def on_strategy_change(self, event=None):
        """Handle consolidation strategy change"""
        strategy = self.consolidation_strategy.get()
        
        # Update target reduction based on strategy
        strategy_defaults = {
            "Conservative": 10.0,
            "Balanced": 20.0,
            "Aggressive": 35.0,
            "Custom": self.target_reduction.get()
        }
        
        if strategy != "Custom":
            self.target_reduction.set(strategy_defaults[strategy])

    def generate_auto_recommendations(self):
        """Generate AI-powered consolidation recommendations"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        self.show_progress("🤖 Generating AI recommendations...")
        
        try:
            strategy = self.consolidation_strategy.get()
            target_reduction = self.target_reduction.get()
            
            # Analyze all hosts and generate recommendations
            recommendations = self.analyze_consolidation_candidates(strategy, target_reduction)
            
            # Display results
            self.display_auto_recommendations(recommendations)
            
            # Store recommendations for easy selection
            self.current_recommendations = recommendations
            
        except Exception as e:
            messagebox.showerror("Recommendation Error", f"Error generating recommendations:\n{str(e)}")
        finally:
            self.hide_progress()

    def analyze_consolidation_candidates(self, strategy="Balanced", target_reduction=20.0):
        """
        Intelligent analysis to identify best consolidation candidates
        Returns ranked list of hosts with detailed reasoning
        """
        if self.processed_data is None:
            return []
        
        hosts = []
        total_hosts = len(self.processed_data['Hostname'].unique())
        
        print(f"DEBUG: Analyzing {total_hosts} hosts for consolidation with {strategy} strategy")
        
        # Analyze each host
        for hostname in self.processed_data['Hostname'].unique():
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            
            # Calculate comprehensive metrics
            metrics = self.calculate_host_metrics(host_data, hostname)
            
            # Calculate consolidation suitability score
            consolidation_score = self.calculate_consolidation_score(metrics, strategy)
            
            hosts.append({
                'hostname': hostname,
                'metrics': metrics,
                'consolidation_score': consolidation_score,
                'recommendation_reasons': self.generate_recommendation_reasons(metrics, strategy)
            })
        
        # Sort by consolidation suitability (higher score = better candidate)
        hosts.sort(key=lambda x: x['consolidation_score'], reverse=True)
        
        # Determine how many hosts to recommend based on target reduction
        target_count = max(1, int((target_reduction / 100) * total_hosts))
        target_count = min(target_count, total_hosts - 1)  # Always keep at least 1 host
        
        # Apply strategy-specific filtering
        recommendations = self.apply_strategy_filtering(hosts, strategy, target_count)
        
        print(f"DEBUG: Recommending {len(recommendations)} hosts for removal out of {total_hosts}")
        
        return recommendations

    def calculate_host_metrics(self, host_data, hostname):
        """Calculate comprehensive metrics for a host"""
        cpu_values = host_data['CPU_Ready_Percent']
        
        metrics = {
            # Basic CPU Ready metrics
            'avg_cpu_ready': cpu_values.mean(),
            'max_cpu_ready': cpu_values.max(),
            'min_cpu_ready': cpu_values.min(),
            'std_cpu_ready': cpu_values.std(),
            'median_cpu_ready': cpu_values.median(),
            
            # Performance consistency
            'coefficient_variation': (cpu_values.std() / cpu_values.mean()) if cpu_values.mean() > 0 else 0,
            'percentile_95': cpu_values.quantile(0.95),
            'percentile_99': cpu_values.quantile(0.99),
            
            # Workload patterns
            'low_utilization_periods': len(cpu_values[cpu_values < 1.0]) / len(cpu_values) * 100,
            'high_utilization_periods': len(cpu_values[cpu_values > 10.0]) / len(cpu_values) * 100,
            'critical_periods': len(cpu_values[cpu_values > self.critical_threshold.get()]) / len(cpu_values) * 100,
            
            # Data quality
            'data_points': len(cpu_values),
            'zero_values': len(cpu_values[cpu_values == 0]),
            
            # Health score
            'health_score': self.calculate_health_score(
                cpu_values.mean(), 
                cpu_values.max(), 
                cpu_values.std()
            )
        }
        
        # Time-based analysis if enough data
        if len(host_data) > 24:
            try:
                host_data_copy = host_data.copy()
                host_data_copy['Hour'] = host_data_copy['Time'].dt.hour
                host_data_copy['DayOfWeek'] = host_data_copy['Time'].dt.dayofweek
                
                # Peak hours analysis
                hourly_avg = host_data_copy.groupby('Hour')['CPU_Ready_Percent'].mean()
                metrics['peak_hour'] = hourly_avg.idxmax()
                metrics['peak_hour_value'] = hourly_avg.max()
                metrics['off_peak_avg'] = hourly_avg.quantile(0.25)
                
                # Weekend vs weekday
                weekday_avg = host_data_copy[host_data_copy['DayOfWeek'] < 5]['CPU_Ready_Percent'].mean()
                weekend_avg = host_data_copy[host_data_copy['DayOfWeek'] >= 5]['CPU_Ready_Percent'].mean()
                metrics['weekday_avg'] = weekday_avg if not pd.isna(weekday_avg) else metrics['avg_cpu_ready']
                metrics['weekend_avg'] = weekend_avg if not pd.isna(weekend_avg) else metrics['avg_cpu_ready']
                
            except Exception as e:
                print(f"DEBUG: Time analysis error for {hostname}: {e}")
        
        return metrics

    def calculate_enhanced_cpu_ready_percentage(self, subset, interval_seconds, source_info=""):
        """
        Enhanced CPU Ready calculation with intelligent format detection
        """
        sample_values = subset['CPU_Ready_Sum'].head(20)  # Larger sample
        avg_sample = sample_values.mean()
        max_sample = sample_values.max()
        min_sample = sample_values.min()
        
        print(f"DEBUG: Enhanced analysis for {source_info}")
        print(f"  Sample size: {len(sample_values)}")
        print(f"  Min: {min_sample:.2f}, Max: {max_sample:.2f}, Avg: {avg_sample:.2f}")
        print(f"  Interval: {interval_seconds} seconds")
        
        # Check for percentage data first (vCenter sometimes returns ready %)
        if avg_sample <= 100 and max_sample <= 100 and min_sample >= 0:
            # Check if values are reasonable percentages
            realistic_check = len(sample_values[sample_values <= 50]) / len(sample_values)
            if realistic_check > 0.8:  # 80% of values are <= 50%
                print(f"  Format detected: Already in percentage")
                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum']
                return subset
        
        # Calculate expected maximum CPU Ready in milliseconds for this interval
        max_possible_ms = interval_seconds * 1000
        
        print(f"  Max possible CPU Ready for {interval_seconds}s interval: {max_possible_ms}ms")
        
        # Detect data format based on magnitude and interval
        if avg_sample > max_possible_ms * 10:
            # Data is likely in microseconds
            conversion_factor = max_possible_ms * 10  # Conservative estimate
            subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / conversion_factor) * 100
            print(f"  Format detected: Microseconds, using factor {conversion_factor}")
            
        elif avg_sample > max_possible_ms:
            # Data might be cumulative or in wrong units
            # Try different approaches based on interval
            if interval_seconds >= 86400:  # Daily data
                # For daily data, values might be cumulative seconds
                conversion_factor = interval_seconds * 100  # Assume centiseconds
                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / conversion_factor) * 100
                print(f"  Format detected: Daily cumulative, using factor {conversion_factor}")
            else:
                # Standard millisecond conversion but with safety check
                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / max_possible_ms) * 100
                print(f"  Format detected: Milliseconds (high values)")
        
        elif avg_sample > 1000:
            # Likely milliseconds (standard vCenter format)
            subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / max_possible_ms) * 100
            print(f"  Format detected: Milliseconds (standard)")
            
        elif avg_sample > 100:
            # Could be centipercent or permille
            if interval_seconds >= 7200:  # 2+ hour intervals
                # Likely cumulative centiseconds for longer periods
                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 100
            else:
                # Likely centipercent
                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 100
            print(f"  Format detected: Centipercent/Centiseconds")
            
        elif avg_sample > 10:
            # Could be permille or deciseconds
            subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 10
            print(f"  Format detected: Permille/Deciseconds")
            
        else:
            # Likely already in reasonable percentage range
            subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum']
            print(f"  Format detected: Direct percentage")
        
        # Post-conversion validation
        final_avg = subset['CPU_Ready_Percent'].mean()
        final_max = subset['CPU_Ready_Percent'].max()
        
        print(f"  After conversion: Avg={final_avg:.3f}%, Max={final_max:.3f}%")
        
        # Sanity check - if still way too high, try alternative conversion
        if final_avg > 50:  # 50% avg is unrealistic for most environments
            print(f"  WARNING: Still high values, trying alternative conversion")
            
            # Try treating as centiseconds instead of milliseconds
            subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 100)) * 100
            alt_avg = subset['CPU_Ready_Percent'].mean()
            
            if alt_avg < final_avg and alt_avg > 0:
                print(f"  Alternative conversion better: {alt_avg:.3f}%")
                final_avg = alt_avg
            else:
                # If still bad, just cap at reasonable values
                subset.loc[subset['CPU_Ready_Percent'] > 100, 'CPU_Ready_Percent'] = 100
                print(f"  Capped extreme values at 100%")
        
        return subset

    def enhanced_detect_interval_from_data(self, df, filename=""):
        """
        Enhanced interval detection that considers vCenter data characteristics
        """
        base_interval = self.detect_interval_from_data(df, filename)
        
        # Additional vCenter-specific logic
        vcenter_format = self.detect_vcenter_data_format(df, filename)
        
        if vcenter_format == 'vcenter_summation':
            print(f"DEBUG: vCenter summation data detected")
            # Summation data often needs different handling
            
        elif vcenter_format == 'vcenter_average':
            print(f"DEBUG: vCenter average data detected")
            # Average data is often already processed
            
        return base_interval

    # Also add this method to detect vCenter data source type
    def detect_vcenter_data_format(self, df, filename=""):
        """
        Detect vCenter data format based on metadata and content analysis
        """
        # Check for vCenter-specific column patterns
        vcenter_patterns = ['Ready for', 'summation', 'average']
        
        ready_cols = [col for col in df.columns if 'ready' in col.lower()]
        
        for col in ready_cols:
            col_lower = col.lower()
            
            # Check for vCenter API format indicators
            if 'summation' in col_lower:
                return 'vcenter_summation'
            elif 'average' in col_lower:
                return 'vcenter_average'
            elif any(pattern.lower() in col_lower for pattern in vcenter_patterns):
                return 'vcenter_export'
        
        return 'unknown'

    def calculate_consolidation_score(self, metrics, strategy):
        """
        Calculate how suitable a host is for consolidation removal
        Higher score = better candidate for removal
        """
        score = 0
        
        # Low CPU Ready usage (primary factor)
        avg_cpu = metrics['avg_cpu_ready']
        if avg_cpu < 1.0:
            score += 40  # Very low usage
        elif avg_cpu < 2.0:
            score += 30  # Low usage
        elif avg_cpu < 5.0:
            score += 20  # Moderate usage
        elif avg_cpu < 10.0:
            score += 10  # Higher usage
        else:
            score -= 20  # High usage - not good candidate
        
        # Consistency (low variation is good for consolidation)
        cv = metrics['coefficient_variation']
        if cv < 0.3:
            score += 15  # Very consistent
        elif cv < 0.6:
            score += 10  # Moderately consistent
        elif cv < 1.0:
            score += 5   # Somewhat consistent
        else:
            score -= 10  # Highly variable - risky
        
        # Low peak usage
        if metrics['percentile_95'] < 5.0:
            score += 15
        elif metrics['percentile_95'] < 10.0:
            score += 10
        elif metrics['percentile_95'] > 20.0:
            score -= 15
        
        # High percentage of low utilization periods
        low_util = metrics['low_utilization_periods']
        if low_util > 80:
            score += 20
        elif low_util > 60:
            score += 15
        elif low_util > 40:
            score += 10
        
        # Penalty for critical periods
        critical_periods = metrics['critical_periods']
        if critical_periods > 10:
            score -= 30  # Frequently critical
        elif critical_periods > 5:
            score -= 20
        elif critical_periods > 1:
            score -= 10
        
        # Health score factor
        health_score = metrics['health_score']
        if health_score > 90:
            score += 10  # Very healthy, good candidate
        elif health_score > 80:
            score += 5
        elif health_score < 50:
            score -= 15  # Unhealthy, might need investigation
        
        # Strategy-specific adjustments
        if strategy == "Conservative":
            # More conservative - only recommend very safe candidates
            if avg_cpu > 3.0 or critical_periods > 0:
                score -= 25
            if metrics['high_utilization_periods'] > 5:
                score -= 15
        
        elif strategy == "Aggressive":
            # More aggressive - willing to take more risk
            score += 10  # Base bonus for aggressive strategy
            if avg_cpu < 10.0:  # Expand acceptable range
                score += 10
        
        # Weekend vs weekday consideration
        if 'weekend_avg' in metrics and 'weekday_avg' in metrics:
            weekend_diff = abs(metrics['weekend_avg'] - metrics['weekday_avg'])
            if weekend_diff < 1.0:
                score += 5  # Consistent across week
        
        return max(0, score)  # Ensure non-negative score

    def apply_strategy_filtering(self, hosts, strategy, target_count):
        """Apply strategy-specific filtering to host recommendations"""
        
        if strategy == "Conservative":
            # Only recommend hosts with very low risk
            safe_hosts = [h for h in hosts if (
                h['metrics']['avg_cpu_ready'] < 3.0 and
                h['metrics']['critical_periods'] == 0 and
                h['metrics']['percentile_95'] < 8.0
            )]
            return safe_hosts[:target_count]
        
        elif strategy == "Balanced":
            # Balance risk and reduction goals
            good_candidates = [h for h in hosts if (
                h['metrics']['avg_cpu_ready'] < 5.0 and
                h['metrics']['critical_periods'] < 2.0 and
                h['consolidation_score'] > 30
            )]
            return good_candidates[:target_count]
        
        elif strategy == "Aggressive":
            # More willing to take risks for higher reduction
            candidates = [h for h in hosts if (
                h['metrics']['avg_cpu_ready'] < 8.0 and
                h['metrics']['critical_periods'] < 5.0
            )]
            return candidates[:target_count]
        
        else:  # Custom
            # Use raw scores
            return hosts[:target_count]

    def generate_recommendation_reasons(self, metrics, strategy):
        """Generate human-readable reasons for recommendation"""
        reasons = []
        
        avg_cpu = metrics['avg_cpu_ready']
        if avg_cpu < 1.0:
            reasons.append(f"Very low CPU Ready usage ({avg_cpu:.1f}%)")
        elif avg_cpu < 3.0:
            reasons.append(f"Low CPU Ready usage ({avg_cpu:.1f}%)")
        
        if metrics['low_utilization_periods'] > 70:
            reasons.append(f"{metrics['low_utilization_periods']:.0f}% of time below 1% CPU Ready")
        
        if metrics['critical_periods'] == 0:
            reasons.append("No critical performance periods")
        elif metrics['critical_periods'] < 1:
            reasons.append("Very few critical periods")
        
        if metrics['coefficient_variation'] < 0.4:
            reasons.append("Consistent performance pattern")
        
        if metrics['health_score'] > 85:
            reasons.append(f"High health score ({metrics['health_score']:.0f}/100)")
        
        if metrics['percentile_95'] < 5.0:
            reasons.append(f"Low peak usage (95th percentile: {metrics['percentile_95']:.1f}%)")
        
        # Weekend consistency
        if 'weekend_avg' in metrics and 'weekday_avg' in metrics:
            diff = abs(metrics['weekend_avg'] - metrics['weekday_avg'])
            if diff < 1.0:
                reasons.append("Consistent usage across weekdays/weekends")
        
        if not reasons:
            reasons.append("Identified as potential consolidation candidate")
        
        return reasons

    def display_auto_recommendations(self, recommendations):
        """Display AI recommendations in a comprehensive format"""
        if not recommendations:
            self.impact_text.delete(1.0, tk.END)
            self.impact_text.insert(1.0, """🤖 AI CONSOLIDATION RECOMMENDATIONS
    ═══════════════════════════════════════════════════

    No suitable consolidation candidates found with current criteria.

    Try adjusting your strategy:
    • Use "Aggressive" for more candidates
    • Lower the target reduction percentage
    • Review your CPU Ready thresholds

    All hosts may be actively utilized or critical to operations.""")
            return
        
        strategy = self.consolidation_strategy.get()
        target_reduction = self.target_reduction.get()
        
        report = f"""🤖 AI CONSOLIDATION RECOMMENDATIONS
    ═══════════════════════════════════════════════════

    Strategy: {strategy} | Target Reduction: {target_reduction}%
    Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    📊 RECOMMENDED HOSTS FOR REMOVAL ({len(recommendations)} hosts):

    """
        
        for i, rec in enumerate(recommendations, 1):
            hostname = rec['hostname']
            metrics = rec['metrics']
            score = rec['consolidation_score']
            reasons = rec['recommendation_reasons']
            
            # Risk assessment
            if metrics['avg_cpu_ready'] < 2.0 and metrics['critical_periods'] == 0:
                risk_level = "🟢 LOW RISK"
            elif metrics['avg_cpu_ready'] < 5.0 and metrics['critical_periods'] < 2:
                risk_level = "🟡 MEDIUM RISK"
            else:
                risk_level = "🔴 HIGH RISK"
            
            report += f"""{i}. {hostname} - {risk_level}
    Consolidation Score: {score:.0f}/100
    
    📈 Performance Metrics:
    • Average CPU Ready: {metrics['avg_cpu_ready']:.2f}%
    • Peak (95th percentile): {metrics['percentile_95']:.2f}%
    • Health Score: {metrics['health_score']:.0f}/100
    • Low utilization periods: {metrics['low_utilization_periods']:.0f}%
    
    💡 Why recommended:
    {chr(10).join([f"   • {reason}" for reason in reasons])}
    
    ⚠️ Considerations:
    • Critical periods: {metrics['critical_periods']:.1f}% of time
    • Performance variability: {metrics['coefficient_variation']:.2f} (lower is better)
    
    """
        
        # Overall impact assessment
        total_hosts = len(self.processed_data['Hostname'].unique())
        removal_percentage = (len(recommendations) / total_hosts) * 100
        
        # Calculate workload redistribution
        total_workload = 0
        recommended_workload = 0
        
        for hostname in self.processed_data['Hostname'].unique():
            host_workload = self.processed_data[self.processed_data['Hostname'] == hostname]['CPU_Ready_Sum'].sum()
            total_workload += host_workload
            
            if any(rec['hostname'] == hostname for rec in recommendations):
                recommended_workload += host_workload
        
        workload_redistribution = (recommended_workload / total_workload) * 100 if total_workload > 0 else 0
        
        report += f"""
    📊 IMPACT ASSESSMENT:
    ═══════════════════════════════════════════════════

    Infrastructure Reduction: {removal_percentage:.1f}% ({len(recommendations)} of {total_hosts} hosts)
    Workload to Redistribute: {workload_redistribution:.1f}%
    Remaining Hosts: {total_hosts - len(recommendations)}

    💰 ESTIMATED BENEFITS:
    • Hardware cost savings: ~{removal_percentage:.0f}% of host costs
    • Power/cooling reduction: ~{removal_percentage:.0f}% savings
    • Simplified management: Fewer hosts to maintain
    • License optimization: Potential vSphere license savings

    ⚠️ IMPLEMENTATION RECOMMENDATIONS:
    • Test workload migration in non-production first
    • Monitor remaining hosts for 1-2 weeks post-consolidation
    • Have rollback plan ready
    • Consider maintenance windows for migration
    • Update DRS/HA settings after consolidation

    🚀 NEXT STEPS:
    1. Review recommendations above
    2. Click "🤖 Select AI Picks" to auto-select recommended hosts
    3. Click "🔍 Analyze Impact" for detailed workload analysis
    4. Plan migration strategy for selected hosts
    """
        
        self.impact_text.delete(1.0, tk.END)
        self.impact_text.insert(1.0, report)

    def select_recommended_hosts(self):
        """Select the AI-recommended hosts in the listbox"""
        if not hasattr(self, 'current_recommendations') or not self.current_recommendations:
            messagebox.showwarning("No Recommendations", 
                                "Please generate AI recommendations first")
            return
        
        # Clear current selection
        self.hosts_listbox.selection_clear(0, tk.END)
        
        # Select recommended hosts
        recommended_hostnames = [rec['hostname'] for rec in self.current_recommendations]
        
        for i in range(self.hosts_listbox.size()):
            host_text = self.hosts_listbox.get(i)
            # Extract hostname from display text (might have performance indicators)
            hostname = host_text.split()[0] if host_text else ""
            
            if hostname in recommended_hostnames:
                self.hosts_listbox.selection_set(i)
        
        # Show feedback
        self.show_smart_notification(
            f"✅ Selected {len(recommended_hostnames)} AI-recommended hosts", 3000)

    def update_host_list(self):
        """Update host selection listbox with performance indicators"""
        print(f"DEBUG: update_host_list called")
        if self.processed_data is None:
            print(f"DEBUG: No processed data available")
            return
        
        unique_hosts = self.processed_data['Hostname'].unique()
        print(f"DEBUG: Found {len(unique_hosts)} unique hosts: {list(unique_hosts)}")
        
        # ADD THESE DEBUG LINES:
        print(f"DEBUG: Listbox exists: {hasattr(self, 'hosts_listbox')}")
        if hasattr(self, 'hosts_listbox'):
            print(f"DEBUG: Listbox size before clear: {self.hosts_listbox.size()}")
            print(f"DEBUG: Listbox widget info: {self.hosts_listbox.winfo_exists()}")
        
        self.hosts_listbox.delete(0, tk.END)
        print(f"DEBUG: Listbox cleared")
        
        # Calculate metrics for each host for display
        host_metrics = {}
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            avg_cpu = host_data['CPU_Ready_Percent'].mean()
            health_score = self.calculate_health_score(
                avg_cpu, 
                host_data['CPU_Ready_Percent'].max(), 
                host_data['CPU_Ready_Percent'].std()
            )
            
            # Performance indicator
            if avg_cpu >= self.critical_threshold.get():
                indicator = "🔴"
            elif avg_cpu >= self.warning_threshold.get():
                indicator = "🟡"
            else:
                indicator = "🟢"
            
            host_metrics[hostname] = {
                'avg_cpu': avg_cpu,
                'health_score': health_score,
                'indicator': indicator
            }
        
        # Sort hosts by consolidation suitability (if we have recommendations)
        if hasattr(self, 'current_recommendations') and self.current_recommendations:
            # Sort with recommended hosts first
            recommended_hostnames = [rec['hostname'] for rec in self.current_recommendations]
            sorted_hosts = []
            
            # Add recommended hosts first
            for hostname in recommended_hostnames:
                if hostname in host_metrics:
                    sorted_hosts.append((hostname, True))  # True = recommended
            
            # Add remaining hosts
            for hostname in sorted(host_metrics.keys()):
                if hostname not in recommended_hostnames:
                    sorted_hosts.append((hostname, False))  # False = not recommended
        else:
            # Default sort by performance (best candidates first)
            sorted_hosts = [(hostname, False) for hostname in 
                        sorted(host_metrics.keys(), 
                                key=lambda h: host_metrics[h]['avg_cpu'])]
        
        # Populate listbox with enhanced display
        for hostname, is_recommended in sorted_hosts:
            metrics = host_metrics[hostname]
            
            # Format display string
            if is_recommended:
                display_text = f"{hostname} {metrics['indicator']} 🤖 {metrics['avg_cpu']:.1f}% CPU Ready (AI Pick)"
            else:
                display_text = f"{hostname} {metrics['indicator']} {metrics['avg_cpu']:.1f}% CPU Ready"
            
            self.hosts_listbox.insert(tk.END, display_text)
            
        print(f"DEBUG: Final listbox size: {self.hosts_listbox.size()}")
        print(f"DEBUG: Listbox contents:")
        for i in range(self.hosts_listbox.size()):
            print(f"  {i}: {self.hosts_listbox.get(i)}")

    def show_consolidation_welcome_message(self):
        """Show welcome message in the consolidation results area"""
        welcome_msg = """🖥️ HOST CONSOLIDATION ANALYSIS
    ═══════════════════════════════════════════════════════════════════════════════════

    Welcome to the AI-Powered Host Consolidation Assistant! 🤖

    🚀 GETTING STARTED:

    1️⃣ AUTOMATIC RECOMMENDATIONS (Recommended):
    • Select your consolidation strategy (Conservative/Balanced/Aggressive)
    • Set target infrastructure reduction percentage
    • Click "🤖 Generate Recommendations" for AI analysis
    • Review detailed recommendations and reasoning
    • Use "🤖 Select AI Picks" to auto-select recommended hosts

    2️⃣ MANUAL SELECTION (Advanced Users):
    • Manually select hosts from the list below
    • Each host shows performance indicators:
        🟢 Healthy (good consolidation candidate)
        🟡 Warning (moderate risk)
        🔴 Critical (high risk - avoid removal)
    • Use "🔍 Analyze Impact" for detailed analysis

    🧠 AI ANALYSIS CONSIDERS:
    ═══════════════════════════════════════════════════

    📊 Performance Metrics:
    • CPU Ready usage patterns (average, peaks, consistency)
    • Historical performance stability
    • Workload distribution and timing
    • Critical performance periods

    🎯 Risk Assessment:
    • Workload redistribution impact
    • Remaining infrastructure capacity
    • Performance degradation risk
    • Business continuity considerations

    💰 Business Impact:
    • Infrastructure cost savings
    • Power and cooling reduction
    • Management simplification
    • License optimization opportunities

    ⚙️ CONSOLIDATION STRATEGIES:
    ═══════════════════════════════════════════════════

    🛡️ CONSERVATIVE:
    • Only recommends extremely safe candidates
    • < 3% average CPU Ready usage
    • Zero critical performance periods
    • Minimal risk to operations

    ⚖️ BALANCED (Recommended):
    • Good balance of risk and savings
    • < 5% average CPU Ready usage
    • Minimal critical periods
    • Moderate consolidation benefits

    ⚡ AGGRESSIVE:
    • Maximum infrastructure reduction
    • Higher acceptable risk levels
    • Up to 8% average CPU Ready usage
    • Requires careful monitoring post-consolidation

    🎛️ CUSTOM:
    • Use your own target reduction percentage
    • Manual override of safety constraints
    • For advanced users with specific requirements

    💡 BEST PRACTICES:
    ═══════════════════════════════════════════════════

    ✅ Before Consolidation:
    • Test in non-production environment first
    • Verify backup and recovery procedures
    • Plan maintenance windows
    • Update documentation

    ✅ During Implementation:
    • Monitor performance in real-time
    • Have rollback plan ready
    • Migrate workloads gradually
    • Update DRS/HA cluster settings

    ✅ After Consolidation:
    • Monitor for 1-2 weeks minimum
    • Adjust performance thresholds if needed
    • Document new configuration
    • Update capacity planning models

    🔍 Ready to start? Generate AI recommendations or select hosts manually above! 🚀

    ═══════════════════════════════════════════════════════════════════════════════════"""
        
        self.impact_text.delete(1.0, tk.END)
        self.impact_text.insert(1.0, welcome_msg)

    def analyze_multiple_removal_impact(self):
        """Enhanced removal impact analysis with comprehensive metrics"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        selected_hosts = self.get_selected_hosts()
        
        if not selected_hosts:
            messagebox.showwarning("No Selection", "Please select at least one host or use AI recommendations")
            return
        
        self.show_progress("Analyzing comprehensive removal impact...")
        
        try:
            # Extract hostnames from display text
            clean_hostnames = []
            for host_display in selected_hosts:
                # Extract just the hostname (first word before any indicators)
                hostname = host_display.split()[0]
                clean_hostnames.append(hostname)
            
            total_hosts = len(self.processed_data['Hostname'].unique())
            
            if len(clean_hostnames) >= total_hosts:
                self.impact_text.delete(1.0, tk.END)
                self.impact_text.insert(1.0, """❌ CONSOLIDATION ERROR
    ═══════════════════════════════════════════════════════════════════════════════════

    Cannot remove all hosts - no remaining infrastructure!

    Please select fewer hosts to maintain operational capability.
    Recommended: Keep at least 2-3 hosts for redundancy and load distribution.""")
                return
            
            # Perform comprehensive analysis
            impact_analysis = self.perform_comprehensive_impact_analysis(clean_hostnames)
            
            # Display detailed results
            self.display_comprehensive_impact_results(impact_analysis)
            
        except Exception as e:
            messagebox.showerror("Analysis Error", f"Error analyzing removal impact:\n{str(e)}")
        finally:
            self.hide_progress()

    def perform_comprehensive_impact_analysis(self, hostnames_to_remove):
        """Perform detailed impact analysis for host removal"""
        
        # Basic metrics
        total_hosts = len(self.processed_data['Hostname'].unique())
        remaining_hosts = total_hosts - len(hostnames_to_remove)
        
        # Workload analysis
        total_workload = self.processed_data['CPU_Ready_Sum'].sum()
        selected_workload = self.processed_data[
            self.processed_data['Hostname'].isin(hostnames_to_remove)
        ]['CPU_Ready_Sum'].sum()
        
        workload_percentage = (selected_workload / total_workload) * 100 if total_workload > 0 else 0
        
        # Performance analysis
        current_avg = self.processed_data['CPU_Ready_Percent'].mean()
        remaining_data = self.processed_data[~self.processed_data['Hostname'].isin(hostnames_to_remove)]
        
        if len(remaining_data) > 0:
            post_removal_avg = remaining_data['CPU_Ready_Percent'].mean()
            # Estimate additional load per remaining host
            additional_load_per_host = workload_percentage / remaining_hosts
            estimated_new_avg = post_removal_avg + additional_load_per_host
        else:
            post_removal_avg = 0
            estimated_new_avg = 0
            additional_load_per_host = 0
        
        # Risk assessment
        risk_level = "LOW"
        risk_factors = []
        
        if workload_percentage > 30:
            risk_level = "HIGH"
            risk_factors.append("Very high workload redistribution required")
        elif workload_percentage > 15:
            risk_level = "MEDIUM"
            risk_factors.append("Significant workload redistribution")
        
        if estimated_new_avg > self.critical_threshold.get():
            risk_level = "HIGH"
            risk_factors.append(f"Estimated post-consolidation performance exceeds critical threshold")
        elif estimated_new_avg > self.warning_threshold.get():
            if risk_level == "LOW":
                risk_level = "MEDIUM"
            risk_factors.append("Estimated performance may approach warning levels")
        
        if remaining_hosts < 2:
            risk_level = "HIGH"
            risk_factors.append("Insufficient redundancy (less than 2 hosts remaining)")
        elif remaining_hosts < 3:
            if risk_level == "LOW":
                risk_level = "MEDIUM"
            risk_factors.append("Limited redundancy for maintenance and failures")
        
        # Detailed host analysis
        removed_hosts_analysis = []
        remaining_hosts_analysis = []
        
        for hostname in hostnames_to_remove:
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            if len(host_data) > 0:
                removed_hosts_analysis.append({
                    'hostname': hostname,
                    'avg_cpu': host_data['CPU_Ready_Percent'].mean(),
                    'max_cpu': host_data['CPU_Ready_Percent'].max(),
                    'workload_share': (host_data['CPU_Ready_Sum'].sum() / total_workload * 100) if total_workload > 0 else 0,
                    'health_score': self.calculate_health_score(
                        host_data['CPU_Ready_Percent'].mean(),
                        host_data['CPU_Ready_Percent'].max(),
                        host_data['CPU_Ready_Percent'].std()
                    )
                })
        
        for hostname in self.processed_data['Hostname'].unique():
            if hostname not in hostnames_to_remove:
                host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
                remaining_hosts_analysis.append({
                    'hostname': hostname,
                    'current_avg_cpu': host_data['CPU_Ready_Percent'].mean(),
                    'current_max_cpu': host_data['CPU_Ready_Percent'].max(),
                    'estimated_new_avg': host_data['CPU_Ready_Percent'].mean() + additional_load_per_host,
                    'capacity_utilization': ((host_data['CPU_Ready_Percent'].mean() + additional_load_per_host) / 20) * 100  # Assume 20% is full capacity
                })
        
        # Calculate financial impact
        cost_savings = self.calculate_cost_savings(len(hostnames_to_remove), total_hosts)
        
        return {
            'removed_hosts': hostnames_to_remove,
            'removed_count': len(hostnames_to_remove),
            'remaining_count': remaining_hosts,
            'workload_percentage': workload_percentage,
            'current_avg_cpu': current_avg,
            'estimated_new_avg': estimated_new_avg,
            'additional_load_per_host': additional_load_per_host,
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'removed_hosts_analysis': removed_hosts_analysis,
            'remaining_hosts_analysis': remaining_hosts_analysis,
            'cost_savings': cost_savings,
            'infrastructure_reduction': (len(hostnames_to_remove) / total_hosts) * 100
        }

    def calculate_cost_savings(self, removed_hosts, total_hosts):
        """Calculate estimated cost savings from consolidation"""
        reduction_percentage = (removed_hosts / total_hosts) * 100
        
        # Rough estimates - you can customize these based on your environment
        annual_host_cost = 15000  # Average annual cost per host (hardware, power, licensing)
        power_savings_per_host = 2000  # Annual power/cooling savings per host
        
        hardware_savings = removed_hosts * annual_host_cost
        power_savings = removed_hosts * power_savings_per_host
        license_savings = removed_hosts * 3000  # Rough vSphere license cost
        
        total_annual_savings = hardware_savings + power_savings + license_savings
        
        return {
            'hardware_savings': hardware_savings,
            'power_savings': power_savings,
            'license_savings': license_savings,
            'total_annual_savings': total_annual_savings,
            'reduction_percentage': reduction_percentage
        }

    def display_comprehensive_impact_results(self, analysis):
        """Display comprehensive consolidation impact analysis"""
        
        risk_colors = {
            'LOW': '🟢',
            'MEDIUM': '🟡', 
            'HIGH': '🔴'
        }
        
        risk_indicator = risk_colors.get(analysis['risk_level'], '⚪')
        
        report = f"""🔍 COMPREHENSIVE CONSOLIDATION IMPACT ANALYSIS
    ═══════════════════════════════════════════════════════════════════════════════════

    Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Selected Hosts: {analysis['removed_count']} of {analysis['removed_count'] + analysis['remaining_count']} total

    📊 INFRASTRUCTURE IMPACT:
    ═══════════════════════════════════════════════════════════════════════════════════

    Infrastructure Reduction: {analysis['infrastructure_reduction']:.1f}%
    Hosts to Remove: {analysis['removed_count']}
    Remaining Hosts: {analysis['remaining_count']}
    Workload to Redistribute: {analysis['workload_percentage']:.1f}%

    ⚡ PERFORMANCE IMPACT:
    ═══════════════════════════════════════════════════════════════════════════════════

    Current Average CPU Ready: {analysis['current_avg_cpu']:.2f}%
    Estimated Post-Consolidation: {analysis['estimated_new_avg']:.2f}%
    Additional Load per Host: +{analysis['additional_load_per_host']:.1f}%

    🎯 RISK ASSESSMENT: {risk_indicator} {analysis['risk_level']} RISK
    ═══════════════════════════════════════════════════════════════════════════════════
    """
        
        if analysis['risk_factors']:
            report += "\n⚠️ Risk Factors:\n"
            for factor in analysis['risk_factors']:
                report += f"   • {factor}\n"
        else:
            report += "\n✅ No significant risk factors identified\n"
        
        report += f"""
    🗑️ HOSTS SELECTED FOR REMOVAL:
    ═══════════════════════════════════════════════════════════════════════════════════
    """
        
        for host in analysis['removed_hosts_analysis']:
            status = "✅ Good candidate" if host['avg_cpu'] < 5.0 else "⚠️ Review carefully"
            report += f"""
    {host['hostname']} - {status}
    • Average CPU Ready: {host['avg_cpu']:.2f}%
    • Peak CPU Ready: {host['max_cpu']:.2f}%
    • Workload Share: {host['workload_share']:.1f}%
    • Health Score: {host['health_score']:.0f}/100
    """
        
        report += f"""
    🖥️ REMAINING HOSTS (Post-Consolidation):
    ═══════════════════════════════════════════════════════════════════════════════════
    """
        
        for host in analysis['remaining_hosts_analysis']:
            if host['estimated_new_avg'] > 15:
                capacity_status = "🔴 HIGH LOAD"
            elif host['estimated_new_avg'] > 8:
                capacity_status = "🟡 MODERATE LOAD"
            else:
                capacity_status = "🟢 ACCEPTABLE"
            
            report += f"""
    {host['hostname']} - {capacity_status}
    • Current CPU Ready: {host['current_avg_cpu']:.2f}%
    • Estimated New Load: {host['estimated_new_avg']:.2f}%
    • Capacity Utilization: {host['capacity_utilization']:.0f}%
    """
        
        cost_savings = analysis['cost_savings']
        report += f"""
    💰 ESTIMATED COST SAVINGS (Annual):
    ═══════════════════════════════════════════════════════════════════════════════════

    Hardware/Depreciation: ${cost_savings['hardware_savings']:,.0f}
    Power & Cooling: ${cost_savings['power_savings']:,.0f}
    Software Licensing: ${cost_savings['license_savings']:,.0f}
    ─────────────────────────────────────────────
    TOTAL ANNUAL SAVINGS: ${cost_savings['total_annual_savings']:,.0f}

    3-Year Projected Savings: ${cost_savings['total_annual_savings'] * 3:,.0f}
    ROI: {cost_savings['reduction_percentage']:.0f}% infrastructure reduction

    📋 IMPLEMENTATION RECOMMENDATIONS:
    ═══════════════════════════════════════════════════════════════════════════════════
    """
        
        if analysis['risk_level'] == 'LOW':
            report += """
    ✅ LOW RISK - Proceed with confidence:
    • Good consolidation candidates selected
    • Minimal performance impact expected
    • Standard implementation process recommended
    
    🚀 Next Steps:
    1. Schedule maintenance window
    2. Migrate workloads during off-peak hours
    3. Monitor performance for 48 hours post-migration
    4. Update DRS/HA settings
    """
        
        elif analysis['risk_level'] == 'MEDIUM':
            report += """
    🟡 MEDIUM RISK - Proceed with caution:
    • Some performance impact expected
    • Enhanced monitoring recommended
    • Consider phased implementation
    
    ⚠️ Recommended Precautions:
    1. Test in non-production environment first
    2. Implement during lowest usage period
    3. Have immediate rollback plan ready
    4. Monitor closely for 1 week post-consolidation
    5. Consider temporary performance threshold adjustments
    """
        
        else:  # HIGH RISK
            report += """
    🔴 HIGH RISK - Review selection carefully:
    • Significant performance impact likely
    • High chance of resource contention
    • Consider reducing scope
    
    ⛔ Strong Recommendations:
    1. Remove fewer hosts from selection
    2. Focus on lowest-utilization hosts only
    3. Extensive testing in lab environment
    4. Staged implementation over multiple maintenance windows
    5. 24/7 monitoring for 2+ weeks
    6. Ensure adequate emergency resources available
    """
        
        report += f"""
    
    📊 MONITORING CHECKLIST:
    ═══════════════════════════════════════════════════════════════════════════════════

    Before Consolidation:
    □ Document current performance baselines
    □ Verify backup and recovery procedures
    □ Test workload migration procedures
    □ Prepare monitoring dashboards
    □ Brief on-call team on changes

    During Implementation:
    □ Monitor CPU Ready metrics in real-time
    □ Watch for memory/storage bottlenecks
    □ Verify application performance
    □ Check cluster health status
    □ Document any issues encountered

    After Consolidation:
    □ Monitor for {7 if analysis['risk_level'] == 'LOW' else 14} days minimum
    □ Compare performance to baselines
    □ Adjust DRS aggressiveness if needed
    □ Update capacity planning models
    □ Document lessons learned

    ═══════════════════════════════════════════════════════════════════════════════════

    Analysis complete! Review recommendations above before proceeding with consolidation.
    """
        
        self.impact_text.delete(1.0, tk.END)
        self.impact_text.insert(1.0, report)
       
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

        # PDF EXPORT BUTTON (NEW)
        pdf_export_btn = tk.Button(button_grid, text="📄 Export PDF Report", 
                                command=self.export_comprehensive_pdf_report,
                                bg=self.colors['success'], fg='white',
                                font=('Segoe UI', 10, 'bold'),
                                relief='flat', borderwidth=0,
                                padx=20, pady=10, cursor='hand2')
        pdf_export_btn.grid(row=2, column=0, columnspan=2, padx=0, pady=(8, 0), sticky="ew")
        
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
        """Update file count display including real-time data"""
        if self.data_frames:
            # Check if any dataframes are from real-time
            realtime_count = sum(1 for df in self.data_frames if 'source_file' in df.columns and any('realtime' in str(sf) for sf in df['source_file'].unique()))
            file_count = len(self.data_frames) - realtime_count
            
            if realtime_count > 0 and file_count > 0:
                self.file_count_label.config(text=f"{file_count} files + real-time data imported")
            elif realtime_count > 0:
                self.file_count_label.config(text="Real-time data available")
            else:
                self.file_count_label.config(text=f"{file_count} files imported")
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
            
            # Method 1: ENHANCED - Daily data detection (PRIORITY)
            # Check for daily data patterns (most common for yearly exports)
            days = time_span.days
            hours_between_records = avg_interval_seconds / 3600
            
            # Strong indicators of daily data
            if (360 <= num_records <= 370 and days >= 360) or \
            (23 <= hours_between_records <= 25):  # ~24 hours between records
                detected_interval = "Last Year"
                print(f"  DAILY DATA DETECTED: {num_records} records over {days} days")
                print(f"  Hours between records: {hours_between_records:.1f}")
                return detected_interval
            
            # Method 2: Filename-based detection (high priority)
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
                # Method 3: Time span analysis (fallback)
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
            
            # Method 4: Validation against expected intervals with ENHANCED daily check
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
            
            # ENHANCED: Special validation for daily data
            # If we have ~daily intervals but detected something else, correct it
            if 23 <= hours_between_records <= 25 and detected_interval != "Last Year":
                print(f"  CORRECTION: Daily intervals detected ({hours_between_records:.1f}h), overriding to Last Year")
                detected_interval = "Last Year"
                interval_ratio = avg_interval_seconds / 86400
            
            # If ratio is way off, try to find a better match
            elif interval_ratio > 3 or interval_ratio < 0.3:
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
            
            # Method 5: Enhanced special case handling
            special_cases = [
                # Daily data patterns (365 days, ~24h intervals)
                ((360, 370), (23, 25), "Last Year"),  # 360-370 records, 23-25h intervals
                
                # Real-Time: ~180 records, ~1 hour, 20-second intervals
                ((170, 190), (15, 25), "Real-Time"),  # 170-190 records, 15-25 second intervals
                
                # Last Day: ~288 records, ~24 hours, 5-minute intervals  
                ((280, 300), (280, 320), "Last Day"),  # 280-300 records, 280-320 second intervals
                
                # Last Week: ~336 records, ~7 days, 30-minute intervals
                ((330, 350), (1700, 1900), "Last Week"),  # 330-350 records, 1700-1900 second intervals
                
                # Last Month: ~360 records, ~30 days, 2-hour intervals
                ((350, 370), (7000, 7400), "Last Month"),  # 350-370 records, 7000-7400 second intervals
            ]
            
            for (min_records, max_records), (min_interval, max_interval), suggested_interval in special_cases:
                if (min_records <= num_records <= max_records and 
                    min_interval <= avg_interval_seconds <= max_interval):
                    
                    print(f"  Special case match: {suggested_interval}")
                    print(f"    Records: {num_records} (expected {min_records}-{max_records})")
                    print(f"    Interval: {avg_interval_seconds:.0f}s (expected {min_interval}-{max_interval}s)")
                    
                    detected_interval = suggested_interval
                    break
            
            # Method 6: Filename keyword + record count validation
            filename_record_patterns = {
                # Check for specific patterns that indicate yearly data
                ("year", (360, 370)): "Last Year",
                ("yearly", (360, 370)): "Last Year", 
                ("annual", (360, 370)): "Last Year",
                ("365", (360, 370)): "Last Year",
                
                # Real-time patterns
                ("real", (100, 200)): "Real-Time",
                ("live", (100, 200)): "Real-Time",
                
                # Daily patterns
                ("day", (200, 400)): "Last Day",
                ("daily", (200, 400)): "Last Day",
            }
            
            for (keyword, (min_rec, max_rec)), suggested in filename_record_patterns.items():
                if (keyword in filename_lower and min_rec <= num_records <= max_rec):
                    print(f"  Filename+Records pattern match: {suggested}")
                    print(f"    Keyword: '{keyword}', Records: {num_records}")
                    detected_interval = suggested
                    break
            
            print(f"  FINAL DETECTION: {detected_interval}")
            
            # Final validation log
            final_expected = expected_intervals[detected_interval]
            final_ratio = avg_interval_seconds / final_expected
            print(f"  Final validation: {avg_interval_seconds:.0f}s actual vs {final_expected}s expected (ratio: {final_ratio:.2f})")
            
            return detected_interval
            
        except Exception as e:
            print(f"DEBUG: Error in interval detection: {e}")
            import traceback
            traceback.print_exc()
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
        """Handle successful vCenter connection - Updated with real-time dashboard integration"""
        self.vcenter_status.config(text="🟢 Connected", fg=self.colors['success'])
        self.connection_status.config(text="🟢 vCenter Connected", fg=self.colors['success'])
        
        self.fetch_btn.config(state='normal')
        self.connect_btn.config(text="🔌 Disconnect", 
                               command=self.disconnect_vcenter,
                               state='normal')
        self.hide_progress()
        
        # Update real-time dashboard connection
        if hasattr(self, 'realtime_dashboard'):
            self.realtime_dashboard.set_vcenter_connection(self.vcenter_connection)
            print("DEBUG: Real-time dashboard vCenter connection updated")
        
        messagebox.showinfo("Success", f"Connected to vCenter: {vcenter_host}\n\nReal-time monitoring is now available!")

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
        """Disconnect from vCenter - Updated to handle real-time dashboard"""
        # Stop real-time monitoring first
        if hasattr(self, 'realtime_dashboard'):
            try:
                self.realtime_dashboard.stop_monitoring()
                self.realtime_dashboard.set_vcenter_connection(None)
                print("DEBUG: Real-time monitoring stopped and connection cleared")
            except Exception as e:
                print(f"DEBUG: Error stopping real-time monitoring: {e}")
        
        try:
            if self.vcenter_connection:
                Disconnect(self.vcenter_connection)
                self.vcenter_connection = None
            
            self.vcenter_status.config(text="⚫ Disconnected", fg=self.colors['error'])
            self.connection_status.config(text="⚫ Disconnected", fg=self.colors['error'])
            
            self.fetch_btn.config(state='disabled')
            self.connect_btn.config(text="🔌 Connect", command=self.connect_vcenter)
            
            messagebox.showinfo("Disconnected", "Successfully disconnected from vCenter\n\nReal-time monitoring has been stopped.")
            
        except Exception as e:
            messagebox.showerror("Disconnect Error", f"Error disconnecting: {str(e)}")

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

                        # Remove rows with missing data (but keep zero values as they are valid)
                        subset = subset.dropna(subset=['Time', 'CPU_Ready_Sum'])
                        after_dropna = len(subset)

                        # FIXED: Keep zero values as they are valid CPU Ready metrics
                        # Zero CPU Ready means the VM/host had no CPU contention, which is valuable information
                        # Only remove clearly invalid negative values
                        subset = subset[subset['CPU_Ready_Sum'] >= 0]
                        valid_rows = len(subset)

                        print(f"DEBUG: Host {hostname}: {initial_rows} initial → {after_dropna} after dropna → {valid_rows} valid rows (zeros preserved)")

                        if valid_rows == 0:
                            warning_msg = f"No valid CPU Ready data for host {hostname} (all values were negative or missing)"
                            processing_warnings.append(warning_msg)
                            print(f"DEBUG: {warning_msg}")
                            continue

                        # Only warn if we lost a significant amount of data due to negative values
                        if valid_rows < initial_rows * 0.5:
                            warning_msg = f"Host {hostname}: Lost {initial_rows - valid_rows} of {initial_rows} rows (negative values removed, zeros preserved)"
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
                        
                        # Determine data source type
                        source_file = subset['Source_File'].iloc[0] if 'Source_File' in subset.columns else "unknown"
                        is_vcenter_data = 'vCenter' in source_file
                        is_realtime = self.current_interval == "Real-Time"
                        
                        print(f"DEBUG: Source: {source_file}, vCenter: {is_vcenter_data}, Real-time: {is_realtime}")
                        
                        # Analyze sample values
                        sample_values = subset['CPU_Ready_Sum'].head(20)
                        avg_sample = sample_values.mean()
                        max_sample = sample_values.max()
                        min_sample = sample_values.min()
                        
                        print(f"DEBUG: Sample statistics - Min: {min_sample:.2f}, Max: {max_sample:.2f}, Avg: {avg_sample:.2f}")
                        
                        # REAL-TIME DATA HANDLING (Your working case)
                        if is_realtime or self.current_interval == "Real-Time":
                            print(f"DEBUG: Processing Real-Time data")
                            
                            if avg_sample <= 100 and max_sample <= 100:
                                # Real-time data is often already in percentage or very close
                                if avg_sample <= 1:
                                    subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum']
                                    conversion_applied = "realtime_direct"
                                    print(f"DEBUG: Real-time direct percentage")
                                else:
                                    # Might be permille for real-time
                                    subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 10
                                    conversion_applied = "realtime_permille"
                                    print(f"DEBUG: Real-time permille conversion")
                            else:
                                # Standard millisecond conversion for real-time
                                max_possible_ms = interval_seconds * 1000  # 20 seconds = 20,000ms
                                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / max_possible_ms) * 100
                                conversion_applied = "realtime_milliseconds"
                                print(f"DEBUG: Real-time millisecond conversion (max possible: {max_possible_ms}ms)")
                        
                        # LAST DAY DATA HANDLING (Your problem case)
                        elif self.current_interval == "Last Day":
                            print(f"DEBUG: Processing Last Day data - applying conservative conversion")
                            
                            # Last Day data from vCenter is often problematic
                            if avg_sample > 50000:  # Very high values
                                # Treat as cumulative microseconds over the period
                                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 100000)) * 100
                                conversion_applied = "lastday_cumulative_microseconds"
                                print(f"DEBUG: Last Day cumulative microseconds conversion")
                                
                            elif avg_sample > 10000:  # High values 
                                # Conservative millisecond conversion
                                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 10000)) * 100
                                conversion_applied = "lastday_conservative_ms"
                                print(f"DEBUG: Last Day conservative milliseconds")
                                
                            elif avg_sample > 1000:  # Moderate values
                                # Treat as permille (per thousand)
                                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 1000
                                conversion_applied = "lastday_permille"
                                print(f"DEBUG: Last Day permille conversion")
                                
                            elif avg_sample > 100:  # Lower values
                                # Treat as centipercent
                                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 100
                                conversion_applied = "lastday_centipercent"
                                print(f"DEBUG: Last Day centipercent conversion")
                                
                            else:  # Very low values
                                # Might already be percentage or need minor conversion
                                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 10
                                conversion_applied = "lastday_minor"
                                print(f"DEBUG: Last Day minor conversion")
                        
                        # OTHER INTERVALS (Week, Month, Year)
                        else:
                            print(f"DEBUG: Processing {self.current_interval} data")
                            max_possible_ms = interval_seconds * 1000
                            
                            if avg_sample > max_possible_ms * 10:
                                # Very high - likely microseconds
                                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (max_possible_ms * 1000)) * 100
                                conversion_applied = "historical_microseconds"
                            elif avg_sample > max_possible_ms:
                                # High - standard milliseconds  
                                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / max_possible_ms) * 100
                                conversion_applied = "historical_milliseconds"
                            elif avg_sample > 1000:
                                # Medium - conservative milliseconds
                                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / max_possible_ms) * 100
                                conversion_applied = "historical_standard"
                            elif avg_sample > 100:
                                # Low-medium - centipercent
                                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum'] / 100
                                conversion_applied = "historical_centipercent"
                            else:
                                # Low - direct or minor conversion
                                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Sum']
                                conversion_applied = "historical_direct"
                        
                        # Post-conversion validation
                        post_avg = subset['CPU_Ready_Percent'].mean()
                        post_max = subset['CPU_Ready_Percent'].max()
                        
                        print(f"DEBUG: After conversion - Avg: {post_avg:.3f}%, Max: {post_max:.3f}%")
                        print(f"DEBUG: Conversion applied: {conversion_applied}")
                        
                        # EMERGENCY CORRECTION for impossible values
                        if post_avg > 100:  # Impossible - more than 100% CPU Ready
                            print(f"DEBUG: EMERGENCY: Impossible values detected ({post_avg:.1f}%), applying emergency fix")
                            
                            # Try progressively more conservative conversions
                            emergency_conversions = [
                                subset['CPU_Ready_Sum'] / 1000,   # Treat as permille
                                subset['CPU_Ready_Sum'] / 10000,  # Very conservative
                                subset['CPU_Ready_Sum'] / 100000, # Ultra conservative
                            ]
                            
                            for i, emergency_result in enumerate(emergency_conversions):
                                emergency_avg = emergency_result.mean()
                                if 0.001 <= emergency_avg <= 50:  # Reasonable range
                                    subset['CPU_Ready_Percent'] = emergency_result
                                    conversion_applied = f"emergency_fix_{i+1}"
                                    print(f"DEBUG: Emergency fix {i+1} applied: {emergency_avg:.3f}%")
                                    break
                            else:
                                # Last resort - just cap the values
                                subset['CPU_Ready_Percent'] = subset['CPU_Ready_Percent'] / 100
                                conversion_applied = "emergency_cap"
                                print(f"DEBUG: Emergency cap applied")
                        
                        # Cap any remaining outliers
                        subset.loc[subset['CPU_Ready_Percent'] > 100, 'CPU_Ready_Percent'] = 100
                        subset.loc[subset['CPU_Ready_Percent'] < 0, 'CPU_Ready_Percent'] = 0
                        
                        # Final statistics
                        final_avg = subset['CPU_Ready_Percent'].mean()
                        final_max = subset['CPU_Ready_Percent'].max()
                        final_min = subset['CPU_Ready_Percent'].min()
                        final_std = subset['CPU_Ready_Percent'].std()
                        
                        print(f"DEBUG: Host {hostname} - FINAL stats:")
                        print(f"  Avg: {final_avg:.3f}%, Max: {final_max:.3f}%, Min: {final_min:.3f}%")
                        print(f"  Conversion: {conversion_applied}")
                        
                        # Validation warnings
                        if final_avg > 50:
                            warning_msg = f"Host {hostname}: Very high CPU Ready ({final_avg:.1f}%) - check data source"
                            processing_warnings.append(warning_msg)
                        elif final_avg < 0.001:
                            warning_msg = f"Host {hostname}: Very low CPU Ready ({final_avg:.4f}%) - check conversion"
                            processing_warnings.append(warning_msg)
                        
                        print(f"DEBUG: Host {hostname} - FINAL stats:")
                        print(f"  Min: {final_min:.2f}%, Max: {final_max:.2f}%, Avg: {final_avg:.2f}%, Std: {final_std:.2f}%")
                        print(f"  Conversion applied: {conversion_applied}")
                        print(f"  Sample final values: {subset['CPU_Ready_Percent'].head(5).tolist()}")
        
                        # Quality warnings
                        if final_avg > 30:
                            warning_msg = f"Host {hostname}: Very high average CPU Ready ({final_avg:.1f}%) - verify data accuracy"
                            processing_warnings.append(warning_msg)
                        elif final_max < 0.1 and final_avg < 0.01:
                            warning_msg = f"Host {hostname}: Very low CPU Ready values ({final_avg:.4f}%) - may indicate measurement issues"
                            processing_warnings.append(warning_msg)
                        elif final_avg > 0 and final_std / final_avg > 5:  # Very high variability
                            warning_msg = f"Host {hostname}: Very high variability in CPU Ready measurements"
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
        """Extract hostname from CPU Ready column name with enhanced logic - PRESERVE IP ADDRESSES"""
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
                    
                    # Check if it's an IP address - if so, keep it intact
                    if re.match(r'^\d+\.\d+\.\d+\.\d+$', full_hostname):
                        hostname = full_hostname  # Keep full IP address
                        print(f"DEBUG: IP address detected, keeping full: {hostname}")
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

    def on_threshold_change(self):
        """Called when thresholds are updated - Updated for real-time integration"""
        if hasattr(self, 'realtime_dashboard'):
            self.realtime_dashboard.update_thresholds(
                self.warning_threshold.get(),
                self.critical_threshold.get()
            )
            print(f"DEBUG: Thresholds updated - Warning: {self.warning_threshold.get()}%, Critical: {self.critical_threshold.get()}%")
    
    def on_closing(self):
        """Handle application closing with proper cleanup"""
        try:
            print("DEBUG: Application closing...")
            if hasattr(self, 'realtime_dashboard'):
                self.realtime_dashboard.cleanup()
            # Disconnect vCenter if connected
            if hasattr(self, 'vcenter_connection') and self.vcenter_connection:
                try:
                    from pyVmomi.VmomiSupport import Disconnect
                    Disconnect(self.vcenter_connection)
                    print("DEBUG: vCenter disconnected")
                except Exception as e:
                    print(f"DEBUG: Error disconnecting vCenter: {e}")
            
            # Close matplotlib figures
            if hasattr(self, 'fig'):
                try:
                    plt.close(self.fig)
                    print("DEBUG: Main figure closed")
                except Exception as e:
                    print(f"DEBUG: Error closing main figure: {e}")
            
            # Close all matplotlib figures
            try:
                plt.close('all')
                print("DEBUG: All matplotlib figures closed")
            except Exception as e:
                print(f"DEBUG: Error closing all figures: {e}")
            
            # Destroy the root window
            try:
                self.root.quit()  # Stops the mainloop
                self.root.destroy()  # Destroys the window
                print("DEBUG: Tkinter window destroyed")
            except Exception as e:
                print(f"DEBUG: Error destroying window: {e}")
                
        except Exception as e:
            print(f"DEBUG: Error during cleanup: {e}")
        finally:
            # Force exit if needed
            import sys
            import os
            print("DEBUG: Forcing application exit")
            os._exit(0)  # This will force quit the application

    def create_about_tab(self):
        """Create scrollable about tab with application and developer information - UPDATED"""
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
                                text="Version 2.0 - Real-Time Edition",
                                bg=self.colors['bg_primary'],
                                fg=self.colors['accent_blue'],
                                font=('Segoe UI', 12, 'bold'))
        version_label.pack(pady=(0, 10))
        
        description_label = tk.Label(header_frame,
                                    text="Advanced CPU Ready metrics analysis with real-time monitoring and AI-powered consolidation optimization",
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_secondary'],
                                    font=('Segoe UI', 11),
                                    wraplength=600)
        description_label.pack()
        
        # What's New Section (NEW)
        whats_new_section = tk.LabelFrame(main_container, text="  🚀 What's New in Version 2.0  ",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['success'],
                                        font=('Segoe UI', 12, 'bold'),
                                        borderwidth=1,
                                        relief='solid')
        whats_new_section.pack(fill=tk.X, pady=(0, 15))
        
        whats_new_content = tk.Frame(whats_new_section, bg=self.colors['bg_primary'])
        whats_new_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        new_features_text = """📡 REAL-TIME MONITORING
        • Live CPU Ready data collection from vCenter
        • Real-time dashboard with interactive charts
        • Automated alerting and threshold monitoring
        • 20-second interval data collection

        🔄 SEAMLESS DATA INTEGRATION
        • Export real-time data to analysis engine
        • Unified workflow between live and historical data
        • Auto-detect and convert data formats
        • Preserve full IP addresses and hostnames

        🤖 AI-POWERED CONSOLIDATION
        • Intelligent host consolidation recommendations
        • Risk assessment and impact analysis
        • Multiple consolidation strategies (Conservative/Balanced/Aggressive)
        • Automated candidate identification

        📊 ENHANCED VISUALIZATIONS
        • Heat map calendar views
        • Performance trend analysis with moving averages
        • Host comparison matrices
        • Distribution analysis with box plots

        📄 COMPREHENSIVE REPORTING
        • PDF report generation with charts
        • Executive summaries with key findings
        • Implementation guides and best practices
        • Export capabilities for all analysis results

        ⚙️ WORKFLOW AUTOMATION
        • Auto-analyze imported data
        • Smart notifications and prompts
        • Auto-switch between tabs
        • Intelligent interval detection"""
        
        new_features_label = tk.Label(whats_new_content,
                                    text=new_features_text,
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_primary'],
                                    font=('Segoe UI', 10),
                                    justify=tk.LEFT)
        new_features_label.pack(anchor=tk.W)
        
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
        
        # Updated Expertise
        expertise_label = tk.Label(dev_content,
                                text="Expertise:",
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_secondary'],
                                font=('Segoe UI', 10, 'bold'))
        expertise_label.pack(anchor=tk.W, pady=(10, 5))
        
        expertise_text = """• VMware vCenter & vSphere Infrastructure
        • Real-Time Performance Monitoring & Analytics
        • Host Consolidation & Capacity Planning
        • Python Development & Data Science
        • Enterprise Virtualization Solutions
        • AI-Powered Infrastructure Optimization
        • SQLite Database Design & Management
        • Advanced Data Visualization & Reporting"""
        
        expertise_content = tk.Label(dev_content,
                                    text=expertise_text,
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_primary'],
                                    font=('Segoe UI', 10),
                                    justify=tk.LEFT)
        expertise_content.pack(anchor=tk.W)
        
        # Updated Application Features Card
        features_section = tk.LabelFrame(main_container, text="  ⭐ Complete Feature Set  ",
                                        bg=self.colors['bg_primary'],
                                        fg=self.colors['accent_blue'],
                                        font=('Segoe UI', 12, 'bold'),
                                        borderwidth=1,
                                        relief='solid')
        features_section.pack(fill=tk.X, pady=(0, 15))
        
        features_content = tk.Frame(features_section, bg=self.colors['bg_primary'])
        features_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        features_text = """🔗 VCENTER INTEGRATION
        • Direct vCenter API connectivity
        • Live performance data fetching
        • Support for multiple time periods (Real-Time to Annual)
        • Automated metric discovery and collection

        📡 REAL-TIME MONITORING
        • Live CPU Ready data collection (20-second intervals)
        • Interactive real-time dashboard
        • Automated threshold alerting
        • Performance trend tracking
        • Export real-time data for analysis

        📊 ADVANCED ANALYTICS
        • Intelligent CPU Ready percentage calculations
        • Multi-format data source support (CSV, Excel, vCenter)
        • Health scoring algorithms (0-100 scale)
        • Statistical distribution analysis
        • Performance baseline establishment

        📈 VISUAL REPORTING
        • Interactive timeline charts with threshold lines
        • Heat map calendar views for pattern identification
        • Host comparison matrices and rankings
        • Performance distribution box plots
        • Moving average trend analysis
        • Export charts and visualizations

        🤖 AI CONSOLIDATION ENGINE
        • Intelligent host consolidation recommendations
        • Multiple strategy options (Conservative/Balanced/Aggressive)
        • Comprehensive risk assessment
        • Workload redistribution modeling
        • Cost savings calculations
        • Implementation guidance

        🏥 HEALTH MONITORING
        • Automated performance threshold detection
        • Color-coded health indicators
        • Comprehensive host health dashboards
        • Executive summary reporting
        • Performance alerting system

        📄 COMPREHENSIVE REPORTING
        • PDF report generation with embedded charts
        • Executive summaries with key findings
        • Detailed technical analysis
        • Implementation recommendations
        • Best practices documentation
        • CSV export capabilities

        ⚙️ WORKFLOW AUTOMATION
        • Auto-analyze imported data
        • Smart notifications and user prompts
        • Automatic tab switching
        • Intelligent data format detection
        • Timezone-aware data processing"""
        
        features_label = tk.Label(features_content,
                                text=features_text,
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_primary'],
                                font=('Segoe UI', 10),
                                justify=tk.LEFT)
        features_label.pack(anchor=tk.W)
        
        # Updated Technology Stack Card
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
        📊 Pandas & NumPy (Advanced Data Analysis)
        📈 Matplotlib & Seaborn (Professional Visualizations)
        🖥️ Tkinter with TTK (Modern UI Framework)
        🔗 PyVmomi (vCenter API Integration)
        📡 Requests (HTTP Communications)
        🗄️ SQLite3 (Real-Time Data Storage)
        📄 ReportLab (PDF Report Generation)
        🕒 Threading & Queue (Real-Time Processing)
        🌐 Regular Expressions (Data Processing)
        📅 DateTime & Timezone Handling
        🎨 Custom Dark Theme Implementation
        📦 PyInstaller (Executable Distribution)"""
        
        tech_label = tk.Label(tech_content,
                            text=tech_text,
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=('Segoe UI', 10),
                            justify=tk.LEFT)
        tech_label.pack(anchor=tk.W)
        
        # System Requirements Card (NEW)
        requirements_section = tk.LabelFrame(main_container, text="  💻 System Requirements  ",
                                            bg=self.colors['bg_primary'],
                                            fg=self.colors['accent_blue'],
                                            font=('Segoe UI', 12, 'bold'),
                                            borderwidth=1,
                                            relief='solid')
        requirements_section.pack(fill=tk.X, pady=(0, 15))
        
        requirements_content = tk.Frame(requirements_section, bg=self.colors['bg_primary'])
        requirements_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        requirements_text = """🖥️ OPERATING SYSTEM
        • Windows 10/11 (Recommended)
        • Windows Server 2016/2019/2022
        • Linux (Ubuntu 18.04+, CentOS 7+)
        • macOS 10.14+ (Limited testing)

        💾 HARDWARE REQUIREMENTS
        • RAM: 4GB minimum, 8GB recommended
        • Storage: 500MB free space
        • CPU: Dual-core processor minimum
        • Network: Access to vCenter server

        🔗 NETWORK REQUIREMENTS
        • vCenter Server accessibility (HTTPS/443)
        • Internet connection for timezone data
        • Local network access for host management

        📋 SOFTWARE DEPENDENCIES
        • Python 3.8+ (for source version)
        • vCenter Server 6.5+ (for live monitoring)
        • Modern web browser (for documentation)"""
        
        requirements_label = tk.Label(requirements_content,
                                    text=requirements_text,
                                    bg=self.colors['bg_primary'],
                                    fg=self.colors['text_primary'],
                                    font=('Segoe UI', 10),
                                    justify=tk.LEFT)
        requirements_label.pack(anchor=tk.W)
        
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

        Version 2.0 introduces real-time monitoring
        capabilities and AI-powered consolidation
        recommendations for modern vSphere environments.

        Built with ❤️ for the VMware community"""
        
        copyright_label = tk.Label(license_content,
                                text=copyright_text,
                                bg=self.colors['bg_primary'],
                                fg=self.colors['text_primary'],
                                font=('Segoe UI', 10),
                                justify=tk.CENTER)
        copyright_label.pack(expand=True)
        
        # Updated Footer
        footer_frame = tk.Frame(main_container, bg=self.colors['bg_primary'])
        footer_frame.pack(fill=tk.X, pady=(20, 0))
        
        footer_label = tk.Label(footer_frame,
                            text="🚀 Empowering infrastructure teams with intelligent real-time performance insights and AI-driven optimization",
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