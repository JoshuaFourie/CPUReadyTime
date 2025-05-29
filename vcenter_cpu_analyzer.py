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

# vCenter integration imports (install with: pip install pyvmomi requests)
try:
    from pyVim.connect import SmartConnect, Disconnect
    from pyVmomi import vim
    import ssl
    import requests
    from requests.auth import HTTPBasicAuth
    VCENTER_AVAILABLE = True
except ImportError:
    VCENTER_AVAILABLE = False
    print("vCenter integration not available. Install pyvmomi and requests: pip install pyvmomi requests")

class CPUReadyAnalyzer:
    def __init__(self, root):
        self.root = root
        self.root.title("vCenter CPU Ready Analysis Tool")
        
        # Get screen dimensions
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        # Calculate appropriate window size (80% of screen, but not larger than optimal)
        window_width = min(1200, int(screen_width * 0.8))
        window_height = min(900, int(screen_height * 0.8))
        
        # Center the window
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
        
        # Make window resizable
        self.root.minsize(800, 600)  # Minimum size
        
        # Add proper window close handling
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Data storage
        self.data_frames = []  # List of DataFrames from imported files
        self.processed_data = None
        self.current_interval = "Last Day"
        self.vcenter_connection = None
        
        # Update intervals in seconds - matching vCenter standard intervals
        self.intervals = {
            "Real-Time": 20,      # vCenter Real-time: 20 second intervals
            "Last Day": 300,      # vCenter Past Day: 5 minute intervals  
            "Last Week": 1800,    # vCenter Past Week: 30 minute intervals
            "Last Month": 7200,   # vCenter Past Month: 2 hour intervals
            "Last Year": 86400    # vCenter Past Year: 1 day intervals
        }
        
        # vCenter performance data collection intervals (for API queries)
        self.vcenter_intervals = {
            "Real-Time": 20,      # 20 seconds - Real-time
            "Last Day": 300,      # 5 minutes - Level 1 (Past Day)
            "Last Week": 1800,    # 30 minutes - Level 2 (Past Week) 
            "Last Month": 7200,   # 2 hours - Level 3 (Past Month)
            "Last Year": 86400    # 1 day - Level 4 (Past Year)
        }
        
        self.setup_ui()
        
    def setup_ui(self):
        # Create main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(4, weight=1)
        
        # File import section
        import_frame = ttk.LabelFrame(main_frame, text="Data Source", padding="10")
        import_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        import_frame.columnconfigure(2, weight=1)
        
        # File import buttons
        ttk.Button(import_frame, text="Import CSV/Excel Files", 
                  command=self.import_files).grid(row=0, column=0, padx=(0, 10))
        
        self.file_count_label = ttk.Label(import_frame, text="No files imported")
        self.file_count_label.grid(row=0, column=1, padx=(0, 10))
        
        ttk.Button(import_frame, text="Clear All Files", 
                  command=self.clear_files).grid(row=0, column=2, padx=(10, 0), sticky=tk.W)
        
        # vCenter integration section
        if VCENTER_AVAILABLE:
            ttk.Separator(import_frame, orient='horizontal').grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
            
            ttk.Label(import_frame, text="🔗 vCenter Integration", font=('TkDefaultFont', 10, 'bold')).grid(row=2, column=0, columnspan=3, sticky=tk.W, pady=(0, 5))
            
            # vCenter connection fields
            vcenter_fields_frame = ttk.Frame(import_frame)
            vcenter_fields_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 5))
            vcenter_fields_frame.columnconfigure(1, weight=1)
            vcenter_fields_frame.columnconfigure(3, weight=1)
            
            ttk.Label(vcenter_fields_frame, text="vCenter:").grid(row=0, column=0, padx=(0, 5))
            self.vcenter_host = ttk.Entry(vcenter_fields_frame, width=20)
            self.vcenter_host.grid(row=0, column=1, padx=(0, 10), sticky=(tk.W, tk.E))
            
            ttk.Label(vcenter_fields_frame, text="Username:").grid(row=0, column=2, padx=(10, 5))
            self.vcenter_user = ttk.Entry(vcenter_fields_frame, width=15)
            self.vcenter_user.grid(row=0, column=3, padx=(0, 10), sticky=(tk.W, tk.E))
            
            ttk.Label(vcenter_fields_frame, text="Password:").grid(row=0, column=4, padx=(10, 5))
            self.vcenter_pass = ttk.Entry(vcenter_fields_frame, show="*", width=15)
            self.vcenter_pass.grid(row=0, column=5, padx=(0, 10))
            
            # Time period selection (vCenter style)
            period_frame = ttk.Frame(import_frame)
            period_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(5, 5))
            
            ttk.Label(period_frame, text="Time Period:").grid(row=0, column=0, padx=(0, 10))
            self.vcenter_period_var = tk.StringVar(value="Last Day")
            vcenter_period_combo = ttk.Combobox(period_frame, textvariable=self.vcenter_period_var,
                                              values=["Real-Time", "Last Day", "Last Week", "Last Month", "Last Year"],
                                              state="readonly", width=15)
            vcenter_period_combo.grid(row=0, column=1, padx=(0, 15))
            
            # Show the actual date range that will be fetched
            self.date_range_label = ttk.Label(period_frame, text="", foreground="gray")
            self.date_range_label.grid(row=0, column=2, padx=(10, 0))
            
            # Update date range display when period changes
            vcenter_period_combo.bind('<<ComboboxSelected>>', self.update_date_range_display)
            self.update_date_range_display()  # Initial update
            
            # vCenter buttons
            vcenter_buttons_frame = ttk.Frame(import_frame)
            vcenter_buttons_frame.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(5, 0))
            
            self.connect_btn = ttk.Button(vcenter_buttons_frame, text="Connect to vCenter", 
                                        command=self.connect_vcenter)
            self.connect_btn.grid(row=0, column=0, padx=(0, 10))
            
            self.fetch_btn = ttk.Button(vcenter_buttons_frame, text="Fetch CPU Ready Data", 
                                      command=self.fetch_vcenter_data, state='disabled')
            self.fetch_btn.grid(row=0, column=1, padx=(0, 10))
            
            self.vcenter_status = ttk.Label(vcenter_buttons_frame, text="Not connected", foreground="red")
            self.vcenter_status.grid(row=0, column=2, padx=(10, 0))
        else:
            ttk.Label(import_frame, text="⚠️ vCenter integration requires: pip install pyvmomi requests", 
                     foreground="orange").grid(row=1, column=0, columnspan=3, pady=5)
        
        # Configuration section
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="10")
        config_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        ttk.Label(config_frame, text="Update Interval:").grid(row=0, column=0, padx=(0, 10))
        
        self.interval_var = tk.StringVar(value="Last Day")
        interval_combo = ttk.Combobox(config_frame, textvariable=self.interval_var, 
                                    values=list(self.intervals.keys()), state="readonly")
        interval_combo.grid(row=0, column=1, padx=(0, 10))
        interval_combo.bind('<<ComboboxSelected>>', self.on_interval_change)
        
        ttk.Button(config_frame, text="Calculate CPU Ready %", 
                  command=self.calculate_cpu_ready).grid(row=0, column=2, padx=(10, 0))
        
        # Results section
        results_frame = ttk.LabelFrame(main_frame, text="Analysis Results", padding="10")
        results_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        results_frame.columnconfigure(0, weight=1)
        
        # Treeview for results
        columns = ('Host', 'Avg CPU Ready %', 'Max CPU Ready %', 'Records')
        self.results_tree = ttk.Treeview(results_frame, columns=columns, show='headings', height=8)
        
        for col in columns:
            self.results_tree.heading(col, text=col)
            self.results_tree.column(col, width=150)
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=scrollbar.set)
        
        self.results_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Host removal analysis section
        removal_frame = ttk.LabelFrame(main_frame, text="Host Removal Analysis", padding="10")
        removal_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        removal_frame.columnconfigure(1, weight=1)

        ttk.Label(removal_frame, text="Select hosts to remove:").grid(row=0, column=0, padx=(0, 10), sticky=tk.W)

        # Create frame for host selection
        host_selection_frame = ttk.Frame(removal_frame)
        host_selection_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(5, 10))
        host_selection_frame.columnconfigure(0, weight=1)

        # Listbox with checkboxes for multiple selection
        self.hosts_listbox = tk.Listbox(host_selection_frame, selectmode=tk.MULTIPLE, height=6)
        self.hosts_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))

        # Scrollbar for listbox
        hosts_scrollbar = ttk.Scrollbar(host_selection_frame, orient=tk.VERTICAL, command=self.hosts_listbox.yview)
        self.hosts_listbox.configure(yscrollcommand=hosts_scrollbar.set)
        hosts_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Selection helper buttons
        selection_buttons_frame = ttk.Frame(host_selection_frame)
        selection_buttons_frame.grid(row=0, column=2, padx=(10, 0), sticky=(tk.N))

        ttk.Button(selection_buttons_frame, text="Select All", 
                command=self.select_all_hosts).grid(row=0, column=0, pady=(0, 5), sticky=(tk.W, tk.E))
        ttk.Button(selection_buttons_frame, text="Clear All", 
                command=self.clear_all_hosts).grid(row=1, column=0, pady=(0, 5), sticky=(tk.W, tk.E))
        ttk.Button(selection_buttons_frame, text="Analyze Impact", 
                command=self.analyze_multiple_removal_impact).grid(row=2, column=0, pady=(10, 0), sticky=(tk.W, tk.E))

        # Results display
        self.impact_label = ttk.Label(removal_frame, text="", foreground="blue", justify=tk.LEFT)
        self.impact_label.grid(row=2, column=0, columnspan=3, pady=(10, 0), sticky=(tk.W, tk.E))
        
        # Chart section
        chart_frame = ttk.LabelFrame(main_frame, text="Visualization", padding="10")
        chart_frame.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)
        
        # Create matplotlib figure
        self.fig, self.ax = plt.subplots(figsize=(12, 5))
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Advanced Analysis Section
        advanced_frame = ttk.LabelFrame(main_frame, text="Advanced Analysis", padding="10")
        advanced_frame.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        advanced_frame.columnconfigure(1, weight=1)
        
        # Threshold Configuration
        threshold_frame = ttk.LabelFrame(advanced_frame, text="🚨 Threshold Alerts", padding="10")
        threshold_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Label(threshold_frame, text="Warning Threshold:").grid(row=0, column=0, padx=(0, 5))
        self.warning_threshold = tk.DoubleVar(value=5.0)
        warning_spin = ttk.Spinbox(threshold_frame, from_=1.0, to=50.0, width=10, 
                                textvariable=self.warning_threshold, increment=1.0)
        warning_spin.grid(row=0, column=1, padx=(0, 5))
        ttk.Label(threshold_frame, text="%").grid(row=0, column=2)
        
        ttk.Label(threshold_frame, text="Critical Threshold:").grid(row=1, column=0, padx=(0, 5))
        self.critical_threshold = tk.DoubleVar(value=15.0)
        critical_spin = ttk.Spinbox(threshold_frame, from_=5.0, to=100.0, width=10,
                                textvariable=self.critical_threshold, increment=5.0)
        critical_spin.grid(row=1, column=1, padx=(0, 5))
        ttk.Label(threshold_frame, text="%").grid(row=1, column=2)
        
        ttk.Button(threshold_frame, text="Apply Thresholds", 
                command=self.apply_thresholds).grid(row=2, column=0, columnspan=3, pady=(10, 0))
        
        # Host Health Scoring
        health_frame = ttk.LabelFrame(advanced_frame, text="🏥 Host Health Dashboard", padding="10")
        health_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(10, 0))
        health_frame.columnconfigure(0, weight=1)
        
        # Health score display
        self.health_text = tk.Text(health_frame, height=8, width=40)
        health_scrollbar = ttk.Scrollbar(health_frame, orient=tk.VERTICAL, command=self.health_text.yview)
        self.health_text.configure(yscrollcommand=health_scrollbar.set)
        self.health_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        health_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Visualization Options
        viz_frame = ttk.LabelFrame(advanced_frame, text="📊 Advanced Visualizations", padding="10")
        viz_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
        ttk.Button(viz_frame, text="📅 Heat Map Calendar", 
                command=self.show_heatmap_calendar).grid(row=0, column=0, padx=(0, 10))
        ttk.Button(viz_frame, text="📈 Performance Trends", 
                command=self.show_performance_trends).grid(row=0, column=1, padx=(0, 10))
        ttk.Button(viz_frame, text="🎯 Host Comparison", 
                command=self.show_host_comparison).grid(row=0, column=2, padx=(0, 10))
        ttk.Button(viz_frame, text="📋 Export Report", 
                command=self.export_analysis_report).grid(row=0, column=3)
  
    def update_removal_options(self):
        """Update the hosts listbox with available hosts"""
        if self.processed_data is None:
            return
            
        # Clear existing items
        self.hosts_listbox.delete(0, tk.END)
        
        # Add all hostnames to listbox
        hostnames = sorted(self.processed_data['Hostname'].unique())
        for hostname in hostnames:
            self.hosts_listbox.insert(tk.END, hostname)

    def select_all_hosts(self):
        """Select all hosts in the listbox"""
        self.hosts_listbox.select_set(0, tk.END)

    def clear_all_hosts(self):
        """Clear all host selections"""
        self.hosts_listbox.selection_clear(0, tk.END)

    def get_selected_hosts(self):
        """Get list of currently selected hosts"""
        selected_indices = self.hosts_listbox.curselection()
        selected_hosts = [self.hosts_listbox.get(i) for i in selected_indices]
        return selected_hosts

    def analyze_multiple_removal_impact(self):
        """Analyze the impact of removing multiple selected hosts"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
            
        selected_hosts = self.get_selected_hosts()
        
        if not selected_hosts:
            messagebox.showwarning("No Selection", "Please select at least one host to remove")
            return
            
        # Check if removing all hosts
        total_hosts = len(self.processed_data['Hostname'].unique())
        if len(selected_hosts) >= total_hosts:
            self.impact_label.config(text="❌ Cannot remove all hosts - no remaining infrastructure!")
            return
        
        print(f"DEBUG: Analyzing removal of hosts: {selected_hosts}")
        
        # Get data for selected hosts and remaining hosts
        selected_hosts_data = self.processed_data[self.processed_data['Hostname'].isin(selected_hosts)]
        remaining_data = self.processed_data[~self.processed_data['Hostname'].isin(selected_hosts)]
        
        if remaining_data.empty:
            self.impact_label.config(text="❌ Cannot remove all hosts - no remaining infrastructure!")
            return
        
        # Calculate current statistics
        current_total_sum = self.processed_data['CPU_Ready_Sum'].sum()
        current_avg_percent = self.processed_data['CPU_Ready_Percent'].mean()
        
        # Calculate workload to be redistributed
        total_workload_to_redistribute = selected_hosts_data['CPU_Ready_Sum'].sum()
        remaining_hosts = remaining_data['Hostname'].unique()
        num_remaining_hosts = len(remaining_hosts)
        
        # Calculate individual contributions of removed hosts
        removed_host_stats = []
        for hostname in selected_hosts:
            host_data = selected_hosts_data[selected_hosts_data['Hostname'] == hostname]
            host_workload = host_data['CPU_Ready_Sum'].sum()
            host_avg_percent = host_data['CPU_Ready_Percent'].mean()
            workload_percentage = (host_workload / current_total_sum) * 100
            removed_host_stats.append({
                'hostname': hostname,
                'workload': host_workload,
                'avg_percent': host_avg_percent,
                'workload_percentage': workload_percentage
            })
        
        # Redistribute workload evenly among remaining hosts
        additional_workload_per_host = total_workload_to_redistribute / num_remaining_hosts
        
        print(f"DEBUG: Total workload to redistribute: {total_workload_to_redistribute}")
        print(f"DEBUG: Additional workload per remaining host: {additional_workload_per_host}")
        
        # Create new dataset with redistributed workload
        redistributed_data = remaining_data.copy()
        interval_seconds = self.intervals[self.current_interval]
        
        # Add redistributed workload to each remaining host
        for hostname in remaining_hosts:
            mask = redistributed_data['Hostname'] == hostname
            host_record_count = mask.sum()
            redistributed_data.loc[mask, 'CPU_Ready_Sum'] += additional_workload_per_host / host_record_count
            # Recalculate CPU Ready % with new workload
            redistributed_data.loc[mask, 'CPU_Ready_Percent'] = (
                redistributed_data.loc[mask, 'CPU_Ready_Sum'] / (interval_seconds * 1000)
            ) * 100
        
        # Calculate new statistics after redistribution
        new_avg_percent = redistributed_data['CPU_Ready_Percent'].mean()
        
        # Calculate individual host impacts
        host_impacts = []
        for hostname in remaining_hosts:
            original_host_avg = remaining_data[remaining_data['Hostname'] == hostname]['CPU_Ready_Percent'].mean()
            new_host_avg = redistributed_data[redistributed_data['Hostname'] == hostname]['CPU_Ready_Percent'].mean()
            increase = new_host_avg - original_host_avg
            host_impacts.append({
                'hostname': hostname,
                'original_avg': original_host_avg,
                'new_avg': new_host_avg,
                'increase': increase
            })
        
        # Calculate overall impact metrics
        avg_increase = new_avg_percent - current_avg_percent
        total_workload_percentage = (total_workload_to_redistribute / current_total_sum) * 100
        
        # Format results with comprehensive analysis
        impact_text = f"📊 REMOVING MULTIPLE HOSTS: {', '.join(selected_hosts)}\n"
        impact_text += f"{'='*60}\n"
        
        # Removed hosts breakdown
        impact_text += f"🗑️  Hosts Being Removed ({len(selected_hosts)}):\n"
        for i, host_stat in enumerate(removed_host_stats, 1):
            impact_text += f"   {i}. {host_stat['hostname']}: {host_stat['avg_percent']:.2f}% avg CPU Ready ({host_stat['workload_percentage']:.1f}% of total workload)\n"
        
        # Overall impact
        impact_text += f"\n📈 Combined Workload Impact:\n"
        impact_text += f"   • Total workload to redistribute: {total_workload_percentage:.1f}%\n"
        impact_text += f"   • Overall avg CPU Ready increase: +{avg_increase:.2f}%\n"
        impact_text += f"   • Additional workload per remaining host: +{(additional_workload_per_host/total_workload_to_redistribute*total_workload_percentage):.1f}%\n"
        
        # Infrastructure changes
        impact_text += f"\n🖥️  Infrastructure Changes:\n"
        impact_text += f"   • Hosts removed: {len(selected_hosts)} ({', '.join(selected_hosts)})\n"
        impact_text += f"   • Remaining hosts: {num_remaining_hosts} ({', '.join(remaining_hosts)})\n"
        impact_text += f"   • Infrastructure reduction: {(len(selected_hosts)/(len(selected_hosts)+num_remaining_hosts)*100):.1f}%\n"
        
        # Per-host impact details
        impact_text += f"\n📋 Remaining Host Impact Details:\n"
        for i, impact in enumerate(host_impacts, 1):
            impact_text += f"   {i}. {impact['hostname']}: {impact['original_avg']:.2f}% → {impact['new_avg']:.2f}% (+{impact['increase']:.2f}%)\n"
        
        # Risk assessment
        if avg_increase > 15:
            impact_text += f"\n🔴 VERY HIGH IMPACT: +{avg_increase:.1f}% increase will likely cause significant performance problems"
        elif avg_increase > 10:
            impact_text += f"\n⚠️  HIGH IMPACT: +{avg_increase:.1f}% increase may cause performance issues"
        elif avg_increase > 5:
            impact_text += f"\n⚡ MODERATE IMPACT: +{avg_increase:.1f}% increase - monitor closely after consolidation"
        else:
            impact_text += f"\n✅ LOW IMPACT: +{avg_increase:.1f}% increase - consolidation appears safe"
        
        # Consolidation efficiency metric
        efficiency = total_workload_percentage / len(selected_hosts)
        impact_text += f"\n💡 Consolidation Efficiency: {efficiency:.1f}% workload reduction per host removed"
        
        self.impact_label.config(text=impact_text)
        
        # Update visualization to show the multiple host removal impact
        self.update_multiple_removal_chart(selected_hosts, redistributed_data)

    def update_multiple_removal_chart(self, removed_hosts, redistributed_data):
        """Update chart to show impact of removing multiple hosts"""
        if self.processed_data is None:
            return
            
        self.ax.clear()
        
        # Get remaining hosts
        remaining_hosts = sorted(redistributed_data['Hostname'].unique())
        
        # Plot original data for remaining hosts (solid lines)
        for hostname in remaining_hosts:
            original_host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            original_host_data = original_host_data.sort_values('Time')
            
            self.ax.plot(original_host_data['Time'], original_host_data['CPU_Ready_Percent'], 
                        linewidth=2, label=f'{hostname} (Original)', alpha=0.7, linestyle='-')
        
        # Plot redistributed data for remaining hosts (dashed lines)
        for hostname in remaining_hosts:
            redistributed_host_data = redistributed_data[redistributed_data['Hostname'] == hostname].copy()
            redistributed_host_data = redistributed_host_data.sort_values('Time')
            
            self.ax.plot(redistributed_host_data['Time'], redistributed_host_data['CPU_Ready_Percent'], 
                        linewidth=2, label=f'{hostname} (After Removal)', 
                        alpha=0.9, linestyle='--')
        
        # Plot the removed hosts data for reference (thin gray lines)
        colors = ['gray', 'darkgray', 'lightgray', 'silver']
        for i, removed_host in enumerate(removed_hosts):
            removed_host_data = self.processed_data[self.processed_data['Hostname'] == removed_host].copy()
            if not removed_host_data.empty:
                removed_host_data = removed_host_data.sort_values('Time')
                color = colors[i % len(colors)]
                self.ax.plot(removed_host_data['Time'], removed_host_data['CPU_Ready_Percent'], 
                            linewidth=1, label=f'{removed_host} (REMOVED)', 
                            alpha=0.5, linestyle=':', color=color)
        
        # Customize chart
        hosts_text = ', '.join(removed_hosts) if len(removed_hosts) <= 3 else f"{len(removed_hosts)} hosts"
        self.ax.set_title(f'CPU Ready % Impact of Removing {hosts_text}\n({self.current_interval} Interval)')
        self.ax.set_xlabel('Time')
        self.ax.set_ylabel('CPU Ready %')
        self.ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        self.ax.grid(True, alpha=0.3)
        
        # Add annotation
        total_workload = sum([
            self.processed_data[self.processed_data['Hostname'] == host]['CPU_Ready_Sum'].sum() 
            for host in removed_hosts
        ])
        total_infrastructure_workload = self.processed_data['CPU_Ready_Sum'].sum()
        workload_percentage = (total_workload / total_infrastructure_workload) * 100
        
        annotation_text = (f'Multiple Host Removal:\n'
                        f'• Removing: {len(removed_hosts)} hosts\n'
                        f'• Combined workload: {workload_percentage:.1f}%\n'
                        f'• Redistributed to: {len(remaining_hosts)} hosts')
        
        self.ax.text(0.02, 0.98, annotation_text, 
                    transform=self.ax.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        # Format x-axis to show dates nicely
        self.fig.autofmt_xdate()
        self.fig.tight_layout()
        self.canvas.draw()  
        
    def update_date_range_display(self, event=None):
        """Update the date range display based on selected vCenter period"""
        period = self.vcenter_period_var.get()
        now = datetime.now()
        
        if period == "Real-Time":
            start_time = now - timedelta(hours=1)  # Last hour for real-time
            range_text = f"({start_time.strftime('%H:%M')} - {now.strftime('%H:%M')} today)"
        elif period == "Last Day":
            start_time = now - timedelta(days=1)
            range_text = f"({start_time.strftime('%m/%d %H:%M')} - {now.strftime('%m/%d %H:%M')})"
        elif period == "Last Week":
            start_time = now - timedelta(weeks=1)
            range_text = f"({start_time.strftime('%m/%d')} - {now.strftime('%m/%d')})"
        elif period == "Last Month":
            start_time = now - timedelta(days=30)
            range_text = f"({start_time.strftime('%m/%d')} - {now.strftime('%m/%d')})"
        elif period == "Last Year":
            start_time = now - timedelta(days=365)
            range_text = f"({start_time.strftime('%Y/%m')} - {now.strftime('%Y/%m')})"
        else:
            range_text = ""
            
        self.date_range_label.config(text=range_text)
    
    def get_vcenter_date_range(self):
        """Calculate actual start and end dates based on selected vCenter period"""
        period = self.vcenter_period_var.get()
        now = datetime.now()
        
        if period == "Real-Time":
            start_date = (now - timedelta(hours=1)).date()
            end_date = now.date()
        elif period == "Last Day":
            start_date = (now - timedelta(days=1)).date()
            end_date = now.date()
        elif period == "Last Week":
            start_date = (now - timedelta(weeks=1)).date()
            end_date = now.date()
        elif period == "Last Month":
            start_date = (now - timedelta(days=30)).date()
            end_date = now.date()
        elif period == "Last Year":
            start_date = (now - timedelta(days=365)).date()
            end_date = now.date()
        else:
            start_date = (now - timedelta(days=1)).date()
            end_date = now.date()
            
        return start_date, end_date

    def connect_vcenter(self):
        """Connect to vCenter server"""
        if not VCENTER_AVAILABLE:
            messagebox.showerror("Error", "vCenter integration not available. Install required packages.")
            return
            
        vcenter_host = self.vcenter_host.get().strip()
        username = self.vcenter_user.get().strip()
        password = self.vcenter_pass.get()
        
        if not all([vcenter_host, username, password]):
            messagebox.showwarning("Missing Information", "Please fill in all vCenter connection fields")
            return
            
        # Disable SSL verification for self-signed certificates
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        try:
            self.vcenter_status.config(text="Connecting...", foreground="orange")
            self.connect_btn.config(state='disabled')
            self.root.update()
            
            # Connect to vCenter
            self.vcenter_connection = SmartConnect(
                host=vcenter_host,
                user=username,
                pwd=password,
                sslContext=context
            )
            
            if self.vcenter_connection:
                self.vcenter_status.config(text="Connected ✓", foreground="green")
                self.fetch_btn.config(state='normal')
                self.connect_btn.config(text="Disconnect", command=self.disconnect_vcenter)
                messagebox.showinfo("Success", f"Connected to vCenter: {vcenter_host}")
            else:
                raise Exception("Connection failed")
                
        except Exception as e:
            self.vcenter_status.config(text="Connection failed", foreground="red")
            self.connect_btn.config(state='normal')
            messagebox.showerror("Connection Error", f"Failed to connect to vCenter:\n{str(e)}")
    
    def disconnect_vcenter(self):
        """Disconnect from vCenter"""
        try:
            if self.vcenter_connection:
                Disconnect(self.vcenter_connection)
                self.vcenter_connection = None
            
            self.vcenter_status.config(text="Disconnected", foreground="red")
            self.fetch_btn.config(state='disabled')
            self.connect_btn.config(text="Connect to vCenter", command=self.connect_vcenter, state='normal')
            
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
        
        # Get the appropriate performance interval
        perf_interval = self.vcenter_intervals[selected_period]
        
        # Show progress dialog
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Fetching Data from vCenter")
        progress_window.geometry("500x180")
        progress_window.transient(self.root)
        progress_window.grab_set()
        
        ttk.Label(progress_window, text="Fetching CPU Ready data from vCenter...", 
                 font=('TkDefaultFont', 10, 'bold')).pack(pady=15)
        
        ttk.Label(progress_window, text=f"Period: {selected_period}").pack(pady=2)
        ttk.Label(progress_window, text=f"Date Range: {start_date} to {end_date}").pack(pady=2)
        ttk.Label(progress_window, text=f"Interval: {perf_interval} seconds").pack(pady=2)
        
        progress_bar = ttk.Progressbar(progress_window, mode='indeterminate')
        progress_bar.pack(pady=15, padx=20, fill=tk.X)
        progress_bar.start()
        
        status_label = ttk.Label(progress_window, text="Connecting to vCenter...")
        status_label.pack(pady=5)
        
        # Run fetch in separate thread to prevent UI freezing
        def fetch_thread():
            try:
                status_label.config(text="Getting host list...")
                self.root.update()
                
                # Get all hosts
                content = self.vcenter_connection.RetrieveContent()
                hosts = self.get_all_hosts(content)
                
                if not hosts:
                    messagebox.showwarning("No Hosts", "No ESXi hosts found in vCenter")
                    return
                
                status_label.config(text=f"Found {len(hosts)} hosts. Fetching {selected_period} performance data...")
                self.root.update()
                
                # Fetch performance data with appropriate interval
                cpu_ready_data = self.fetch_cpu_ready_metrics(content, hosts, start_date, end_date, perf_interval)
                
                if cpu_ready_data:
                    # Convert to DataFrame and add to data_frames
                    df = pd.DataFrame(cpu_ready_data)
                    df['source_file'] = f'vCenter_{selected_period}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}'
                    
                    self.data_frames.append(df)
                    self.file_count_label.config(text=f"{len(self.data_frames)} data sources loaded")
                    
                    # Auto-set the interval to match the fetched data
                    self.interval_var.set(selected_period)
                    self.current_interval = selected_period
                    
                    messagebox.showinfo("Success", 
                                      f"✅ Successfully fetched vCenter data!\n\n"
                                      f"📊 Period: {selected_period}\n"
                                      f"🖥️  Hosts: {len(hosts)}\n"
                                      f"📅 Date Range: {start_date} to {end_date}\n"
                                      f"📈 Total Records: {len(cpu_ready_data)}\n"
                                      f"⏱️  Interval: {perf_interval} seconds\n\n"
                                      f"💡 Analysis interval automatically set to '{selected_period}'")
                else:
                    messagebox.showwarning("No Data", f"No CPU Ready data found for {selected_period}")
                    
            except Exception as e:
                messagebox.showerror("Fetch Error", f"Error fetching data from vCenter:\n{str(e)}")
            finally:
                progress_window.destroy()
        
        # Start fetch thread
        threading.Thread(target=fetch_thread, daemon=True).start()
    
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
        
        container.Destroy()
        return hosts
        
    def fetch_cpu_ready_metrics(self, content, hosts, start_date, end_date, interval_seconds):
        """Fetch CPU Ready metrics for all hosts with specified interval"""
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
        
        # Check what's actually available for performance data
        print("DEBUG: Checking performance intervals and providers...")
        
        # Convert dates to vCenter format
        start_time = datetime.combine(start_date, datetime.min.time())
        end_time = datetime.combine(end_date, datetime.max.time())
        
        # Calculate time difference to determine if we should use real-time vs historical
        time_diff = (end_time - start_time).total_seconds()
        
        # For recent data (last few hours), try real-time first
        if time_diff <= 3600:  # Less than 1 hour - use real-time
            print("DEBUG: Using real-time data approach")
            interval_id = 20  # Real-time interval
        else:
            # Try to find the right historical interval
            available_intervals = perf_manager.historicalInterval
            print("DEBUG: Available historical intervals:")
            for interval in available_intervals:
                print(f"  - ID: {interval.key}, Period: {interval.samplingPeriod}s, Name: {interval.name}")
            
            # Use the most appropriate interval based on time range
            if time_diff <= 86400:  # Less than 1 day
                interval_id = 1  # Past day (300s)
            elif time_diff <= 604800:  # Less than 1 week  
                interval_id = 2  # Past week (1800s)
            elif time_diff <= 2592000:  # Less than 1 month
                interval_id = 3  # Past month (7200s)
            else:
                interval_id = 4  # Past year (86400s)
            
            print(f"DEBUG: Selected historical interval ID: {interval_id}")
        
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
                    instance=""  # Try empty instance instead of "*"
                )
                
                # For real-time data, use different approach
                if time_diff <= 3600:
                    # Real-time query
                    query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=[metric_spec],
                        intervalId=20,  # Real-time
                        maxSample=1
                    )
                else:
                    # Historical query - try without explicit start/end times first
                    query_spec = vim.PerformanceManager.QuerySpec(
                        entity=host,
                        metricId=[metric_spec],
                        intervalId=interval_id,
                        maxSample=100  # Limit samples to avoid timeouts
                    )
                
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
                                if i < len(value_info.value) and value_info.value[i] >= 0:
                                    total_ready += value_info.value[i]
                            
                            # Add to results
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
                
                # Try alternative approach - query available metrics for this host
                try:
                    print(f"DEBUG: Trying alternative query for {hostname}...")
                    available_metrics = perf_manager.QueryAvailablePerfMetric(entity=host)
                    cpu_ready_metrics = [m for m in available_metrics if m.counterId == counter_info.key]
                    print(f"DEBUG: Found {len(cpu_ready_metrics)} CPU Ready metrics available for {hostname}")
                    
                    if cpu_ready_metrics:
                        # Try with the specific instance from available metrics
                        for metric in cpu_ready_metrics[:1]:  # Just try the first one
                            try:
                                metric_spec = vim.PerformanceManager.MetricId(
                                    counterId=counter_info.key,
                                    instance=metric.instance
                                )
                                
                                query_spec = vim.PerformanceManager.QuerySpec(
                                    entity=host,
                                    metricId=[metric_spec],
                                    maxSample=10
                                )
                                
                                perf_data = perf_manager.QueryPerf(querySpec=[query_spec])
                                
                                if perf_data and perf_data[0].value:
                                    print(f"DEBUG: Alternative query successful for {hostname}")
                                    for i, sample_info in enumerate(perf_data[0].sampleInfo):
                                        timestamp = sample_info.timestamp
                                        total_ready = 0
                                        for value_info in perf_data[0].value:
                                            if i < len(value_info.value) and value_info.value[i] >= 0:
                                                total_ready += value_info.value[i]
                                        
                                        cpu_ready_data.append({
                                            'Time': timestamp.isoformat() + 'Z',
                                            f'Ready for {hostname}': total_ready,
                                            'Hostname': hostname.split('.')[0]
                                        })
                                    break
                            except Exception as e2:
                                print(f"DEBUG: Alternative query also failed: {e2}")
                    
                except Exception as e2:
                    print(f"DEBUG: Could not query available metrics: {e2}")
                
                continue
        
        print(f"DEBUG: Total records collected: {len(cpu_ready_data)}")
        return cpu_ready_data
    
    def import_files(self):
        file_paths = filedialog.askopenfilenames(
            title="Select CSV or Excel files",
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        
        if not file_paths:
            return
            
        successful_imports = 0
        
        for file_path in file_paths:
            try:
                if file_path.lower().endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                
                # Validate required columns
                if not self.validate_dataframe(df):
                    messagebox.showwarning("Invalid File", 
                                         f"File {Path(file_path).name} does not have required columns (Time, Ready for hostname)")
                    continue
                
                # Add filename for reference
                df['source_file'] = Path(file_path).name
                self.data_frames.append(df)
                successful_imports += 1
                
            except Exception as e:
                messagebox.showerror("Import Error", f"Error importing {Path(file_path).name}: {str(e)}")
        
        if successful_imports > 0:
            self.file_count_label.config(text=f"{len(self.data_frames)} files imported")
            messagebox.showinfo("Import Complete", f"Successfully imported {successful_imports} files")
        
    def validate_dataframe(self, df):
        # Check for Time column
        time_cols = [col for col in df.columns if 'time' in col.lower()]
        if not time_cols:
            return False
            
        # Check for Ready for hostname pattern (can be with $ or actual hostname)
        ready_cols = [col for col in df.columns if 'ready for' in col.lower()]
        if not ready_cols:
            return False
            
        return True

    def setup_advanced_ui(self):
        """Add advanced features UI - call this after the main setup_ui"""
        
        # Advanced Analysis Tab (add to main frame)
        advanced_frame = ttk.LabelFrame(self.main_frame, text="Advanced Analysis", padding="10")
        advanced_frame.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        advanced_frame.columnconfigure(1, weight=1)
        
        # Threshold Configuration
        threshold_frame = ttk.LabelFrame(advanced_frame, text="🚨 Threshold Alerts", padding="10")
        threshold_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Label(threshold_frame, text="Warning Threshold:").grid(row=0, column=0, padx=(0, 5))
        self.warning_threshold = tk.DoubleVar(value=5.0)
        warning_spin = ttk.Spinbox(threshold_frame, from_=1.0, to=50.0, width=10, 
                                textvariable=self.warning_threshold, increment=1.0)
        warning_spin.grid(row=0, column=1, padx=(0, 5))
        ttk.Label(threshold_frame, text="%").grid(row=0, column=2)
        
        ttk.Label(threshold_frame, text="Critical Threshold:").grid(row=1, column=0, padx=(0, 5))
        self.critical_threshold = tk.DoubleVar(value=15.0)
        critical_spin = ttk.Spinbox(threshold_frame, from_=5.0, to=100.0, width=10,
                                textvariable=self.critical_threshold, increment=5.0)
        critical_spin.grid(row=1, column=1, padx=(0, 5))
        ttk.Label(threshold_frame, text="%").grid(row=1, column=2)
        
        ttk.Button(threshold_frame, text="Apply Thresholds", 
                command=self.apply_thresholds).grid(row=2, column=0, columnspan=3, pady=(10, 0))
        
        # Host Health Scoring
        health_frame = ttk.LabelFrame(advanced_frame, text="🏥 Host Health Dashboard", padding="10")
        health_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(10, 0))
        health_frame.columnconfigure(0, weight=1)
        
        # Health score display
        self.health_text = tk.Text(health_frame, height=8, width=40)
        health_scrollbar = ttk.Scrollbar(health_frame, orient=tk.VERTICAL, command=self.health_text.yview)
        self.health_text.configure(yscrollcommand=health_scrollbar.set)
        self.health_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        health_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Visualization Options
        viz_frame = ttk.LabelFrame(advanced_frame, text="📊 Advanced Visualizations", padding="10")
        viz_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
        ttk.Button(viz_frame, text="📅 Heat Map Calendar", 
                command=self.show_heatmap_calendar).grid(row=0, column=0, padx=(0, 10))
        ttk.Button(viz_frame, text="📈 Performance Trends", 
                command=self.show_performance_trends).grid(row=0, column=1, padx=(0, 10))
        ttk.Button(viz_frame, text="🎯 Host Comparison", 
                command=self.show_host_comparison).grid(row=0, column=2, padx=(0, 10))
        ttk.Button(viz_frame, text="📋 Export Report", 
                command=self.export_analysis_report).grid(row=0, column=3)

    def apply_thresholds(self):
        """Apply threshold analysis to current data"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        warning_level = self.warning_threshold.get()
        critical_level = self.critical_threshold.get()
        
        # Analyze each host against thresholds
        health_report = "🏥 HOST HEALTH ANALYSIS\n"
        health_report += "=" * 50 + "\n\n"
        
        hosts_summary = []
        
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            
            avg_cpu_ready = host_data['CPU_Ready_Percent'].mean()
            max_cpu_ready = host_data['CPU_Ready_Percent'].max()
            std_cpu_ready = host_data['CPU_Ready_Percent'].std()
            
            # Calculate health score (0-100)
            health_score = self.calculate_health_score(avg_cpu_ready, max_cpu_ready, std_cpu_ready, 
                                                    warning_level, critical_level)
            
            # Determine status
            if avg_cpu_ready >= critical_level:
                status = "🔴 CRITICAL"
                icon = "🚨"
            elif avg_cpu_ready >= warning_level:
                status = "🟡 WARNING"
                icon = "⚠️"
            else:
                status = "🟢 HEALTHY"
                icon = "✅"
            
            # Time above thresholds
            warning_time = len(host_data[host_data['CPU_Ready_Percent'] >= warning_level])
            critical_time = len(host_data[host_data['CPU_Ready_Percent'] >= critical_level])
            total_time = len(host_data)
            
            warning_pct = (warning_time / total_time) * 100
            critical_pct = (critical_time / total_time) * 100
            
            hosts_summary.append({
                'hostname': hostname,
                'health_score': health_score,
                'status': status,
                'avg_cpu_ready': avg_cpu_ready,
                'max_cpu_ready': max_cpu_ready,
                'warning_pct': warning_pct,
                'critical_pct': critical_pct
            })
        
        # Sort by health score (worst first)
        hosts_summary.sort(key=lambda x: x['health_score'])
        
        # Build report
        for i, host in enumerate(hosts_summary, 1):
            health_report += f"{i}. {host['hostname']} - {host['status']}\n"
            health_report += f"   Health Score: {host['health_score']:.0f}/100\n"
            health_report += f"   Avg CPU Ready: {host['avg_cpu_ready']:.2f}%\n"
            health_report += f"   Max CPU Ready: {host['max_cpu_ready']:.2f}%\n"
            health_report += f"   Time > Warning: {host['warning_pct']:.1f}%\n"
            health_report += f"   Time > Critical: {host['critical_pct']:.1f}%\n"
            
            # Recommendations
            if host['health_score'] < 50:
                health_report += "   💡 RECOMMEND: Immediate attention required\n"
            elif host['health_score'] < 70:
                health_report += "   💡 RECOMMEND: Monitor closely, consider workload redistribution\n"
            elif host['avg_cpu_ready'] < 2:
                health_report += "   💡 RECOMMEND: Good consolidation candidate\n"
            else:
                health_report += "   💡 STATUS: Performing within acceptable ranges\n"
            
            health_report += "\n"
        
        # Add summary recommendations
        health_report += "📋 SUMMARY RECOMMENDATIONS:\n"
        health_report += "=" * 30 + "\n"
        
        critical_hosts = [h for h in hosts_summary if h['avg_cpu_ready'] >= critical_level]
        warning_hosts = [h for h in hosts_summary if warning_level <= h['avg_cpu_ready'] < critical_level]
        healthy_hosts = [h for h in hosts_summary if h['avg_cpu_ready'] < warning_level]
        consolidation_candidates = [h for h in healthy_hosts if h['avg_cpu_ready'] < 2]
        
        if critical_hosts:
            health_report += f"🚨 {len(critical_hosts)} hosts need immediate attention\n"
        if warning_hosts:
            health_report += f"⚠️ {len(warning_hosts)} hosts require monitoring\n"
        if consolidation_candidates:
            health_report += f"🎯 {len(consolidation_candidates)} hosts are consolidation candidates\n"
        
        health_report += f"✅ {len(healthy_hosts)} hosts performing well\n"
        
        # Display in health text widget
        self.health_text.delete(1.0, tk.END)
        self.health_text.insert(1.0, health_report)

    def calculate_health_score(self, avg_cpu_ready, max_cpu_ready, std_cpu_ready, warning_level, critical_level):
        """Calculate a health score from 0-100 based on CPU Ready metrics"""
        
        # Base score starts at 100
        score = 100
        
        # Penalize based on average CPU Ready
        if avg_cpu_ready >= critical_level:
            score -= 50  # Major penalty for critical levels
        elif avg_cpu_ready >= warning_level:
            score -= 25  # Moderate penalty for warning levels
        else:
            # Small penalty proportional to warning level
            score -= (avg_cpu_ready / warning_level) * 10
        
        # Penalize based on maximum CPU Ready (spikes)
        if max_cpu_ready >= critical_level * 2:
            score -= 30  # High spikes are concerning
        elif max_cpu_ready >= critical_level:
            score -= 15
        
        # Penalize based on variability (standard deviation)
        if std_cpu_ready > warning_level:
            score -= 15  # High variability indicates instability
        
        # Ensure score is between 0 and 100
        return max(0, min(100, score))

    def show_heatmap_calendar(self):
        """Display CPU Ready data as a heat map calendar"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create new window for heatmap
        heatmap_window = tk.Toplevel(self.root)
        heatmap_window.title("CPU Ready Heat Map Calendar")
        heatmap_window.geometry("1000x700")
        
        # Create matplotlib figure
        fig, axes = plt.subplots(nrows=len(self.processed_data['Hostname'].unique()), 
                                ncols=1, figsize=(12, 3 * len(self.processed_data['Hostname'].unique())))
        
        if len(self.processed_data['Hostname'].unique()) == 1:
            axes = [axes]  # Make it iterable for single host
        
        # Get date range
        start_date = self.processed_data['Time'].min().date()
        end_date = self.processed_data['Time'].max().date()
        
        # Create custom colormap (green -> yellow -> red)
        colors = ['#00ff00', '#ffff00', '#ff8000', '#ff0000', '#800000']  # Green to dark red
        n_bins = 100
        custom_cmap = LinearSegmentedColormap.from_list('cpu_ready', colors, N=n_bins)
        
        for idx, hostname in enumerate(sorted(self.processed_data['Hostname'].unique())):
            ax = axes[idx]
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            
            # Group by date and calculate daily average
            host_data['Date'] = host_data['Time'].dt.date
            daily_avg = host_data.groupby('Date')['CPU_Ready_Percent'].mean().reset_index()
            
            # Create date range for calendar
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Create calendar grid data
            calendar_data = []
            calendar_dates = []
            
            for single_date in date_range:
                date_val = single_date.date()
                cpu_ready_val = daily_avg[daily_avg['Date'] == date_val]['CPU_Ready_Percent']
                
                if len(cpu_ready_val) > 0:
                    calendar_data.append(cpu_ready_val.iloc[0])
                else:
                    calendar_data.append(0)  # No data for this date
                
                calendar_dates.append(date_val)
            
            # Create calendar layout
            weeks = []
            week_dates = []
            current_week = []
            current_week_dates = []
            
            for i, (date_val, cpu_val) in enumerate(zip(calendar_dates, calendar_data)):
                current_week.append(cpu_val)
                current_week_dates.append(date_val)
                
                # If it's Sunday or the last day, complete the week
                if date_val.weekday() == 6 or i == len(calendar_dates) - 1:
                    # Pad week to 7 days if needed
                    while len(current_week) < 7:
                        current_week.append(0)
                        current_week_dates.append(None)
                    
                    weeks.append(current_week)
                    week_dates.append(current_week_dates)
                    current_week = []
                    current_week_dates = []
            
            # Convert to numpy array for plotting
            calendar_array = np.array(weeks)
            
            # Create heatmap
            im = ax.imshow(calendar_array, cmap=custom_cmap, aspect='auto', 
                        vmin=0, vmax=max(20, max(calendar_data)))
            
            # Set labels
            ax.set_title(f'{hostname} - CPU Ready % Heat Map', fontsize=12, fontweight='bold')
            ax.set_xlabel('Day of Week')
            ax.set_ylabel('Week')
            
            # Set day labels
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            ax.set_xticks(range(7))
            ax.set_xticklabels(days)
            
            # Add colorbar
            cbar = plt.colorbar(im, ax=ax)
            cbar.set_label('CPU Ready %', rotation=270, labelpad=15)
            
            # Add date annotations for significant values
            warning_threshold = self.warning_threshold.get()
            for week_idx, week in enumerate(weeks):
                for day_idx, value in enumerate(week):
                    if value >= warning_threshold:
                        ax.text(day_idx, week_idx, f'{value:.1f}%', 
                            ha='center', va='center', fontsize=8, 
                            color='white' if value > 10 else 'black',
                            fontweight='bold')
        
        plt.tight_layout()
        
        # Embed in tkinter window
        canvas = FigureCanvasTkAgg(fig, master=heatmap_window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Add legend explanation
        legend_frame = ttk.Frame(heatmap_window)
        legend_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(legend_frame, text="📊 Heat Map Legend:", font=('TkDefaultFont', 10, 'bold')).pack(anchor=tk.W)
        ttk.Label(legend_frame, text="🟢 Green: Low CPU Ready (< 5%) - Good performance").pack(anchor=tk.W)
        ttk.Label(legend_frame, text="🟡 Yellow: Moderate CPU Ready (5-10%) - Monitor").pack(anchor=tk.W)
        ttk.Label(legend_frame, text="🟠 Orange: High CPU Ready (10-15%) - Warning").pack(anchor=tk.W)
        ttk.Label(legend_frame, text="🔴 Red: Critical CPU Ready (> 15%) - Action needed").pack(anchor=tk.W)

    def show_performance_trends(self):
        """Show advanced performance trend analysis"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create trends window
        trends_window = tk.Toplevel(self.root)
        trends_window.title("Performance Trends Analysis")
        trends_window.geometry("1200x800")
        
        # Create figure with subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # 1. Moving Average Trends
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            host_data = host_data.sort_values('Time')
            
            # Calculate moving averages
            host_data['MA_3'] = host_data['CPU_Ready_Percent'].rolling(window=3, center=True).mean()
            host_data['MA_7'] = host_data['CPU_Ready_Percent'].rolling(window=7, center=True).mean()
            
            ax1.plot(host_data['Time'], host_data['CPU_Ready_Percent'], alpha=0.3, label=f'{hostname} (Raw)')
            ax1.plot(host_data['Time'], host_data['MA_7'], linewidth=2, label=f'{hostname} (7-period MA)')
        
        ax1.set_title('CPU Ready % Moving Averages')
        ax1.set_ylabel('CPU Ready %')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. Peak Analysis
        peak_data = []
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            peaks = host_data['CPU_Ready_Percent'].nlargest(5)  # Top 5 peaks
            peak_data.extend([(hostname, peak) for peak in peaks])
        
        hostnames = [item[0] for item in peak_data]
        peak_values = [item[1] for item in peak_data]
        
        ax2.scatter(range(len(peak_values)), peak_values, c=['red' if x > 15 else 'orange' if x > 10 else 'yellow' for x in peak_values], s=100, alpha=0.7)
        ax2.set_title('Top Performance Peaks by Host')
        ax2.set_ylabel('CPU Ready %')
        ax2.set_xlabel('Peak Instance')
        
        # 3. Performance Distribution
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            ax3.hist(host_data['CPU_Ready_Percent'], bins=20, alpha=0.5, label=hostname)
        
        ax3.axvline(self.warning_threshold.get(), color='orange', linestyle='--', label='Warning Threshold')
        ax3.axvline(self.critical_threshold.get(), color='red', linestyle='--', label='Critical Threshold')
        ax3.set_title('CPU Ready % Distribution')
        ax3.set_xlabel('CPU Ready %')
        ax3.set_ylabel('Frequency')
        ax3.legend()
        
        # 4. Time-based patterns (hourly if applicable)
        if len(self.processed_data) > 24:  # Only if we have enough data points
            self.processed_data['Hour'] = self.processed_data['Time'].dt.hour
            hourly_avg = self.processed_data.groupby(['Hour', 'Hostname'])['CPU_Ready_Percent'].mean().reset_index()
            
            for hostname in sorted(self.processed_data['Hostname'].unique()):
                host_hourly = hourly_avg[hourly_avg['Hostname'] == hostname]
                ax4.plot(host_hourly['Hour'], host_hourly['CPU_Ready_Percent'], marker='o', label=hostname)
            
            ax4.set_title('Average CPU Ready % by Hour of Day')
            ax4.set_xlabel('Hour of Day')
            ax4.set_ylabel('Average CPU Ready %')
            ax4.legend()
            ax4.grid(True, alpha=0.3)
            ax4.set_xticks(range(0, 24, 2))
        else:
            ax4.text(0.5, 0.5, 'Insufficient data\nfor hourly analysis', 
                    ha='center', va='center', transform=ax4.transAxes, fontsize=12)
            ax4.set_title('Hourly Pattern Analysis')
        
        plt.tight_layout()
        
        # Embed in tkinter window
        canvas = FigureCanvasTkAgg(fig, master=trends_window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def show_host_comparison(self):
        """Show detailed host-by-host comparison"""
        if self.processed_data is None:
            messagebox.showwarning("No Data", "Please calculate CPU Ready % first")
            return
        
        # Create comparison window
        comparison_window = tk.Toplevel(self.root)
        comparison_window.title("Host Performance Comparison")
        comparison_window.geometry("1000x600")
        
        # Create comparison table
        columns = ('Host', 'Avg %', 'Max %', 'Min %', 'Std Dev', 'Health Score', 'Status', 'Recommendation')
        tree = ttk.Treeview(comparison_window, columns=columns, show='headings', height=15)
        
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=100)
        
        # Calculate comprehensive stats for each host
        comparison_data = []
        for hostname in sorted(self.processed_data['Hostname'].unique()):
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname]
            
            avg_cpu = host_data['CPU_Ready_Percent'].mean()
            max_cpu = host_data['CPU_Ready_Percent'].max()
            min_cpu = host_data['CPU_Ready_Percent'].min()
            std_cpu = host_data['CPU_Ready_Percent'].std()
            
            health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu, 
                                                    self.warning_threshold.get(), 
                                                    self.critical_threshold.get())
            
            if avg_cpu >= self.critical_threshold.get():
                status = "🔴 Critical"
                recommendation = "Immediate attention"
            elif avg_cpu >= self.warning_threshold.get():
                status = "🟡 Warning"
                recommendation = "Monitor closely"
            elif avg_cpu < 2:
                status = "🟢 Healthy"
                recommendation = "Consolidation candidate"
            else:
                status = "🟢 Healthy"
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
        
        # Sort by health score (worst first)
        comparison_data.sort(key=lambda x: x['health'])
        
        # Populate table
        for data in comparison_data:
            tree.insert('', 'end', values=(
                data['hostname'],
                f"{data['avg']:.2f}%",
                f"{data['max']:.2f}%", 
                f"{data['min']:.2f}%",
                f"{data['std']:.2f}%",
                f"{data['health']:.0f}/100",
                data['status'],
                data['recommendation']
            ))
        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(comparison_window, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def export_analysis_report(self):
        """Export comprehensive analysis report"""
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
                    
                    # Time above thresholds
                    warning_time = len(host_data[host_data['CPU_Ready_Percent'] >= self.warning_threshold.get()])
                    critical_time = len(host_data[host_data['CPU_Ready_Percent'] >= self.critical_threshold.get()])
                    total_time = len(host_data)
                    
                    health_score = self.calculate_health_score(avg_cpu, max_cpu, std_cpu,
                                                            self.warning_threshold.get(),
                                                            self.critical_threshold.get())
                    
                    report_data.append({
                        'Hostname': hostname,
                        'Average_CPU_Ready_Percent': round(avg_cpu, 2),
                        'Maximum_CPU_Ready_Percent': round(max_cpu, 2),
                        'Minimum_CPU_Ready_Percent': round(min_cpu, 2),
                        'Standard_Deviation': round(std_cpu, 2),
                        'Health_Score': round(health_score, 0),
                        'Total_Records': total_time,
                        'Time_Above_Warning_Threshold': warning_time,
                        'Time_Above_Critical_Threshold': critical_time,
                        'Percent_Time_Warning': round((warning_time/total_time)*100, 1),
                        'Percent_Time_Critical': round((critical_time/total_time)*100, 1),
                        'Data_Source': host_data['Source_File'].iloc[0] if 'Source_File' in host_data.columns else 'vCenter',
                        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Warning_Threshold_Used': self.warning_threshold.get(),
                        'Critical_Threshold_Used': self.critical_threshold.get()
                    })
                
                # Convert to DataFrame and export
                report_df = pd.DataFrame(report_data)
                report_df.to_csv(filename, index=False)
                
                messagebox.showinfo("Export Complete", f"Analysis report exported to:\n{filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export report:\n{str(e)}")
    
    def clear_files(self):
        self.data_frames = []
        self.processed_data = None
        self.file_count_label.config(text="No files imported")
        self.clear_results()
        
    def on_interval_change(self, event):
        self.current_interval = self.interval_var.get()
        
    def calculate_cpu_ready(self):
        if not self.data_frames:
            messagebox.showwarning("No Data", "Please import CSV/Excel files first or fetch data from vCenter")
            return
            
        try:
            # Combine all dataframes
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
            
            self.update_results_display()
            self.update_chart()
            self.update_removal_options()
            
        except Exception as e:
            print(f"DEBUG: Full error details: {e}")
            import traceback
            traceback.print_exc()
            messagebox.showerror("Calculation Error", f"Error calculating CPU Ready %: {str(e)}")
   
    def update_results_display(self):
        # Clear existing results
        self.clear_results()
        
        if self.processed_data is None:
            return
            
        # Group by hostname and calculate statistics
        stats = self.processed_data.groupby('Hostname')['CPU_Ready_Percent'].agg([
            'mean', 'max', 'count'
        ]).reset_index()
        
        # Insert data into treeview
        for _, row in stats.iterrows():
            self.results_tree.insert('', 'end', values=(
                row['Hostname'],
                f"{row['mean']:.2f}%",
                f"{row['max']:.2f}%",
                int(row['count'])
            ))
    
    def update_chart(self):
        if self.processed_data is None:
            return
            
        self.ax.clear()
        
        # Create time series plot with separate lines for each host
        hostnames = sorted(self.processed_data['Hostname'].unique())
        
        for hostname in hostnames:
            host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            host_data = host_data.sort_values('Time')
            
            self.ax.plot(host_data['Time'], host_data['CPU_Ready_Percent'], 
                        marker='o', markersize=3, linewidth=1.5, label=hostname, alpha=0.8)
        
        self.ax.set_title(f'CPU Ready % Over Time by Host ({self.current_interval} Interval)')
        self.ax.set_xlabel('Time')
        self.ax.set_ylabel('CPU Ready %')
        self.ax.legend()
        self.ax.grid(True, alpha=0.3)
        
        # Format x-axis to show dates nicely
        self.fig.autofmt_xdate()
        self.fig.tight_layout()
        self.canvas.draw()
    
    def update_removal_options(self):
        """Update the hosts listbox with available hosts"""
        if self.processed_data is None:
            return
            
        # Clear existing items
        self.hosts_listbox.delete(0, tk.END)
        
        # Add all hostnames to listbox
        hostnames = sorted(self.processed_data['Hostname'].unique())
        for hostname in hostnames:
            self.hosts_listbox.insert(tk.END, hostname)
        
    def analyze_removal_impact(self):
        if self.processed_data is None or not self.removal_var.get():
            messagebox.showwarning("Selection Required", "Please calculate CPU Ready % and select a host to remove")
            return
            
        selected_host = self.removal_var.get()
        
        # Get data for selected host and remaining hosts
        selected_host_data = self.processed_data[self.processed_data['Hostname'] == selected_host]
        remaining_data = self.processed_data[self.processed_data['Hostname'] != selected_host]
        
        if remaining_data.empty:
            self.impact_label.config(text="Cannot remove all hosts - no remaining infrastructure!")
            return
            
        if selected_host_data.empty:
            self.impact_label.config(text="Selected host not found in data!")
            return
        
        # Calculate current statistics
        current_total_sum = self.processed_data['CPU_Ready_Sum'].sum()
        current_avg_percent = self.processed_data['CPU_Ready_Percent'].mean()
        
        # Calculate workload to be redistributed
        workload_to_redistribute = selected_host_data['CPU_Ready_Sum'].sum()
        remaining_hosts = remaining_data['Hostname'].unique()
        num_remaining_hosts = len(remaining_hosts)
        
        # Redistribute workload evenly among remaining hosts
        additional_workload_per_host = workload_to_redistribute / num_remaining_hosts
        
        # Create new dataset with redistributed workload
        redistributed_data = remaining_data.copy()
        interval_seconds = self.intervals[self.current_interval]
        
        # Add redistributed workload to each remaining host
        for hostname in remaining_hosts:
            mask = redistributed_data['Hostname'] == hostname
            redistributed_data.loc[mask, 'CPU_Ready_Sum'] += additional_workload_per_host / mask.sum()
            # Recalculate CPU Ready % with new workload
            redistributed_data.loc[mask, 'CPU_Ready_Percent'] = (
                redistributed_data.loc[mask, 'CPU_Ready_Sum'] / (interval_seconds * 1000)
            ) * 100
        
        # Calculate new statistics after redistribution
        new_avg_percent = redistributed_data['CPU_Ready_Percent'].mean()
        new_total_sum = redistributed_data['CPU_Ready_Sum'].sum()
        
        # Calculate individual host impacts
        host_impacts = []
        for hostname in remaining_hosts:
            original_host_avg = remaining_data[remaining_data['Hostname'] == hostname]['CPU_Ready_Percent'].mean()
            new_host_avg = redistributed_data[redistributed_data['Hostname'] == hostname]['CPU_Ready_Percent'].mean()
            increase = new_host_avg - original_host_avg
            host_impacts.append(f"{hostname}: +{increase:.2f}%")
        
        # Calculate overall impact metrics
        avg_increase = new_avg_percent - current_avg_percent
        workload_percentage = (workload_to_redistribute / current_total_sum) * 100
        
        # Format results with better layout and styling
        impact_text = (f"📊 REMOVING HOST: {selected_host}\n"
                      f"{'='*50}\n"
                      f"📈 Workload Impact:\n"
                      f"   • Total workload to redistribute: {workload_percentage:.1f}%\n"
                      f"   • Overall avg CPU Ready increase: +{avg_increase:.2f}%\n"
                      f"   • Workload per remaining host: +{(additional_workload_per_host/workload_to_redistribute*workload_percentage):.1f}%\n"
                      f"\n🖥️  Infrastructure Changes:\n"
                      f"   • Remaining hosts: {num_remaining_hosts}\n"
                      f"   • Hosts after removal: {', '.join(remaining_hosts)}\n"
                      f"\n📋 Per-Host Impact Details:\n")
        
        # Add each host impact on separate lines with better formatting
        for i, impact in enumerate(host_impacts, 1):
            hostname, increase = impact.split(': ')
            impact_text += f"   {i}. {hostname}: {increase} CPU Ready\n"
        
        # Add recommendation based on impact level
        if avg_increase > 10:
            impact_text += f"\n⚠️  HIGH IMPACT: +{avg_increase:.1f}% increase may cause performance issues"
        elif avg_increase > 5:
            impact_text += f"\n⚡ MODERATE IMPACT: +{avg_increase:.1f}% increase - monitor closely"
        else:
            impact_text += f"\n✅ LOW IMPACT: +{avg_increase:.1f}% increase - safe to proceed"
        
        self.impact_label.config(text=impact_text)
        
        # Update visualization to show the redistribution impact
        self.update_redistribution_chart(selected_host, redistributed_data)
    
    def update_redistribution_chart(self, removed_host, redistributed_data):
        """Update chart to show time series before/after redistribution comparison"""
        if self.processed_data is None:
            return
            
        self.ax.clear()
        
        # Get remaining hosts
        remaining_hosts = sorted(redistributed_data['Hostname'].unique())
        
        # Plot original data for remaining hosts (solid lines)
        for hostname in remaining_hosts:
            original_host_data = self.processed_data[self.processed_data['Hostname'] == hostname].copy()
            original_host_data = original_host_data.sort_values('Time')
            
            self.ax.plot(original_host_data['Time'], original_host_data['CPU_Ready_Percent'], 
                        linewidth=2, label=f'{hostname} (Original)', alpha=0.7, linestyle='-')
        
        # Plot redistributed data for remaining hosts (dashed lines)
        for hostname in remaining_hosts:
            redistributed_host_data = redistributed_data[redistributed_data['Hostname'] == hostname].copy()
            redistributed_host_data = redistributed_host_data.sort_values('Time')
            
            self.ax.plot(redistributed_host_data['Time'], redistributed_host_data['CPU_Ready_Percent'], 
                        linewidth=2, label=f'{hostname} (After Redistribution)', 
                        alpha=0.9, linestyle='--')
        
        # Plot the removed host data for reference (thin gray line)
        removed_host_data = self.processed_data[self.processed_data['Hostname'] == removed_host].copy()
        if not removed_host_data.empty:
            removed_host_data = removed_host_data.sort_values('Time')
            self.ax.plot(removed_host_data['Time'], removed_host_data['CPU_Ready_Percent'], 
                        linewidth=1, label=f'{removed_host} (REMOVED)', 
                        alpha=0.5, linestyle=':', color='gray')
        
        # Customize chart
        self.ax.set_title(f'CPU Ready % Over Time - Impact of Removing {removed_host}\n({self.current_interval} Interval)')
        self.ax.set_xlabel('Time')
        self.ax.set_ylabel('CPU Ready %')
        self.ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        self.ax.grid(True, alpha=0.3)
        
        # Add annotation showing the workload redistribution
        removed_host_avg = self.processed_data[self.processed_data['Hostname'] == removed_host]['CPU_Ready_Percent'].mean()
        workload_to_redistribute = self.processed_data[self.processed_data['Hostname'] == removed_host]['CPU_Ready_Sum'].sum()
        total_workload = self.processed_data['CPU_Ready_Sum'].sum()
        workload_percentage = (workload_to_redistribute / total_workload) * 100
        
        annotation_text = (f'Removed Host Impact:\n'
                          f'• {removed_host}: {removed_host_avg:.2f}% avg\n'
                          f'• {workload_percentage:.1f}% of total workload\n'
                          f'• Redistributed to {len(remaining_hosts)} hosts')
        
        self.ax.text(0.02, 0.98, annotation_text, 
                    transform=self.ax.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        # Format x-axis to show dates nicely
        self.fig.autofmt_xdate()
        self.fig.tight_layout()
        self.canvas.draw()
    
    def clear_results(self):
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        self.impact_label.config(text="")
        self.ax.clear()
        self.canvas.draw()

    def on_closing(self):
        """Handle application closing properly"""
        try:
            # Disconnect from vCenter if connected
            if hasattr(self, 'vcenter_connection') and self.vcenter_connection:
                Disconnect(self.vcenter_connection)
            
            # Close matplotlib figure properly
            if hasattr(self, 'fig'):
                plt.close(self.fig)
            
            # Close all matplotlib figures
            plt.close('all')
            
            # Destroy the tkinter window
            self.root.quit()
            self.root.destroy()
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
        finally:
            # Force exit if needed
            import sys
            sys.exit(0)

def main():
    try:
        root = tk.Tk()
        app = CPUReadyAnalyzer(root)
        root.mainloop()
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
    finally:
        # Ensure clean exit
        try:
            plt.close('all')
        except:
            pass
        print("Application closed successfully")

if __name__ == "__main__":
    main()