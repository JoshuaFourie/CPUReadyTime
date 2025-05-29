import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
from pathlib import Path
import re

class CPUReadyAnalyzer:
    def __init__(self, root):
        self.root = root
        self.root.title("CPU Ready Analysis Tool")
        self.root.geometry("1200x800")
        
        # Add proper window close handling
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Data storage
        self.data_frames = []  # List of DataFrames from imported files
        self.processed_data = None
        self.current_interval = "Past Day"
        
        # Update intervals in seconds
        self.intervals = {
            "Realtime": 20,
            "Past Day": 300,
            "Past Week": 1800,
            "Past Month": 7200,
            "Past Year": 86400
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
        import_frame = ttk.LabelFrame(main_frame, text="Import Files", padding="10")
        import_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        ttk.Button(import_frame, text="Import CSV/Excel Files", 
                  command=self.import_files).grid(row=0, column=0, padx=(0, 10))
        
        self.file_count_label = ttk.Label(import_frame, text="No files imported")
        self.file_count_label.grid(row=0, column=1)
        
        ttk.Button(import_frame, text="Clear All Files", 
                  command=self.clear_files).grid(row=0, column=2, padx=(10, 0))
        
        # Configuration section
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="10")
        config_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        ttk.Label(config_frame, text="Update Interval:").grid(row=0, column=0, padx=(0, 10))
        
        self.interval_var = tk.StringVar(value="Past Day")
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
        
        ttk.Label(removal_frame, text="Select hosts to remove:").grid(row=0, column=0, padx=(0, 10))
        
        self.removal_var = tk.StringVar()
        self.removal_combo = ttk.Combobox(removal_frame, textvariable=self.removal_var, 
                                        state="readonly", width=30)
        self.removal_combo.grid(row=0, column=1, padx=(0, 10))
        
        ttk.Button(removal_frame, text="Analyze Impact", 
                  command=self.analyze_removal_impact).grid(row=0, column=2, padx=(10, 0))
        
        self.impact_label = ttk.Label(removal_frame, text="", foreground="blue")
        self.impact_label.grid(row=1, column=0, columnspan=3, pady=(10, 0))
        
        # Chart section
        chart_frame = ttk.LabelFrame(main_frame, text="Visualization", padding="10")
        chart_frame.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)
        
        # Create matplotlib figure
        self.fig, self.ax = plt.subplots(figsize=(10, 4))
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
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
    
    def clear_files(self):
        self.data_frames = []
        self.processed_data = None
        self.file_count_label.config(text="No files imported")
        self.clear_results()
        
    def on_interval_change(self, event):
        self.current_interval = self.interval_var.get()
        
    def calculate_cpu_ready(self):
        if not self.data_frames:
            messagebox.showwarning("No Data", "Please import CSV/Excel files first")
            return
            
        try:
            # Combine all dataframes
            combined_data = []
            
            for df in self.data_frames:
                # Find time and ready columns
                time_col = next((col for col in df.columns if 'time' in col.lower()), None)
                ready_col = next((col for col in df.columns if 'ready for' in col.lower()), None)
                
                if not time_col or not ready_col:
                    continue
                
                # Extract hostname from column name
                if '$' in ready_col:
                    # Format: "Ready for $hostname"
                    hostname_match = re.search(r'\$(\w+)', ready_col)
                    hostname = hostname_match.group(1) if hostname_match else "Unknown"
                else:
                    # Format: "Ready for full.hostname.domain"
                    hostname_match = re.search(r'Ready for (.+)', ready_col)
                    if hostname_match:
                        full_hostname = hostname_match.group(1).strip()
                        # Extract just the first part of the hostname
                        hostname = full_hostname.split('.')[0]
                    else:
                        hostname = "Unknown"
                
                # Process data
                subset = df[[time_col, ready_col, 'source_file']].copy()
                subset.columns = ['Time', 'CPU_Ready_Sum', 'Source_File']
                subset['Hostname'] = hostname
                
                # Convert time to datetime
                subset['Time'] = pd.to_datetime(subset['Time'])
                
                # Calculate CPU Ready %
                interval_seconds = self.intervals[self.current_interval]
                subset['CPU_Ready_Percent'] = (subset['CPU_Ready_Sum'] / (interval_seconds * 1000)) * 100
                
                combined_data.append(subset)
            
            if not combined_data:
                messagebox.showerror("Processing Error", "No valid data found in imported files")
                return
                
            self.processed_data = pd.concat(combined_data, ignore_index=True)
            self.update_results_display()
            self.update_chart()
            self.update_removal_options()
            
        except Exception as e:
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
        if self.processed_data is None:
            return
            
        hostnames = sorted(self.processed_data['Hostname'].unique())
        self.removal_combo['values'] = hostnames
        
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
    
    def on_closing(self):
        """Handle application closing properly"""
        try:
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
    
    def clear_results(self):
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        self.impact_label.config(text="")
        self.ax.clear()
        self.canvas.draw()

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