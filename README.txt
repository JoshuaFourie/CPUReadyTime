# vCenter CPU Ready Analyzer

A comprehensive tool for analyzing VMware ESXi host CPU Ready performance metrics and planning infrastructure consolidation.

## 🚀 Features

### Data Sources
- **Direct vCenter Integration** - Connect to VMware vCenter Server and fetch live performance data
- **CSV/Excel Import** - Import exported performance data from vCenter or other monitoring tools
- **Multiple Time Periods** - Real-Time, Last Day, Last Week, Last Month, Last Year (matching vCenter intervals)

### Analysis Capabilities
- **CPU Ready % Calculations** - Uses VMware's standard formula: `(CPU Ready Sum / (Interval × 1000)) × 100`
- **Multi-Host Analysis** - Analyze entire infrastructure at once
- **Time Series Visualization** - Interactive charts showing CPU Ready trends over time
- **Host Performance Statistics** - Average, maximum, and record counts per host

### Consolidation Planning
- **Multi-Host Removal Analysis** - Select multiple hosts for removal and see combined impact
- **Workload Redistribution** - Calculate how workloads redistribute to remaining hosts
- **Impact Assessment** - Get Low/Moderate/High risk recommendations
- **Consolidation Efficiency** - Measure workload reduction per host removed
- **Visual Before/After Comparison** - Charts showing original vs. redistributed performance

## 📋 System Requirements

### Prerequisites
- Windows 10/11 or Windows Server 2016+
- Network connectivity to vCenter Server (for direct integration)
- 4GB RAM minimum, 8GB recommended
- 100MB free disk space

### For Development/Source Code
- Python 3.8 or higher
- Required Python packages (see Installation section)

## 🛠️ Installation

### Option 1: Standalone Executable (Recommended)
1. Download `vCenter_CPU_Analyzer.exe`
2. Place in desired folder
3. Run directly - no installation required!

### Option 2: Python Source Code
1. Install Python 3.8+ from python.org
2. Install required packages:
```bash
pip install pandas numpy matplotlib seaborn tkinter openpyxl xlrd pyvmomi requests
```
3. Run the application:
```bash
python vcenter_cpu_analyzer.py
```

## 🎯 Quick Start Guide

### Method 1: vCenter Integration (Recommended)
1. **Connect to vCenter**
   - Enter vCenter server IP/hostname
   - Enter username and password
   - Click "Connect to vCenter"
   - Wait for "Connected ✓" status

2. **Select Time Period**
   - Choose from: Real-Time, Last Day, Last Week, Last Month, Last Year
   - See automatic date range calculation

3. **Fetch Data**
   - Click "Fetch CPU Ready Data"
   - Wait for data collection (progress dialog shows status)
   - Success message shows hosts found and records collected

4. **Analyze Performance**
   - Click "Calculate CPU Ready %"
   - Review results table and time series chart
   - Check for hosts with high CPU Ready percentages

5. **Plan Consolidation**
   - Select hosts to remove (Ctrl+click for multiple)
   - Click "Analyze Impact"
   - Review workload redistribution and risk assessment

### Method 2: File Import
1. **Export Data from vCenter**
   - Export CPU Ready performance data as CSV/Excel
   - Ensure columns include: Time, "Ready for [hostname]"

2. **Import Files**
   - Click "Import CSV/Excel Files"
   - Select your exported files
   - Files are validated automatically

3. **Follow steps 4-5 from Method 1**

## 📊 Understanding the Results

### CPU Ready % Interpretation
- **0-5%**: Excellent performance, no CPU contention
- **5-10%**: Good performance, minor contention during peaks
- **10-20%**: Moderate contention, monitor closely
- **20%+**: High contention, performance impact likely

### Impact Assessment Levels
- **✅ LOW IMPACT** (<5% increase): Safe to proceed with consolidation
- **⚡ MODERATE IMPACT** (5-10% increase): Monitor closely after consolidation
- **⚠️ HIGH IMPACT** (10-15% increase): May cause performance issues
- **🔴 VERY HIGH IMPACT** (15%+ increase): Likely to cause significant problems

### Consolidation Efficiency
- Measures workload reduction percentage per host removed
- Higher efficiency = better consolidation candidates
- Typical good efficiency: 10-25% per host

## 🔧 Advanced Usage

### Time Period Selection Guidelines
- **Real-Time**: Last hour, 20-second intervals - for immediate troubleshooting
- **Last Day**: Last 24 hours, 5-minute intervals - for daily analysis
- **Last Week**: Last 7 days, 30-minute intervals - for weekly trends
- **Last Month**: Last 30 days, 2-hour intervals - for capacity planning
- **Last Year**: Last 365 days, daily intervals - for long-term trends

### Multi-Host Removal Strategies
1. **Incremental Approach**: Remove 1-2 hosts at a time, analyze impact
2. **Workload-Based**: Remove hosts with lowest CPU Ready percentages first
3. **Efficiency-Based**: Target hosts that provide best consolidation efficiency
4. **Risk-Balanced**: Balance infrastructure reduction with performance risk

### Data Export Tips
When exporting from vCenter:
- Use Performance > Advanced view
- Select CPU Ready (millisecond) metric
- Choose appropriate time range and interval
- Export as CSV for best compatibility

## ❓ Troubleshooting

### Connection Issues
**Problem**: Cannot connect to vCenter
**Solutions**:
- Verify vCenter server address and credentials
- Check network connectivity
- Ensure vCenter is running and accessible
- Try with administrator account
- Check for firewall blocking (port 443)

### Data Retrieval Issues
**Problem**: "No CPU Ready data found"
**Solutions**:
- Check vCenter statistics levels (Administration > System Configuration > Statistics)
- Verify CPU metrics are being collected
- Try different time periods (start with "Last Day")
- Ensure ESXi hosts are connected and not in maintenance mode

### Performance Issues
**Problem**: Application runs slowly
**Solutions**:
- Use shorter time periods for initial analysis
- Limit to essential hosts only
- Close other applications to free memory
- Consider using "Last Week" or "Last Month" for large environments

### Import Issues
**Problem**: CSV/Excel import fails
**Solutions**:
- Verify file contains "Time" column and "Ready for [hostname]" columns
- Check for proper timestamp format (ISO 8601)
- Ensure data is not corrupted
- Try saving Excel files as CSV format

## 🔒 Security Considerations

### vCenter Credentials
- Credentials are not stored permanently
- Use read-only vCenter account when possible
- SSL verification disabled for self-signed certificates (common in vCenter)
- Connection is automatically closed when application exits

### Data Privacy
- No performance data is transmitted outside your network
- All analysis is performed locally
- Data is stored in memory only during session
- No data persistence between sessions

## 🆘 Support & Contact

### Common Error Messages
- **"Invalid interval"**: Try different time period or use file import
- **"No hosts found"**: Check vCenter connection and host status
- **"Calculation error"**: Verify data format and try re-importing
- **"Connection timeout"**: Check network connectivity to vCenter

### Getting Help
For issues or questions:
1. Check this README for troubleshooting steps
2. Verify system requirements are met
3. Test with a simple scenario first (single host, short time period)
4. Check vCenter logs for performance data collection issues

## 📄 License & Disclaimer

This tool is provided for infrastructure analysis and planning purposes. Users are responsible for:
- Validating results before making infrastructure changes
- Testing consolidation plans in non-production environments
- Following organization change management procedures
- Backing up critical systems before consolidation

The tool provides analysis based on historical performance data. Actual results may vary based on workload changes, seasonal patterns, and other factors not captured in historical metrics.

## 🔄 Version History

### v1.0
- Initial release with basic CPU Ready analysis
- Single host removal analysis
- CSV/Excel import functionality

### v2.0 (Current)
- Added vCenter direct integration
- Multiple time period support matching vCenter intervals
- Multi-host removal analysis
- Enhanced visualization with before/after comparison
- Improved error handling and debugging
- Professional impact assessment with risk levels
- Consolidation efficiency metrics

---

**Built for VMware administrators who need to optimize their infrastructure efficiently and safely.**