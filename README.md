vCenter CPU Ready Analyzer - Version 2.0 Real-Time Edition

A comprehensive tool for real-time monitoring and analysis of VMware ESXi host CPU Ready performance metrics with AI-powered infrastructure consolidation planning.

🚀 Features

Real-Time Monitoring
- Live Performance Dashboard - Real-time CPU Ready monitoring with 20-second intervals
- Interactive Charts - Live updating performance graphs with threshold alerts
- Automated Data Collection - Continuous monitoring with SQLite database storage
- Export Real-Time Data - Export collected data for comprehensive analysis
- Threshold Alerting - Configurable warning and critical thresholds with visual indicators

Data Sources
- Direct vCenter Integration - Connect to VMware vCenter Server and fetch live/historical performance data
- CSV/Excel Import - Import exported performance data from vCenter or other monitoring tools
- Real-Time Collection - Live monitoring with seamless integration to analysis engine
- Multiple Time Periods - Real-Time, Last Day, Last Week, Last Month, Last Year (matching vCenter intervals)
- Intelligent Data Detection - Automatic format detection and conversion for different data sources

Advanced Analysis Capabilities
- CPU Ready Calculations - Enhanced calculations using VMware's CPU Readiness metric (percentage-based)
- Multi-Host Analysis - Analyze entire infrastructure with real-time and historical data
- Time Series Visualization - Interactive charts with threshold lines and trend analysis
- Host Performance Statistics - Comprehensive metrics including health scores (0-100 scale)
- Performance Distribution - Statistical analysis with box plots and percentile calculations
- Heat Map Calendar - Visual calendar showing daily performance patterns

AI-Powered Consolidation Planning
- Intelligent Recommendations - AI engine analyzes performance patterns and suggests optimal consolidation candidates
- Multiple Strategies - Conservative, Balanced, and Aggressive consolidation approaches
- Risk Assessment - Comprehensive analysis of consolidation risks and mitigation strategies
- Multi-Host Removal Analysis - Advanced modeling of removing multiple hosts simultaneously
- Workload Redistribution - Detailed analysis of how workloads redistribute to remaining hosts
- Impact Assessment - Low/Moderate/High risk recommendations with detailed reasoning
- Cost Savings Analysis - Estimated annual savings from infrastructure reduction
- Implementation Guidance - Step-by-step recommendations for safe consolidation

Advanced Visualizations
- Heat Map Calendar Views - Color-coded calendar showing performance patterns over time
- Performance Trend Analysis - Moving averages, peak analysis, and hourly patterns
- Host Comparison Matrix - Side-by-side performance rankings and health scores
- Distribution Analysis - Box plots showing performance variability across hosts
- Before/After Visualization - Charts showing original vs. post-consolidation performance

Comprehensive Reporting
- PDF Report Generation - Professional reports with embedded charts and analysis
- Executive Summaries - High-level findings and recommendations for management
- Technical Documentation - Detailed analysis with implementation guides
- Export Capabilities - CSV export for all analysis results and metrics

📋 System Requirements

Prerequisites
- Windows 10/11 or Windows Server 2016+ (Primary support)
- Linux (Ubuntu 18.04+, CentOS 7+) - Full compatibility
- macOS 10.14+ (Limited testing)
- Network connectivity to vCenter Server (for direct integration and real-time monitoring)
- 4GB RAM minimum, 8GB recommended for real-time monitoring
- 500MB free disk space (includes SQLite database for real-time data)

For Development/Source Code
- Python 3.8 or higher
- Required Python packages (see Installation section)

🛠️ Installation

Option 1: Standalone Executable (Recommended)
1. Download `vCenter_CPU_Analyzer_v2.exe`
2. Place in desired folder
3. Run directly - no installation required!
4. Real-time monitoring database will be created automatically

Option 2: Python Source Code
1. Install Python 3.8+ from python.org
2. Install required packages:
```bash
pip install pandas numpy matplotlib seaborn tkinter openpyxl xlrd pyvmomi requests reportlab sqlite3
```
3. Run the application:
```bash
python vcenter_cpu_analyzerv.py
```

🎯 Quick Start Guide

Method 1: Real-Time Monitoring (Recommended)
1. Connect to vCenter
   - Enter vCenter server IP/hostname
   - Enter username and password
   - Click "Connect to vCenter"
   - Wait for "Connected ✓" status

2. Start Real-Time Monitoring
   - Navigate to "Real-Time Dashboard" tab
   - Click "Start Monitoring"
   - Watch live CPU Ready data collection
   - Monitor threshold alerts and performance trends

3. Export for Analysis
   - After collecting sufficient data (5-10 minutes minimum)
   - Click "Use for Analysis" to export real-time data
   - Data is automatically integrated into analysis engine
   - Continue with steps 4-5 below

4. Analyze Performance
   - Review results table and interactive time series charts
   - Check health scores and status indicators
   - Identify hosts with high CPU Ready percentages

5. AI-Powered Consolidation Planning
   - Navigate to "Hosts" tab
   - Click "Generate Recommendations"
   - Select consolidation strategy (Conservative/Balanced/Aggressive)
   - Review AI recommendations with detailed reasoning
   - Click "Select AI Picks" to auto-select recommended hosts
   - Click "Analyze Impact" for comprehensive consolidation analysis

Method 2: Historical Data Analysis
1. Connect to vCenter
   - Follow connection steps from Method 1

2. Select Time Period
   - Choose from: Real-Time, Last Day, Last Week, Last Month, Last Year
   - See automatic date range calculation

3. Fetch Historical Data
   - Click "Fetch CPU Ready Data"
   - Wait for data collection (progress dialog shows status)
   - Success message shows hosts found and records collected

4. Follow steps 4-5 from Method 1

Method 3: File Import
1. Export Data from vCenter
   - Export CPU Ready performance data as CSV/Excel
   - Ensure columns include: Time, "Ready for [hostname]"

2. Import Files
   - Click "Import CSV/Excel Files"
   - Select your exported files
   - Files are validated and formats auto-detected

3. Follow steps 4-5 from Method 1

📊 Understanding the Results

CPU Ready Interpretation
- 0-2%: Excellent performance, ideal for consolidation
- 2-5%: Good performance, safe consolidation candidates
- 5-10%: Moderate performance, monitor after consolidation
- 10-20%: High contention, investigate before consolidation
- 20%+: Critical contention, address before considering consolidation

Health Scores (0-100 Scale)
- 90-100: Excellent health, prime consolidation candidate
- 80-89: Good health, suitable for consolidation
- 70-79: Fair health, monitor closely
- 60-69: Poor health, investigate performance issues
- Below 60: Critical health, requires immediate attention

AI Consolidation Risk Levels
- 🟢 LOW RISK: Minimal performance impact, safe to proceed
- 🟡 MEDIUM RISK: Some impact expected, enhanced monitoring recommended
- 🔴 HIGH RISK: Significant impact likely, reduce scope or implement in phases

Real-Time Alert Types
- Warning Alerts: CPU Ready exceeds warning threshold (default 5%)
- Critical Alerts: CPU Ready exceeds critical threshold (default 15%)
- Performance Trends: Sustained high CPU Ready over multiple intervals

🔧 Advanced Usage

Real-Time Monitoring Best Practices
- Monitor for at least 30 minutes for meaningful analysis
- Consider business hours vs. off-hours patterns
- Set appropriate thresholds based on environment
- Export data regularly for historical analysis
- Use real-time data for immediate troubleshooting

Time Period Selection Guidelines
- Real-Time: Live monitoring, 20-second intervals - for immediate issues and live analysis
- Last Day: Last 24 hours, 5-minute intervals - for daily patterns and troubleshooting
- Last Week: Last 7 days, 30-minute intervals - for weekly trends and planning
- Last Month: Last 30 days, 2-hour intervals - for capacity planning
- Last Year: Last 365 days, daily intervals - for long-term trends and annual planning

AI Consolidation Strategies
1. Conservative Strategy: Maximum safety, minimal risk
   - Only recommends hosts with <3% average CPU Ready
   - Zero critical performance periods
   - Highest confidence in success

2. Balanced Strategy: Optimal risk/reward ratio
   - Recommends hosts with <5% average CPU Ready
   - Minimal critical periods allowed
   - Good balance of savings and safety

3. Aggressive Strategy: Maximum consolidation
   - Accepts higher performance thresholds
   - Focuses on infrastructure reduction
   - Requires careful monitoring post-implementation

Advanced Visualization Usage
- Heat Map Calendar: Identify seasonal patterns and maintenance windows
- Performance Trends: Use moving averages to smooth out noise
- Host Comparison: Rank hosts by consolidation suitability
- Distribution Analysis: Understand performance variability

Multi-Host Removal Strategies
1. AI-Guided Approach: Use AI recommendations for optimal selection
2. Performance-Based: Remove lowest CPU Ready hosts first
3. Efficiency-Based: Target hosts providing best consolidation ratios
4. Risk-Managed: Balance infrastructure reduction with performance risk
5. Phased Implementation: Remove hosts in multiple waves with monitoring

❓ Troubleshooting

Real-Time Monitoring Issues
Problem: Real-time monitoring not starting
Solutions:
- Verify vCenter connection is active
- Check CPU Readiness metric availability in vCenter
- Ensure hosts are connected and not in maintenance mode
- Try restarting monitoring after reconnecting to vCenter

Problem: Real-time data shows incorrect values
Solutions:
- Verify vCenter performance statistics levels
- Check that CPU Ready metrics are enabled
- Compare with vCenter UI to validate values
- Use "Verify Data" button to check conversion accuracy

Connection Issues
Problem: Cannot connect to vCenter
Solutions:
- Verify vCenter server address and credentials
- Check network connectivity and firewall settings
- Ensure vCenter is running and accessible on port 443
- Try with administrator account
- Verify SSL certificate acceptance

Data Retrieval Issues
Problem: "No CPU Ready data found"
Solutions:
- Check vCenter statistics levels (Administration > System Configuration > Statistics)
- Verify CPU metrics are being collected at appropriate intervals
- Try different time periods (start with "Last Day")
- Ensure ESXi hosts are connected and collecting performance data
- Check vCenter disk space for statistics storage

Performance Issues
Problem: Application runs slowly with real-time monitoring
Solutions:
- Reduce number of monitored hosts if possible
- Increase monitoring interval in real-time dashboard settings
- Clear old real-time data using "Clear History" button
- Close other applications to free memory
- Consider using historical analysis for large environments

Import Issues
Problem: CSV/Excel import fails or shows wrong values
Solutions:
- Verify file contains "Time" column and "Ready for [hostname]" columns
- Check timestamp format compatibility
- Ensure CPU Ready values are in correct units (milliseconds for historical)
- Try saving Excel files as CSV format
- Use manual interval selection if auto-detection fails

AI Recommendation Issues
Problem: AI generates no recommendations
Solutions:
- Ensure sufficient performance data is available
- Try different consolidation strategies
- Lower target reduction percentage
- Verify all hosts have adequate monitoring data
- Check that CPU Ready values are within expected ranges

🔒 Security Considerations

vCenter Credentials
- Credentials are not stored permanently or logged
- Use dedicated read-only vCenter account when possible
- SSL verification disabled for self-signed certificates (common in vCenter deployments)
- Connection is automatically closed when application exits
- Real-time monitoring stops when vCenter connection is lost

Data Privacy
- No performance data is transmitted outside your network
- All analysis is performed locally on your machine
- Real-time data stored locally in encrypted SQLite database
- Data can be cleared manually using application controls
- No telemetry or usage data collection

Real-Time Database Security
- SQLite database stored locally with restricted access
- Database contains only performance metrics and timestamps
- No sensitive vCenter credentials stored in database
- Database can be relocated or encrypted using OS-level tools

🆘 Support & Contact

Common Error Messages
- "Invalid interval": Try different time period or use file import method
- "No hosts found": Check vCenter connection status and host availability
- "Calculation error": Verify data format and try re-importing with manual settings
- "Connection timeout": Check network connectivity and vCenter availability
- "Real-time collection failed": Verify vCenter permissions and metric availability
- "Export failed": Check available disk space and file permissions

Getting Help
For issues or questions:
1. Check this README for comprehensive troubleshooting steps
2. Verify system requirements are met
3. Test with simple scenario first (single host, short time period)
4. Check vCenter logs for performance data collection issues
5. Use application's built-in verification tools
6. Contact: joshua.fourie@outlook.com for technical support

📄 License & Disclaimer

This tool is provided for infrastructure analysis and planning purposes. Users are responsible for:
- Validating all results before making infrastructure changes
- Testing consolidation plans in non-production environments first
- Following organizational change management procedures
- Backing up critical systems before implementing consolidation
- Monitoring performance after consolidation implementation

The tool provides analysis based on historical and real-time performance data. Actual results may vary based on:
- Workload changes and seasonal patterns
- Application behavior not captured in CPU Ready metrics
- Infrastructure changes and updates
- Business requirements and service level agreements

Real-time monitoring provides current performance snapshots but cannot predict future workload changes or guarantee consolidation success.

🔄 Version History

v1.0 - Initial Release
- Basic CPU Ready analysis
- Single host removal analysis
- CSV/Excel import functionality

v2.0 - Real-Time Edition (Current)
- Real-time monitoring with live dashboard
- AI-powered consolidation recommendations
- Enhanced vCenter integration with CPU Readiness metric
- Comprehensive PDF reporting with embedded charts
- Heat map calendar and advanced visualizations
- Multi-strategy consolidation planning
- Health scoring and risk assessment
- Automated workflow with smart notifications
- SQLite database for real-time data storage
- Timezone-aware data processing
- Professional dark theme interface
- Comprehensive error handling and debugging

Built for VMware administrators who need intelligent, real-time infrastructure optimization with AI-powered insights for safe and efficient consolidation planning.