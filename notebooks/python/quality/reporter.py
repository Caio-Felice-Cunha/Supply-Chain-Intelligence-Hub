"""
Quality Reporter
================
Generate comprehensive data quality reports.
"""

import pandas as pd
from typing import List, Dict
from datetime import datetime
import json


class DataQualityReporter:
    """Generate comprehensive data quality reports"""
    
    @staticmethod
    def generate_summary_report(
        validation_results: List,
        profiles: Dict[str, Dict],
        anomalies: Dict[str, Dict]
    ) -> Dict:
        """
        Generate comprehensive summary report.
        
        Args:
            validation_results: List of ValidationResult objects
            profiles: Dictionary of table profiles
            anomalies: Dictionary of anomaly detection results
            
        Returns:
            Summary report dictionary
        """
        # Validation summary
        total_rules = len(validation_results)
        passed_rules = sum(1 for r in validation_results if r.passed)
        failed_rules = total_rules - passed_rules
        
        # Profile summary
        total_tables = len(profiles)
        total_rows = sum(p.get('row_count', 0) for p in profiles.values())
        total_columns = sum(p.get('column_count', 0) for p in profiles.values())
        
        # Anomaly summary
        total_anomalies = sum(
            sum(col_data.get('count', 0) for col_data in anom.get('outliers_detected', {}).values())
            for anom in anomalies.values()
        )
        
        return {
            'report_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'validation_summary': {
                'total_rules': total_rules,
                'passed': passed_rules,
                'failed': failed_rules,
                'pass_rate': f"{(passed_rules / total_rules * 100):.2f}%" if total_rules > 0 else "N/A"
            },
            'data_summary': {
                'total_tables': total_tables,
                'total_rows': total_rows,
                'total_columns': total_columns
            },
            'anomaly_summary': {
                'total_outliers_detected': total_anomalies
            }
        }
    
    @staticmethod
    def validation_results_to_dataframe(validation_results: List) -> pd.DataFrame:
        """Convert validation results to DataFrame"""
        data = []
        for result in validation_results:
            data.append({
                'rule_name': result.rule_name,
                'status': '✓ PASS' if result.passed else '✗ FAIL',
                'severity': result.severity,
                'message': result.message,
                'affected_rows': result.affected_rows,
                'affected_percentage': f"{result.affected_percentage:.2f}%",
                'timestamp': result.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            })
        return pd.DataFrame(data)
    
    @staticmethod
    def export_to_json(report_data: Dict, output_path: str = 'quality_report.json'):
        """Export report to JSON file"""
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        print(f"✓ Report exported to: {output_path}")
    
    @staticmethod
    def generate_html_report(
        validation_results: List,
        profiles: Dict[str, Dict],
        anomalies: Dict[str, Dict],
        output_path: str = 'data_quality_report.html'
    ):
        """Generate HTML data quality report"""
        
        passed = sum(1 for r in validation_results if r.passed)
        failed = sum(1 for r in validation_results if not r.passed)
        
        validation_rows = ""
        for result in validation_results:
            status_class = 'pass' if result.passed else 'fail'
            validation_rows += f"""
            <tr class="{status_class}">
                <td>{result.rule_name}</td>
                <td>{result.severity}</td>
                <td>{result.message}</td>
                <td>{result.affected_rows}</td>
                <td>{result.affected_percentage:.2f}%</td>
            </tr>
            """
        
        # Profile summary rows
        profile_rows = ""
        for table_name, profile in profiles.items():
            profile_rows += f"""
            <tr>
                <td>{table_name}</td>
                <td>{profile.get('row_count', 0):,}</td>
                <td>{profile.get('column_count', 0)}</td>
                <td>{profile.get('duplicate_rows', 0):,}</td>
                <td>{profile.get('memory_usage_mb', 0):.2f} MB</td>
            </tr>
            """
        
        # Anomaly summary rows
        anomaly_rows = ""
        for table_name, anom_data in anomalies.items():
            for col, col_data in anom_data.get('outliers_detected', {}).items():
                anomaly_rows += f"""
                <tr>
                    <td>{table_name}</td>
                    <td>{col}</td>
                    <td>{col_data.get('method', 'N/A')}</td>
                    <td>{col_data.get('count', 0)}</td>
                    <td>{col_data.get('percentage', 0):.2f}%</td>
                </tr>
                """
        
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Data Quality Report</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                    margin: 0;
                    padding: 20px;
                    background: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    padding: 30px;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #333;
                    border-bottom: 3px solid #4CAF50;
                    padding-bottom: 10px;
                }}
                h2 {{
                    color: #555;
                    margin-top: 30px;
                }}
                .summary {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin: 20px 0;
                }}
                .summary-card {{
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 6px;
                    border-left: 4px solid #4CAF50;
                }}
                .summary-card h3 {{
                    margin: 0 0 10px 0;
                    color: #666;
                    font-size: 14px;
                }}
                .summary-card .value {{
                    font-size: 28px;
                    font-weight: bold;
                    color: #333;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }}
                th {{
                    background: #4CAF50;
                    color: white;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                }}
                td {{
                    padding: 10px;
                    border-bottom: 1px solid #ddd;
                }}
                tr:hover {{
                    background: #f5f5f5;
                }}
                .pass {{
                    background: #e8f5e9;
                }}
                .fail {{
                    background: #ffebee;
                }}
                .timestamp {{
                    color: #999;
                    font-size: 14px;
                    text-align: center;
                    margin-top: 30px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Data Quality Report</h1>
                
                <div class="summary">
                    <div class="summary-card">
                        <h3>Total Rules</h3>
                        <div class="value">{len(validation_results)}</div>
                    </div>
                    <div class="summary-card">
                        <h3>Passed</h3>
                        <div class="value" style="color: #4CAF50;">{passed}</div>
                    </div>
                    <div class="summary-card">
                        <h3>Failed</h3>
                        <div class="value" style="color: #f44336;">{failed}</div>
                    </div>
                    <div class="summary-card">
                        <h3>Pass Rate</h3>
                        <div class="value">{(passed / len(validation_results) * 100):.1f}%</div>
                    </div>
                </div>
                
                <h2>Validation Results</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Rule Name</th>
                            <th>Severity</th>
                            <th>Message</th>
                            <th>Affected Rows</th>
                            <th>Percentage</th>
                        </tr>
                    </thead>
                    <tbody>
                        {validation_rows}
                    </tbody>
                </table>
                
                <h2>Data Profiles</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Table Name</th>
                            <th>Row Count</th>
                            <th>Column Count</th>
                            <th>Duplicate Rows</th>
                            <th>Memory Usage</th>
                        </tr>
                    </thead>
                    <tbody>
                        {profile_rows}
                    </tbody>
                </table>
                
                <h2>Anomaly Detection</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Table</th>
                            <th>Column</th>
                            <th>Method</th>
                            <th>Outlier Count</th>
                            <th>Percentage</th>
                        </tr>
                    </thead>
                    <tbody>
                        {anomaly_rows}
                    </tbody>
                </table>
                
                <div class="timestamp">
                    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </div>
            </div>
        </body>
        </html>
        """
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        print(f"✓ HTML report generated: {output_path}")
