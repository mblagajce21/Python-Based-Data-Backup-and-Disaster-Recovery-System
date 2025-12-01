import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import json


def format_size(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def format_duration(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def generate_html_email(backup_report):
    
    overall_status = backup_report.get("overall_status", "unknown")
    start_time = backup_report.get("start_time", "")
    end_time = backup_report.get("end_time", "")
    total_duration = backup_report.get("total_duration", 0)
    sources_results = backup_report.get("sources", [])
    
    if overall_status == "success":
        status_color = "#28a745"
        status_text = "SUCCESS"
    elif overall_status == "partial":
        status_color = "#ffc107"
        status_text = "PARTIAL SUCCESS"
    else:
        status_color = "#dc3545"
        status_text = "FAILED"
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, {status_color} 0%, {status_color}dd 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 28px;
            font-weight: 600;
        }}
        .summary {{
            padding: 30px;
            background-color: #f8f9fa;
            border-bottom: 1px solid #dee2e6;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .summary-item {{
            background: white;
            padding: 15px;
            border-radius: 6px;
            border-left: 4px solid {status_color};
        }}
        .summary-item label {{
            display: block;
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 5px;
        }}
        .summary-item value {{
            display: block;
            font-size: 20px;
            font-weight: 600;
            color: #212529;
        }}
        .sources {{
            padding: 30px;
        }}
        .sources h2 {{
            margin-top: 0;
            color: #212529;
            font-size: 20px;
            border-bottom: 2px solid #dee2e6;
            padding-bottom: 10px;
        }}
        .source-card {{
            background: #f8f9fa;
            border-radius: 6px;
            padding: 20px;
            margin-bottom: 20px;
            border-left: 4px solid #ddd;
        }}
        .source-card.success {{
            border-left-color: #28a745;
        }}
        .source-card.failed {{
            border-left-color: #dc3545;
        }}
        .source-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        .source-name {{
            font-size: 18px;
            font-weight: 600;
            color: #212529;
        }}
        .source-status {{
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .source-status.success {{
            background-color: #d4edda;
            color: #155724;
        }}
        .source-status.failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .source-metrics {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .metric {{
            background: white;
            padding: 12px;
            border-radius: 4px;
        }}
        .metric-label {{
            font-size: 11px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .metric-value {{
            font-size: 16px;
            font-weight: 600;
            color: #212529;
            margin-top: 5px;
        }}
        .error-message {{
            background-color: #f8d7da;
            color: #721c24;
            padding: 12px;
            border-radius: 4px;
            margin-top: 10px;
            font-size: 14px;
            border: 1px solid #f5c6cb;
        }}
        .footer {{
            padding: 20px 30px;
            background-color: #f8f9fa;
            border-top: 1px solid #dee2e6;
            text-align: center;
            color: #6c757d;
            font-size: 12px;
        }}
        .footer a {{
            color: {status_color};
            text-decoration: none;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Backup Report - {status_text}</h1>
        </div>
        
        <div class="summary">
            <h2 style="margin-top: 0; color: #212529;">Summary</h2>
            <div class="summary-grid">
                <div class="summary-item">
                    <label>Start Time</label>
                    <value>{start_time}</value>
                </div>
                <div class="summary-item">
                    <label>End Time</label>
                    <value>{end_time}</value>
                </div>
                <div class="summary-item">
                    <label>Total Duration</label>
                    <value>{format_duration(total_duration)}</value>
                </div>
                <div class="summary-item">
                    <label>Sources Processed</label>
                    <value>{len(sources_results)}</value>
                </div>
            </div>
        </div>
        
        <div class="sources">
            <h2>Backup Sources</h2>
"""
    
    for source in sources_results:
        source_name = source.get("name", "Unknown")
        source_type = source.get("type", "unknown")
        source_status = source.get("status", "unknown")
        duration = source.get("duration", 0)
        files_count = source.get("files_count", 0)
        total_size = source.get("total_size", 0)
        error_msg = source.get("error", "")
        
        status_class = "success" if source_status == "success" else "failed"
        status_badge = "success" if source_status == "success" else "failed"
        status_text_display = "Success" if source_status == "success" else "Failed"
        
        html += f"""
            <div class="source-card {status_class}">
                <div class="source-header">
                    <div class="source-name">{source_name}</div>
                    <div class="source-status {status_badge}">{status_text_display}</div>
                </div>
                <div style="color: #6c757d; font-size: 14px; margin-bottom: 10px;">
                    Type: {source_type.upper()}
                </div>
                <div class="source-metrics">
                    <div class="metric">
                        <div class="metric-label">Duration</div>
                        <div class="metric-value">{format_duration(duration)}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Files</div>
                        <div class="metric-value">{files_count}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Total Size</div>
                        <div class="metric-value">{format_size(total_size)}</div>
                    </div>
                </div>
"""
        
        if error_msg:
            html += f"""
                <div class="error-message">
                    <strong>Error:</strong> {error_msg}
                </div>
"""
        
        html += """
            </div>
"""
    
    html += f"""
        </div>
        
        <div class="footer">
            <p>This is an automated backup notification from your Python-Based Data Backup and Disaster Recovery System.</p>
            <p>Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html


def send_email(config, backup_report):
    
    email_config = config.get("email", {})
    
    if not email_config.get("enabled", False):
        print("Email notifications are disabled in config.")
        return False
    
    overall_status = backup_report.get("overall_status", "unknown")
    send_on_success = email_config.get("send_on_success", True)
    send_on_failure = email_config.get("send_on_failure", True)
    
    if overall_status == "success" and not send_on_success:
        print("Skipping email notification for successful backup (disabled in config).")
        return False
    
    if overall_status == "failed" and not send_on_failure:
        print("Skipping email notification for failed backup (disabled in config).")
        return False
    
    smtp_server = email_config.get("smtp_server")
    smtp_port = email_config.get("smtp_port", 587)
    use_tls = email_config.get("use_tls", True)
    sender_email = email_config.get("sender_email")
    sender_password = email_config.get("sender_password")
    recipient_emails = email_config.get("recipient_emails", [])
    
    if not all([smtp_server, sender_email, sender_password, recipient_emails]):
        print("Email configuration incomplete. Please check config.json")
        return False
    
    msg = MIMEMultipart('alternative')
    
    if overall_status == "success":
        subject_status = "SUCCESS"
    elif overall_status == "partial":
        subject_status = "PARTIAL SUCCESS"
    else:
        subject_status = "FAILED"
    
    msg['Subject'] = f"Backup Report - {subject_status} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)
    
    html_content = generate_html_email(backup_report)
    
    html_part = MIMEText(html_content, 'html')
    msg.attach(html_part)
    
    try:
        print(f"\nSending email notification to {len(recipient_emails)} recipient(s)...")
        
        if use_tls:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        
        print(f"Email notification sent successfully to: {', '.join(recipient_emails)}")
        return True
        
    except Exception as e:
        print(f"Failed to send email notification: {e}")
        return False


def test_email_config(config_path="config.json"):
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    test_report = {
        "overall_status": "success",
        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "end_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_duration": 45.67,
        "sources": [
            {
                "name": "Test Source",
                "type": "device",
                "status": "success",
                "duration": 45.67,
                "files_count": 10,
                "total_size": 1024 * 1024 * 15.5,
                "error": ""
            }
        ]
    }
    
    return send_email(config, test_report)


if __name__ == "__main__":
    print("Testing email configuration...")
    test_email_config()
