import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import schedule
import time
import json
from datetime import datetime
from poc import run_backup, load_config
from email_notifier import send_email


def scheduled_backup_job():
    print(f"\n{'='*60}")
    print(f"Scheduled backup started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    backup_report = None
    try:
        backup_report = run_backup()
        print(f"\n{'='*60}")
        print(f"Scheduled backup completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Status: {backup_report['overall_status'].upper()}")
        print(f"{'='*60}\n")
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"ERROR: Scheduled backup failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Error: {e}")
        print(f"{'='*60}\n")
        
        backup_report = {
            "overall_status": "failed",
            "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_duration": 0,
            "sources": [{
                "name": "Backup System",
                "type": "system",
                "status": "failed",
                "duration": 0,
                "files_count": 0,
                "total_size": 0,
                "error": str(e)
            }]
        }
    
    if backup_report:
        try:
            config = load_config()
            send_email(config, backup_report)
        except Exception as e:
            print(f"Failed to send email notification: {e}")


def setup_schedule():
    config = load_config()
    
    if "schedule" not in config:
        print("No schedule configuration found in config.json")
        return False
    
    sched_config = config["schedule"]
    
    if not sched_config.get("enabled", False):
        print("Scheduling is disabled in config.json")
        return False
    
    day_of_week_str = sched_config.get("day_of_week", "monday").lower()
    hour = sched_config.get("hour", 6)
    minute = sched_config.get("minute", 0)
    
    time_str = f"{hour:02d}:{minute:02d}"
    
    day_mapping = {
        "monday": schedule.every().monday,
        "tuesday": schedule.every().tuesday,
        "wednesday": schedule.every().wednesday,
        "thursday": schedule.every().thursday,
        "friday": schedule.every().friday,
        "saturday": schedule.every().saturday,
        "sunday": schedule.every().sunday
    }
    
    days = [d.strip() for d in day_of_week_str.split(",")]
    
    scheduled_days = []
    for day in days:
        if day not in day_mapping:
            print(f"Invalid day: {day}. Skipping...")
            continue
        
        day_mapping[day].at(time_str).do(scheduled_backup_job)
        scheduled_days.append(day.capitalize())
    
    if not scheduled_days:
        print(f"No valid days found in: {day_of_week_str}")
        return False
    
    print(f"\n{'='*60}")
    print(f"Backup Scheduler Started")
    print(f"{'='*60}")
    if len(scheduled_days) == 1:
        print(f"Schedule: Every {scheduled_days[0]} at {time_str}")
    else:
        print(f"Schedule: Every {', '.join(scheduled_days)} at {time_str}")
    print(f"Bucket: {config['bucket']}")
    print(f"Email Notifications: {'Enabled' if config.get('email', {}).get('enabled', False) else 'Disabled'}")
    print(f"{'='*60}\n")
    
    return True


def main():
    if not setup_schedule():
        print("Failed to setup schedule. Exiting.")
        return
    
    print("Scheduler is running. Press Ctrl+C to stop.\n")
    

    next_run = schedule.next_run()
    if next_run:
        print(f"Next backup scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")


if __name__ == "__main__":
    main()
