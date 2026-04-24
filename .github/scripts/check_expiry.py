import pandas as pd
import os
import sys
from datetime import datetime, timedelta

def check_expiry(feed_name):
    alerts = []
    warning_date = datetime.now() + timedelta(days=7)
    
    max_date = None

    # Check standard calendar
    if os.path.exists('calendar.txt'):
        cal = pd.read_csv('calendar.txt', dtype=str)
        if not cal.empty and 'end_date' in cal.columns:
            max_date_str = cal['end_date'].max()
            max_date = datetime.strptime(max_date_str, '%Y%m%d')

    # Check exception dates (sometimes feeds use this exclusively)
    if os.path.exists('calendar_dates.txt'):
        cal_d = pd.read_csv('calendar_dates.txt', dtype=str)
        if not cal_d.empty and 'date' in cal_d.columns:
            max_exc_str = cal_d['date'].max()
            max_exc_date = datetime.strptime(max_exc_str, '%Y%m%d')
            
            # Use whichever date goes further into the future
            if max_date is None or max_exc_date > max_date:
                max_date = max_exc_date

    # Evaluate the date
    if max_date:
        if max_date <= warning_date:
            alerts.append(f"🚨 **GTFS Expiry Warning:** The `{feed_name}` feed ends on **{max_date.strftime('%Y-%m-%d')}**!")
    else:
        alerts.append(f"⚠️ Could not find any valid calendar dates for `{feed_name}`.")

    # Write alert to the root of the repository so the YAML can easily find it
    if alerts:
        workspace = os.environ.get('GITHUB_WORKSPACE', '..')
        alert_file = os.path.join(workspace, "expiry_alert.txt")
        
        with open(alert_file, "w") as f:
            f.write("\n\n".join(alerts))
        print(f"Alert generated for {feed_name}.")
    else:
        print(f"{feed_name} feed is healthy and not expiring soon.")

if __name__ == "__main__":
    # We pass the feed name from the YAML environment variables
    feed_name = sys.argv[1] if len(sys.argv) > 1 else "Unknown Feed"
    check_expiry(feed_name)
