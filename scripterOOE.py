import time
import subprocess
from datetime import datetime, timedelta

# List of Python scripts to run
scripts = [
   r'S:\OOE\EDI_PROJECT-\EDI-Backend\FTP2RackOOE.py'
]

# Define the time to trigger the script (24-hour format)
scheduled_hour = 8
scheduled_minute = 00

def is_scheduled_time(now):
    """Check if the current time matches the scheduled time."""
    return now.hour == scheduled_hour and now.minute == scheduled_minute

def run_scripts():
    """Run the listed scripts."""
    for script_path in scripts:
        try:
            print(f"Running script: {script_path}")
            subprocess.check_call(['python', script_path])
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running the script {script_path}: {e}")
        except Exception as e:
            print(f"Unexpected error with script {script_path}: {e}")

print("Waiting for the scheduled time...")
while True:
    now = datetime.now()
    next_trigger = datetime(now.year, now.month, now.day, scheduled_hour, scheduled_minute)

    # If the current time has already passed the trigger time today, schedule for tomorrow
    if now > next_trigger:
        next_trigger += timedelta(days=1)

    # Calculate the time to sleep until the next trigger
    sleep_duration = (next_trigger - now).total_seconds()
    print(f"Sleeping for {sleep_duration} seconds until the next scheduled time: {next_trigger}")
    time.sleep(sleep_duration)

    # Recheck the time to ensure accuracy
    if is_scheduled_time(datetime.now()):
        run_scripts()

# import time
# import subprocess

# # List of Python scripts to run
# scripts = [
#     r'C:\Users\abc-admin\Desktop\OOE\EDI_PROJECT-\EDI-Backend\FTP2RackOOE.py'  
# ]
# run_interval = 60

# while True:
#     for script_path in scripts:
#         try:
#             # Execute each script using subprocess
#             print(f"Running script: {script_path}")
#             subprocess.check_call(['python', script_path])
#         except subprocess.CalledProcessError as e:
#             # Log or handle errors in script execution
#             print(f"Error occurred while running the script {script_path}: {e}")
#         except Exception as e:
#             # Handle unexpected errors
#             print(f"Unexpected error with script {script_path}: {e}")

#     # Wait for the specified interval before the next iteration of the loop
#     print(f"Waiting for {run_interval} seconds before next run...")
#     time.sleep(run_interval)
