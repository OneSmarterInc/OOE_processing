import pysftp
import os
import smtplib
from datetime import datetime, time
from email.mime.text import MIMEText
import time as sleep_time
import re
from logging_setup import logger

# SFTP server details
sftp_host = "sftp.abcwv.com"
sftp_username = "ooe_abc"
sftp_password = "dKX7i!7K2(K"
sftp_inbound_folder = "/Inbound"

# Local directory on Windows server
local_folder = r"S:\OOE\Input"

# SFTP connection options (if required)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # Use this only if host keys are not configured

# Strings to include in file names (case insensitive)
include_strings = ["OOE", "ooe"]

# Email configuration
email_sender = "support@disruptionsim.com"
email_recipient = "akshay.kumar@onesmarter.com"
email_recp2 = "Vikram@vikramsethi.com"
email_recp3 = 'dprasad@abchldg.com'
email_subject = "File Not Received Alert For OOE"
email_subject_recieved = "File Received From FTP"
email_smtp_server = "mail.privateemail.com"
email_smtp_port = 465
email_username = "support@disruptionsim.com"
email_password = "Onesmarter@2023"

# Time window configuration
alert_start_time = "08:00"  # Start time for the alert window
alert_end_time = "12:30"    # End time for the alert window


def is_within_time_window():
    """Check if the current time is within the configured time window."""
    now = datetime.now().time()
    start_hour, start_minute = map(int, alert_start_time.split(":"))
    end_hour, end_minute = map(int, alert_end_time.split(":"))
    start_time = time(start_hour, start_minute)
    end_time = time(end_hour, end_minute)
    return start_time <= now <= end_time


def send_email(email_recp):
    """Send an email notification."""
    msg = MIMEText(f"No files matching the inclusion criteria (OOE) were found in the SFTP inbound folder between {alert_start_time} and {alert_end_time}.")
    msg["Subject"] = email_subject
    msg["From"] = email_sender
    msg["To"] = email_recipient

    try:
        with smtplib.SMTP_SSL(email_smtp_server, email_smtp_port) as server:
            server.login(email_username, email_password)
            server.sendmail(email_sender, email_recp, msg.as_string())
        print("Alert email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


def get_email_body_file_received():
    """Return the HTML body for file received notification."""
    body = """
    <html>
    <body>
        <p>File received from FTP and Downloaded.</p>
    </body>
    </html>
    """
    return body

import requests

def send_email_via_fastapi(email, subject, body_html):
    url = "http://104.153.122.230:8127/send-email"
    payload = {
        "email": email,
        "subject": subject,
        "body": body_html
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url, data=payload, headers=headers)

    print(f"Email sent to {email}: {response.status_code}")
    print(response.text)

def send_email_file_recieved(email_recp):
    """Send an email notification."""
    msg = MIMEText(f"File recived from FTP")
    msg["Subject"] = email_subject_recieved
    msg["From"] = email_sender
    msg["To"] = email_recipient

    try:
        with smtplib.SMTP_SSL(email_smtp_server, email_smtp_port) as server:
            server.login(email_username, email_password)
            server.sendmail(email_sender, email_recp, msg.as_string())
        print("Alert email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


def transfer_files():
    """Download matching files from the SFTP server."""
    matching_files_found = False

    with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
        print("Connected to SFTP server.")
        print("Available directories:", sftp.listdir())

        # Change to the inbound directory
        sftp.cwd(sftp_inbound_folder)

        # List all files in the directory
        for filename in sftp.listdir():
            # Skip files that contain "AHH" (case insensitive)
            if "ahh" in filename.lower():
                print(f"Skipping {filename} (contains 'AHH').")
                continue

            # Check if the file name contains any included string (case insensitive)
            if not any(include_str.lower() in filename.lower() for include_str in include_strings):
                print(f"Skipping {filename} (does not match inclusion criteria).")
                continue

            matching_files_found = True
            remote_file = f"{sftp_inbound_folder}/{filename}"
            local_file = os.path.join(local_folder, filename)

            # Download the file
            print(f"Downloading {filename}...")
            try:
                sftp.get(remote_file, local_file)
                print(f"Successfully downloaded {filename} to {local_file}.")

                # Delete the file from the SFTP server after successful download
                print(f"Deleting {filename} from SFTP server...")
                sftp.remove(remote_file)
                print(f"Successfully deleted {filename} from SFTP server.")
            except Exception as e:
                print(f"Failed to download or delete {filename}: {e}")

    return matching_files_found


def run_periodic_check():
    """Run the file transfer check periodically until the alert window ends."""
    while True:
        try:
            # Check if the current time is within the time window
            if True:
                files_found = transfer_files()
                if files_found:
                    print("Matching files found and downloaded. No need to send email.")
                    logger.info(f"File received and Downloaded")
                    e_body = get_email_body_file_received()
                    send_email_via_fastapi(email_recipient,"File Received From FTP",e_body)
                    send_email_via_fastapi(email_recp2,"File Received From FTP",e_body)
                    send_email_via_fastapi(email_recp3,"File Received From FTP",e_body)
                    return
                print("No matching files found. Retrying in 1 minute.")
                sleep_time.sleep(60) 
            else:
                print(f"Time {datetime.now().time()} has passed the alert window.")
                send_email(email_recipient)
                send_email(email_recp2)
                send_email(email_recp3)
                break  
        except Exception as e:
            print(f"An error occurred: {e}. Retrying in 1 minute...")
            sleep_time.sleep(60)  # Wait for 1 minute before retrying


if __name__ == "__main__":
    run_periodic_check()


# import pysftp
# import os
# import smtplib
# from datetime import datetime, time
# from email.mime.text import MIMEText
# import time as sleep_time

# # SFTP server details
# sftp_host = "sftp.abcwv.com"
# sftp_username = "ooe_abc"
# sftp_password = "dKX7i!7K2(K"
# sftp_inbound_folder = "/Inbound"

# # Local directory on Windows server
# local_folder = r"C:\Users\abc-admin\Desktop\OOE\Input"

# # SFTP connection options (if required)
# cnopts = pysftp.CnOpts()
# cnopts.hostkeys = None  # Use this only if host keys are not configured

# # Strings to include in file names (case insensitive)
# include_strings = ["OOE", "ooe"]

# # Email configuration
# email_sender = "support@disruptionsim.com"
# email_recipient = "akshay.kumar@onesmarter.com"
# email_subject = "File Not Received Alert For OOE"
# email_smtp_server = "mail.privateemail.com"
# email_smtp_port = 465
# email_username = "support@disruptionsim.com"
# email_password = "Onesmarter@2023"

# # Time window configuration
# alert_start_time = "08:00"  # Start time for the alert window
# alert_end_time = "09:30"    # End time for the alert window

# def is_within_time_window():
#     """Check if the current time is within the configured time window."""
#     now = datetime.now().time()
#     start_hour, start_minute = map(int, alert_start_time.split(":"))
#     end_hour, end_minute = map(int, alert_end_time.split(":"))
#     start_time = time(start_hour, start_minute)
#     end_time = time(end_hour, end_minute)
#     return start_time <= now <= end_time

# def send_email():
#     """Send an email notification."""
#     msg = MIMEText(f"No files matching the inclusion criteria (OOE) were found in the SFTP inbound folder between {alert_start_time} and {alert_end_time}.")
#     msg["Subject"] = email_subject
#     msg["From"] = email_sender
#     msg["To"] = email_recipient

#     try:
#         with smtplib.SMTP_SSL(email_smtp_server, email_smtp_port) as server:
#             server.login(email_username, email_password)
#             server.sendmail(email_sender, email_recipient, msg.as_string())
#         print("Alert email sent successfully.")
#     except Exception as e:
#         print(f"Failed to send email: {e}")

# def transfer_files():
#     matching_files_found = False

#     with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
#         print("Connected to SFTP server.")
#         print("Available directories:", sftp.listdir())

#         # Change to the inbound directory
#         sftp.cwd(sftp_inbound_folder)

#         # List all files in the directory
#         for filename in sftp.listdir():
#             # Check if the file name contains any included string (case insensitive)
#             if not any(include_str.lower() in filename.lower() for include_str in include_strings):
#                 print(f"Skipping {filename} (does not match inclusion criteria).")
#                 continue

#             matching_files_found = True
#             remote_file = f"{sftp_inbound_folder}/{filename}"
#             local_file = os.path.join(local_folder, filename)

#             # Download the file
#             print(f"Downloading {filename}...")
#             try:
#                 sftp.get(remote_file, local_file)
#                 print(f"Successfully downloaded {filename} to {local_file}.")

#                 # Delete the file from the SFTP server after successful download
#                 print(f"Deleting {filename} from SFTP server...")
#                 sftp.remove(remote_file)
#                 print(f"Successfully deleted {filename} from SFTP server.")
#             except Exception as e:
#                 print(f"Failed to download or delete {filename}: {e}")

#     return matching_files_found

# def run_periodic_check():
#     """Run the file transfer check periodically until the alert window ends."""
#     while True:
#         if is_within_time_window():
#             files_found = transfer_files()
#             if files_found:
#                 print("Matching files found and downloaded. No need to send email.")
#                 return
#             sleep_time.sleep(60)  # Check again after 1 minute
#         else:
#             print(f"Time {datetime.now().time()} has passed the alert window.")
#             send_email()
#             break

# if __name__ == "__main__":
#     run_periodic_check()
