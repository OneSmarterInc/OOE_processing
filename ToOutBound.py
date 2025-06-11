import pysftp
import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from logging_setup import logger

# # SFTP server details
# sftp_host = "sftp.abcwv.com"
# sftp_username = "ooe_abc"
# sftp_password = "dKX7i!7K2(K"
# sftp_outbound_folder = "/Outbound"

#SFTP server details (Production)
sftp_host = "edi-sftp.anthem.com"
sftp_username = "mw61149e"
sftp_password = "6KlN6hRu"
sftp_outbound_folder = "/SFTP/inbound"


# Local directory on Windows server
local_folder = r"S:\OOE\EDI_PROJECT-\EDI-Backend\media\output_outbound"

# Email configuration
email_sender = "support@disruptionsim.com"
email_recipient = "akshay.kumar@onesmarter.com"
email_subject = "File Upload Notification"
email_smtp_server = "mail.privateemail.com"
email_smtp_port = 465
email_username = "support@disruptionsim.com"
email_password = "Onesmarter@2023"

# SFTP connection options (if required)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # Use this only if host keys are not configured

def send_email(file_name):
    """Send an email notification for the uploaded file."""
    msg = MIMEText(f"The file '{file_name}' has been successfully uploaded to the SFTP server.")
    msg["Subject"] = email_subject
    msg["From"] = email_sender
    msg["To"] = email_recipient

    try:
        with smtplib.SMTP_SSL(email_smtp_server, email_smtp_port) as server:
            server.login(email_username, email_password)
            server.sendmail(email_sender, email_recipient, msg.as_string())
        print(f"Notification email sent for {file_name}.")
    except Exception as e:
        print(f"Failed to send email for {file_name}: {e}")

def upload_files():
        
        """Uploads files from the local folder to the SFTP outbound folder."""
        with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
            print("Connected to SFTP server.")
            print("Available directories:", sftp.listdir())

            # Change to the outbound directory
            sftp.cwd(sftp_outbound_folder)

            # List all files in the local directory
            for filename in os.listdir(local_folder):
                local_file = os.path.join(local_folder, filename)

                # Ensure it's a file and not a directory
                if not os.path.isfile(local_file):
                    print(f"Skipping {filename} (not a file).")
                    continue

                remote_file = f"{sftp_outbound_folder}/{filename}"

                try:
                    # Upload the file
                    print(f"Uploading {filename} to SFTP server...")
                    sftp.put(local_file, remote_file)
                    print(f"Uploaded {filename} successfully.")
                    logger.info('File sent to Anthem')

                    # Send email notification
                    send_email(filename)

                    # Delete the file from the local directory after successful upload
                    os.remove(local_file)
                    print(f"Deleted {filename} from local directory.")

                except Exception as e:
                    print(f"Failed to upload {filename}: {e}")

if __name__ == "__main__":
    upload_files()
