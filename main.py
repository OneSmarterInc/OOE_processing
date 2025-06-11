# import os
# import time
# from datetime import datetime
# import shutil
# import django
# from django.core.files.storage import FileSystemStorage


# # Set up Django environment
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'edi.settings')  # Replace 'your_project_name' with your Django project name
# django.setup()

# from myapp.models import files  # Import the files model
# from myapp.processinputfile import parse_edi_to_csv, send_success_email, send_error_email

# input_folder = r"C:\Users\avina\OneDrive\Desktop\Input"
# output_folder = r"C:\Users\avina\OneDrive\Desktop\Output"
# system_folder = r"C:\Users\avina\OneDrive\Desktop\edi-backend\edi\csv_files"
# archive_folder = r"C:\Users\avina\OneDrive\Desktop\Archive"

# def monitor_input_folder():
#     email = 'avinashkalmegh93@gmail.com'  # Set the recipient email for notifications
#     file_processed_today = False  # Track if a file has been processed today

#     while True:
#         current_time = datetime.now()
#         files_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

#         if files_list:
#             for file in files_list:
#                 file_path = os.path.join(input_folder, file)
                
                
#                 storage = FileSystemStorage(location='media/input_files/')
#                 filename = storage.save(file.name, file)
#                 path = storage.path(filename)
                    
#                 csv_storage = FileSystemStorage(location='media/csv_files/')
#                 print("aaaa",csv_storage)
#                 # Extract file name, type, and date from the file name
#                 file_name = os.path.basename(file_path)
#                 file_name_csv = os.path.splitext(file_name)[0] + ".csv"
#                 parts = file_name.split("_")
#                 file_type = parts[1] if len(parts) > 1 else "Unknown"
#                 file_date = parts[2].split(".")[0] if len(parts) > 2 else None  # Extract MM-DD-YYYY format

#                 # Save the file record to the database
#                 file_record = files.objects.create(
#                         file_name=file_name,
#                         file_type=file_type,
#                         file_date=file_date,
#                         file_path=f"{file_name_csv}",  # Path relative to MEDIA_ROOT
#                         created_by="System",
#                         upload_status=False,  # Initially set as not uploaded
#                         email_sent_status=False,
#                         input_file_path=path
#                     )

#                 # Process the file and get the path to the generated CSV output
#                 output_file_path = parse_edi_to_csv(file_path, output_folder,system_folder)
                    

#                 # Move the original file to the archive folder
#                 shutil.move(file_path, os.path.join(archive_folder, file_name))

#                 # Send success email with the output file attached
#                 send_success_email(email, file_name, output_file_path)

#                 # Update database record after successful email
#                 file_record.upload_status = True
#                 file_record.email_sent_status = True
#                 file_record.email_sent_to = email
#                 file_record.save()

#                 # Mark that a file has been processed today
#                 file_processed_today = True
#                 if file_processed_today != True:
#                         file_record.upload_status = False
#                         file_record.email_sent_status = False
#                         file_record.save()

#                 # Send error email in case of failure
#                 send_error_email(email, file_name, "File processing failed")
                
#         else:
#             # Reset the file_processed_today flag at midnight
#             if current_time.hour == 0 and current_time.minute == 0:
#                 file_processed_today = False

#             # Send a "no file processed" email at 11:00 am if no files were processed today
#             if current_time.hour == 11 and current_time.minute == 0 and not file_processed_today:
#                 send_error_email(
#                     email=email,
#                     file_name="No File",
#                     error_message="No file was found in the input folder by 11:00 AM. Please check if any file needs to be processed."
#                 )
#                 print("Alert email sent: No file found by 11:00 AM.")
#                 time.sleep(60)  # Avoid sending multiple emails within the same minute

#         time.sleep(10)

# if __name__ == "__main__":
#     monitor_input_folder()



import os
import time
from datetime import datetime
import shutil

import django
from django.core.files.storage import FileSystemStorage
from django.core.files import File  # Import File class
from logging_setup import logger

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'edi.settings')  # Replace 'edi.settings' with your Django settings module
django.setup()

from myapp.models import files  # Import the files model
from myapp.processinputfile import parse_edi_to_csv,send_success_email, send_error_email

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

def build_success_email_body(file_name):
    body = f"""
    <html>
    <body>
        <p>The file <strong>{file_name}</strong> was processed successfully.</p>
        <p>The processed file and original input are available on the server.</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body

input_folder = r"S:\OOE\Input"
output_folder = r"S:\OOE\Output"
system_folder = r"S:\OOE\EDI_PROJECT-\EDI-Backend\media\csv_files"
archive_folder = r"S:\OOE\Archive"
output_outbound_folder = r"S:\OOE\EDI_PROJECT-\EDI-Backend\media\output_outbound"

def monitor_input_folder():
    email = 'akshay.kumar@onesmarter.com'
    email2 = 'Vikram@vikramsethi.com'
    email3 = 'dprasad@abchldg.com'  # Set the recipient email for notifications
    file_processed_today = False  # Track if a file has been processed today

    while True:
        current_time = datetime.now()
        files_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

        if files_list:
            for file_name in files_list:
                file_path = os.path.join(input_folder, file_name)
                file_record = None  # Initialize file_record to None
                
                if True:
                    # Open the file and create a Django File object
                    with open(file_path, 'rb') as f:
                        django_file = File(f)
                        storage = FileSystemStorage(location='S:/OOE/EDI_PROJECT-/EDI-Backend/media/input_files')

                        filename = storage.save(file_name, django_file)
                        path = storage.path(filename)

                    csv_storage = FileSystemStorage(location='media/csv_files/')
                    print("Storage:", csv_storage)
                    
                    # Extract file name, type, and date from the file name
                    file_name_csv = os.path.splitext(file_name)[0] + ".csv"
                    file_name_xlsx = os.path.splitext(file_name)[0] + ".xlsx"
                    parts = file_name.split("_")
                    file_type = parts[1] if len(parts) > 1 else "Unknown"
                    file_date = parts[2].split(".")[0] if len(parts) > 2 else None  # Extract MM-DD-YYYY format

                    # Save the file record to the database
                    print("Test2", file_name_csv)
                    logger.info("Processing Started")
                    output_file_path,segment_paths = parse_edi_to_csv(file_path, output_folder, system_folder)
                    print('dhdhdhdhdhd')
                    print(segment_paths)
                    print(file_name_csv)
                    in_file_path = file_path
                    file_record = files.objects.create(
                        file_name=file_name,
                        file_type=file_type,
                        file_date=file_date,
                        file_path = f"media/csv_files/{file_name_csv}",
                        xlsx_file_path = f"media/csv_files/{file_name_xlsx}",
                        #Path relative to MEDIA_ROOT
                        created_by="System",
                        upload_status=False,  # Initially set as not uploaded
                        email_sent_status=False,
                        input_file_path=path,
                        edi_segment_path = segment_paths
                    )

                    # Process the file and get the path to the generated CSV output

                    # Move the original file to the archive folder
                    print('Moving files to archive')

                    # Send success email with the output file attached
                    email_body = build_success_email_body(file_name)
                    send_email_via_fastapi(email,f"Processing Successful: {file_name}",email_body)
                    send_email_via_fastapi(email2,f"Processing Successful: {file_name}",email_body)
                    send_email_via_fastapi(email3,f"Processing Successful: {file_name}",email_body)
                    # send_success_email(email, file_name, output_file_path,in_file_path)
                    # send_success_email(email2,file_name,output_file_path,in_file_path)
                    # send_success_email(email3,file_name,output_file_path,in_file_path)

                    print('Moving files to archive')
                    shutil.move(file_path, os.path.join(archive_folder, file_name))
                    print("process doneP")
                    logger.info("Processing Completed")
                    # Update database record after successful email
                    file_record.upload_status = True
                    file_record.email_sent_status = True
                    file_record.email_sent_to = email
                    file_record.save()

                    # Mark that a file has been processed today
                    file_processed_today = True

                # except Exception as e:
                #     # Update database record after failure, if record exists
                #     if file_record:
                #         file_record.upload_status = False
                #         file_record.email_sent_status = False
                #         file_record.save()

                #     # Send error email in case of failure
                #     send_error_email(email, file_name, str(e))
                #     print(f"Error processing {file_name}: {e}")

        else:
            # Reset the file_processed_today flag at midnight
            if current_time.hour == 0 and current_time.minute == 0:
                file_processed_today = False

            # Send a "no file processed" email at 11:00 am if no files were processed today
            if current_time.hour == 10 and current_time.minute == 0 and not file_processed_today:
                # send_error_email(
                #     email=email,
                #     file_name="No File",
                #     error_message="No file was found in the input folder by 10:00 AM. Please check if any file needs to be processed."
                # )
                pass
                print("Alert email sent: No file found by 11:00 AM.")
                time.sleep(60)  # Avoid sending multiple emails within the same minute

        time.sleep(10)

if __name__ == "__main__":
    monitor_input_folder()
