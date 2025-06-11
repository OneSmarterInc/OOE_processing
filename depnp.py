
import os
import time
from datetime import datetime
import shutil
import django
from django.core.files.storage import FileSystemStorage
from django.core.files import File  # Import File class
from logging_setup import logger
from empyp import insert_to_sql_server
from elghp import parse_edi_to_csv_for_sql_server

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'edi.settings')  # Replace 'edi.settings' with your Django settings module
django.setup()

from myapp.models import files  # Import the files model


input_folder = r"S:\OOE\Temp_Input"
archive_folder = r"S:\OOE\Temp_Archive"


def monitor_input_folder():
    
    file_processed_today = False  # Track if a file has been processed today

    while True:
        current_time = datetime.now()
        files_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]

        if files_list:
            for file_name in files_list:
                file_path = os.path.join(input_folder, file_name)
                file_record = None  # Initialize file_record to None
                
                if True:
                    out_data,filedate = parse_edi_to_csv_for_sql_server(file_path)
                    insert_to_sql_server(out_data,filedate)
                    shutil.move(file_path, os.path.join(archive_folder, file_name))

                    # Send success email with the output file attached
                    
                
                    logger.info("Processing Completed")
                    # Update database record after successful email
                    

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
                
                print("Alert email sent: No file found by 11:00 AM.")
                time.sleep(60)  # Avoid sending multiple emails within the same minute

        time.sleep(10)

if __name__ == "__main__":
    monitor_input_folder()
