import os
import django
import datetime
import time
from shutil import move
from django.conf import settings

# Set the DJANGO_SETTINGS_MODULE environment variable
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'edi.settings')  # Replace 'edi' with your project name

# Initialize Django
django.setup()

from myapp.models import files, Archive  # Replace 'myapp' with your Django app name

# Calculate the threshold date (15 days ago)
threshold_date = datetime.datetime.now() - datetime.timedelta(days=15)

def archive_old_files():
    # Query for files older than 15 days
    old_files = files.objects.filter(created_at__lt=threshold_date)

    for file in old_files:
        # Move file record to Archive table
        archived_file = Archive.objects.create(
            file_name=file.file_name,
            file_type=file.file_type,
            file_date=file.file_date,
            created_at=file.created_at,
            created_by=file.created_by,
            file_path=file.file_path,
            upload_status=file.upload_status,
            email_sent_status=file.email_sent_status,
            email_sent_to=file.email_sent_to,
            input_file_path=file.input_file_path
        )

        # Move file physically to an archive directory
        archive_folder = os.path.join(settings.MEDIA_ROOT, 'archive')
        os.makedirs(archive_folder, exist_ok=True)
        archived_path = os.path.join(archive_folder, os.path.basename(file.file_path.path))

        # Move file to archive folder
        move(file.file_path.path, archived_path)

        # Update file path in the Archive table and delete the old record
        archived_file.file_path = f'archive/{os.path.basename(file.file_path.name)}'
        archived_file.save()
        file.delete()

    print(f"Archived {len(old_files)} files successfully.")

if __name__ == "__main__":
    while True:
        # Check if the time is midnight (00:00)
        current_time = datetime.datetime.now().time()
        if current_time.hour == 20 and current_time.minute == 2:
            archive_old_files()
            print("Archiving process completed.")
            time.sleep(60)  # Wait for 1 minute to avoid multiple triggers within the same minute
        else:
            # Wait for a short period before checking the time again
            time.sleep(30)
