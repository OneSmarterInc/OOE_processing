import os
import time
import subprocess

while True:
    # Use subprocess.run() to execute 'ToOutBound.py'
    subprocess.run(['python', r'S:\OOE\EDI_PROJECT-\EDI-Backend\ToOutBound.py'])
    
    # Wait for 60 seconds
    time.sleep(600)
