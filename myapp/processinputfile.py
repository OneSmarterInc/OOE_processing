import csv
import os
import pyodbc
import smtplib
import pandas as pd,re
from decimal import Decimal
import numpy as np
import random,string
import json
from datetime import datetime, timedelta, date
import shutil
from multiprocessing import Queue
import pickle
from email.mime.text import MIMEText
import openpyxl
from openpyxl import Workbook
from email.mime.multipart import MIMEMultipart 
from email.mime.application import MIMEApplication
import sqlite3
from datetime import datetime
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO

# import requests

# url = "http://104.153.122.230:8127/send-email"

# payload = {
#     "email": "krushnarajjadhav015@gmail.com",
#     "subject": "Test",
#     "body": "Hello from FastAPI!"
# }

# # Set headers manually
# headers = {"Content-Type": "application/x-www-form-urlencoded"}

# response = requests.post(url, data=payload, headers=headers)

# print(response.status_code)
# print(response.text)



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



def build_reins_ssns_body(ssn_list):
    term_table = ""
    if ssn_list:
        term_table = "<br/><br/><strong>Reinitiated SSNs:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th></tr>"
        for entry in ssn_list:
            term_table += f"<tr><td>{entry}</td></tr>"
        term_table += "</table>"

    body = f"""
    <p>Reinstated SSNs with date less than term date:</p>
    {term_table}
    """
    return body

def build_success_email_count_reinstate_body(term_ssn_list):
    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += f"<tr><td>{entry['SSN']}</td><td>{entry['TERM DATE']}</td></tr>"
        term_table += "</table>"

    body = f"""
    <p>Count list for reinstate SSNs:</p>
    {term_table}
    """
    return body


def build_success_email_count_body(count_list, term_ssn_list):
    count_table = """
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Source</th><th>Total Subscribers</th><th>Total Spouse</th><th>Other Dependents</th></tr>
        <tr><td>Excel</td><td>{}</td><td>{}</td><td>{}</td></tr>
        <tr><td>DB2</td><td>{}</td><td>{}</td><td>{}</td></tr>
    </table>
    """.format(
        count_list[0]['excel'][0]['total_subscriber'], 
        count_list[0]['excel'][1]['total_spouse'], 
        count_list[0]['excel'][2]['other dependents'],
        count_list[1]['db2'][0]['total_subscriber'], 
        count_list[1]['db2'][1]['total_spouse'], 
        count_list[1]['db2'][2]['other dependents']
    )

    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += f"<tr><td>{entry['SSN']}</td><td>{entry['TERM DATE']}</td></tr>"
        term_table += "</table>"

    body = f"""
    <p>Count list for both Excel and DB2:</p>
    {count_table}
    {term_table}
    """
    return body

count_list = [
    {
        'excel': [
            {'total_subscriber': 120},
            {'total_spouse': 45},
            {'other dependents': 30}
        ]
    },
    {
        'db2': [
            {'total_subscriber': 118},
            {'total_spouse': 44},
            {'other dependents': 31}
        ]
    }
]


term_ssn_list = [
    {'SSN': '123-45-6789', 'TERM DATE': '2025-06-01'},
    {'SSN': '987-65-4321', 'TERM DATE': '2025-05-20'},
    {'SSN': '555-66-7777', 'TERM DATE': '2025-04-15'}
]

# html_body = build_success_email_count_body(count_list,term_ssn_list)
# send_email_via_fastapi("krushnarajjadhav015@gmail.com","subject",html_body)

def build_success_email_term_body(count_list):
    body = f"""
    <p>These are the SSNs added to DB2 and Excel with a Term date {count_list}.</p>
    """
    return body


def build_missing_ssn_email_body(missing_ssn, missing_in_inventory):
    body = f"""
    <html>
    <body>
        <p>Hello,</p>

        <p>Here is the report for Member IDs:</p>

        <h3>Missing SSN (present in inventory, not in current data):</h3>
        <p>{', '.join(missing_ssn) if missing_ssn else 'None'}</p>

        <h3>Missing inventory SSN (present in current data, not in inventory):</h3>
        <p>{', '.join(missing_in_inventory) if missing_in_inventory else 'None'}</p>

        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_error_log_email_body(file_name, error_message, error_logs):
    # Convert error_logs to HTML table
    table_rows = ""
    for key, value in error_logs.items():
        table_rows += f"<tr><td>{key}</td><td>{value}</td></tr>"

    html_table = f"""
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Member ID</th><th>Group Number</th></tr>
        {table_rows}
    </table>
    """

    body = f"""
    <html>
    <body>
        <p><strong>Group numbers not found for the following member ID(s):</strong> {error_message}</p>
        <br>
        {html_table}
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_member_id_email_body(new_ids, missing_ids):
    body = f"""
    <html>
    <body>
        <p>Hello,</p>

        <p>Here is the report for Member IDs:</p>

        <h3>New Member IDs (present today, not in previous data):</h3>
        <p>{', '.join(new_ids) if new_ids else 'None'}</p>

        <h3>Missing Member IDs (present in previous data, not today):</h3>
        <p>{', '.join(missing_ids) if missing_ids else 'None'}</p>

        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


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


def build_error_email_body(file_name, error_message):
    body = f"""
    <html>
    <body>
        <p>The file <strong>{file_name}</strong> failed to process.</p>
        <p><strong>Reason:</strong> {error_message}</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body



def build_error_email_variance_body(file_name, p_type):
    body = f"""
    <html>
    <body>
        <p>The file <strong>{file_name}</strong> has a variance in <strong>{p_type}</strong> of more than five percent.</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_count_single_body(count_list, term_ssn_list):
    count_table = """
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Source</th><th>Total Subscribers</th><th>Total Spouse</th><th>Other Dependents</th></tr>
        <tr><td>Excel</td><td>{}</td><td>{}</td><td>{}</td></tr>
        <tr><td>DB2</td><td>{}</td><td>{}</td><td>{}</td></tr>
    </table>
    """.format(
        count_list[0]['excel'][0]['total_subscriber'], 
        count_list[0]['excel'][1]['total_spouse'], 
        count_list[0]['excel'][2]['other dependents'],
        count_list[1]['db2'][0]['total_subscriber'], 
        count_list[1]['db2'][1]['total_spouse'], 
        count_list[1]['db2'][2]['other dependents']
    )

    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += "<tr><td>{}</td><td>{}</td></tr>".format(entry['SSN'], entry['TERM DATE'])
        term_table += "</table>"

    body = f"""
    <html>
    <body>
        <p>Count list for both Excel and DB2:</p>
        {count_table}
        {term_table}
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_term_body(count_list):
    body = f"""
    <html>
    <body>
        <p>These are the SSN(s) added to DB2 and Excel with a Term date:</p>
        <p>{count_list}</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_count_everything_fine_body(count_list):
    body = f"""
    <html>
    <body>
        <p>Count matched for both Excel and DB2:</p>
        <p>{count_list}</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


# Email Configuration

# Email Configuration
smtp_config = {
    'host': 'mail.privateemail.com',
    'port': 465,
    'user': 'support@disruptionsim.com',
    'password': 'Onesmarter@2023'
}

csv_headers = [
    "SUB/DEP", "LAST NAME", "FIRST NAME", "SSN","TEMP SSN","SEX", "DOB", "DEP LAST NAME", "DEP FIRST NAME",
    "DEP SSN", "DEP SEX", "DEP DOB","CUSTODIAL PARENT","LOCAL", "PLAN", "CLASS", "EFF DATE", "TERM DATE", "ID",
    "ADDRESS 1", "ADDRESS 2", "CITY", "STATE", "ZIP", "PHONE", "EMAIL", "STATUS", "TYPE","MEMBER ID","DEP ADDRESS","DEP CITY","DEP STATE","DEP ZIP"
]


def send_success_email_count(emails, count_list, term_ssn_list):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = "Count and term data for xlsx and db2"

    count_table = """
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Source</th><th>Total Subscribers</th><th>Total Spouse</th><th>Other Dependents</th></tr>
        <tr><td>Excel</td><td>{}</td><td>{}</td><td>{}</td></tr>
        <tr><td>DB2</td><td>{}</td><td>{}</td><td>{}</td></tr>
    </table>
    """.format(
        count_list[0]['excel'][0]['total_subscriber'], 
        count_list[0]['excel'][1]['total_spouse'], 
        count_list[0]['excel'][2]['other dependents'],
        count_list[1]['db2'][0]['total_subscriber'], 
        count_list[1]['db2'][1]['total_spouse'], 
        count_list[1]['db2'][2]['other dependents']
    )

    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += "<tr><td>{}</td><td>{}</td></tr>".format(entry['SSN'], entry['TERM DATE'])
        term_table += "</table>"

    body = f"""
    <p>Count list for both Excel and DB2:</p>
    {count_table}
    {term_table}
    """

    msg.attach(MIMEText(body, 'html'))

    for email in emails:
        msg['To'] = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
        print(f"Success email sent to {email}.")


def reins_ssns_email_less_than_term(emails,ssn_list):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = "Count and term data for xlsx and db2"


    term_table = ""
    if ssn_list:
        term_table = "<br/><br/><strong>Reinititated SSNs:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>SSN</th></tr>"
        for entry in ssn_list:
            term_table += "<tr><td>{}</td><td>{}</td></tr>".format(entry, '')
        term_table += "</table>"

    body = f"""
    <p>reinstate ssns with date less than term date:</p>
    {term_table}
    """

    msg.attach(MIMEText(body, 'html'))

    for email in emails:
        msg['To'] = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
        print(f"Success email sent to {email}.")



def send_success_email_count_reinstate(emails,term_ssn_list):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = "Count and term data for xlsx and db2"


    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += "<tr><td>{}</td><td>{}</td></tr>".format(entry['SSN'], entry['TERM DATE'])
        term_table += "</table>"

    body = f"""
    <p>Count list for reinstate ssns:</p>
    {term_table}
    """

    msg.attach(MIMEText(body, 'html'))

    for email in emails:
        msg['To'] = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
        print(f"Success email sent to {email}.")

def send_success_email_count_single(email, count_list, term_ssn_list):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = "Count and term data for xlsx and db2"

    count_table = """
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Source</th><th>Total Subscribers</th><th>Total Spouse</th><th>Other Dependents</th></tr>
        <tr><td>Excel</td><td>{}</td><td>{}</td><td>{}</td></tr>
        <tr><td>DB2</td><td>{}</td><td>{}</td><td>{}</td></tr>
    </table>
    """.format(
        count_list[0]['excel'][0]['total_subscriber'], 
        count_list[0]['excel'][1]['total_spouse'], 
        count_list[0]['excel'][2]['other dependents'],
        count_list[1]['db2'][0]['total_subscriber'], 
        count_list[1]['db2'][1]['total_spouse'], 
        count_list[1]['db2'][2]['other dependents']
    )

    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += "<tr><td>{}</td><td>{}</td></tr>".format(entry['SSN'], entry['TERM DATE'])
        term_table += "</table>"

    body = f"""
    <p>Count list for both Excel and DB2:</p>
    {count_table}
    {term_table}
    """

    msg.attach(MIMEText(body, 'html'))

    msg['To'] = email
    server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
    print(f"Success email sent to {email}.")

def send_success_email_term(emails,count_list):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = f"SSN with term date"

    body = f"""
    <p>These are the ssn added to db2 and excel with a Term date {count_list}.</p>
    """
    msg.attach(MIMEText(body, 'html'))
    
    for email in emails:
        msg['To'] = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
        print(f"Success email sent to {email}.")

def send_success_email_count_everything_fine(emails,count_list,term_ssn_list):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = f"Count matched for xlsx and db2"

    body = f"""
    <p>Count list for both excel and db2 {count_list}.</p>
    """
    msg.attach(MIMEText(body, 'html'))
    
    for email in emails:
        msg['To'] = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
        print(f"Success email sent to {email}.")



def send_success_email(email, file_name, output_path, input_file_path):
    try:
        # Connect to SMTP server
        server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
        server.login(smtp_config['user'], smtp_config['password'])

        # Prepare email
        msg = MIMEMultipart()
        msg['From'] = smtp_config['user']
        msg['To'] = email
        msg['Subject'] = f"Processing Successful: {file_name}"

        body = f"""
        <p>The file <strong>{file_name}</strong> was processed successfully.</p>
        <p>Please find the processed file and original input attached.</p>
        """
        msg.attach(MIMEText(body, 'html'))

        # Adjust filename if needed
        if file_name.endswith('.X12'):
            file_name = file_name.replace('.X12', '.csv')

        # Attach the processed output file
        with open(output_path, 'rb') as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(file_name))
            part['Content-Disposition'] = f'attachment; filename="processed_{file_name}"'
            msg.attach(part)

        # Attach the original input file
        with open(input_file_path, 'rb') as f:
            input_filename = os.path.basename(input_file_path)
            part = MIMEApplication(f.read(), Name=input_filename)
            part['Content-Disposition'] = f'attachment; filename="original_{input_filename}"'
            msg.attach(part)

        # Send the message
        server.send_message(msg)
        print(f"✅ Success email sent for {file_name} to {email}")

    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def send_error_email(email, file_name, error_message):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['To'] = email
    msg['Subject'] = f"Processing Failed: {file_name}"

    body = f"""
    <p>The file <strong>{file_name}</strong> failed to process.</p>
    <p><strong>Reason:</strong> {error_message}</p>
    """
    msg.attach(MIMEText(body, 'html'))

    server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
    server.quit()
    print(f"Error email sent for {file_name} to {email}")


def send_error_email_variance(email,file_name,p_type):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['To'] = email
    msg['Subject'] = f"Processing Failed: {file_name}"

    body = f"""
    <p>The file <strong>{file_name}</strong> has a variance in {p_type} of more than five percentage  .</p>
    """
    msg.attach(MIMEText(body, 'html'))

    server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
    server.quit()
    print(f"Error email sent for {file_name} to {email}")

def send_member_id_email(new_ids, missing_ids,email):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['To'] = email
    msg['Subject'] = f"Processing Successful"

    # HTML body for a well-structured email
    body = f"""
    <html>
    <body>
        <p>Hello,</p>

        <p>Here is the report for Member IDs:</p>

        <h3>New Member IDs (present today, not in previous data):</h3>
        <p>{', '.join(new_ids) if new_ids else 'None'}</p>

        <h3>Missing Member IDs (present in previous data, not today):</h3>
        <p>{', '.join(missing_ids) if missing_ids else 'None'}</p>

        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(body, "html"))
    server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
    server.quit()
    print(f"Success email sent to {email}")


def send_error_log_email(email, file_name, error_message, error_logs):
    # Step 1: Convert error_logs to DataFrame
    data = [{"Member ID": key, "Group Number": value} for key, value in error_logs.items()]
    df = pd.DataFrame(data)

    # Step 2: Save DataFrame to an in-memory Excel file
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Error Logs')
    excel_buffer.seek(0)

    # Step 3: Prepare the email content
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['To'] = ", ".join(email)
    msg['Subject'] = f"Group Numbers Not Found: {file_name}"

    # Email body
    body = f"""
    <p><strong>Group numbers not found for the following member ID(s):</strong> {error_message}</p>
    """
    msg.attach(MIMEText(body, 'html'))

    # Step 4: Attach the Excel file
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(excel_buffer.read())
    encoders.encode_base64(part)
    part.add_header(
        'Content-Disposition',
        f'attachment; filename="ErrorLogs_{file_name}.xlsx"'
    )
    msg.attach(part)

    # Step 5: Send the email
    server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
    server.quit()
    print(f"Group number log email sent for {file_name} to {email}")

def send_missing_ssn_email(missing_ssn,missing_in_inventory,email):
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['To'] = email
    msg['Subject'] = f"Processing Successful"

    # HTML body for a well-structured email
    body = f"""
    <html>
    <body>
        <p>Hello,</p>

        <p>Here is the report for Member IDs:</p>

        <h3>Missing ssn (present in inventory, not in current data):</h3>
        <p>{', '.join(missing_ssn) if missing_ssn else 'None'}</p>

        <h3>Missing inventory ssn (present in current data, not in inventory):</h3>
        <p>{', '.join(missing_in_inventory) if missing_in_inventory else 'None'}</p>

        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(body, "html"))
    server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
    server.quit()
    print(f"Success email sent to {email}")


def parse_edi_to_csv(input_file_path, output_directory,system_directory):
    os.makedirs(output_directory, exist_ok=True)
    os.makedirs(system_directory, exist_ok=True)
    output_csv_path = os.path.join(output_directory, os.path.basename(input_file_path))
    system_csv_path = os.path.join(system_directory, os.path.basename(input_file_path))
    file_name = input_file_path.split("/")[-1]  # Extracts 'EDI_834_11-15-2024_3KXST5r.X12'
    #date_part = file_name.split("_")[2]
    #print("Extracted Date:", date_part)
    #date_part = str(date_part[:10])
    date_part='NA'
    with open(input_file_path, 'r') as file:
        edi_data = file.read()
    segments = edi_data.strip().split("~")
    csv_data = []
    current_subscriber = {}
    dependents = []
    error_logs = {}
    segment_list = []
    parsed_data_list = []
    total_parsed_data = []
    cus_data_list = []
    k = 0
    def extract_segment_data(segment, delimiter="*"):
        return segment.split(delimiter)

    for segment in segments:
        each_segments = segment.split("*") 
        segment_name = each_segments[0]  
        parsed_data = {}
        if segment_name in ["ISA", "GS", "ST", "BGN", "DTP", "N1", "INS", "REF", "NM1", 
                            "PER", "N3", "N4", "DMG", "HD", "SE", "GE", "IEA"]:
            parsed_data[segment_name] = "*".join(each_segments[1:])
            parsed_data_list.append(parsed_data)
            if segment_name == "HD":
                total_parsed_data.append({k:parsed_data_list})
                k += 1
                parsed_data_list = []
        else:
            print(f"Skipping unknown segment: {segment_name}")
        
        elements = extract_segment_data(segment)
        segment_id = elements[0]
        if segment_id not in segment_list:
            segment_list.append(segment_id)
        if segment_id == "REF":
            member_id_code = elements[1]
            if(member_id_code == "0F"):
                member_id = elements[2]
        if segment_id == "INS":
            relationship_code = elements[2]
            if relationship_code == '18':
                Sub = "Subscriber"
                Type = '18'
            else:
                dependent_type = elements[2]
                if dependent_type == '01':
                    Sub = "Spouse"
                    Type= dependent_type
                elif dependent_type == '19':
                    Sub = "Child"
                    Type = dependent_type
                else:
                    Sub = "Dependent"
                    Type= dependent_type
            if elements[1] == 'Y':
                status = 'Active'
            elif elements[1] == 'N':
                status = 'Inactive'
            else:
                status = ''

        elif segment_id == "NM1" and elements[1] == "IL":
            if current_subscriber:
                csv_data.append(current_subscriber)
                current_subscriber = {}
            sss = elements[-1] if len(elements) > 8 else ""
            sss = sss.replace("-", "").strip()
            if len(sss) == 9:
                sss = f"{sss[:3]}-{sss[3:5]}-{sss[5:]}"
            elif len(sss) == 8:
                sss = f"{sss[:2]}-{sss[2:4]}-{sss[4:]}"
            else:
                sss = "" 
            person = {
                "LAST NAME": elements[3] if len(elements) > 3 else "",
                "FIRST NAME": elements[4] if len(elements) > 4 else "",
                "SSN": sss,
                "SUB/DEP": Sub,
                "STATUS":status,
                "TYPE":Type,
                "MEMBER ID": member_id
            }
            current_subscriber.update(person)

        elif segment_id == "DMG" and len(elements) > 2:
            dob = elements[2]
            person = dependents[-1] if dependents else current_subscriber
            person["DOB"] = f"{dob[4:6]}/{dob[6:]}/{dob[:4]}" if len(dob) == 8 else ""
            person["SEX"] = elements[3] if len(elements) > 3 else ""
        
        elif "REF*17" in segment:
            data = segment.split("*")
            cus_data = data[-1]
            person["CUSTODIAL PARENT"] = cus_data

        elif segment_id == "N3" and len(elements) > 1:
            address = elements[1]
            person = dependents[-1] if dependents else current_subscriber
            person["ADDRESS 1"] = address

        elif segment_id == "N4" and len(elements) > 3:
            city, state, zip_code = elements[1:4]
            zerocode = zip_code.zfill(5)
            zip_code = str(zip_code).strip()
            if len(zip_code) < 5:
                zip_code = zip_code.zfill(5)
            elif len(zip_code) > 5:
                zip_code = zip_code[:5] 
            person = dependents[-1] if dependents else current_subscriber
            person.update({"CITY": city, "STATE": state, "ZIP": str(zip_code)})
        elif segment_id == "PER" and len(elements) > 3:
            phone = elements[-1]
            person = dependents[-1] if dependents else current_subscriber
            person["PHONE"] = phone

        elif segment_id == "HD" and len(elements) > 2:
            current_subscriber["PLAN"] = elements[1]
            current_subscriber["CLASS"] = elements[3] if len(elements) > 3 else ""

        elif segment_id == "DTP" and len(elements) > 2:
            if elements[1] == "348":
                eff_date = elements[-1]
                current_subscriber["EFF DATE"] = f"{eff_date[4:6]}/{eff_date[6:]}/{eff_date[:4]}" if len(eff_date) == 8 else ""
            elif elements[1] == "349":
                term_date = elements[-1]
                current_subscriber["TERM DATE"] = f"{term_date[:4]}/{term_date[4:6]}/{term_date[6:]}" if len(term_date) == 8 else ""

        elif segment_id == "REF" and len(elements) > 2 and elements[1] == "1L":
            current_subscriber["ID"] = elements[2]
            if elements[2] == "L11958M001":
                current_subscriber["PLAN"] = str("01")
                current_subscriber["CLASS"] = "01"
            
            elif elements[2] == "L11958M002":
                current_subscriber["PLAN"] = str("01")
                current_subscriber["CLASS"] = "02"
                
            elif elements[2] == "L11958MD01":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "SS"
                
            elif elements[2] == "L11958MR01":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "R8"
                
            elif elements[2] == "L11958MR02":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "D9"
                
            elif elements[2] == "L11958MR03":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "R1"    
                
            elif elements[2] == "L11958MR04":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "D2"       
                
            elif elements[2] == "L11958MR05":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "M8"             
                
            elif elements[2] == "L11958MR06":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "M9"    
                
            elif elements[2] == "L11958MR07":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "M1"
                
            elif elements[2] == "L11958MR08":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "M2"   
                
            elif elements[2] == "L11958MR09":
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "D0"     
                
            else:
                current_subscriber["PLAN"] = "01"
                current_subscriber["CLASS"] = "01"

                error_logs[member_id] = elements[2]   
    errorFileName =  os.path.basename(input_file_path)
    # print(errorFileName)
    # if(len(error_logs) >0):
    #     error_message = "Missing group numbers for the given Member IDs"
    #     email = ['krishnarajjadhav2003@gmail.com']
    #     send_error_log_email(email, errorFileName, error_message, error_logs) 

    conn = sqlite3.connect("db.sqlite3")
    cursor = conn.cursor()
    import pandas as pd
    flattened_data = []
    flattened_data = []
    for group in total_parsed_data:
        for group_id, records in group.items():
            for record in records:
                for key, value in record.items():
                    flattened_data.append({'group_id': group_id, 'key': key, 'value': value})

    df = pd.DataFrame(flattened_data)
    df = df.groupby(['group_id', 'key'], as_index=False).agg({'value': 'first'})

    pivot_df = df.pivot(index='group_id', columns='key', values='value').reset_index()
    pivot_df = pivot_df.where(pd.notnull(pivot_df), None)
    pivot_df.drop(columns=['group_id'],inplace=True)
    for column in pivot_df.columns:
        pivot_df[column] = pivot_df[column].str.replace('*', '  ', regex=False)
        pivot_df[column] = pivot_df[column].drop_duplicates().reset_index(drop=True)
    pivot_df = pivot_df.fillna(' ')
    pivot_df['Date_edi'] = date_part
    random_number = random.randint(0, 9999)
    random_alphabet = random.choice(string.ascii_uppercase) 
    result = f"{random_alphabet}{random_number:04}"
    out_dir = "media/csv_files/"
    pivot_df_data = pivot_df.to_dict(orient="records")
    segment_df = ''
    # try:
    #     edi_excel_path = os.path.join(out_dir, f"edi_segment_data_{result}.xlsx")
    #     segment_df.to_excel(edi_excel_path)
    # except:
    #     edi_excel_path = "S:\\OOE\\EDI_PROJECT-\\EDI-Backend\\media\\csv_files"
    #     new_path = os.path.join(edi_excel_path,f"edi_segment_data_{result}.xlsx")
    #     print(new_path)
    #     segment_df.to_excel(new_path)
    #     edi_excel_path = new_path
    edi_excel_path = ' '
    def write_to_queue(data, queue_file):
        try:
            with open(queue_file, "ab") as file:
                pickle.dump(data, file)
            print("Data added to the queue.")
        except Exception as e:
            print(f"Error writing to the queue: {e}")
    write_to_queue(pivot_df_data, "queue_file.pkl")

    # send_data_to_serever(pivot_df_data)
    # send_data_in_json_form(pivot_df_data)
    conn.close()
    csv_data.append(current_subscriber)
    csv_data.extend(dependents)
    input_filename = os.path.splitext(os.path.basename(input_file_path))[0]
    output_csv_path = os.path.join(output_directory, f"{input_filename}.csv")
    output_xlsx_path = os.path.join(out_dir, f"{input_filename}.xlsx")
    system_csv_path = os.path.join(system_directory, f"{input_filename}.csv")
    for row in csv_data:
        if 'ID' in row.keys():
            row['STATUS'] = row['ID']
        else:
            row['STATUS'] = ''
        if 'TYPE' in row.keys():
            row['ID'] = row['TYPE']
        else:
            row['ID'] = ''
        row['TYPE'] = ''
        if 'SSN' in row.keys():
            row['TEMP SSN'] = row['SSN']
        else:
            row['TEMP SSN'] = ''

    for path in [output_csv_path, system_csv_path]:
      
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "Sheet1"
        worksheet.append(csv_headers)
        current_subscriber_ssn = None
        subscriber_address = None
        subscriber_city = None
        subscriber_zip = None
        subscriber_state = None
        previous_custodial_parent = None
        for row in csv_data:
            row["PLAN"] = row["PLAN"].zfill(2)
            row["CLASS"] = row["CLASS"].zfill(2)
            for key in row.keys():
                row[key] = str(row[key]) if row[key] is not None else ""

            if 'SUB/DEP' in row.keys():
                if row['SUB/DEP'] != 'Subscriber':
                    row['DEP FIRST NAME'] = str(row.get('FIRST NAME', "")).ljust(20)
                    row['DEP LAST NAME'] = str(row.get('LAST NAME', "")).ljust(20)
                    row['DEP DOB'] = str(row.get('DOB', "")).ljust(20)
                    row['DEP SSN'] = str(row.get('TEMP SSN', "")).ljust(20)
                    row['DEP SEX'] = str(row.get('SEX', "")).ljust(20)

            if 'SEX' in row.keys():
                if row['SEX'] == 'M' and row['SUB/DEP'] == 'Child':
                    row['SUB/DEP'] = 'SON'.ljust(20)
                elif row['SEX'] == 'F' and row['SUB/DEP'] == 'Child':
                    row['SUB/DEP'] = 'DAUGHTER'.ljust(20)
            if 'SUB/DEP' in row.keys():
                if row['SUB/DEP'] == 'Dependent':
                    row['SUB/DEP'] = 'OTHER'.ljust(20)

                if row["SUB/DEP"] == "Subscriber":
                    current_subscriber_ssn = row["SSN"]
                else:
                    row["SSN"] = current_subscriber_ssn
                if row["SUB/DEP"] == "Subscriber":
                    if "ADDRESS 1" in row and row["ADDRESS 1"]:
                        subscriber_address = row["ADDRESS 1"]
                    if 'ZIP' in row.keys() and 'CITY' in row.keys() and 'STATE' in row.keys():
                        subscriber_zip = row["ZIP"]
                        subscriber_city = row["CITY"]
                        subscriber_state = row["STATE"]
                else:
                    if "ADDRESS 1" in row and row["ADDRESS 1"]:
                        if row["ADDRESS 1"] != subscriber_address:
                            row["DEP ADDRESS"] = row["ADDRESS 1"]
                            row["ADDRESS 1"] = subscriber_address
                    if 'ZIP' in row.keys():    
                        if row["ZIP"] != subscriber_zip:
                                row["DEP ZIP"] = row["ZIP"]
                                row["ZIP"] = subscriber_zip
                    if 'CITY' in row.keys():
                        if row["CITY"] != subscriber_city:
                                row["DEP CITY"] = row["CITY"]
                                row["CITY"] = subscriber_city
                    if 'STATE' in row.keys():    
                        if row["STATE"] != subscriber_state:
                                row["DEP STATE"] = row["STATE"]
                                row["STATE"] = subscriber_state                            

            worksheet.append([row.get(header, "") for header in csv_headers])
        

    workbook.save(path)

    cus_df = parse_custodial_data(csv_data)
    # dam = 'A:\OOE_BACKUP\OUTPUT'
    # fel = "custard.xlsx"
    # pat = os.path.join(dam,fel)
    # cus_df.to_excel(pat)
    total_subscribers = cus_df['SUB/DEP'].str.lower().value_counts().get('subscriber', 0)
    total_spouse_count = cus_df['SUB/DEP'].str.lower().value_counts().get('spouse',0)
    total_dependents = len(cus_df) - total_subscribers
    other_dependents = len(cus_df)-(total_subscribers+total_spouse_count)
    term_check_df = cus_df
    current_date = datetime.now()
    match = re.search(r'(\d{4})(\d{2})(\d{2})', file_name)
    if match:
        year, month, day = match.groups()
        formatted_dates = f"{month}/{day}/{year}"
        m = str(month)
        d = str(day)
        y = str(year)
        print(formatted_dates)
    else:
        formatted_dates = date
    day = current_date.strftime("%A")
    date = formatted_dates
    connection = sqlite3.connect('db.sqlite3')  
    cursor = connection.cursor()
    try:
        insert_query = """
        INSERT INTO myapp_count_model (filename, subscriber_count, spouse_count, dependent_count, date, day)
        VALUES (?, ?, ?, ?, ?);
        """

        cursor.execute(insert_query, (input_filename, total_subscribers,total_spouse_count, total_dependents, date, day))

        connection.commit()
        connection.close()
    except:
        pass

    mssql_conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=ABCCOLUMBUSSQL2;'
            'DATABASE=EDIDATABASE;'
            'UID=sa;'
            'PWD=ChangeMe#2024;'
        )
    
    mssql_cursor = mssql_conn.cursor()
    insert_query = """
    INSERT INTO myapp_mssql_count_model (filename, subscriber_count, spouse_count, other_dependent_count, date, day)
    VALUES (?, ?, ?, ?, ?, ?); """
    input_filename = ''
    total_subscribers = str(int(total_subscribers))
    total_spouse_count = str(int(total_spouse_count))
    total_dependents = str(int(other_dependents))
    date = str(date)
    day = ''

    try:
        mssql_cursor.execute(insert_query, (input_filename, total_subscribers, total_spouse_count, total_dependents, date, day))
        mssql_conn.commit()
    except Exception as e:
        print(f"Error er: {e}")

    connection.close()

    total_subscribers = int(total_subscribers)
    total_spouse_count = int(total_spouse_count)
    total_dependents = int(total_dependents)

    column_positions = {
    'ADDRESS2': 19,  # Column T
    'TERM DATE': 16,  # Column Q
    'LOCAL': 11,  # Column L
    'EMAIL': 24,  # Column Y
}
    
    def inventory_check(df):
        reinstate_ssns  =  []
        conn = sqlite3.connect('db.sqlite3')
        mssql_conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=ABCCOLUMBUSSQL2;'
            'DATABASE=EDIDATABASE;'
            'UID=sa;'
            'PWD=ChangeMe#2024;'
        )
        mssql_cursor = mssql_conn.cursor()
        inventory_data_query = "SELECT temp_ssn, flag,first_name,last_name,member_id FROM myapp_inventory_table_data"
        inventory_data = pd.read_sql_query(inventory_data_query, conn)
        print(inventory_data)
        current_date = datetime.now().strftime('%Y-%m-%d')
        missing_ssn = inventory_data[~inventory_data['temp_ssn'].isin(df['TEMP SSN'])]
        missing_in_inventory = df[~df['TEMP SSN'].isin(inventory_data['temp_ssn'])]
        new_ssns_apr = missing_in_inventory['TEMP SSN'].tolist()
        reinstate_ssns = df[df['TEMP SSN'].isin(inventory_data[inventory_data['flag'] == 'Y']['temp_ssn'])]['TEMP SSN'].tolist()
        print("reinstate_ssns",reinstate_ssns)
        match = re.search(r'(\d{4})(\d{2})(\d{2})', file_name)
        if match:
            year, month, day = match.groups()
            formatted_date = f"{month}/{day}/{year}"
            print(formatted_date)
        today = formatted_date
        cutoff_date = datetime(2025, 1, 1)
        re_cursor = conn.cursor()
        print(reinstate_ssns)
        if len(reinstate_ssns) >= 1:
            update_fields = ["EHHLTH", "EHDISB", "EHEF1Y", "EHEF1M", "EHEF1D"]
            temp_values = ["A", "A",y,m,d ]
            for ssn in reinstate_ssns:
                ssn = ssn.replace('-','')
                set_clause = ', '.join([f"{col} = ?" for col in update_fields])
                sql = f"UPDATE myapp_empyhltp SET {set_clause} WHERE EHSSN = ?"

                mssql_cursor.execute(sql, temp_values + [ssn])
            mssql_conn.commit()

            update_fields_depnp = ["DPSTAT", "DPEFDY","DPEFDM","DPEFDD"]
            temp_values_dep = ["A",y,m,d ]
            print("jack ma")
            copy_reinstate_ssns = reinstate_ssns
            for ssn in reinstate_ssns:
                ssn = ssn.replace('-','')
                set_clause = ', '.join([f"{col} = ?" for col in update_fields_depnp])
                sql = f"UPDATE myapp_depnp SET {set_clause} WHERE DPDSSN = ?"

                mssql_cursor.execute(sql, temp_values_dep + [ssn])
            mssql_conn.commit()

        for ssn in reinstate_ssns:
            # in_ssn = ssn.zfill(9)
            inv_ssn = f"{ssn[:3]}-{ssn[3:5]}-{ssn[5:]}"
            row = df[df['TEMP SSN'] == ssn].iloc[0]
            # re_cursor.execute("""
            #     SELECT term_date,term_date_last_month
            #     FROM myapp_inventory_table_data 
            #     WHERE temp_ssn = ?
            # """, (ssn,))
            
            # result = re_cursor.fetchone()
            # term = result[0]
            # print('tss',term)
            # last_date = pd.to_datetime(result[1])
            # print("lss",last_date)
            # date_term = pd.to_datetime(term)
            # mark_date = pd.to_datetime('03/11/2025')
            # if date_term <= mark_date:
            #     first_day_of_month = date_term.replace(day=1)
            #     last_day_previous_month = first_day_of_month - timedelta(days=1)
            #     last_date = last_day_previous_month
            #     print("last_date",last_date)
            eff_date = pd.to_datetime(row['EFF DATE'])
            # term_date_str = last_date  
            # term_date = term_date_str
            re_cursor.execute("""
                SELECT ssn, dep_ssn
                FROM myapp_inventory_table_data
                WHERE temp_ssn = ?
            """, (ssn,))

            host = '104.153.122.227'
            port = '23'
            database = 'S78F13CW'
            user = 'ONEPYTHON'
            password = 'pa33word'

            connection_string = (
                f"DRIVER={{iSeries Access ODBC Driver}};"
                f"SYSTEM={host};"
                f"PORT={port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"PROTOCOL=TCPIP;"
            )

            # Connect to AS400
            as400_conn = pyodbc.connect(connection_string)
            as400_cursor = as400_conn.cursor()
            result = re_cursor.fetchone()
            if result:
                ssn_val, depssn_val = result
                print(ssn_val,depssn_val,ssn)
                ssn = str(ssn)
                ssn = ssn.replace('-','')
                if ssn_val and depssn_val:
                    as400_cursor.execute("""
                    SELECT DPTDTY, DPTDTM, DPTDTD, DPSTAT, DPEFDM, DPEFDY, DPEFDD
                    FROM ooedf.depnp
                    WHERE DPDSSN = ? and DPSTAT = 'D'
                """, (ssn,))
                    dependent_data = as400_cursor.fetchone()
                    print("yuyp",dependent_data)
                    try:
                        t_year, t_month, t_day, dp_status, e_month, e_year, e_day = dependent_data
                        try:
                            term_date = str(f"{t_month}/{t_day}/{t_year}")
                            term_date = datetime(int(t_year),int(t_month),int(t_day))
                        except:
                            term_date = datetime(int(2025),int(1),int(1))
                    except:
                        term_date = datetime(int(2025),int(1),int(1))
                elif ssn_val or depssn_val:
                    as400_cursor.execute("""
                        SELECT EHHLTH, EHEF2Y, EHEF2M, EHEF2D, EHLTDY, EHLTDM, EHLTDD
                        FROM ooedf.empyhltp
                        WHERE EHSSN = ?
                    """, (ssn,))
                    member_data = as400_cursor.fetchone()
                    try:
                        m_status,m_ef_year,m_ef_month,m_ef_day,t_year,t_month,t_day = member_data
                        try:
                            term_date = datetime(int(t_year),int(t_month),int(t_day))
                        except:
                            term_date = datetime(int(2025),int(1),int(1))
                    except:
                        term_date = datetime(int(2025),int(1),int(1))
            
            if eff_date < cutoff_date:
                if isinstance(term_date, datetime):
                    new_eff_date_dt = term_date + timedelta(days=1)
                else:
                    term_date_dt = datetime.strptime(term_date, "%m/%d/%Y")
                    new_eff_date_dt = term_date_dt + timedelta(days=1)
                new_eff_date = new_eff_date_dt.strftime("%m/%d/%Y")
            elif eff_date >= cutoff_date and eff_date >= term_date:
                new_eff_date = eff_date
            elif eff_date >= cutoff_date and eff_date < term_date:
                if isinstance(term_date, datetime):
                    new_eff_date_dt = term_date + timedelta(days=1)
                else:
                    term_date_dt = datetime.strptime(term_date, "%m/%d/%Y")
                    new_eff_date_dt = term_date_dt + timedelta(days=1)
                new_eff_date = new_eff_date_dt.strftime("%m/%d/%Y")

            new_eff_date = pd.to_datetime(new_eff_date).strftime('%m/%d/%Y')
            new_eff_date = str(new_eff_date)
            remark_text = f"This SSN was reinstated on {today}"
            print('update_ssn',ssn)
            i_ssn = ssn.zfill(9)
            ir_ssn = f"{i_ssn[:3]}-{i_ssn[3:5]}-{i_ssn[5:]}"
            print("jack",ir_ssn)
            conn.cursor().execute("""
                UPDATE myapp_inventory_table_data
                SET eff_date = ?, reinstate_date = ?, flag = '', remark = ?
                WHERE temp_ssn = ? AND flag = 'Y'
            """, (new_eff_date, today, remark_text, ir_ssn))

        conn.commit()

        r_list = list(reinstate_ssns)
        try:
            html_body = build_success_email_count_reinstate_body(r_list)
            emails=["krushnarajjadhav015@gmail.com","akshay.kumar@onesmarter.com"]
            for e in emails:
                send_email_via_fastapi(e,"Count list for reinstate ssns:",html_body)
            # send_success_email_count_reinstate(emails=["krushnarajjadhav015@gmail.com","akshay.kumar@onesmarter.com"], term_ssn_list=r_list)
        except Exception as e:
            print('re_instance_error',e)
        new_ssn_df = missing_in_inventory
        new_ssn_df = new_ssn_df[['FIRST NAME','LAST NAME','MEMBER ID']]
        new_ssn_df.rename(columns={'FIRST NAME':'first_name','LAST NAME':'last_name','MEMBER ID':'member_id'},inplace=True)
        new_ssn_df['filename'] = file_name
        new_ssn_df['file_date'] = date
        data_to_insert = new_ssn_df.to_dict(orient='records')
        columns = list(new_ssn_df.columns)
        column_names = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT INTO myapp_recent_data ({column_names}) VALUES ({placeholders})"
        try:
            if data_to_insert:
                mssql_cursor.executemany(insert_query, [tuple(row.values()) for row in data_to_insert])
                mssql_conn.commit() 
                print("Missing records inserted successfully!")
        except Exception as e:
            print('erl:', e)
        missing_ssn_for_status = missing_ssn['temp_ssn'].tolist()
        missing_records = missing_ssn[missing_ssn['flag'] != 'Y']
        missing_ssns = missing_records['temp_ssn'].tolist()
        fil_ssn = [ssn for ssn in missing_ssns if ssn is not None]
        if len(fil_ssn) >= 1:
            today = datetime.today()
            last_day_previous_month = datetime(today.year, today.month, 1) - timedelta(days=1)
            t_year = last_day_previous_month.strftime("%Y")
            t_month = last_day_previous_month.strftime("%m")
            t_day = last_day_previous_month.strftime("%d")
            update_fields = ["EHHLTH", "EHDISB", "EHLTDY", "EHLTDM", "EHLTDD"]
            temp_values = ["D", "D",t_year,t_month,t_day ]
            for ssn in fil_ssn:
                ssn = ssn.replace('-','')
                set_clause = ', '.join([f"{col} = ?" for col in update_fields])
                sql = f"UPDATE myapp_empyhltp SET {set_clause} WHERE EHSSN = ?"

                mssql_cursor.execute(sql, temp_values + [ssn])
            mssql_conn.commit()

            update_fields_depnp = ["DPSTAT", "DPTDTY","DPTDTM","DPTDTD"]
            temp_values_dep = ["D",t_year,t_month,t_day ]
            for ssn in fil_ssn:
                ssn = ssn.replace('-','')
                set_clause = ', '.join([f"{col} = ?" for col in update_fields_depnp])
                sql = f"UPDATE myapp_depnp SET {set_clause} WHERE DPDSSN = ?"

                mssql_cursor.execute(sql, temp_values_dep + [ssn])
            mssql_conn.commit()
        cric_cursor = conn.cursor()
        for ssn in reinstate_ssns:
            cric_cursor.execute("""
                SELECT term_date, member_id 
                FROM myapp_inventory_table_data 
                WHERE temp_ssn = ?
            """, (ssn,))
            
            result = cric_cursor.fetchone()
            print(result)
            if result:
                termdate, member_id = result
                remark = f"This SSN was reinstated on {today}"
                conn.cursor().execute("""
                    INSERT INTO myapp_sssn_logs (ssn, remark, date, term_date, member_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (ssn, remark, today, termdate, member_id))
        conn.commit()
        if fil_ssn:
            for ssn in fil_ssn:
                remark = f"This SSN was termed on {today}"
                conn.cursor().execute("""
                    INSERT INTO myapp_sssn_logs (ssn, remark, date)
                    VALUES (?, ?, ?)
                """, (ssn, remark, today))
            conn.commit()
        termed_member_df = missing_records[missing_records['temp_ssn'].isin(fil_ssn)]
        print(termed_member_df.columns)
        termed_df = termed_member_df[['first_name','last_name','member_id']]
        today = datetime.today()
        last_day_previous_month = datetime(today.year, today.month, 1) - timedelta(days=1)
        termed_date_last_month = last_day_previous_month.strftime("%m/%d/%Y")
        termed_df['filename'] = file_name
        termed_df['file_date'] = date
        def last_date_of_current_month():
            today = datetime.today()
            last_day_of_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            return last_day_of_month.strftime("%m/%d/%Y")
        termed_df['term_date'] = last_date_of_current_month()
        termed_data_to_insert = termed_df.to_dict(orient='records')
        termed_columns = list(termed_df.columns)
        termed_column_names = ", ".join(termed_columns)
        termed_placeholders = ", ".join(["?" for _ in termed_columns])
        termed_insert_query = f"INSERT INTO myapp_termed_members ({termed_column_names}) VALUES ({termed_placeholders})"
        try:
            if termed_data_to_insert:
                mssql_cursor.executemany(termed_insert_query, [tuple(row.values()) for row in termed_data_to_insert])
                mssql_conn.commit() 
                print("Termed records inserted successfully!")
        except Exception as e:
            print('erl_termed:', e)
        try:
            droped_members = len(fil_ssn)
            new_members_added = len(data_to_insert)
            member_query = """
            INSERT INTO myapp_member_count (new_members, dropped_members, file_date) 
            VALUES (?, ?, ?);
        """
            mssql_cursor.execute(member_query, (new_members_added, droped_members,date))
            mssql_conn.commit()
        except Exception as e:
            print("error memeber count",e)
            
        # try:
        #     hr_query = f"""
        #     SELECT * 
        #     FROM myapp_inventory_table_data
        #     WHERE temp_ssn IN ({','.join(['?' for _ in missing_ssns])})
        #     """

        #     hrdf = pd.read_sql(hr_query, mssql_conn, params=missing_ssns)
        #     insert_query = """
        #     INSERT INTO myapp_history_data_table (col1, col2, col3, ...) 
        #     VALUES (?, ?, ?, ...)
        #     """

        #     for index, row in hrdf.iterrows():
        #         mssql_cursor.execute(insert_query, row['col1'], row['col2'], row['col3'], ...)  # Adjust columns
        #     mssql_conn.commit()
        # except Exception as e:
        #     print('herrr',e)

        missing_data = pd.DataFrame()
        try:
            today_ssns = df['TEMP SSN'].tolist()
            update_query = """
            UPDATE myapp_eligibility_status_table
            SET Eligibility_status = 'Active', reason = ''
            WHERE temp_ssn = ?
            """
            with conn:
                for ssn in today_ssns:
                    conn.cursor().execute(update_query, (ssn,))
        except Exception as e:
            print('er',e)
        try:
            pass
            html_body = build_missing_ssn_email_body(missing_ssns,missing_in_inventory['TEMP SSN'].tolist())
            send_email_via_fastapi('akshay.kumar@onesmarter.com',"missing ssn",html_body)
            send_email_via_fastapi('Vikram@vikramsethi.com',"missing ssn",html_body)
            # send_missing_ssn_email(missing_ssns,missing_in_inventory['TEMP SSN'].tolist(),'akshay.kumar@onesmarter.com')
            # send_missing_ssn_email(missing_ssns,missing_in_inventory['TEMP SSN'].tolist(),'Vikram@vikramsethi.com')
        except:
            print("something went wrong with ssn email")
        fil_ssn = [ssn for ssn in missing_ssns if ssn is not None]
        print("missing ssn in current file", fil_ssn)
        # print("misssing ssn in inventory data",missing_in_inventory['TEMP SSN'])
        miss_ssn = missing_in_inventory['TEMP SSN'].tolist()
        ms_ssn = [ssn for ssn in miss_ssn if ssn is not None]
        conn_missing = sqlite3.connect("db.sqlite3")
        cursor_missing = conn_missing.cursor()

        ms_ssn = [ssn for ssn in miss_ssn if ssn is not None]

        # today = date.today()
        first_day = today.replace(day=1)

        DPEFDY1 = first_day.strftime("%Y")
        DPEFDM1 = first_day.strftime("%m")
        DPEFDD1 = first_day.strftime("%d")

        for ssn in ms_ssn:
            if ssn:
                insert_query = """
                    INSERT INTO myapp_new_records (SSN, DPEFDY, DPEFDM, DPEFDD)
                    VALUES (?, ?, ?, ?)
                """
                values = (str(ssn), DPEFDY1, DPEFDM1, DPEFDD1)
                cursor_missing.execute(insert_query, values)

        conn_missing.commit()
        conn_missing.close()
        print("Missing SSNs in inventory data:", ms_ssn)
        if missing_ssns:
            placeholders = ', '.join(['?'] * len(missing_ssns))
            query = f"SELECT * FROM myapp_inventory_table_data WHERE temp_ssn IN ({placeholders})"
            try:
                placeholders2 = ', '.join(['?'] * len(missing_ssn_for_status))
                query2 = f"SELECT * FROM myapp_inventory_table_data WHERE temp_ssn IN ({placeholders2})"
                missing_data_for_status =  pd.read_sql_query(query2, conn, params=missing_ssn_for_status)
            except:
                pass
            missing_data = pd.read_sql_query(query, conn, params=missing_ssns)
            rename_columns_reversed = {
                'LAST NAME': 'last_name',
                'FIRST NAME': 'first_name',
                'SSN': 'ssn',
                'SUB/DEP': 'sub_dep',
                'STATUS': 'status',
                'TYPE': 'type',
                'PHONE': 'phone',
                'ADDRESS 1': 'address1',
                'CITY': 'city',
                'STATE': 'state',
                'ZIP': 'zip',
                'DOB': 'dob',
                'SEX': 'sex',
                'PLAN': 'plan',
                'CLASS': 'class_field',
                'EFF DATE': 'eff_date',
                'ID': 'id_field',
                'DEP FIRST NAME': 'dep_first_name',
                'DEP LAST NAME': 'dep_last_name',
                'DEP DOB': 'dep_dob',
                'DEP SSN': 'dep_ssn',
                'DEP SEX': 'dep_sex',
                'CUSTODIAL PARENT': 'custodial_parent',
                'CUSTODIAL ADDRESS 1': 'custodial_address1',
                'CUSTODIAL ADDRESS 2': 'custodial_address2',
                'CUSTODIAL CITY': 'custodial_city',
                'CUSTODIAL STATE': 'custodial_state',
                'CUSTODIAL ZIP': 'custodial_zip',
                'CUSTODIAL PHONE': 'custodial_phone',
                'ADDRESS2': 'address2',
                'MEMBER ID': 'member_id',
                'EDI_DATE': 'date_edi',
                'filename': 'filename',
                'TEMP SSN': 'temp_ssn',
                'TERM DATE': 'term_date'
            }

            missing_data.rename(columns=rename_columns_reversed, inplace=True)
            try:
                missing_data_for_status.rename(columns=rename_columns_reversed,inplace=True)
                missing_data_for_status['Eligibility_status'] = 'Inactive'
                missing_data_for_status['reason'] = "Not present in today's data"
                missing_data_for_status.to_sql('myapp_eligibility_status_table', conn, if_exists='append', index=False)
            except Exception as e:
                print('err',e)
            match = re.search(r'(\d{4})(\d{2})(\d{2})', file_name)
            if match:
                year, month, day = match.groups()
                formatted_date = f"{month}/{day}/{year}"
                print(formatted_date)
            else:
                formatted_date = date
            column_mapping = {
            'sub_dep': 'SUB/DEP',
            'last_name': 'LAST NAME',
            'first_name': 'FIRST NAME',
            'ssn': 'SSN',
            'temp_ssn': 'TEMP SSN',
            'sex': 'SEX',
            'dob': 'DOB',
            'dep_last_name': 'DEP LAST NAME',
            'dep_first_name': 'DEP FIRST NAME',
            'dep_ssn': 'DEP SSN',
            'dep_sex': 'DEP SEX',
            'dep_dob': 'DEP DOB',
            'custodial_parent': 'CUSTODIAL PARENT',
            'plan': 'PLAN',
            'class_field': 'CLASS',
            'eff_date': 'EFF DATE',
            'id_field': 'ID',
            'address1': 'ADDRESS 1',
            'city': 'CITY',
            'state': 'STATE',
            'zip': 'ZIP',
            'phone': 'PHONE',
            'status': 'STATUS',
            'type': 'TYPE',
            'member_id': 'MEMBER ID',
            'custodial_address1': 'CUSTODIAL ADDRESS 1',
            'custodial_address2': 'CUSTODIAL ADDRESS 2',
            'custodial_city': 'CUSTODIAL CITY',
            'custodial_state': 'CUSTODIAL STATE',
            'custodial_zip': 'CUSTODIAL ZIP',
            'custodial_phone': 'CUSTODIAL PHONE'
        }
            missing_data = missing_data.drop(columns=['filename', 'date_edi', 'flag'], errors='ignore')
            missing_data.rename(columns=column_mapping, inplace=True)
            today = datetime.today()
            last_day_prev_month = datetime(today.year, today.month, 1) - timedelta(days=1)
            formatted_date_last_month = last_day_prev_month.strftime("%m/%d/%Y")
            if len(missing_data) == 0:
                missing_data = pd.DataFrame([])
            else:
                missing_data['TERM DATE'] = last_date_of_current_month()
            print('mad max fury',missing_data.columns)
            print('arrival',df.columns)
            combined_df = pd.concat([df, missing_data], ignore_index=True)
            combined_df['PHONE'] = combined_df['PHONE'].astype(str).str.replace(r'\.0$', '', regex=True)
            combined_df['PLAN'] = combined_df['PLAN'].astype(str).apply(lambda x: x.zfill(2) if len(x) == 1 else x)
            print('combined',combined_df.columns)
            update_query = f"UPDATE myapp_inventory_table_data SET flag = 'Y',term_date = '{formatted_date}',term_date_last_month = '{formatted_date_last_month}' WHERE temp_ssn IN ({placeholders})"
            try:
                mssql_update_query = f"UPDATE myapp_mssql__data SET flag = 'Y',term_date = '{formatted_date}' WHERE temp_ssn IN ({placeholders})"
                mssql_conn.execute(mssql_update_query,missing_ssns)
            except:
                pass
            conn.execute(update_query, missing_ssns)
            conn.commit()
            mssql_conn.commit()
            mssql_conn.close()
            # conn.close()
        else:
            combined_df = df

 
        if not missing_in_inventory.empty:
            print("init")
            # conn = sqlite3.connect("db.sqlite3")
            match = re.search(r'(\d{4})(\d{2})(\d{2})', file_name)
            if match:
                year, month, day = match.groups()
                formatted_date = f"{year}-{month}-{day}"
                print(formatted_date)
            else:
                formatted_date = ''
            cursor = conn.cursor()
            missing_in_inventory['date_edi'] = formatted_date
            data_to_insert = missing_in_inventory.to_dict(orient='records')
            key_to_column = {
                'LAST NAME': 'last_name',
                'FIRST NAME': 'first_name',
                'SSN': 'ssn',
                'SUB/DEP': 'sub_dep',
                'STATUS': 'status',
                'TYPE': 'type',
                'PHONE': 'phone',
                'ADDRESS 1': 'address1',
                'CITY': 'city',
                'STATE': 'state',
                'ZIP': 'zip',
                'DOB': 'dob',
                'SEX': 'sex',
                'PLAN': 'plan',
                'CLASS': 'class_field',
                'EFF DATE': 'eff_date',
                'ID': 'id_field',
                'DEP FIRST NAME': 'dep_first_name',
                'DEP LAST NAME': 'dep_last_name',
                'DEP DOB': 'dep_dob',
                'DEP SSN': 'dep_ssn',
                'DEP SEX': 'dep_sex',
                'CUSTODIAL PARENT': 'custodial_parent',
                'CUSTODIAL ADDRESS 1': 'custodial_address1',
                'CUSTODIAL ADDRESS 2': 'custodial_address2',
                'CUSTODIAL CITY': 'custodial_city',
                'CUSTODIAL STATE': 'custodial_state',
                'CUSTODIAL ZIP': 'custodial_zip',
                'CUSTODIAL PHONE': 'custodial_phone',
                'ADDRESS2': 'address2',
                'MEMBER ID': 'member_id',
                'date_edi': 'date_edi',
                'TEMP SSN': 'temp_ssn',
            }


            db_columns = list(key_to_column.values())

            values = [
                tuple(record.get(key, '').strip() if isinstance(record.get(key), str) else record.get(key)
                    for key in key_to_column.keys())
                for record in data_to_insert
            ]
    

            placeholders = ", ".join(["?"] * len(db_columns)) 
            insert_query = f"""
            INSERT INTO myapp_inventory_table_data ({', '.join(db_columns)})
            VALUES ({placeholders})
            """
            cursor.executemany(insert_query, values)

            # try:
            #     his_insert_query = f"""
            #     INSERT INTO myapp_history_data_table ({', '.join(db_columns)})
            #     VALUES ({placeholders})
            #     """
            #     cursor.executemany(his_insert_query,values)
            # except Exception as e:
            #     print("errrrrr",e)

            conn.commit()
            conn.close()

            print("Missing data inserted successfully!")

        return combined_df,reinstate_ssns,formatted_date,new_ssns_apr,fil_ssn

    print("dfdfdfdfd")
    cus_df,reinstate_ssns,today,new_ssns_apr,fil_ssn = inventory_check(cus_df)
    # dira = r'A:\OOE_BACKUP\OUTPUT'
    # fils = 'cus_checker.xlsx'
    # pth = os.path.join(dira,fils)
    # cus_df.to_excel(pth)
    print(cus_df.columns)
    print('helleoe')
    cus_df['EFF DATE'] = pd.to_datetime(cus_df['EFF DATE'])
    cutoff_date = datetime(2025, 1, 1)
    conni = sqlite3.connect('db.sqlite3')
    cric_cursor = conni.cursor()
    # last_day_previous_month = (datetime.date.today().replace(day=1) - timedelta(days=1))
    # last_day_of_previoud_month = last_day_previous_month.strftime('%m/%d/%Y')
    if reinstate_ssns:
        print('jump')
        today_str = today
        cus_df['EFF DATE'] = pd.to_datetime(cus_df['EFF DATE'])
        host = '104.153.122.227'
        port = '23'
        database = 'S78F13CW'
        user = 'ONEPYTHON'
        password = 'pa33word'

        connection_string = (
                f"DRIVER={{iSeries Access ODBC Driver}};"
                f"SYSTEM={host};"
                f"PORT={port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"PROTOCOL=TCPIP;"
            )

            # Connect to AS400
        as400_conn = pyodbc.connect(connection_string)
        as400_cursor = as400_conn.cursor()    
        directory = r"SSN_Files"
        file_name = "cus_df_output.xlsx"
        file_path = os.path.join(directory, file_name)

        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)

        # Save the DataFrame to Excel
        cus_df.to_excel(file_path, index=False)
        gr_ssns = []
        for ssn in reinstate_ssns:
            
            # cric_cursor.execute("""
            #     SELECT term_date,term_date_last_month 
            #     FROM myapp_inventory_table_data 
            #     WHERE temp_ssn = ?
            # """, (ssn,))
            # result = cric_cursor.fetchone()
            # term = result[0]
            # last_date = pd.to_datetime(result[1])
            # date_term = pd.to_datetime(term)
            # print(date_term)
            # mark_date = pd.to_datetime('03/11/2025')
            # if date_term <= mark_date:
            #     first_day_of_month = date_term.replace(day=1)
            #     last_day_previous_month = first_day_of_month - timedelta(days=1)
            #     last_date = last_day_previous_month
            # if result:
            #     term_date = last_date
            #     if term_date:
            #         term_date = pd.to_datetime(term_date)
            #     else:
            #         term_date = today  
            # else:
            #     term_date = today  

            cric_cursor.execute("""
                SELECT ssn, dep_ssn
                FROM myapp_inventory_table_data
                WHERE temp_ssn = ?
            """, (ssn,))


            result = cric_cursor.fetchone()

            if result:
                ssn_val, depssn_val = result
                ssn = str(ssn)
                ssn = ssn.replace('-','')
                if ssn_val and depssn_val:
                    as400_cursor.execute("""
                    SELECT DPTDTY, DPTDTM, DPTDTD, DPSTAT, DPEFDM, DPEFDY, DPEFDD
                    FROM ooedf.depnp
                    WHERE DPDSSN = ?
                """, (ssn,))
                    dependent_data = as400_cursor.fetchone()
                    t_year,t_month,t_day,e_status,e_year,e_month,e_day = dependent_data
                    term_date = str(f"{t_month}/{t_day}/{t_year}")
                elif ssn_val or depssn_val:
                    as400_cursor.execute("""
                        SELECT EHHLTH, EHEF2Y, EHEF2M, EHEF2D, EHLTDY, EHLTDM, EHLTDD
                        FROM ooedf.empyhltp
                        WHERE EHSSN = ?
                    """, (ssn,))
                    member_data = as400_cursor.fetchone()
                    m_status,m_ef_year,m_ef_month,m_ef_day,t_year,t_month,t_day = member_data
                    term_date = str(f"{t_month}/{t_day}/{t_year}")

            in_ssn = ssn.zfill(9)
            formatted_ssn = f"{in_ssn[:3]}-{in_ssn[3:5]}-{in_ssn[5:]}"
            idx = cus_df[cus_df['TEMP SSN'] == formatted_ssn].index
            print('ronnnnnnn',idx,ssn)
            if not idx.empty:
                eff_date = cus_df.loc[idx[0], 'EFF DATE']
                print("drax",eff_date)
                if isinstance(eff_date, str) and '/' in eff_date:
                    eff_date = datetime.strptime(eff_date, "%m/%d/%Y")

                if isinstance(term_date, str) and '/' in term_date:
                    try:
                        term_date = datetime.strptime(term_date, "%m/%d/%Y")
                    except:
                        term_date = datetime(2025, 4, 30)

                if isinstance(cutoff_date, str) and '/' in cutoff_date:
                    cutoff_date = datetime.strptime(cutoff_date, "%m/%d/%Y")

                if pd.isna(eff_date):
                    new_eff_date = today_str
                elif eff_date < cutoff_date:
                    print("term",term_date)
                    # new_eff_date_dt = term_date + timedelta(days=1)
                    new_eff_date_dt = datetime.today().replace(day=1).strftime('%m/%d/%Y')
                    new_eff_date = new_eff_date_dt
                elif eff_date >= cutoff_date and eff_date > term_date:
                    new_eff_date = eff_date.strftime("%m/%d/%Y")
                elif eff_date >= cutoff_date and eff_date < term_date:
                    # new_eff_date_dt = term_date + timedelta(days=1)
                    new_eff_date_dt = datetime.today().replace(day=1).strftime('%m/%d/%Y')
                    new_eff_date = new_eff_date_dt
                else:
                    new_eff_date = today_str

                first_day_current_month = datetime.today().replace(day=1)
                if eff_date >= cutoff_date and eff_date < term_date and term_date > first_day_current_month:
                    gr_ssns.append(ssn)
                    
                print("this is new eff date",new_eff_date)
                cus_df.at[idx[0], 'EFF DATE'] = new_eff_date

        

        def fetch_and_prepare_df_emp(data, columns):
            processed_data = [
                tuple(str(item) if isinstance(item, Decimal) else item for item in row)
                for row in data
            ]
            df = pd.DataFrame(processed_data, columns=columns)
            df['DATE1'] = pd.to_datetime(df[['EHEF2Y', 'EHEF2M', 'EHEF2D']].astype(str).agg('-'.join, axis=1), errors='coerce')
            df['DATE2'] = pd.to_datetime(df[['EHEF1Y', 'EHEF1M', 'EHEF1D']].astype(str).agg('-'.join, axis=1), errors='coerce')
            df['EFF DATE'] = df[['DATE1', 'DATE2']].max(axis=1)

            return df

        
        emp_data = as400_cursor.execute("""
            SELECT 
                EHSSN, 
                EHEF2Y, EHEF2M, EHEF2D,
                EHEF1Y, EHEF1M, EHEF1D
            FROM ooedf.empyhltp
        """).fetchall()

        columns = ['EHSSN', 'EHEF2Y', 'EHEF2M', 'EHEF2D', 'EHEF1Y', 'EHEF1M', 'EHEF1D']
        df_emp = fetch_and_prepare_df_emp(emp_data, columns)

        def fetch_and_prepare_df(data, columns):
            processed_data = [
                tuple(str(item) if isinstance(item, Decimal) else item for item in row)
                for row in data
            ]
            df = pd.DataFrame(processed_data, columns=columns)
            df['EFF DATE'] = (
                df[columns[2]].astype(str) + '/' +
                df[columns[3]].astype(str) + '/' +
                df[columns[1]].astype(str)
            )
            return df

        dep_data = as400_cursor.execute("""
            SELECT 
                DPDSSN, 
                DPEFDY, 
                DPEFDM, 
                DPEFDD 
            FROM ooedf.depnp
        """).fetchall()
        df_dep = fetch_and_prepare_df(dep_data, ['DPDSSN', 'DPEFDY', 'DPEFDM', 'DPEFDD'])

        eff_date_mapping = {
            **dict(zip(df_emp['EHSSN'].astype(str).str.zfill(9), df_emp['EFF DATE'])),
            **dict(zip(df_dep['DPDSSN'].astype(str).str.zfill(9), df_dep['EFF DATE']))
        }

        cus_df['TEMP SSN (NO HYPHEN)'] = cus_df['TEMP SSN'].str.replace('-', '').str.zfill(9)
        mask_non_reinstate = ~cus_df['TEMP SSN'].isin(reinstate_ssns)
        cus_df.loc[mask_non_reinstate, 'EFF DATE'] = cus_df.loc[mask_non_reinstate, 'TEMP SSN (NO HYPHEN)'].map(eff_date_mapping)
        cus_df['EFF DATE'].fillna('01/01/2025', inplace=True)
        con_new = sqlite3.connect("db.sqlite3")
        cur_new = con_new.cursor()

        cur_new.execute("SELECT SSN, DPEFDY, DPEFDM, DPEFDD FROM myapp_new_records")
        rows = cur_new.fetchall()

        ssn_date_map = {}
        for row in rows:
            ssn, year, month, day = row
            eff_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            ssn_date_map[ssn] = eff_date

        filtered_ssns = [ssn for ssn in ssn_date_map.keys() if ssn not in fil_ssn]

        mask = cus_df['TEMP SSN'].isin(filtered_ssns)
        cus_df.loc[mask, 'EFF DATE'] = cus_df.loc[mask, 'TEMP SSN'].map(ssn_date_map)

        con_new.close()
        cus_df.drop(columns=['TEMP SSN (NO HYPHEN)'], inplace=True)
        first_day = datetime.today().replace(day=1).strftime('%m/%d/%Y')
        print(first_day)
        new_ssns_apr = [ssn for ssn in new_ssns_apr if ssn]
        print(new_ssns_apr)
        cus_df.loc[cus_df['TEMP SSN'].isin(new_ssns_apr), 'EFF DATE'] = first_day
        updated_rows = cus_df[cus_df['TEMP SSN'].isin(new_ssns_apr)]
        print(updated_rows[['TEMP SSN', 'EFF DATE']])
        email = ['krushnarajjadhav015@gmail.com','akshay.kumar@onesmarter.com','Vikram@vikramsethi.com','dprasad@abchldg.com', 'krisjones2003@gmail.com']
        # reins_ssns_email_less_than_term(email,gr_ssns)
        if '286-11-6600' in cus_df['TEMP SSN'].values:
            print(cus_df[cus_df['TEMP SSN'] == '286-11-6600'][['TEMP SSN', 'EFF DATE']])

        print("EFF DATE mapping for both employees and dependents completed successfully.")

        # sqlite_connection = sqlite3.connect('db.sqlite3')
        # sqlite_cursor = sqlite_connection.cursor()

        # sqlite_cursor.execute("SELECT temp_ssn, eff_date FROM myapp_inventory_table_data")
        # eff_date_mapping = dict(sqlite_cursor.fetchall())

        # sqlite_connection.close()

        # mask_non_reinstate = ~cus_df['TEMP SSN'].isin(reinstate_ssns)
        # cus_df.loc[mask_non_reinstate, 'EFF DATE'] = cus_df.loc[mask_non_reinstate, 'TEMP SSN'].map(eff_date_mapping)
        # cus_df['EFF DATE'].fillna('01/01/2025', inplace=True)

    else:
        host = '104.153.122.227'
        port = '23'
        database = 'S78F13CW'
        user = 'ONEPYTHON'
        password = 'pa33word'

        connection_string = (
                f"DRIVER={{iSeries Access ODBC Driver}};"
                f"SYSTEM={host};"
                f"PORT={port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"PROTOCOL=TCPIP;"
            )

            # Connect to AS400
        as400_conn = pyodbc.connect(connection_string)
        as400_cursor = as400_conn.cursor() 
        def fetch_and_prepare_df(data, columns):
            processed_data = [
                tuple(str(item) if isinstance(item, Decimal) else item for item in row)
                for row in data
            ]
            df = pd.DataFrame(processed_data, columns=columns)
            df['EFF DATE'] = (
                df[columns[2]].astype(str) + '/' +
                df[columns[3]].astype(str) + '/' +
                df[columns[1]].astype(str)
            )
            return df
        emp_data = as400_cursor.execute("""
            SELECT 
                EHSSN, 
                EHEF2Y, 
                EHEF2M, 
                EHEF2D 
            FROM ooedf.empyhltp
        """).fetchall()
        df_emp = fetch_and_prepare_df(emp_data, ['EHSSN', 'EHEF2Y', 'EHEF2M', 'EHEF2D'])

        dep_data = as400_cursor.execute("""
            SELECT 
                DPDSSN, 
                DPEFDY, 
                DPEFDM, 
                DPEFDD 
            FROM ooedf.depnp
        """).fetchall()
        df_dep = fetch_and_prepare_df(dep_data, ['DPDSSN', 'DPEFDY', 'DPEFDM', 'DPEFDD'])

        eff_date_mapping = {
            **dict(zip(df_emp['EHSSN'].astype(str).str.zfill(9), df_emp['EFF DATE'])),
            **dict(zip(df_dep['DPDSSN'].astype(str).str.zfill(9), df_dep['EFF DATE']))
        }

        cus_df['TEMP SSN (NO HYPHEN)'] = cus_df['TEMP SSN'].str.replace('-', '').str.zfill(9)
        mask_non_reinstate = ~cus_df['TEMP SSN'].isin(reinstate_ssns)
        cus_df.loc[mask_non_reinstate, 'EFF DATE'] = cus_df.loc[mask_non_reinstate, 'TEMP SSN (NO HYPHEN)'].map(eff_date_mapping)
        cus_df['EFF DATE'].fillna('01/01/2025', inplace=True)
        cus_df.drop(columns=['TEMP SSN (NO HYPHEN)'], inplace=True)

        if '286-11-6600' in cus_df['TEMP SSN'].values:
            print(cus_df[cus_df['TEMP SSN'] == '286-11-6600'][['TEMP SSN', 'EFF DATE']])

        print("EFF DATE mapping for both employees and dependents completed successfully.")
        print("EFF DATE mapping for 'ooedf.depnp' completed successfully in else part.")  
        # sqlite_connection = sqlite3.connect('db.sqlite3')
        # sqlite_cursor = sqlite_connection.cursor()
        # sqlite_cursor.execute("SELECT temp_ssn, eff_date FROM myapp_inventory_table_data")
        # eff_date_mapping = dict(sqlite_cursor.fetchall())
        # sqlite_connection.close()
        # cus_df['EFF DATE'] = cus_df['TEMP SSN'].map(eff_date_mapping).fillna('01/01/2025')
    try:
        custodial_parent_df = cus_df[(cus_df['CUSTODIAL PARENT'].notna()) & (cus_df['CUSTODIAL PARENT'].str.strip() != "")]
        column_mapping = {
        'SUB/DEP': 'sub_dep',
        'LAST NAME': 'last_name',
        'FIRST NAME': 'first_name',
        'SSN': 'ssn',
        'SEX': 'sex',
        'DOB': 'dob',
        'DEP LAST NAME': 'dep_last_name',
        'DEP FIRST NAME': 'dep_first_name',
        'DEP SSN': 'dep_ssn',
        'DEP SEX': 'dep_sex',
        'DEP DOB': 'dep_dob',
        'LOCAL': 'address1',
        'PLAN': 'plan',
        'CLASS': 'class_field',
        'EFF DATE': 'eff_date',
        'TERM DATE': 'term_date',
        'ID': 'id_field',
        'ADDRESS 1': 'address1',
        'ADDRESS2': 'address2',
        'CITY': 'city',
        'STATE': 'state',
        'ZIP': 'zip',
        'PHONE': 'phone',
        'EMAIL': 'email',  
        'STATUS': 'status',
        'TYPE': 'type',
        'MEMBER ID': 'member_id',
        'CUSTODIAL PARENT': 'custodial_parent',
        'CUSTODIAL ADDRESS 1': 'custodial_address1',
        'CUSTODIAL ADDRESS 2': 'custodial_address2',
        'CUSTODIAL CITY': 'custodial_city',
        'CUSTODIAL STATE': 'custodial_state',
        'CUSTODIAL ZIP': 'custodial_zip',
        'CUSTODIAL PHONE': 'custodial_phone',
    }
        custodial_parent_df.rename(columns=column_mapping, inplace=True)
        print(custodial_parent_df.columns)
        custodial_parent_data = custodial_parent_df.to_dict(orient='records')
        conn = sqlite3.connect('db.sqlite3')  # Ensure the path to your DB is correct
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO myapp_custodial_data_table 
        (last_name, first_name, ssn, sub_dep, status, type, phone, address1, city, state, zip, dob, sex, 
        plan, class_field, eff_date, id_field, dep_first_name, dep_last_name, dep_dob, dep_ssn, dep_sex, 
        custodial_parent, custodial_address1, custodial_address2, custodial_city, custodial_state, 
        custodial_zip, custodial_phone, address2, member_id, date_edi, filename, temp_ssn, term_date)
        VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            with mssql_conn:
                mssql_cursor.executemany(insert_query, [tuple(d.values()) for d in custodial_parent_data])
        except Exception as e:
            print("error cus", e)
    except Exception as e:
        print("error",e)

    try:
        cus_df.drop(columns=['TEMP SSN','filename','flag'],inplace=True)
    except:
        pass
    if 'TEMP SSN' in cus_df.columns:
        cus_df.drop(columns=['TEMP SSN'],inplace=True)
    if 'filename' in cus_df.columns:
        cus_df.drop(columns=['filename'],inplace=True)
    if 'TEMP SSN' in cus_df.columns:
        cus_df.drop(columns=['TEMP SSN'],inplace=True)
    if 'flag' in cus_df.columns:
        cus_df.drop(columns=['flag'],inplace=True)
    if 'id' in cus_df.columns:
        cus_df.drop(columns=['id'],inplace=True)
    print(cus_df.columns)
    
    for col, position in column_positions.items():
        if col not in cus_df.columns:
            cus_df.insert(min(position, len(cus_df.columns)), col, '')
    cus_df.to_csv(output_csv_path)
    print("www",output_xlsx_path)
    try:
        cus_df.to_excel(output_xlsx_path)
        try:
            snl_path = "S:\\SNL-EXCEL"
            snl_xlsx_path = os.path.join(snl_path,f"{input_filename}.xlsx")
            cus_df.to_excel(snl_xlsx_path)
        except Exception as e:
            print("error_log",e)
    except:
        excel_path = "S:\\OOE\\EDI_PROJECT-\\EDI-Backend\\media\\csv_files"
        try:
            snl_path = "S:\\SNL-EXCEL"
            snl_xlsx_path = os.path.join(snl_path,f"{input_filename}.xlsx")
            print("gghghght",snl_xlsx_path)
            cus_df.to_excel(snl_xlsx_path)
        except Exception as e:
            print("error_log",e)
        new_path = os.path.join(excel_path,f"{input_filename}.xlsx")
        # cus_df.to_excel(new_path)
    cus_df['EDI_DATE'] = date
    
    cus_df_dict = cus_df.to_dict(orient="records")
    DB_NAME = "db.sqlite3"
    def get_previous_business_day():
        today = datetime.today().date()
        if today.weekday() == 0:  # Monday
            return today - timedelta(days=3)  # Friday
        else:
            return today - timedelta(days=1)
    # def fetch_data_by_date(date):
    #     conn = sqlite3.connect(DB_NAME)
    #     cursor = conn.cursor()
    #     query = "SELECT member_id FROM myapp_edi_user_data WHERE date_edi = ?"
    #     cursor.execute(query, (date,))
    #     data = [row[0] for row in cursor.fetchall()]
    #     conn.close()
    #     return set(data)
    
    # previous_date = get_previous_business_day().strftime("%Y-%m-%d")
    # previous_data = fetch_data_by_date(previous_date)
    # previ
    # if not previous_data:
    #     print(f"No data available for the previous day: {previous_date}. Exiting process.")
    # today_data = set(cus_df['MEMBER ID'].dropna())

    # new_ids = today_data - previous_data
    # missing_ids = previous_data - today_data
    # send_member_id_email(new_ids,missing_ids)
    def write_to_queue_django(data, queue_file):
        try:
            with open(queue_file, "ab") as file:
                pickle.dump(data, file)
            print("django input filename added to the queue.")
        except Exception as e:
            print(f"Error writing to the queue: {e}")
    write_to_queue_django(cus_df_dict, "django_queue_file.pkl")
    cus_df.drop(columns=['EDI_DATE'],inplace=True)
    #calculate_count_variance(total_subscribers,total_dependents,input_filename)
    output_outbound_folder = r"S:/OOE/EDI_PROJECT-/EDI-Backend/media/output_outbound"
    file_name = os.path.splitext(os.path.basename(input_file_path))[0]
    output_file_pth = os.path.join(output_outbound_folder, f"{file_name}.834")
    def write_to_queue_input(data, queue_file):
        try:
            with open(queue_file, "ab") as file:
                pickle.dump(data, file)
            print("input filename added to the queue.")
        except Exception as e:
            print(f"Error writing to the queue: {e}")
    write_to_queue_input(file_name, "input_queue_file.pkl")
    is_monday = datetime.today().weekday() == 0
    is_first_date = datetime.today().day == 1
    try:
        if is_first_date and is_monday:
            shutil.copy(input_file_path, output_file_pth)
            print(f"File has been saved to: {output_file_pth}")
        elif is_first_date:
            shutil.copy(input_file_path, output_file_pth)
            print(f"File has been saved to: {output_file_pth}")
        elif is_monday:
            shutil.copy(input_file_path, output_file_pth)
            print(f"File has been saved to: {output_file_pth}")
        else:
            print("Today is not monday or first day of the month")
    except Exception as e:
        print(f"Error while saving file: {e}")
        print(f"CSV generated successfully at: {output_csv_path} and {system_csv_path}")

    # print("rex",cus_df)
    # output_dir = r"S:\OOE\EDI_PROJECT-\Output_Files"
    # os.makedirs(output_dir, exist_ok=True)

    # # Define the output file path
    # output_file = os.path.join(output_dir, "cus_df_output.xlsx")

    # Save the DataFrame to Excel
    # cus_df.to_excel(output_file, index=False)
    sus_df = cus_df.copy()  # Make a copy to avoid modifying cus_df
    sus_df['SSN'] = sus_df['SSN'].astype(str).str.strip().replace(['nan', 'NaT', '0', ''], pd.NA)
    sus_df['DEP SSN'] = sus_df['DEP SSN'].astype(str).str.strip().replace(['nan', 'NaT', '0', ''], pd.NA)
    emp_df = sus_df[(sus_df['SSN'].notna()) & (sus_df['DEP SSN'].isna())].reset_index(drop=True)
    # print(emp_df)
    depen_df = sus_df[(sus_df['SSN'].notna()) & (sus_df['DEP SSN'].notna())].reset_index(drop=True)

    def split_date(date_str):
        # print("check date",date_str)
        if pd.isna(date_str) or date_str == "" or date_str == "0":
            return 0, 0, 0
        if isinstance(date_str, pd.Timestamp):
            return date_str.year, date_str.month, date_str.day
        try:
            parts = date_str.strip().split('/')
            # print("parts",parts)
            if len(parts) != 3:
                # print("not parts",parts)
                return 0, 0, 0
            month, day, year = map(int, parts)
            return year, month, day
        except (ValueError, TypeError):
            return 0, 0, 0
    print(emp_df['EFF DATE'])
    print(emp_df)
    emp_df['EFF DATE'] = emp_df['EFF DATE'].astype(str).str.strip().replace(['nan', 'NaT', '0'], '')
    emp_df[['EHEF1Y', 'EHEF1M', 'EHEF1D']] = emp_df['EFF DATE'].apply(lambda x: pd.Series(split_date(x)))
    emp_df[['EHLTDY', 'EHLTDM', 'EHLTDD']] = emp_df['TERM DATE'].apply(lambda x: pd.Series(split_date(x)))

    depen_df[['DPEFDY', 'DPEFDM', 'DPEFDD']] = depen_df['EFF DATE'].apply(lambda x: pd.Series(split_date(x)))
    depen_df[['DPTDTY', 'DPTDTM', 'DPTDTD']] = depen_df['TERM DATE'].apply(lambda x: pd.Series(split_date(x)))
    depen_df[['DPDOBM', 'DPDOBD', 'DPDOBY']] = depen_df['DOB'].str.split('/', expand=True)
    depen_df[['DPDOBM', 'DPDOBD', 'DPDOBY']] = depen_df[['DPDOBM', 'DPDOBD', 'DPDOBY']].fillna('0')

    emp_df.rename(columns={
    "SSN": "EHSSN",
    "PLAN": "EHPLAN",
    "CLASS": "EHCLAS",
    "FIRST NAME": "EHFRNAME",
    "LAST NAME": "EHLSNAME"
}, inplace=True)


    depen_df.rename(columns={
        "SSN": "DPSSN",
        "DEP SSN": "DPDSSN",
        "LAST NAME": "DPLNAME",
        "SEX": "DPSEX",
        "PLAN": "DPPLAN",
        "CLASS": "DPCLAS"
    }, inplace=True)



    depen_df["DPNAME"] = depen_df["FIRST NAME"] + " " + depen_df["DPLNAME"]

    depen_df.drop(columns=["FIRST NAME"], inplace=True)

    def insert_empyhltp():
        query = "SELECT EHSSN FROM myapp_empyhltp"
        existing_ssns = pd.read_sql(query, mssql_conn)['EHSSN'].tolist()
        existing_ssns = [ssn.replace('-', '').zfill(9) for ssn in existing_ssns]
        emp_df['EHSSN'] = (
            emp_df['EHSSN']
            .astype(str)
            .str.strip()
            .str.replace('-', '')
            .dropna()
            .str.zfill(9)
        )
        emp_df['EHSSN'] = emp_df['EHSSN'].replace('', pd.NA).dropna()
        new_entries_df = emp_df[~emp_df['EHSSN'].isin(existing_ssns)]
        print('emp',new_entries_df)
        table_columns_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'myapp_empyhltp'"
        table_columns = set(pd.read_sql(table_columns_query, mssql_conn)['COLUMN_NAME'])
        filtered_df = new_entries_df[[col for col in new_entries_df.columns if col in table_columns]]

        for _, row in filtered_df.iterrows():
            placeholders = ', '.join(['?'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO myapp_empyhltp ({columns}) VALUES ({placeholders})"
            mssql_cursor.execute(sql, tuple(row.values))
        mssql_conn.commit()
        print("Data inserted successfully! empyhltp")

    def insert_depnp():
        query = "SELECT DPDSSN FROM myapp_depnp"
        existing_ssns = pd.read_sql(query, mssql_conn)['DPDSSN'].tolist()
        # Clean the existing SSNs
        existing_ssns = [ssn.replace('-', '').zfill(9) for ssn in existing_ssns]
        depen_df['DPDSSN'] = (
            depen_df['DPDSSN']
            .astype(str)
            .str.strip()
            .str.replace('-', '')
            .dropna()
            .str.zfill(9)
        )
        depen_df['DPDSSN'] = depen_df['DPDSSN'].replace('', pd.NA).dropna()
        # print(existing_ssns)
        new_entries_df = depen_df[~depen_df['DPDSSN'].isin(existing_ssns)]
        print('dep',new_entries_df)
        table_columns_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'myapp_depnp'"
        table_columns = set(pd.read_sql(table_columns_query, mssql_conn)['COLUMN_NAME'])
        filtered_df = new_entries_df[[col for col in new_entries_df.columns if col in table_columns]]

        for _, row in filtered_df.iterrows():
            placeholders = ', '.join(['?'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO myapp_depnp ({columns}) VALUES ({placeholders})"
            mssql_cursor.execute(sql, tuple(row.values))
        mssql_conn.commit()
        print("Data inserted successfully! empyhltp")

    
    insert_empyhltp()
    insert_depnp()

    mapping_dict = {
    'F1': 'SUB/DEP',
    'F2': 'LAST NAME',
    'F3': 'FIRST NAME',
    'F4': 'SSN',
    'F5': 'SEX',
    'F6': 'DOB',
    'F7': 'DEP LAST NAME',
    'F8': 'DEP FIRST NAME',
    'F9': 'DEP SSN',
    'F10': 'DEP SEX',
    'F11': 'DEP DOB',
    'F12': 'LOCAL',
    'F13': 'PLAN',
    'F14': 'CLASS',
    'F15': 'EFF DATE',
    'F16': 'TERM DATE',
    'F17': 'ID',
    'F18': 'ADDRESS 1',
    'F19': 'ADDRESS2',
    'F20': 'CITY',
    'F21': 'STATE',
    'F22': 'ZIP',
    'F23': 'PHONE',
    'F24': 'EMAIL',
    'F25': 'STATUS',
    'F26': 'TYPE',
    'F27': 'MEMBER ID',
    'F28': 'CUSTODIAL PARENT',
    'F29': 'CUSTODIAL ADDRESS 1',
    'F30': 'CUSTODIAL ADDRESS 2',
    'F31': 'CUSTODIAL CITY',
    'F32': 'CUSTODIAL STATE',
    'F33': 'CUSTODIAL ZIP',
    'F34': 'CUSTODIAL PHONE',
}

# Reverse the mapping dictionary to map values to keys
    reverse_mapping = {value: key for key, value in mapping_dict.items()}
    db2_df = cus_df
    # dirt = r'A:\OOE_BACKUP\OUTPUT'
    # filname = 'db2_checkers.xlsx'
    # pathth = os.path.join(dirt,filname)
    # db2_df.to_excel(pathth)
    term_check_db2_df = db2_df
    print("term_check_db2_df",term_check_db2_df.columns)
    print("term_check_df",term_check_df.columns)
    term_check_db2_df['EFF DATE'] = term_check_db2_df['EFF DATE'].astype(str)
    term_check_df['EFF DATE'] = term_check_df['EFF DATE'].astype(str)
    try:
        term_check_db2_df['term_date'] = term_check_db2_df['term_date'].astype(str)
    except:
        term_check_db2_df['TERM DATE'] = term_check_db2_df['TERM DATE'].astype(str)
    extra_rows = term_check_db2_df.merge(term_check_df, how='left', indicator=True).query('_merge == "left_only"')
    print(extra_rows,"yes_bro_got yes")
    print(type(extra_rows),"this is the type")
    extra_rows['TERM DATE'] = extra_rows['TERM DATE'].replace('',np.nan)
    print(extra_rows[extra_rows['TERM DATE'].notna()])
    ssn_term_date_list = extra_rows[extra_rows['TERM DATE'].notna()].apply(
    lambda row: {
        'SSN': row['SSN'] if row['SUB/DEP'] == 'Subscriber' else row['DEP SSN'],
        'TERM DATE': row['TERM DATE']
    },
    axis=1
    ).tolist()
    print(ssn_term_date_list)
    print(len(ssn_term_date_list))
    # dir  = "media/csv_files"
    # filename = "check_term_date.csv"
    # path = os.path.join(dir,filename)
    # cus_df.to_csv(path)
    try:
        db2_df.drop(columns=['EDI_DATE'],inplace=True)
    except:
        pass
    db2_df['CITY'] = db2_df['CITY'].apply(lambda x: x[:16] if isinstance(x, str) else x)
    print(db2_df.columns)
    print(len(db2_df.columns))
    print('db2 initiated')
    # Rename the columns using the reversed mapping dictionary
    db2_df.rename(columns=reverse_mapping, inplace=True)
    db2_df.fillna(' ',inplace=True)
    column_max_lengths = {
    'F1': 16,
    'F2': 16,
    'F3': 17,
    'F4': 11,
    'F5': 1,
    'F6': 10,
    'F7': 19,
    'F8': 20,
    'F9': 11,
    'F10': 16,
    'F11': 13,
    'F12': 5,
    'F13': 4,
    'F14': 5,
    'F15': 10,
    'F16': 10,
    'F17': 2,
    'F18': 30,
    'F19': 9,
    'F20': 16,
    'F21': 5,
    'F22': 5,
    'F23': 12,
    'F24': 32,
    'F25': 9,
    'F26': 10,
    'F27': 10,
    'F28': 30,
    'F29': 30,
    'F30': 6,
    'F31': 16,
    'F32': 5,
    'F33': 5,
    'F34': 11
}

    for column, max_length in column_max_lengths.items():
        if column in db2_df.columns:
            db2_df[column] = db2_df[column].apply(lambda x: str(x)[:max_length] if pd.notnull(x) else x)
    db2_total_subscribers = db2_df['F1'].str.lower().value_counts().get('subscriber', 0)
    db2_total_spouse_count = db2_df['F1'].str.lower().value_counts().get('spouse',0)
    print(type(len(db2_df)))
    # non_null_values = db2_df[db2_df['F16'].notna()]['F16']
    # print('not null  value',non_null_values.to_list())

    print(total_subscribers+total_spouse_count)

    db2_other_dependents = len(db2_df)-int((total_subscribers+total_spouse_count))
    try:
        a = 1+1
        if db2_total_subscribers-total_subscribers!=0:
            count_list = [{"excel":[{"total_subscriber":total_subscribers},{"total_spouse":total_spouse_count},{"other dependents":other_dependents}]},{"db2":[{"total_subscriber":db2_total_subscribers},{"total_spouse":db2_total_spouse_count},{"other dependents":db2_other_dependents}]}]
            print(count_list)
            emails = ['krushnarajjadhav015@gmail.com','akshay.kumar@onesmarter.com','Vikram@vikramsethi.com','dprasad@abchldg.com']
            if len(ssn_term_date_list)!= 0:
                h_body = build_success_email_count_body(count_list,ssn_term_date_list)
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                # send_success_email_count(emails,count_list,ssn_term_date_list)
                try:
                    ht_body = build_success_email_count_single_body(count_list,ssn_term_date_list)
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,ssn_term_date_list)
                except:
                    pass
            else:
                h_body = build_success_email_count_body(count_list,[])
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                try:
                    ht_body = build_success_email_count_single_body(count_list,[])
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,[])
                except:
                    pass
        elif db2_total_spouse_count - total_spouse_count !=0:
            count_list = [{"excel":[{"total_subscriber":total_subscribers},{"total_spouse":total_spouse_count},{"other dependents":other_dependents}]},{"db2":[{"total_subscriber":db2_total_subscribers},{"total_spouse":db2_total_spouse_count},{"other dependents":db2_other_dependents}]}]
            print(count_list)
            emails = ['krushnarajjadhav015@gmail.com','akshay.kumar@onesmarter.com','Vikram@vikramsethi.com','dprasad@abchldg.com']
            if len(ssn_term_date_list)!= 0:
                h_body = build_success_email_count_body(count_list,ssn_term_date_list)
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                # send_success_email_count(emails,count_list,ssn_term_date_list)
                try:
                    ht_body = build_success_email_count_single_body(count_list,ssn_term_date_list)
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,ssn_term_date_list)
                except:
                    pass
            else:
                h_body = build_success_email_count_body(count_list,[])
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                try:
                    ht_body = build_success_email_count_single_body(count_list,[])
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,[])
                except:
                    pass

        elif db2_other_dependents - db2_other_dependents !=0:
            count_list = [{"excel":[{"total_subscriber":total_subscribers},{"total_spouse":total_spouse_count},{"other dependents":other_dependents}]},{"db2":[{"total_subscriber":db2_total_subscribers},{"total_spouse":db2_total_spouse_count},{"other dependents":db2_other_dependents}]}]
            print(count_list)
            emails = ['krushnarajjadhav015@gmail.com','akshay.kumar@onesmarter.com','Vikram@vikramsethi.com','dprasad@abchldg.com']
            if len(ssn_term_date_list)!= 0:
                h_body = build_success_email_count_body(count_list,ssn_term_date_list)
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                # send_success_email_count(emails,count_list,ssn_term_date_list)
                try:
                    ht_body = build_success_email_count_single_body(count_list,ssn_term_date_list)
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,ssn_term_date_list)
                except:
                    pass
            else:
                h_body = build_success_email_count_body(count_list,[])
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                try:
                    ht_body = build_success_email_count_single_body(count_list,[])
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,[])
                except:
                    pass
        else:
            count_list = [{"excel":[{"total_subscriber":total_subscribers},{"total_spouse":total_spouse_count},{"other dependents":other_dependents}]},{"db2":[{"total_subscriber":db2_total_subscribers},{"total_spouse":db2_total_spouse_count},{"other dependents":db2_other_dependents}]}]
            print(count_list)
            emails = ['krushnarajjadhav015@gmail.com','akshay.kumar@onesmarter.com','dprasad@abchldg.com']
            if len(ssn_term_date_list)!= 0:
                h_body = build_success_email_count_body(count_list,ssn_term_date_list)
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                # send_success_email_count(emails,count_list,ssn_term_date_list)
                try:
                    ht_body = build_success_email_count_single_body(count_list,ssn_term_date_list)
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,ssn_term_date_list)
                except:
                    pass
            else:
                h_body = build_success_email_count_body(count_list,[])
                for e in emails:
                    send_email_via_fastapi(e,"Count and term data for xlsx and db2",h_body)
                try:
                    ht_body = build_success_email_count_single_body(count_list,[])
                    send_email_via_fastapi('akshay.kumar@onesmarter.com',"Count and term data for xlsx and db2",ht_body)
                    # send_success_email_count_single('akshay.kumar@onesmarter.com',count_list,[])
                except:
                    pass

    except Exception as e:

        count_list = [{"excel":[{"total_subscriber":total_subscribers},{"total_spouse":total_spouse_count},{"other dependents":other_dependents}]},{"db2":[{"total_subscriber":db2_total_subscribers},{"total_spouse":db2_total_spouse_count},{"other dependents":db2_other_dependents}]}]
        print("got the error",count_list)
        print("Email not send due to",e)
    count_list = [{"excel":[{"total_subscriber":total_subscribers},{"total_spouse":total_spouse_count},{"other dependents":other_dependents}]},{"db2":[{"total_subscriber":db2_total_subscribers},{"total_spouse":db2_total_spouse_count},{"other dependents":db2_other_dependents}]}]
    print('sort','error cauisng',count_list)
    print("wrapping up")

    db2_df = db2_df[['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11',
       'F12', 'F13', 'F14', 'F15','F16', 'F17', 'F18','F19', 'F20', 'F21',
       'F22', 'F23', 'F24', 'F25', 'F26', 'F27', 'F28', 'F29', 'F30', 'F31', 'F32',
       'F33', 'F34']]

    try:
        dir  = "media/csv_files"
        filename = "check_term_date_archane.csv"
        path = os.path.join(dir,filename)
        db2_df.to_csv(path)
    except Exception as e:
        print("dir",e)
    if db2_df['F17'].dtype == 'int64':
        db2_df = db2_df[db2_df['F17'] > 0]
    
    elif db2_df['F17'].dtype == 'object':  
        db2_df['F17'] = db2_df['F17'].apply(lambda x: x if str(x).isdigit() else str(x).replace(".", "").strip() if str(x).replace(".", "").isdigit() else None)
        db2_df = db2_df.dropna(subset=['F17'])
    db2_df['F16'] = pd.to_datetime(db2_df['F16'], errors='coerce').dt.strftime('%m/%d/%Y')
    print("db2 columns",db2_df.columns)
    
    db2_df['F23'] = db2_df['F23'].replace("None", "")
    updated_rows_db2 = db2_df[db2_df['F9'].isin(new_ssns_apr)]
    print(updated_rows_db2[['F9', 'F15']])
    db2_df['F15'] = pd.to_datetime(db2_df['F15'], errors='coerce')
    updated_rows_db2 = db2_df[db2_df['F4'].isin(new_ssns_apr)]
    print(updated_rows_db2[['F4', 'F15']])
    db2_df['F15'] = db2_df['F15'].dt.strftime('%m/%d/%Y').fillna('01/01/2025')
    db2_df.loc[
        db2_df['F4'].isin(new_ssns_apr) &
        db2_df['F4'].notna() &
        (db2_df['F9'].isna() | db2_df['F9'].astype(str).str.strip().eq('')),
        'F15'
    ] = first_day

    db2_df.loc[db2_df['F9'].isin(new_ssns_apr), 'F15'] = first_day
    updated_rows_db2 = db2_df[db2_df['F4'].isin(new_ssns_apr)]
    print(updated_rows_db2[['F4', 'F15']])
    db2_df.fillna('',inplace=True)
    if 'TERM_DATE' in db2_df.columns:
        db2_df.drop(columns=['TERM_DATE'],inplace=True)
        print('gggfgf',db2_df.columns)
    elif 'TERM DATE' in db2_df.columns:
        db2_df.drop(columns=['TERM DATE'],inplace=True)
        print("bnbnb",db2_df.columns)
    ddf_dict = db2_df.to_dict(orient='records')
    print(ddf_dict[0])
    def write_filename_to_buffer(filename, buffer_file):
        data = {'filename': filename}
        with open(buffer_file, 'w') as file:
            json.dump(data, file)
        print(f"Filename '{filename}' written to {buffer_file}.")
    write_filename_to_buffer(file_name,'new_file_buffer.json')
    cutoff = datetime.strptime('01/01/2025', '%m/%d/%Y')

    for record in ddf_dict:
        date_str = record.get('F15')
        if date_str:
            try:
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                if date_obj < cutoff:
                    record['F15'] = '01/01/2025'
            except ValueError:
                print(f"Invalid date format: {date_str}")

    def write_to_queue(data, queue_file):
        try:
            with open(queue_file, "ab") as file:
                pickle.dump(data, file)
            print("Data added to the db2 queue.")
        except Exception as e:
            print(f"Error writing to the queue: {e}")
    write_to_queue(ddf_dict, "db2_queue.pkl")
    with open('output_db_dict_new.txt', 'w') as file:
        json.dump(ddf_dict, file, indent=4)

    print("Data written successfully!")
    return output_csv_path,edi_excel_path

def parse_custodial_data(csv_data):
    new_df = pd.DataFrame(csv_data)
    # dam = 'A:\OOE_BACKUP\OUTPUT'
    # fer = "original_custurd.xlsx"
    # par = os.path.join(dam,fer)
    # new_df.to_excel(par)
    new_df['CUSTODIAL ADDRESS 1'] = ''
    new_df['CUSTODIAL ADDRESS 2'] = ''
    new_df['CUSTODIAL CITY'] = ''
    new_df['CUSTODIAL STATE'] = ''
    new_df['CUSTODIAL ZIP'] = ''
    new_df['CUSTODIAL PHONE'] = ''
    if 'ID' in new_df.columns:
        new_df['ID'] = pd.to_numeric(new_df['ID'], errors='coerce')
        condition = new_df['ID'] == 15
    else:
        new_df['id_field'] = pd.to_numeric(new_df['id_field'], errors='coerce')
        condition = new_df['id_field'] == 15
    new_df.fillna('', inplace=True)

    if 'ADDRESS 1' in new_df.columns:
        new_df.loc[condition, 'CUSTODIAL ADDRESS 1'] = new_df.loc[condition, 'ADDRESS 1']
    elif 'address1' in new_df.columns:
        new_df.loc[condition, 'custodial_address_1'] = new_df.loc[condition,'address1']
    if 'ADDRESS 2' in new_df.columns:
        new_df.loc[condition, 'CUSTODIAL ADDRESS 2'] = new_df.loc[condition, 'ADDRESS 2']
    elif 'address2' in new_df.columns:
        new_df.loc[condition, 'custodial_address_2'] = new_df.loc[condition,'address2']
    if 'CITY' in new_df.columns:
        new_df.loc[condition, 'CUSTODIAL CITY'] = new_df.loc[condition, 'CITY']
    elif 'city' in new_df.columns:
        new_df.loc[condition, 'custodial_city'] = new_df.loc[condition,'city']
    if 'STATE' in new_df.columns:
        new_df.loc[condition, 'CUSTODIAL STATE'] = new_df.loc[condition, 'STATE']
    elif 'state' in new_df.columns:
        new_df.loc[condition, 'custodial_state'] = new_df.loc[condition,'state']
    if 'ZIP' in new_df.columns:
        new_df.loc[condition, 'CUSTODIAL ZIP'] = new_df.loc[condition, 'ZIP']
    elif 'zip' in new_df.columns:
        new_df.loc[condition, 'custodial_zip'] = new_df.loc[condition,'zip']
    if 'PHONE' in new_df.columns:
        new_df.loc[condition, 'CUSTODIAL PHONE'] = new_df.loc[condition, 'PHONE']
    elif 'phone' in new_df.columns:
        new_df.loc[condition, 'custodial_phone'] = new_df.loc[condition,'phone']
    
    sdf = new_df
    sdf_data = sdf.to_dict(orient="records")
    previous_custodial_parent = None
    custodial_parent_column = [row.get("CUSTODIAL PARENT", None) for row in sdf_data]
    shifted_custodial_parent_column = [None] + custodial_parent_column[:-1]

    # for row, shifted_value in zip(sdf_data, shifted_custodial_parent_column):
    #     row["CUSTODIAL PARENT"] = shifted_value

    for row in sdf_data:
        if row["SUB/DEP"] != "Subscriber":
                if not row.get("CUSTODIAL ADDRESS 1") or row.get("DEP ADDRESS"):
                        if row.get("DEP ADDRESS"):
                            row["CUSTODIAL ADDRESS 1"] = row["DEP ADDRESS"]
                        elif row.get("ADDRESS 1"):
                            row["CUSTODIAL ADDRESS 1"] = row["ADDRESS 1"]
                if not row.get("CUSTODIAL ZIP") or row.get("DEP ZIP"):
                        if row.get("DEP ZIP"):
                            row["CUSTODIAL ZIP"] = row["DEP ZIP"]
                        elif row.get("ZIP"):
                            row["CUSTODIAL ZIP"] = row["ZIP"]


                if not row.get("CUSTODIAL CITY") or row.get("DEP CITY"): 
                        if row.get("DEP CITY"):
                            row["CUSTODIAL CITY"] = row["DEP CITY"]
                        elif row.get("CITY"):
                            row["CUSTODIAL CITY"] = row["CITY"]


                if not row.get("CUSTODIAL STATE"):
                        if row.get("DEP STATE"):
                            row["CUSTODIAL STATE"] = row["DEP STATE"]
                        elif row.get("STATE"):
                            row["CUSTODIAL STATE"] = row["STATE"]

    sdf = pd.DataFrame(sdf_data)
    desired_order = [
        "SUB/DEP", "LAST NAME", "FIRST NAME", "SSN", "TEMP SSN", "SEX", "DOB",
        "DEP LAST NAME", "DEP FIRST NAME", "DEP SSN", "DEP SEX", "DEP DOB",
        "CUSTODIAL PARENT", "LOCAL", "PLAN", "CLASS", "EFF DATE", "TERM DATE",
        "ID", "ADDRESS 1", "ADDRESS 2", "CITY", "STATE", "ZIP", "PHONE", 
        "EMAIL", "STATUS", "TYPE", "MEMBER ID"
    ]

    existing_columns = sdf.columns.tolist()
    columns_in_order = [col for col in desired_order if col in existing_columns]
    other_columns = [col for col in existing_columns if col not in desired_order]


    final_column_order = columns_in_order + other_columns

    sdf = sdf[final_column_order]
    sdf.drop(columns=['DEP ADDRESS','DEP ZIP','DEP STATE','DEP CITY'],inplace=True)
    output_folder = "media/csv_files/"
    fil  = "new_custodial_report_EDI_834_12-24-2024.xlsx"
    path = os.path.join(output_folder,fil)
    return sdf

def calculate_count_variance(sub_count,dep_count,input_file):
    current_date = datetime.now()
    previous_business_day = current_date - timedelta(days=1)
    while previous_business_day.weekday() > 4: 
        previous_business_day -= timedelta(days=1)

    current_date_str = current_date.strftime("%Y-%m-%d")
    previous_date_str = previous_business_day.strftime("%Y-%m-%d")

    print("Today is:", current_date.strftime("%A, %Y-%m-%d"))
    print("Previous business day:", previous_business_day.strftime("%A, %Y-%m-%d"))

    connection = sqlite3.connect('db.sqlite3')
    cursor = connection.cursor()

    fetch_query = """
    SELECT subscriber_count, dependent_count
    FROM myapp_count_model
    WHERE date = ?;
    """

    today_counts = True

    cursor.execute(fetch_query, (previous_date_str,))
    previous_counts = cursor.fetchone()

    connection.close()

    if today_counts and previous_counts:
        today_subscribers, today_dependents = sub_count,dep_count
        prev_subscribers, prev_dependents = map(int, previous_counts)

        sub_variance = abs(today_subscribers - prev_subscribers) / prev_subscribers * 100
        dep_variance = abs(today_dependents - prev_dependents) / prev_dependents * 100

        email = 'akshay.kumar@onesmarter.com'
        if sub_variance > 5:  
            pass
            # send_error_email_variance(email,input_file,"Subscriber Count")
        else:
            pass

        if dep_variance > 5:
            pass
        #    send_error_email_variance(email,input_file,"Dependent Count")
        else:
            pass
    else:
        print("Data for one or both dates is missing!")


# print('done')
# output_folder = "media/csv_files/"
# J=output_folder
# archive_folder = "media/archive/"
# os.makedirs(output_folder, exist_ok=True)
# os.makedirs(archive_folder, exist_ok=True)

# input_file_path = r"C:/Users/91942/Downloads/EDI_834_12-24-2024.X12"
# output_file_path= parse_edi_to_csv(input_file_path, output_folder,J)


