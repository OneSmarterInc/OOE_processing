import json
import time
import pyodbc
import pickle
import smtplib
import os
from multiprocessing import Process
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO
from email.mime.text import MIMEText
import openpyxl
from openpyxl import Workbook
from email.mime.multipart import MIMEMultipart 
from email.mime.application import MIMEApplication
from logging_setup import logger


def read_from_queue(queue_file):

    if not os.path.exists(queue_file):
        return None

    try:
        with open(queue_file, "rb") as file:
            data_list = []
            while True:
                try:
                    data_list.append(pickle.load(file))
                except EOFError:
                    break

        if not data_list:
            return None

        with open(queue_file, "wb") as file:
            for data in data_list[1:]:
                pickle.dump(data, file)

        return data_list[0]

    except Exception as e:
        print(f"Error reading from the queue: {e}")
        return None
    

# Connection details
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

# Schema and table details
schema_name = 'OOEDF'
table_name = 'OOEFLOAD'

def insert_data_to_DB2(ddf_dict, batch_size=1000):
    try:
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Remove whitespace from string values in the dictionary
        stripped_ddf_dict = [
            {key: (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            for record in ddf_dict
        ]

        # Delete existing records in the table (optional, based on your requirements)
        delete_query = f"DELETE FROM {schema_name}.{table_name} WITH NC"
        cursor.execute(delete_query)

        if stripped_ddf_dict:
            # Use keys from the first record for column names
            columns = ', '.join(stripped_ddf_dict[0].keys())
            placeholders = ', '.join(['?'] * len(stripped_ddf_dict[0]))
            insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders}) WITH NC"

            # Insert data in batches
            for i in range(0, len(stripped_ddf_dict), batch_size):
                batch = stripped_ddf_dict[i:i + batch_size]
                data_to_insert = [tuple(record.values()) for record in batch]
                cursor.executemany(insert_query, data_to_insert)
                connection.commit()  # Commit after each batch

        print("Data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if connection:
            connection.close()
            print("Connection closed.")


smtp_config = {
    'host': 'mail.privateemail.com',
    'port': 465,
    'user': 'support@disruptionsim.com',
    'password': 'Onesmarter@2023'
}

reciver_emails = ['akshay.kumar@onesmarter.com','Vikram@vikramsethi.com','dprasad@abchldg.com',"krushnarajjadhav015@gmail.com"]

def read_filename_from_buffer(buffer_file='new_file_buffer.json'):
    with open(buffer_file, 'r') as file:
        data = json.load(file)
    filename = data.get('filename')
    print(f"Filename '{filename}' read from {buffer_file}.")
    return filename

def delete_buffer_file(buffer_file='new_file_buffer.json'):
    if os.path.exists(buffer_file):
        os.remove(buffer_file)
        print(f"{buffer_file} has been deleted.")
    else:
        print(f"{buffer_file} does not exist.")


def send_success_email(emails):
    file = read_filename_from_buffer()
    delete_buffer_file()
    
    server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
    server.login(smtp_config['user'], smtp_config['password'])

    msg = MIMEMultipart()
    msg['From'] = smtp_config['user']
    msg['Subject'] = f"Insertion Successful to Database DB2"

    body = f"""
    <p>Database Insertion for file {file}.834 was processed successfully.</p>
    """
    msg.attach(MIMEText(body, 'html'))
    
    for email in emails:
        msg['To'] = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
        print(f"Success email sent to {email}.")


def build_success_email_body_db2():
    file = read_filename_from_buffer()
    delete_buffer_file()
    body = f"""
    <p>Database Insertion for file {file}.834 was processed successfully.</p>
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

from datetime import datetime

now = datetime.now()
cutoff = now.replace(hour=15, minute=45, second=0, microsecond=0)


if __name__ == "__main__":
    queue_file = "db2_queue.pkl"

    while True:
        # data = read_from_queue(queue_file)
        if True:
            print('Got the data')
            with open(r"S:\OOE\output_db_dict_new.txt", "r") as file:
                    data = json.load(file)
            try:
                logger.info("Insertion started to DB2")
                process = Process(target=insert_data_to_DB2, args=(data,))
                process.start()
                process.join()
            except Exception as e:
                print(e)
                break
            # send_success_email(reciver_emails)
            em_body = build_success_email_body_db2()
            for email in reciver_emails:
                send_email_via_fastapi(email,"Insertion Successful to Database DB2",em_body)
            logger.info("Insertion Completed to DB2")
            print('insertion completed')
        else:
            print("Queue is Empty")
            time.sleep(3)

# import pandas as pd

# df = pd.read_excel(r"S:\OOE\filtered11.xlsx")
# print(df.columns)
# mapping_dict = {
#     'F1': 'SUB/DEP',
#     'F2': 'LAST NAME',
#     'F3': 'FIRST NAME',
#     'F4': 'SSN',
#     'F5': 'SEX',
#     'F6': 'DOB',
#     'F7': 'DEP LAST NAME',
#     'F8': 'DEP FIRST NAME',
#     'F9': 'DEP SSN',
#     'F10': 'DEP SEX',
#     'F11': 'DEP DOB',
#     'F12': 'LOCAL',
#     'F13': 'PLAN',
#     'F14': 'CLASS',
#     'F15': 'EFF DATE',
#     'F16': 'TERM_DATE',
#     'F17': 'ID',
#     'F18': 'ADDRESS 1',
#     'F19': 'ADDRESS2',
#     'F20': 'CITY',
#     'F21': 'STATE',
#     'F22': 'ZIP',
#     'F23': 'PHONE',
#     'F24': 'EMAIL',
#     'F25': 'STATUS',
#     'F26': 'TYPE',
#     'F27': 'MEMBER ID',
#     'F28': 'CUSTODIAL PARENT',
#     'F29': 'CUSTODIAL ADDRESS 1',
#     'F30': 'CUSTODIAL ADDRESS 2',
#     'F31': 'CUSTODIAL CITY',
#     'F32': 'CUSTODIAL STATE',
#     'F33': 'CUSTODIAL ZIP',
#     'F34': 'CUSTODIAL PHONE',
# }

# reverse_mapping_dict = {v: k for k, v in mapping_dict.items()}

# # Rename columns using the reversed dictionary
# df = df.rename(columns=reverse_mapping_dict)
# ordered_columns = [f"F{i}" for i in range(1, 35)]
# df = df[ordered_columns] 
# df.fillna('',inplace=True)
# column_max_lengths = {
#     'F1': 16,
#     'F2': 16,
#     'F3': 17,
#     'F4': 11,
#     'F5': 1,
#     'F6': 10,
#     'F7': 19,
#     'F8': 20,
#     'F9': 11,
#     'F10': 16,
#     'F11': 13,
#     'F12': 5,
#     'F13': 4,
#     'F14': 5,
#     'F15': 10,
#     'F16': 10,
#     'F17': 2,
#     'F18': 30,
#     'F19': 9,
#     'F20': 16,
#     'F21': 5,
#     'F22': 5,
#     'F23': 12,
#     'F24': 32,
#     'F25': 9,
#     'F26': 10,
#     'F27': 10,
#     'F28': 30,
#     'F29': 30,
#     'F30': 6,
#     'F31': 16,
#     'F32': 5,
#     'F33': 5,
#     'F34': 11
# }

# for column, max_length in column_max_lengths.items():
#     if column in df.columns:
#             df[column] = df[column].apply(lambda x: str(x)[:max_length] if pd.notnull(x) else x)
# # df.replace('cQNAN', '', inplace=True)
# # # df['F11'] = pd.to_numeric(df['F11'], errors='coerce').fillna('')

# # # df = df.astype(str).replace('NaT', '')
# # df['F11'] = df['F11'].astype(str).replace('NaT', '')
# # df['F6'] = pd.to_datetime(df['F6']).dt.strftime('%m/%d/%Y')
# # df['F15'] = pd.to_datetime(df['F15']).dt.strftime('%m/%d/%Y')
# # df['F24'] = ''
# # df['F11'] =  ''
# # Print the updated column names to verify
# df['F23'] = df['F23'].astype(str).replace('.','')
# df['F34'] = df['F34'].astype(str).replace('.','')
# df['F13'] = df['F13'].astype(str)
# df['F23'] = df['F23'].astype(str).str[:10]
# df['F34'] = df['F34'].astype(str).str[:10]



# print(df.columns)
# df_dict = df.to_dict(orient='records')
# # print(df_dict[0])
# # print(df_dict[1])
# # print(df_dict[2])
# # print(df_dict[3])

# insert_data_to_DB2(df_dict)