# import sqlite3

# # Connect to SQLite database (or create it if it doesn't exist)

# import json
# import time
# import pyodbc
# import pickle
# import smtplib
# import os
# from multiprocessing import Process
# from email.mime.base import MIMEBase
# from email import encoders
# from io import BytesIO
# from email.mime.text import MIMEText
# import openpyxl
# from openpyxl import Workbook
# from email.mime.multipart import MIMEMultipart 
# from email.mime.application import MIMEApplication

# def read_from_queue(queue_file):

#     if not os.path.exists(queue_file):
#         return None

#     try:
#         with open(queue_file, "rb") as file:
#             data_list = []
#             while True:
#                 try:
#                     data_list.append(pickle.load(file))
#                 except EOFError:
#                     break

#         if not data_list:
#             return None

#         with open(queue_file, "wb") as file:
#             for data in data_list[1:]:
#                 pickle.dump(data, file)

#         return data_list[0]

#     except Exception as e:
#         print(f"Error reading from the queue: {e}")
#         return None
    
# smtp_config = {
#     'host': 'mail.privateemail.com',
#     'port': 465,
#     'user': 'support@disruptionsim.com',
#     'password': 'Onesmarter@2023'
# }

# reciver_email = 'krishnarajjadhav2003@gmail.com'

# def send_success_email(email,filename):
    
                        
#     server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
#     server.login(smtp_config['user'], smtp_config['password'])

#     msg = MIMEMultipart()
#     msg['From'] = smtp_config['user']
#     msg['To'] = email
#     msg['Subject'] = f"Insertion Successful to Database"

#     body = f"""
#     <p>Database Dajango Insertion for file {"input_file_name"}.834 was processed successfully.</p>
#     """
#     msg.attach(MIMEText(body, 'html'))
    

#     server.send_message(msg, from_addr=smtp_config['user'], to_addrs=email)
#     server.quit()
#     print(f"Success email sent for  to {email}")



# def insert_data_to_django_db(data_dict):

#     conn = sqlite3.connect("db.sqlite3")
#     cursor = conn.cursor()


#     key_to_column = {
#         'LAST NAME': 'last_name',
#         'FIRST NAME': 'first_name',
#         'SSN': 'ssn',
#         'SUB/DEP': 'sub_dep',
#         'STATUS': 'status',
#         'TYPE': 'type',
#         'PHONE': 'phone',
#         'ADDRESS 1': 'address1',
#         'CITY': 'city',
#         'STATE': 'state',
#         'ZIP': 'zip',
#         'DOB': 'dob',
#         'SEX': 'sex',
#         'PLAN': 'plan',
#         'CLASS': 'class_field',
#         'EFF DATE': 'eff_date',
#         'ID': 'id_field',
#         'DEP FIRST NAME': 'dep_first_name',
#         'DEP LAST NAME': 'dep_last_name',
#         'DEP DOB': 'dep_dob',
#         'DEP SSN': 'dep_ssn',
#         'DEP SEX': 'dep_sex',
#         'CUSTODIAL PARENT': 'custodial_parent',
#         'CUSTODIAL ADDRESS 1': 'custodial_address1',
#         'CUSTODIAL ADDRESS 2': 'custodial_address2',
#         'CUSTODIAL CITY': 'custodial_city',
#         'CUSTODIAL STATE': 'custodial_state',
#         'CUSTODIAL ZIP': 'custodial_zip',
#         'CUSTODIAL PHONE': 'custodial_phone',
#         'ADDRESS2': 'address2',
#         'MEMBER ID': 'member_id',
#         'EDI_DATE': 'date_edi',
#         'EMAIL': 'temp_ssn',
#         'filename':"filename"
#     }

#     # Columns for the database
#     db_columns = list(key_to_column.values())

#     # Convert list of dictionaries into list of tuples using the mapping
#     values = [
#         tuple(record.get(key, '').strip() if isinstance(record.get(key), str) else record.get(key)
#             for key in key_to_column.keys())
#         for record in data_dict
#     ]

#     # Define SQL insertion query
#     placeholders = ", ".join(["?"] * len(db_columns))  # Create placeholders for values
#     insert_query = f"""
#     INSERT INTO myapp_edi_user_data ({', '.join(db_columns)})
#     VALUES ({placeholders})
#     """

#     # Perform bulk insertion
#     cursor.executemany(insert_query, values)

#     # Commit changes and close the connection
#     conn.commit()
#     conn.close()

#     print("Bulk data inserted successfully!")

# if __name__ == "__main__":
#     queue_file = "django_queue_file.pkl"

#     while True:
#         data = read_from_queue(queue_file)
#         if data:
#             print('Got the data')
#             process = Process(target=insert_data_to_django_db, args=(data,))
#             process.start()
#             process.join()
#             send_success_email(reciver_email,"input_file_name")
#             print('django insertion completed')
#         else:
#             print("Queue is empty. Waiting for data...")
#             time.sleep(3)


import pandas as pd
import pyodbc
from tqdm import tqdm
from sqlalchemy import create_engine

# Step 1: Load Excel data
excel_file_path = r'S:\\OOE\\EDI_PROJECT-\\EDI-Backend\\export2_april_8.xlsx'  # Replace with your actual file path
df = pd.read_excel(excel_file_path)
df = df.astype(str)
# Step 2: Connect to SQL Server
# conn = pyodbc.connect( 
#     'DRIVER={ODBC Driver 17 for SQL Server};'
#     'SERVER=ABCCOLUMBUSSQL2;'
#     'DATABASE=EDIDATABASE;'
#     'UID=sa;'
#     'PWD=ChangeMe#2024;'
# )
# cursor = conn.cursor()
# cursor.fast_executemany = True  # Enable fast bulk insert

# # Step 3: Prepare data
# data = [tuple(x) for x in df.to_numpy()]
# columns = ', '.join(f'[{col}]' for col in df.columns)
# placeholders = ', '.join(['?'] * len(df.columns))
# query = f"INSERT INTO myapp_mssql_inventory_table_data ({columns}) VALUES ({placeholders})"

# # Step 5: Progress bar with chunked insert
# chunk_size = 1000
# for i in tqdm(range(0, len(data), chunk_size), desc="Inserting data"):
#     chunk = data[i:i+chunk_size]
#     cursor.executemany(query, chunk)
#     conn.commit()


# # Step 5: Clean up
# cursor.close()
# conn.close()
# print("✅ Bulk insert completed successfully.")

engine = create_engine(
    'mssql+pyodbc://sa:ChangeMe#2024@ABCCOLUMBUSSQL2/EDIDATABASE?driver=ODBC+Driver+17+for+SQL+Server'
)

# Step 4: Bulk insert using to_sql
num_cols = len(df.columns)
safe_chunk_size = 2000 // num_cols  # Keep below 2100 param marker limit

# Step 5: Insert with progress bar
for start in tqdm(range(0, len(df), safe_chunk_size), desc="🚀 Inserting data in chunks"):
    chunk = df.iloc[start:start+safe_chunk_size]
    chunk.to_sql(
        'myapp_mssql_inventory_table_data',
        con=engine,
        if_exists='append',
        index=False,
        method=None
    )
print("✅ Data inserted successfully using SQLAlchemy.")

print("✅ Data inserted successfully using SQLAlchemy.")