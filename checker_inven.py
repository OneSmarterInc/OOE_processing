import sqlite3
import pandas as pd

# # Database and table details
# db_path = "db.sqlite3"
# table_name = "myapp_inventory_table_data"
# excel_path = "inventory_data_uPDATE_till_21.xlsx"

# # Connect to SQLite database
# conn = sqlite3.connect(db_path)

# # Read data from the table
# df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

# # Save to Excel
# df.to_excel(excel_path, index=False, engine="openpyxl")

# # Close connection
# conn.close()

# print(f"Data exported successfully to {excel_path}")

import sqlite3
import pandas as pd

db_path = r"db.sqlite3"
table_name = "myapp_inventory_table_data"
excel_path = r"S:\OOE\EDI_PROJECT-\EDI-Backend\export8june.xlsx"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

df = pd.read_excel(excel_path, engine="openpyxl")

columns = ", ".join(df.columns)
placeholders = ", ".join(["?" for _ in df.columns])
insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

try:
    cursor.executemany(insert_query, df.values.tolist())
    conn.commit()
    print("Data imported successfully into SQLite table.")
except Exception as e:
    print("Error inserting data:", e)
finally:
    conn.close()


# import pandas as pd
# import os 
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
# from logging_setup import logger

# def send_addttional_to_db2(cus_df):
#     cus_df['local'] = ''
#     cus_df['email'] = ''
#     mapping_dict = {
#     'F1': 'sub_dep',
#     'F2': 'last_name',
#     'F3': 'first_name',
#     'F4': 'ssn',
#     'F5': 'sex',
#     'F6': 'dob',
#     'F7': 'dep_last_name',
#     'F8': 'dep_first_name',
#     'F9': 'dep_ssn',
#     'F10': 'dep_sex',
#     'F11': 'dep_dob',
#     'F12': 'local',  
#     'F13': 'plan',
#     'F14': 'class_field',
#     'F15': 'eff_date',
#     'F16': 'term_date',
#     'F17': 'id_field',
#     'F18': 'address1',
#     'F19': 'address2',
#     'F20': 'city',
#     'F21': 'state',
#     'F22': 'zip',
#     'F23': 'phone',
#     'F24': 'email',  
#     'F25': 'status',
#     'F26': 'type',
#     'F27': 'member_id',
#     'F28': 'custodial_parent',
#     'F29': 'custodial_address1',
#     'F30': 'custodial_address2',
#     'F31': 'custodial_city',
#     'F32': 'custodial_state',
#     'F33': 'custodial_zip',
#     'F34': 'custodial_phone'
# }

#     reverse_mapping = {value: key for key, value in mapping_dict.items()}
#     db2_df = cus_df

#     try:
#         db2_df.drop(columns=['edi_date'],inplace=True)
#     except:
#         pass
#     db2_df['city'] = db2_df['city'].apply(lambda x: x[:16] if isinstance(x, str) else x)
#     db2_df.rename(columns=reverse_mapping, inplace=True)
#     db2_df.fillna(' ',inplace=True)
#     column_max_lengths = {
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

#     for column, max_length in column_max_lengths.items():
#         if column in db2_df.columns:
#             db2_df[column] = db2_df[column].apply(lambda x: str(x)[:max_length] if pd.notnull(x) else x)
#     print("wrapping up")
#     db2_df = db2_df[['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11',
#        'F12', 'F13', 'F14', 'F15','F16', 'F17', 'F18','F19', 'F20', 'F21',
#        'F22', 'F23', 'F24', 'F25', 'F26', 'F27', 'F28', 'F29', 'F30', 'F31', 'F32',
#        'F33', 'F34']]
#     return db2_df


# host = '104.153.122.227'
# port = '23'
# database = 'S78F13CW'
# user = 'ONEPYTHON'
# password = 'pa33word'

# connection_string = (
#     f"DRIVER={{iSeries Access ODBC Driver}};"
#     f"SYSTEM={host};"
#     f"PORT={port};"
#     f"DATABASE={database};"
#     f"UID={user};"
#     f"PWD={password};"
#     f"PROTOCOL=TCPIP;"
# )

# # Schema and table details
# schema_name = 'OOEDF'
# table_name = 'OOEFLOAD'

# def insert_data_to_DB2(ddf_dict, batch_size=1000):
#     try:
#         connection = pyodbc.connect(connection_string)
#         print("Connected to the database.")
#         cursor = connection.cursor()

#         stripped_ddf_dict = [
#             {key: (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
#             for record in ddf_dict
#         ]

        
    
#         if stripped_ddf_dict:
#             # Use keys from the first record for column names
#             columns = ', '.join(stripped_ddf_dict[0].keys())
#             placeholders = ', '.join(['?'] * len(stripped_ddf_dict[0]))
#             insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders}) WITH NC"

#             # Insert data in batches
#             for i in range(0, len(stripped_ddf_dict), batch_size):
#                 batch = stripped_ddf_dict[i:i + batch_size]
#                 data_to_insert = [tuple(record.values()) for record in batch]
#                 cursor.executemany(insert_query, data_to_insert)
#                 connection.commit()  # Commit after each batch

#         print("Data inserted successfully.")

#     except Exception as e:
#         print(f"An error occurred: {e}")

#     finally:
#         if connection:
#             connection.close()
#             print("Connection closed.")



# cus_df = pd.read_excel(r"S:\OOE\filtered_results_updated.xlsx")
# db2_ddf = send_addttional_to_db2(cus_df)
# db2_ddf_dict = db2_ddf.to_dict(orient='records')
# insert_data_to_DB2(db2_ddf_dict)


# import pandas as pd
# import pyodbc
# import os

# # ✅ Step 1: Read Excel File
# excel_path = r"S:\OOE\EDI_PROJECT-\EDI-Backend\inventory_data.xlsx"
# df = pd.read_excel(excel_path, engine="openpyxl")
# df = df.fillna('').astype(str)  # Replace None with empty string & Convert all to str

# # ✅ Strip Extra Spaces
# df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# # ✅ Step 2: Save as CSV for Bulk Insert
# csv_file_path = r"C:\Users\abc-admin\Desktop\bulk_data.csv"
# df.to_csv(csv_file_path, index=False, header=False)  # No headers, as SQL table already has columns

# # ✅ Step 3: Define SQL Server Connection
# server = "ABCCOLUMBUSSQL2"
# database = "EDIDATABASE"
# username = "sa"
# password = "ChangeMe#2024"

# conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
# conn = pyodbc.connect(conn_str)
# cursor = conn.cursor()

# # ✅ Step 4: Bulk Insert Query
# bulk_insert_query = f"""
# BULK INSERT myapp_mssql_inventory_table_data
# FROM '{csv_file_path}'
# WITH (
#     FIELDTERMINATOR = ',',
#     ROWTERMINATOR = '\\n',
#     FIRSTROW = 1
# )
# """

# # ✅ Execute BULK INSERT
# cursor.execute(bulk_insert_query)
# conn.commit()
# conn.close()

# # ✅ Step 5: Cleanup CSV File (Optional)
# os.remove(csv_file_path)

# print("✅ Bulk Insert Completed Successfully!")
