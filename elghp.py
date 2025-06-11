import csv
import os
import pyodbc
import smtplib
import pandas as pd,re
import random,string
import json
from datetime import datetime, timedelta
import time
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


csv_headers = [
    "SUB/DEP", "LAST NAME", "FIRST NAME", "SSN","TEMP SSN","SEX", "DOB", "DEP LAST NAME", "DEP FIRST NAME",
    "DEP SSN", "DEP SEX", "DEP DOB","CUSTODIAL PARENT","LOCAL", "PLAN", "CLASS", "EFF DATE", "TERM DATE", "ID",
    "ADDRESS 1", "ADDRESS 2", "CITY", "STATE", "ZIP", "PHONE", "EMAIL", "STATUS", "TYPE","MEMBER ID","DEP ADDRESS","DEP CITY","DEP STATE","DEP ZIP"
]


def parse_edi_to_csv_for_sql_server(input_file_path):
    file_name = input_file_path.split("/")[-1]
    match = re.search(r'(\d{4})(\d{2})(\d{2})', file_name)
    if match:
        year, month, day = match.groups()
        formatted_date = f"{month}/{day}/{year}"
        print(formatted_date)
    else:
        formatted_date = 'NA'  # Extracts 'EDI_834_11-15-2024_3KXST5r.X12'
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
    conn.close()
    csv_data.append(current_subscriber)
    csv_data.extend(dependents)
    input_filename = os.path.splitext(os.path.basename(input_file_path))[0]
    output_csv_path = 'C:'
    output_xlsx_path = os.path.join(out_dir, f"{input_filename}.xlsx")
    system_csv_path = 'C:'
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
        

    
    return csv_data,formatted_date