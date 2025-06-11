import sqlite3
import pyodbc
from datetime import datetime

# DB2 connection
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

try:
    db2_connection = pyodbc.connect(connection_string)
    print("Connected to DB2 database.")
    db2_cursor = db2_connection.cursor()
except Exception as e:
    print("DB2 connection error:", e)
    exit()

# SQLite connection

import pandas as pd
# Fetch records

df = pd.read_excel(r"S:\OOE\DEp.xlsx")
ssn_list = df['SSN'].dropna().astype(str).str.replace('-', '', regex=False).tolist()
print("First 10 records:")

import pandas as pd
dep_ssn = True
data = []
track = 0
# Process each row
for temp_ssn in ssn_list:
    mark_ssn = temp_ssn
    temp_ssn = str(temp_ssn)
    temp_ssn = temp_ssn.replace('-','')
    # print(f"\nProcessing SSN: {temp_ssn}")

    latest_eff_date = None
    latest_term_date = None
    if temp_ssn:
        if dep_ssn:
            # Effective date (Status A)
            print(track)
            db2_cursor.execute("""
                SELECT DPEFDM, DPEFDD, DPEFDY, DPTDTY, DPTDTM, DPTDTD,DPSTAT,DPDSSN
                FROM ooedf.depnp
                WHERE DPDSSN = ? 
            """, (temp_ssn,))
            rows =  db2_cursor.fetchall()
            for row in rows:
                try:
                    eff_date = datetime(int(row.DPEFDY), int(row.DPEFDM), int(row.DPEFDD)) if all([row.DPEFDY, row.DPEFDM, row.DPEFDD]) else None
                    term_date = datetime(int(row.DPTDTY), int(row.DPTDTM), int(row.DPTDTD)) if all([row.DPTDTY, row.DPTDTM, row.DPTDTD]) else None

                    data.append({
                        'SSN': row.DPDSSN,
                        'Effective Date': eff_date.strftime('%m/%d/%Y') if eff_date else '',
                        'Termination Date': term_date.strftime('%m/%d/%Y') if term_date else '',
                        'Health Code (EHHLTH)': row.DPSTAT
                    })

                    # Update latest dates
                    if eff_date and (not latest_eff_date or eff_date > latest_eff_date):
                        latest_eff_date = eff_date
                    if term_date and (not latest_term_date or term_date > latest_term_date):
                        latest_term_date = term_date

                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue
            track += 1

            # Termination date (Status D)
            # db2_cursor.execute("""
            #     SELECT DPTDTY, DPTDTM, DPTDTD
            #     FROM ooedf.depnp
            #     WHERE DPDSSN = ? AND DPSTAT = 'D'
            # """, (temp_ssn,))
            # for row in db2_cursor.fetchall():
            #     try:
            #         date_obj = datetime(int(row.DPTDTY), int(row.DPTDTM), int(row.DPTDTD))
            #         print(temp_ssn,"jacks2",date_obj)
            #         if not latest_term_date or date_obj > latest_term_date:
            #             latest_term_date = date_obj
            #     except Exception as e:
            #         print(e)
            #         continue
        else:
            # Effective date (EHHLTH = A)
            print(track)
            db2_cursor.execute("""
                SELECT EHSSN, EHEF1M, EHEF1D, EHEF1Y, EHLTDY, EHLTDM, EHLTDD, EHHLTH
                FROM ooedf.empyhltp
                WHERE EHSSN = ?
            """, (temp_ssn,))

            rows = db2_cursor.fetchall()

            # Store the data in a list of dictionaries
            for row in rows:
                try:
                    eff_date = datetime(int(row.EHEF1Y), int(row.EHEF1M), int(row.EHEF1D)) if all([row.EHEF1Y, row.EHEF1M, row.EHEF1D]) else None
                    term_date = datetime(int(row.EHLTDY), int(row.EHLTDM), int(row.EHLTDD)) if all([row.EHLTDY, row.EHLTDM, row.EHLTDD]) else None

                    data.append({
                        'SSN': row.EHSSN,
                        'Effective Date': eff_date.strftime('%m/%d/%Y') if eff_date else '',
                        'Termination Date': term_date.strftime('%m/%d/%Y') if term_date else '',
                        'Health Code (EHHLTH)': row.EHHLTH
                    })

                    # Update latest dates
                    if eff_date and (not latest_eff_date or eff_date > latest_eff_date):
                        latest_eff_date = eff_date
                    if term_date and (not latest_term_date or term_date > latest_term_date):
                        latest_term_date = term_date

                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue

            track += 1
            # Save to Excel
df = pd.DataFrame(data)
df.to_excel('emp_records.xlsx', index=False)
print("Data saved to emp_records.xlsx")
