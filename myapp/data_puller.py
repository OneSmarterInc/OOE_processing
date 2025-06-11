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
try:
    sqlite_connection = sqlite3.connect(r'EDI_PROJECT-\EDI-Backend\db.sqlite3')  # Replace with actual path
    sqlite_cursor = sqlite_connection.cursor()
    print("Connected to SQLite database.")
except Exception as e:
    print("SQLite connection error:", e)
    exit()

# Fetch records
sqlite_cursor.execute("SELECT temp_ssn, dep_ssn, ssn FROM myapp_inventory_table_data")
records = sqlite_cursor.fetchall()
print("First 10 records:")
for record in records[:10]:
    print(record)

# Process each row
for temp_ssn, dep_ssn, ssn in records:
    mark_ssn = temp_ssn
    temp_ssn = str(temp_ssn)
    temp_ssn = temp_ssn.replace('-','')
    print(f"\nProcessing SSN: {temp_ssn}")

    latest_eff_date = None
    latest_term_date = None
    if temp_ssn and ssn:
        if dep_ssn:
            # Effective date (Status A)
            db2_cursor.execute("""
                SELECT DPEFDM, DPEFDD, DPEFDY
                FROM ooedf.depnp
                WHERE DPDSSN = ? AND DPSTAT = 'A'
            """, (temp_ssn,))
            for row in db2_cursor.fetchall():
                try:
                    date_obj = datetime(int(row.DPEFDY), int(row.DPEFDM), int(row.DPEFDD))
                    print("jacks1",date_obj)
                    if not latest_eff_date or date_obj > latest_eff_date:
                        latest_eff_date = date_obj
                except Exception as e:
                    print(e)
                    continue

            # Termination date (Status D)
            db2_cursor.execute("""
                SELECT DPTDTY, DPTDTM, DPTDTD
                FROM ooedf.depnp
                WHERE DPDSSN = ? AND DPSTAT = 'D'
            """, (temp_ssn,))
            for row in db2_cursor.fetchall():
                try:
                    date_obj = datetime(int(row.DPTDTY), int(row.DPTDTM), int(row.DPTDTD))
                    print("jacks2",date_obj)
                    if not latest_term_date or date_obj > latest_term_date:
                        latest_term_date = date_obj
                except Exception as e:
                    print(e)
                    continue
        else:
            # Effective date (EHHLTH = A)
            db2_cursor.execute("""
                SELECT EHEF1M, EHEF1D, EHEF1Y, EHLTDY, EHLTDM, EHLTDD
                FROM ooedf.empyhltp
                WHERE EHSSN = ? 
            """, (temp_ssn,))
            for row in db2_cursor.fetchall():
                print(row)
                try:
                    if all([row.EHEF1Y, row.EHEF1M, row.EHEF1D]):
                        date_obj = datetime(int(row.EHEF1Y), int(row.EHEF1M), int(row.EHEF1D))
                    else:
                        date_obj = None

                    if all([row.EHLTDY, row.EHLTDM, row.EHLTDD]):
                        date_obj_term = datetime(int(row.EHLTDY), int(row.EHLTDM), int(row.EHLTDD))
                    else:
                        date_obj_term = None
                        
                    print("jacks3",date_obj,date_obj_term)
                    if not latest_eff_date or date_obj > latest_eff_date:
                        latest_eff_date = date_obj
                    if not latest_term_date or date_obj_term > latest_term_date:
                        latest_term_date = date_obj_term
                except Exception as e :
                    print(e)
                    continue


    # Format for SQLite update
    formatted_eff_date = latest_eff_date.strftime('%m/%d/%Y') if latest_eff_date else None
    formatted_term_date = latest_term_date.strftime('%m/%d/%Y') if latest_term_date else None
    if formatted_term_date is not None:
        with open("output_dates.txt", "a") as file:
            file.write(f"mark_ssn = {mark_ssn}  term_date = {formatted_term_date}\n")
    print(f"Updating SSN {ssn} with eff_date: {formatted_eff_date}, term_date: {formatted_term_date}")

    # # Update SQLite table
    # sqlite_cursor.execute("""
    #     UPDATE myapp_inventory_table_data
    #     SET eff_date = ?, term_date = ?
    #     WHERE ssn = ?
    # """, (formatted_eff_date, formatted_term_date, mark_ssn))
    # sqlite_connection.commit()

# Cleanup
sqlite_connection.close()
db2_cursor.close()
db2_connection.close()
print("\nAll updates completed and connections closed.")
