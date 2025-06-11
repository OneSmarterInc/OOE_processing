import pyodbc
import logging
import pandas as pd

# Enable logging
logging.basicConfig(level=logging.DEBUG)

# Database connection details
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

# Table name and schema
schema_name = 'OOEDF'
table_name = 'OOEFLOAD'

def fetch_first_n_rows(n):
    try:
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Execute SELECT query to fetch the first `n` rows
        select_query = f"SELECT * FROM {schema_name}.{table_name} FETCH FIRST {n} ROWS ONLY"
        cursor.execute(select_query)

        # Fetch rows and get column names
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]  # Get column names
        df = pd.DataFrame.from_records(rows, columns=columns)  # Convert to DataFrame

        # Print the DataFrame
        print(f"First {n} rows in the table:")
        print(df.columns)
        print(df.iloc[0].to_dict())
        print(df['F17'])
        return df

    except Exception as e:
        print(f"Error: {e}")
        logging.error(e)
        return None

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

# # Fetch and display the first 20 rows

def fetch_last_row():
    try:
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Query to get the last row of the table using RRN()
        last_row_query = f"SELECT * FROM {schema_name}.{table_name} ORDER BY RRN({schema_name}.{table_name}) DESC FETCH FIRST 1 ROW ONLY"
        cursor.execute(last_row_query)

        # Fetch the last row
        last_row = cursor.fetchone()

        # Print the last row
        if last_row:
            print("Last row in the table:")
            print(last_row)
        else:
            print("No data in the table.")

    except Exception as e:
        print(f"Error: {e}")
        logging.error(e)

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

# Fetch and display the last row
# fetch_last_row()

def update_last_row():
    try:
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Update query for the last row using RRN()
        update_query = f"""
        UPDATE {schema_name}.{table_name}
        SET F13 = '01', 
            F23 = CASE 
                    WHEN F23 LIKE '%.0' THEN LEFT(F23, LENGTH(F23) - 2) 
                    ELSE F23 
                  END
        WHERE RRN({schema_name}.{table_name}) = (
            SELECT MAX(RRN({schema_name}.{table_name})) FROM {schema_name}.{table_name}
        )
        WITH NC
        """

        cursor.execute(update_query)
        connection.commit()
        print("Last row updated successfully!")

    except Exception as e:
        print(f"Error: {e}")
        logging.error(e)

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

# Run the update function
# update_last_row()


# df = fetch_first_n_rows(20)

import pyodbc
import logging

# Enable logging
logging.basicConfig(level=logging.DEBUG)

# Database connection details
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

# Table name and schema

def fetch_table_info():
    try:
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Query to get the total number of rows in the table
        count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]  # Get the total row count

        # Print total number of rows
        print(f"Total number of rows in the table: {total_rows}")

        # Query to get the last row of the table
        last_row_query = f"SELECT * FROM {schema_name}.{table_name} ORDER BY <primary_key_column> DESC FETCH FIRST 1 ROW ONLY"
        cursor.execute(last_row_query)

        # Fetch the last row
        last_row = cursor.fetchone()
        
        # Print the last row
        if last_row:
            print("Last row in the table:")
            print(last_row['F17'])
        else:
            print("No data in the table.")

    except Exception as e:
        print(f"Error: {e}")
        logging.error(e)

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

# Fetch and display the total number of rows and last row
fetch_table_info()
# def fetch_records_by_date(date_value):
#     try:
#         connection = pyodbc.connect(connection_string)
#         print("Connected to the database.")
#         cursor = connection.cursor()

#         # Query to fetch records where F16 equals the given date_value
#         select_query = f"SELECT * FROM {schema_name}.{table_name} WHERE F1 = ?"
#         cursor.execute(select_query, date_value)

#         # Fetch rows
#         rows = cursor.fetchall()

#         # Print the fetched rows
#         if rows:
#             print(f"Records with F16 = '{date_value}':")
#             for row in rows:
#                 print(row)
#         else:
#             print(f"No records found with F16 = '{date_value}'.")

#     except Exception as e:
#         print(f"Error: {e}")
#         logging.error(e)

#     finally:
#         if connection:
#             connection.close()
#             print("Database connection closed.")

# # Fetch and display records where F16 = "2025-01-07"
# # fetch_records_by_date("Subscriber")
# def fetch_count_by_date(date_value):
#     try:
#         connection = pyodbc.connect(connection_string)
#         print("Connected to the database.")
#         cursor = connection.cursor()

#         # Query to count records where F1 equals the given date_value
#         count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE F1 = ?"
#         cursor.execute(count_query, date_value)

#         # Fetch the count
#         count = cursor.fetchone()[0]

#         # Print the count
#         print(f"Count of records with F1 = '{date_value}': {count}")

#     except Exception as e:
#         print(f"Error: {e}")
#         logging.error(e)

#     finally:
#         if connection:
#             connection.close()
#             print("Database connection closed.")

# # Count and display records where F1 = "Subscriber"
# # fetch_count_by_date("Subscriber")

# def fetch_table_columns():
#     try:
#         connection = pyodbc.connect(connection_string)
#         print("Connected to the database.")
#         cursor = connection.cursor()

#         # Query to get column information
#         column_info_query = f"SELECT COLUMN_NAME FROM QSYS2.SYSCOLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' WITH NC"
#         cursor.execute(column_info_query)

#         # Fetch all column names
#         columns = cursor.fetchall()

#         # Print the column names
#         if columns:
#             print("Columns in the table:")
#             for column in columns:
#                 print(column.COLUMN_NAME)
#         else:
#             print("No columns found in the table.")

#     except Exception as e:
#         print(f"Error: {e}")
#         logging.error(e)

#     finally:
#         if connection:
#             connection.close()
#             print("Database connection closed.")

# def check_columns_exist():
#     try:
#         connection = pyodbc.connect(connection_string)
#         print("Connected to the database.")
#         cursor = connection.cursor()

#         # Query to get column names from the table
#         column_query = f"""
#         SELECT COLUMN_NAME
#         FROM QSYS2.SYSCOLUMNS
#         WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
#         """
#         cursor.execute(column_query)

#         # Fetch all column names
#         columns = [row[0] for row in cursor.fetchall()]

#         # Columns to check
#         columns_to_check = ['F1', 'F2', 'F3']

#         # Check if each column exists in the table
#         for column in columns_to_check:
#             if column in columns:
#                 print(f"Column {column} exists in the table.")
#             else:
#                 print(f"Column {column} does not exist in the table.")

#     except Exception as e:
#         print(f"Error: {e}")
#         logging.error(e)

#     finally:
#         if connection:
#             connection.close()
#             print("Database connection closed.")

# # Run the check
# # check_columns_exist()

# # Call the function to fetch and print column names
# # fetch_table_columns()

# def print_table_structure_alternate():
#     try:
#         connection = pyodbc.connect(connection_string)
#         cursor = connection.cursor()

#         # Execute a query to get no rows but set up the cursor to have column metadata
#         query = f"SELECT * FROM {schema_name}.{table_name} WHERE 1=0"
#         cursor.execute(query)

#         # Fetch column names from cursor description
#         columns = [column[0] for column in cursor.description]

#         # Print the table headers (column names)
#         print("Table structure:")
#         print("\t".join(columns))

#     except Exception as e:
#         print(f"Error: {e}")

#     finally:
#         if connection:
#             connection.close()
#             print("Database connection closed.")

# # Run the function
# # print_table_structure_alternate()
# import os
# def fetch_emssn():
#     try:
#         # Establish database connection
#         connection = pyodbc.connect(connection_string)
#         print("Connected to the database.")
#         cursor = connection.cursor()

#         # Query to fetch EMSSN from the ooedf.empyp table
#         select_query = "SELECT DPCLAS FROM ooedf.depnp"
#         cursor.execute(select_query)

#         # Fetch all records
#         emssn_records = cursor.fetchall()

#         # Define the directory and file path
#         directory = 'check_media/ssn_count'
#         os.makedirs(directory, exist_ok=True)  # Create the directory if it doesn't exist
#         output_file_path = os.path.join(directory, 'emssn_records.txt')

#         # Write the EMSSN values to the file
#         with open(output_file_path, 'w') as file:
#             for record in emssn_records:
#                 file.write(f"{record[0]}\n")

#         print(f"EMSSN records saved to {output_file_path}")

#     except Exception as e:
#         print(f"Error: {e}")
#         logging.error(e)

#     finally:
#         if 'connection' in locals() and connection:
#             connection.close()
#             print("Database connection closed.")

def fetch_emssn_count():
    try:
        # Establish database connection
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Query to count the number of EMSSN records
        count_query = """
            SELECT COUNT(EMSSN)
            FROM ooedf.empyp
            WHERE EXISTS (
                SELECT 1
                FROM ooedf.elghp
                WHERE ooedf.elghp.ELSSN = ooedf.empyp.EMSSN
            )
            """
        cursor.execute(count_query)

        # Fetch the count
        count = cursor.fetchone()[0]

        # Print the count
        print(f"Count of EMSSN records: {count}")

    except Exception as e:
        print(f"Error: {e}")
        logging.error(e)

    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

# fetch_emssn()
# fetch_emssn_count()