import pyodbc

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

def insert_data_to_DB2(ddf_dict):
    try:
        # Connect to the database
        connection = pyodbc.connect(connection_string)
        print("Connected to the database.")
        cursor = connection.cursor()

        # Strip extra spaces from values in the dictionary
        stripped_ddf_dict = [
            {key: (value.strip() if isinstance(value, str) else value) for key, value in record.items()}
            for record in ddf_dict
        ]

        # Construct the INSERT query dynamically based on dictionary keys
        for record in stripped_ddf_dict:
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['?'] * len(record))
            insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders}) WITH NC"
            print(record)    
            cursor.execute(insert_query, tuple(record.values()))

        # Commit the transaction
        connection.commit()
        print("Data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        if connection:
            connection.close()
            print("Connection closed.")



