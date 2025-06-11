import pandas as pd
import pyodbc
from tqdm import tqdm

# Database connection details
conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=ABCCOLUMBUSSQL2;'
            'DATABASE=EDIDATABASE;'
            'UID=sa;'
            'PWD=ChangeMe#2024;'
        )
cursor = conn.cursor()


df = pd.read_excel(r"S:\OOE\output_data_empyp.xlsx")
df.fillna('',inplace=True)
# Load the DataFrame (assuming it is already loaded as 'df')
table_name = 'myapp_empyhltp'

# Get the column names from the database table
cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
db_columns = set(row[0] for row in cursor.fetchall())

# Filter the DataFrame to only include columns that exist in the database
matching_columns = [col for col in df.columns if col in db_columns]
filtered_df = df[matching_columns]

# Batch size for insertion
batch_size = 500

# Prepare the SQL insert statement
columns = ', '.join(matching_columns)
placeholders = ', '.join(['?'] * len(matching_columns))
sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

# Batch insert with progress bar
for i in tqdm(range(0, len(filtered_df), batch_size), desc="Inserting Data", unit="batch"):
    batch = filtered_df.iloc[i:i + batch_size].values.tolist()
    cursor.executemany(sql, batch)

# Commit the transaction and close the connection
conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully.")
