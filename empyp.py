# import pandas as pd
# import pyodbc
from dateutil import parser
# from tqdm import tqdm

# conn = pyodbc.connect(
#     'DRIVER={ODBC Driver 17 for SQL Server};'
#     'SERVER=ABCCOLUMBUSSQL2;'
#     'DATABASE=EDIDATABASE;'
#     'UID=sa;'
#     'PWD=ChangeMe#2024;'
# )

# cursor = conn.cursor()

# def insert_to_sql_server(output_file_data, file_date):
#     insert_df = pd.DataFrame(output_file_data)
#     print(insert_df.columns)
    
#     if "ADDRESS 2" not in insert_df.columns:
#         insert_df["ADDRESS 2"] = ""

#     insert_df["marital_status"] = None
#     spouse_ssns = insert_df.loc[insert_df["SUB/DEP"] == "Spouse", "SSN"].unique()
#     insert_df.loc[
#         (insert_df["SSN"].isin(spouse_ssns)) & (insert_df["SUB/DEP"] == "Subscriber"),
#         "marital_status",
#     ] = "Married"

#     insert_df["CLIENT"] = "OOE"
#     insert_df["EFF DATE"] = pd.to_datetime(insert_df["EFF DATE"])
#     insert_df["eff_date_day"] = insert_df["EFF DATE"].dt.day
#     insert_df["eff_date_month"] = insert_df["EFF DATE"].dt.month
#     insert_df["eff_date_year"] = insert_df["EFF DATE"].dt.year

#     def convert_to_date_components(date_string):
#         try:
#             parsed_date = parser.parse(date_string)
#             return {"year": parsed_date.year, "month": parsed_date.month, "day": parsed_date.day}
#         except (parser.ParserError, TypeError, ValueError):
#             return {"year": 2024, "month": 12, "day": 31}

#     elghp_df = insert_df.copy()
#     elghp_df["ELSSN"] = elghp_df.apply(lambda x: x["SSN"] if x["SUB/DEP"] == "Subscriber" else x["DEP SSN"], axis=1)
#     elghp_df = elghp_df[["ELSSN", "PLAN", "CLASS", "CLIENT", "eff_date_year", "eff_date_month", "eff_date_day"]]
#     elghp_df.columns = ["ELSSN", "ELPLAN", "ELCLAS", "ELCLNT", "ELEPDY", "ELEPDM", "ELEPDD"]
    
#     filtered_df = insert_df[insert_df["SUB/DEP"] != "Subscriber"]
#     depnp_df = filtered_df.copy()
#     depnp_df["date_components"] = depnp_df["DOB"].apply(convert_to_date_components)
#     depnp_df['DPTYPE'] = depnp_df['SUB/DEP']
#     depnp_df['DPDSSN'] = depnp_df['DEP SSN']
#     depnp_df['DPCLAS'] = depnp_df['CLASS']
#     depnp_df['DPPLAN'] = depnp_df['PLAN']
#     depnp_df["DPDOBY"] = depnp_df["date_components"].apply(lambda x: x["year"])
#     depnp_df["DPDOBM"] = depnp_df["date_components"].apply(lambda x: x["month"])
#     depnp_df["DPDOBD"] = depnp_df["date_components"].apply(lambda x: x["day"])
#     depnp_df["DPNAME"] = depnp_df["FIRST NAME"].fillna('') + " " + depnp_df["LAST NAME"].fillna('')
#     depnp_df["DPNAME"] = depnp_df["DPNAME"].str.strip()
#     depnp_df = depnp_df[["DPDOBY", "DPDOBM", "DPDOBD", "SEX", "SSN", "CLIENT","DPTYPE","DPDSSN","DPCLAS","DPPLAN","DPNAME"]]
#     depnp_df.columns = ["DPDOBY", "DPDOBM", "DPDOBD", "DPSEX", "DPSSN", "DPCLNT","DPTYPE","DPDSSN","DPCLAS","DPPLAN","DPNAME"]

# #     new_filtered_df = insert_df[insert_df["SUB/DEP"] == "Subscriber"]
# #     empyp_df = new_filtered_df.copy()
# #     empyp_df["date_components"] = empyp_df["DOB"].apply(convert_to_date_components)
# #     empyp_df['EMMEM'] = empyp_df['MEMBER ID']
# #     empyp_df['EMPHON'] = empyp_df['PHONE']
# #     empyp_df["EMDOBY"] = empyp_df["date_components"].apply(lambda x: x["year"])
# #     empyp_df["EMDOBM"] = empyp_df["date_components"].apply(lambda x: x["month"])
# #     empyp_df["EMDOBD"] = empyp_df["date_components"].apply(lambda x: x["day"])
# #     empyp_df["EMNAME"] = empyp_df["FIRST NAME"].fillna('') + " " + empyp_df["LAST NAME"].fillna('')
# #     empyp_df["EMPNAME"] = empyp_df["EMNAME"].str.strip()
# #     empyp_df = empyp_df[[
# #         "EMDOBY", "EMDOBM", "EMDOBD", "SEX", "SSN", "CITY", "STATE", "ZIP", "ADDRESS 1", "ADDRESS 2", "CLIENT", "marital_status","EMMEM","EMPHON","EMNAME"
# #     ]]
# #     empyp_df.columns = ["EMDOBY", "EMDOBM", "EMDOBD", "EMSEX", "EMSSN", "EMCITY", "EMST", "EMZIP5", "EMADR1", "EMADR2", "EMCLNT", "EMMS","EMMEM","EMPHON","EMNAME"]

# #     elghp_df['file_date'] = file_date
# #     depnp_df['file_date'] = file_date
# #     empyp_df['file_date'] = file_date
# #     empyp_df.fillna('',inplace=True)
# #     elghp_df.fillna('',inplace=True)
# #     depnp_df.fillna('',inplace=True)
    
# #     elghp_data = elghp_df.to_dict(orient="records")
# #     depnp_data = depnp_df.to_dict(orient="records")
# #     empyp_data = empyp_df.to_dict(orient="records")

# #     def insert_data(table_name, data):
# #         if not data:
# #             return
# #         columns = data[0].keys()
# #         placeholders = ", ".join(["?" for _ in columns])
# #         column_names = ", ".join(columns)
# #         query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
# #         values = [tuple(d[col] for col in columns) for d in data]
        
# #         for i in tqdm(range(0, len(values), 1000), desc=f'Inserting into {table_name}'):
# #             cursor.executemany(query, values[i:i+100])
# #             conn.commit()

# #     # insert_data("myapp_elghp", elghp_data)
# #     insert_data("myapp_depnp", depnp_data)
# #     insert_data("myapp_empyp", empyp_data)

# import pandas as pd
# import pyodbc
# from dateutil import parser
# from tqdm import tqdm

# conn = pyodbc.connect(
#     'DRIVER={ODBC Driver 17 for SQL Server};'
#     'SERVER=ABCCOLUMBUSSQL2;'
#     'DATABASE=EDIDATABASE;'
#     'UID=sa;'
#     'PWD=ChangeMe#2024;'
# )

# cursor = conn.cursor()

# def fetch_existing_ssns(table_name, column_name):
#     """Fetch existing SSNs from the given table."""
#     query = f"SELECT {column_name} FROM {table_name}"
#     cursor.execute(query)
#     return set(row[0] for row in cursor.fetchall())

# def insert_data(table_name, data):
#     """Insert data in batches of 1000 records."""
#     if not data:
#         return
#     columns = data[0].keys()
#     placeholders = ", ".join(["?" for _ in columns])
#     column_names = ", ".join(columns)
#     query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
#     values = [tuple(d[col] for col in columns) for d in data]
    
#     for i in tqdm(range(0, len(values), 1000), desc=f'Inserting into {table_name}'):
#         cursor.executemany(query, values[i:i+100])
#         conn.commit()

# def update_empyp_status(ssns_to_update):
#     """Mark extra SSNs in empyp as terminated."""
#     if not ssns_to_update:
#         return
#     query = f"""
#         UPDATE myapp_empyp 
#         SET EMTERM = ?, EMFLAG = ? 
#         WHERE EMSSN IN ({','.join(['?'] * len(ssns_to_update))})
#     """
#     cursor.execute(query, ('2025-01-01', True, *ssns_to_update))
#     conn.commit()

# def update_depnp_status(ssns_to_update):
#     """Mark extra SSNs in depnp as terminated."""
#     if not ssns_to_update:
#         return
#     query = f"""
#         UPDATE myapp_depnp 
#         SET DPTERM = ?, DPFLAG = ? 
#         WHERE DPDSSN IN ({','.join(['?'] * len(ssns_to_update))})
#     """
#     print("Query:", query)
#     print("Parameters:", ('2025-01-01', True, *ssns_to_update))
#     cursor.execute(query, ('2025-01-01', True, *ssns_to_update))
#     conn.commit()

# def convert_to_date_components(date_string):
#         try:
#             parsed_date = parser.parse(date_string)
#             return {"year": parsed_date.year, "month": parsed_date.month, "day": parsed_date.day}
#         except (parser.ParserError, TypeError, ValueError):
#             return {"year": 2024, "month": 12, "day": 31}

# def insert_to_sql_server(output_file_data, file_date):
#     insert_df = pd.DataFrame(output_file_data)
    
#     # Ensure required columns exist
#     if "ADDRESS 2" not in insert_df.columns:
#         insert_df["ADDRESS 2"] = ""

#     insert_df["marital_status"] = None
#     spouse_ssns = insert_df.loc[insert_df["SUB/DEP"] == "Spouse", "SSN"].unique()
#     insert_df.loc[
#         (insert_df["SSN"].isin(spouse_ssns)) & (insert_df["SUB/DEP"] == "Subscriber"),
#         "marital_status",
#     ] = "Married"

#     insert_df["CLIENT"] = "OOE"
#     insert_df["EFF DATE"] = pd.to_datetime(insert_df["EFF DATE"])
#     insert_df["eff_date_day"] = insert_df["EFF DATE"].dt.day
#     insert_df["eff_date_month"] = insert_df["EFF DATE"].dt.month
#     insert_df["eff_date_year"] = insert_df["EFF DATE"].dt.year

#     elghp_df = insert_df.copy()
#     elghp_df["ELSSN"] = elghp_df.apply(lambda x: x["SSN"] if x["SUB/DEP"] == "Subscriber" else x["DEP SSN"], axis=1)
#     elghp_df = elghp_df[["ELSSN", "PLAN", "CLASS", "CLIENT", "eff_date_year", "eff_date_month", "eff_date_day"]]
#     elghp_df.columns = ["ELSSN", "ELPLAN", "ELCLAS", "ELCLNT", "ELEPDY", "ELEPDM", "ELEPDD"]

#     # **Delete existing data from ELGHP before inserting new records**
#     cursor.execute("DELETE FROM myapp_elghp")
#     conn.commit()

#     # Insert all ELGHP records first
#     elghp_df['file_date'] = file_date
#     elghp_df.fillna('', inplace=True)
#     elghp_data = elghp_df.to_dict(orient="records")
#     insert_data("myapp_elghp", elghp_data)

#     # Fetch existing SSNs from empyp and depnp
#     existing_empyp_ssns = fetch_existing_ssns("myapp_empyp", "EMSSN")
#     existing_depnp_ssns = fetch_existing_ssns("myapp_depnp", "DPDSSN")
#     elghp_ssns = fetch_existing_ssns("myapp_elghp", "ELSSN")

#     # Process EMPYP (Subscribers)
#     new_filtered_df = insert_df[insert_df["SUB/DEP"] == "Subscriber"]
#     empyp_df = new_filtered_df.copy()
#     empyp_df["date_components"] = empyp_df["DOB"].apply(convert_to_date_components)
#     empyp_df['EMMEM'] = empyp_df['MEMBER ID']
#     empyp_df['EMPHON'] = empyp_df['PHONE']
#     empyp_df["EMDOBY"] = empyp_df["date_components"].apply(lambda x: x["year"])
#     empyp_df["EMDOBM"] = empyp_df["date_components"].apply(lambda x: x["month"])
#     empyp_df["EMDOBD"] = empyp_df["date_components"].apply(lambda x: x["day"])
#     empyp_df["EMNAME"] = empyp_df["FIRST NAME"].fillna('') + " " + empyp_df["LAST NAME"].fillna('')
#     empyp_df["EMPNAME"] = empyp_df["EMNAME"].str.strip()
#     empyp_df = empyp_df[[
#         "EMDOBY", "EMDOBM", "EMDOBD", "SEX", "SSN", "CITY", "STATE", "ZIP", "ADDRESS 1", "ADDRESS 2", "CLIENT", "marital_status","EMMEM","EMPHON","EMNAME"
#     ]]
#     empyp_df.columns = ["EMDOBY", "EMDOBM", "EMDOBD", "EMSEX", "EMSSN", "EMCITY", "EMST", "EMZIP5", "EMADR1", "EMADR2", "EMCLNT", "EMMS","EMMEM","EMPHON","EMNAME"]
#     empyp_df['file_date'] = file_date
#     empyp_df.fillna('', inplace=True)
#     empyp_data = empyp_df.to_dict(orient="records")

#     # Insert only missing EMPYP records
#     missing_empyp = [record for record in empyp_data if record["EMSSN"] not in existing_empyp_ssns]
#     insert_data("myapp_empyp", missing_empyp)

#     # Find extra EMPYP SSNs and mark them
#     extra_empyp_ssns = existing_empyp_ssns - elghp_ssns
#     if len(extra_empyp_ssns)!=0:
#         update_empyp_status(extra_empyp_ssns)

#     # Process DEPNP (Dependents)
#     filtered_df = insert_df[insert_df["SUB/DEP"] != "Subscriber"]
#     depnp_df = filtered_df.copy()
#     depnp_df["date_components"] = depnp_df["DOB"].apply(convert_to_date_components)
#     depnp_df['DPTYPE'] = depnp_df['SUB/DEP']
#     depnp_df['DPDSSN'] = depnp_df['DEP SSN']
#     depnp_df['DPCLAS'] = depnp_df['CLASS']
#     depnp_df['DPPLAN'] = depnp_df['PLAN']
#     depnp_df["DPDOBY"] = depnp_df["date_components"].apply(lambda x: x["year"])
#     depnp_df["DPDOBM"] = depnp_df["date_components"].apply(lambda x: x["month"])
#     depnp_df["DPDOBD"] = depnp_df["date_components"].apply(lambda x: x["day"])
#     depnp_df["DPNAME"] = depnp_df["FIRST NAME"].fillna('') + " " + depnp_df["LAST NAME"].fillna('')
#     depnp_df["DPNAME"] = depnp_df["DPNAME"].str.strip()
#     depnp_df = depnp_df[["DPDOBY", "DPDOBM", "DPDOBD", "SEX", "SSN", "CLIENT","DPTYPE","DPDSSN","DPCLAS","DPPLAN","DPNAME"]]
#     depnp_df.columns = ["DPDOBY", "DPDOBM", "DPDOBD", "DPSEX", "DPSSN", "DPCLNT","DPTYPE","DPDSSN","DPCLAS","DPPLAN","DPNAME"]
#     depnp_df['file_date'] = file_date
#     depnp_df.fillna('', inplace=True)
#     depnp_data = depnp_df.to_dict(orient="records")

#     # Insert only missing DEPNP records
#     missing_depnp = [record for record in depnp_data if record["DPDSSN"] not in existing_depnp_ssns]
#     insert_data("myapp_depnp", missing_depnp)

#     # Find extra DEPNP SSNs and mark them
#     extra_depnp_ssns = existing_depnp_ssns - elghp_ssns
#     if len(extra_depnp_ssns)!=0:
#         update_depnp_status(extra_depnp_ssns)

#     print("Data processing completed successfully.")


import pyodbc
import pandas as pd
from tqdm import tqdm

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=ABCCOLUMBUSSQL2;'
    'DATABASE=EDIDATABASE;'
    'UID=sa;'
    'PWD=ChangeMe#2024;'
)
cursor = conn.cursor()

def convert_to_date_components(date_string):
        try:
            parsed_date = parser.parse(date_string)
            return {"year": parsed_date.year, "month": parsed_date.month, "day": parsed_date.day}
        except (parser.ParserError, TypeError, ValueError):
            return {"year": 2024, "month": 12, "day": 31}


def insert_to_sql_server(output_file_data, file_date):
    insert_df = pd.DataFrame(output_file_data)
    
    # Ensure required columns exist
    if "ADDRESS 2" not in insert_df.columns:
        insert_df["ADDRESS 2"] = ""

    insert_df["marital_status"] = None
    spouse_ssns = insert_df.loc[insert_df["SUB/DEP"] == "Spouse", "SSN"].unique()
    insert_df.loc[
        (insert_df["SSN"].isin(spouse_ssns)) & (insert_df["SUB/DEP"] == "Subscriber"),
        "marital_status",
    ] = "Married"

    insert_df["CLIENT"] = "OOE"
    insert_df["EFF DATE"] = pd.to_datetime(insert_df["EFF DATE"])
    insert_df["eff_date_day"] = insert_df["EFF DATE"].dt.day
    insert_df["eff_date_month"] = insert_df["EFF DATE"].dt.month
    insert_df["eff_date_year"] = insert_df["EFF DATE"].dt.year
    new_filtered_df = insert_df[insert_df["SUB/DEP"] == "Subscriber"]
    empyp_df = new_filtered_df.copy()
    empyp_df["date_components"] = empyp_df["DOB"].apply(convert_to_date_components)
    empyp_df['EMMEM'] = empyp_df['MEMBER ID']
    empyp_df['EMPHON'] = empyp_df['PHONE']
    empyp_df["EMDOBY"] = empyp_df["date_components"].apply(lambda x: x["year"])
    empyp_df["EMDOBM"] = empyp_df["date_components"].apply(lambda x: x["month"])
    empyp_df["EMDOBD"] = empyp_df["date_components"].apply(lambda x: x["day"])
    empyp_df["EMNAME"] = empyp_df["FIRST NAME"].fillna('') + " " + empyp_df["LAST NAME"].fillna('')
    empyp_df["EMPNAME"] = empyp_df["EMNAME"].str.strip()
    empyp_df = empyp_df[[
        "EMDOBY", "EMDOBM", "EMDOBD", "SEX", "SSN", "CITY", "STATE", "ZIP", "ADDRESS 1", "ADDRESS 2", "CLIENT", "marital_status","EMMEM","EMPHON","EMNAME","EFF DATE"
    ]]
    empyp_df.columns = ["EMDOBY", "EMDOBM", "EMDOBD", "EMSEX", "EMSSN", "EMCITY", "EMST", "EMZIP5", "EMADR1", "EMADR2", "EMCLNT", "EMMS","EMMEM","EMPHON","EMNAME","EMEFFDATE"]
    empyp_df['file_date'] = file_date
    empyp_df.fillna('', inplace=True)


    # Process DEPNP (Dependents)
    filtered_df = insert_df[insert_df["SUB/DEP"] != "Subscriber"]
    depnp_df = filtered_df.copy()
    depnp_df["date_components"] = depnp_df["DOB"].apply(convert_to_date_components)
    depnp_df['DPTYPE'] = depnp_df['SUB/DEP']
    depnp_df['DPDSSN'] = depnp_df['DEP SSN']
    depnp_df['DPCLAS'] = depnp_df['CLASS']
    depnp_df['DPPLAN'] = depnp_df['PLAN']
    depnp_df["DPDOBY"] = depnp_df["date_components"].apply(lambda x: x["year"])
    depnp_df["DPDOBM"] = depnp_df["date_components"].apply(lambda x: x["month"])
    depnp_df["DPDOBD"] = depnp_df["date_components"].apply(lambda x: x["day"])
    depnp_df["DPNAME"] = depnp_df["FIRST NAME"].fillna('') + " " + depnp_df["LAST NAME"].fillna('')
    depnp_df["DPNAME"] = depnp_df["DPNAME"].str.strip()
    depnp_df = depnp_df[["DPDOBY", "DPDOBM", "DPDOBD", "SEX", "SSN", "CLIENT","DPTYPE","DPDSSN","DPCLAS","DPPLAN","DPNAME","EFF DATE"]]
    depnp_df.columns = ["DPDOBY", "DPDOBM", "DPDOBD", "DPSEX", "DPSSN", "DPCLNT","DPTYPE","DPDSSN","DPCLAS","DPPLAN","DPNAME","DPEFFDATE"]
    depnp_df['file_date'] = file_date
    depnp_df.fillna('', inplace=True)

    def fetch_existing_data(table_name, ssn_column):
        cursor.execute(f"SELECT {ssn_column} FROM {table_name}")
        return pd.DataFrame(cursor.fetchall(), columns=[column[0] for column in cursor.description])
    
    def fetch_existing_data_depnp(table_name, ssn_column):
        cursor.execute(f"SELECT {ssn_column} FROM {table_name}")
        return pd.DataFrame(cursor.fetchall(), columns=[column[0] for column in cursor.description])

    df_empyp = fetch_existing_data("myapp_empyp", "EMSSN")
    df_depnp = fetch_existing_data_depnp("myapp_depnp", "DPDSSN")

    df_empyp["EMSSN"] = df_empyp["EMSSN"].astype(str)
    df_depnp["DPDSSN"] = df_depnp["DPDSSN"].astype(str)

    if df_empyp.empty and df_depnp.empty:
        empyp_insert_query = """
            INSERT INTO myapp_empyp (EMDOBY, EMDOBM, EMDOBD, EMSEX, EMSSN, EMCITY, EMST, EMZIP5, EMADR1, EMADR2, EMCLNT, EMMS, EMMEM, EMPHON, EMNAME, EMEFFDATE, file_date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        depnp_insert_query = """
            INSERT INTO myapp_depnp (DPDOBY, DPDOBM, DPDOBD, DPSEX, DPSSN, DPCLNT, DPTYPE, DPDSSN, DPCLAS, DPPLAN, DPNAME, DPEFFDATE, file_date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        batch_size = 1000
        data_depnp = depnp_df.values.tolist()
        data_empyp = empyp_df.values.tolist()

        with tqdm(total=len(data_empyp), desc="Inserting into myapp_empyp") as pbar:
            for i in range(0, len(data_empyp), batch_size):
                batch = data_empyp[i : i + 1000]  
                cursor.executemany(empyp_insert_query, batch)  
                pbar.update(len(batch))
        conn.commit()
        with tqdm(total=len(data_depnp), desc="Inserting into myapp_depnp") as pbar:
            for i in range(0, len(data_depnp), batch_size):
                batch = data_depnp[i : i + 1000]  
                cursor.executemany(depnp_insert_query, batch)  
                pbar.update(len(batch))

        conn.commit()

        

    else:
        existing_empyp_ssns = [item.split(",")[0].strip("()'") for item in df_empyp["EMSSN"].to_list()]
        try:
            existing_depnp_ssns =[item.split(",")[0].strip("()'") for item in df_depnp["DPDSSN"].to_list()]
        except:
            existing_depnp_ssns = set(row["DPDSSN"] for row in df_depnp.itertuples())
        new_empyp_entries = empyp_df[~empyp_df["EMSSN"].isin(existing_empyp_ssns)]
        new_depnp_entries = depnp_df[~depnp_df["DPDSSN"].isin(existing_depnp_ssns)]

        empyp_insert_query = """
            INSERT INTO myapp_empyp (EMDOBY, EMDOBM, EMDOBD, EMSEX, EMSSN, EMCITY, EMST, EMZIP5, EMADR1, EMADR2, EMCLNT, EMMS, EMMEM, EMPHON, EMNAME, EMEFFDATE, file_date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        depnp_insert_query = """
            INSERT INTO myapp_depnp (DPDOBY, DPDOBM, DPDOBD, DPSEX, DPSSN, DPCLNT, DPTYPE, DPDSSN, DPCLAS, DPPLAN, DPNAME, DPEFFDATE, file_date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 

        """
        batch_size = 1000
        data_empyp = new_empyp_entries.values.tolist()
        with tqdm(total=len(data_empyp), desc="Inserting into myapp_empyp") as pbar:
            for i in range(0, len(data_empyp), batch_size):
                batch = data_empyp[i : i + 1000]  
                cursor.executemany(empyp_insert_query, batch)  
                pbar.update(len(batch))
        conn.commit()
        

        data_depnp = new_depnp_entries.values.tolist()
        with tqdm(total=len(data_depnp), desc="Inserting into myapp_depnp") as pbar:
            for i in range(0, len(data_depnp), batch_size):
                batch = data_depnp[i : i + 1000]  
                cursor.executemany(depnp_insert_query, batch)  
                pbar.update(len(batch))

        

        current_empyp_ssns = set(empyp_df["EMSSN"])
        current_depnp_ssns = set(depnp_df["DPDSSN"])

        missing_empyp_ssns = set(existing_empyp_ssns) - current_empyp_ssns
        missing_depnp_ssns = set(existing_depnp_ssns) - current_depnp_ssns


        # empyp_update_query = "UPDATE myapp_empyp SET EMFLAG = ?, EMTERM = ? WHERE EMSSN = ?"
        # depnp_update_query = "UPDATE myapp_depnp SET DPFLAG = ?, DPTERM = ? WHERE DPDSSN = ?"
        # reappeared_empyp_update_query = "UPDATE myapp_empyp SET EMEFFDATE = ?, EMFLAG = ?, EMTERM = ? WHERE EMSSN = ?"
        # reappeared_depnp_update_query = "UPDATE myapp_depnp SET DPEFFDATE=?, EMFLAG = ?, EMTERM = ? WHERE DPDSSN = ?"

        # if missing_empyp_ssns:
        #     cursor.executemany(empyp_update_query, [(1, file_date, ssn) for ssn in missing_empyp_ssns])

        # if missing_depnp_ssns:
        #     cursor.executemany(depnp_update_query, [(1, file_date, ssn) for ssn in missing_depnp_ssns])

        # reappeared_empyp_ssns = current_empyp_ssns.intersection(missing_empyp_ssns)
        # reappeared_depnp_ssns = current_depnp_ssns.intersection(missing_depnp_ssns)

        # if reappeared_empyp_ssns:
        #     cursor.executemany(reappeared_empyp_update_query, [(file_date, 0, None, ssn) for ssn in reappeared_empyp_ssns])

        # if reappeared_depnp_ssns:
        #     cursor.executemany(reappeared_depnp_update_query, [(file_date, 0, None, ssn) for ssn in reappeared_depnp_ssns])

        conn.commit()

