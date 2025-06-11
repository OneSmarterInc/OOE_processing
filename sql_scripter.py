import json
import time
import pyodbc
import pickle
import os
from multiprocessing import Process

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

def process_data(pivot_df_data):
    subfield_mappings = {
        "ISA": ["ISA01", "ISA03", "ISA05", "ISA06", "ISA07", "ISA08", "ISA09", "ISA10", "ISA11", "ISA12", "ISA13", "ISA14", "ISA15", "ISA16"],
        "GS": ["GS01", "GS02", "GS03", "GS04", "GS05", "GS06", "GS07", "GS08"],
        "ST": ["ST01", "ST02", "ST03"],
        "BGN": ["BGN01", "BGN02", "BGN03", "BGN04", "BGN05", "BGN08"],
        "REF": ["REF01", "REF02"],
        "DTP": ["DTP01", "DTP02", "DTP03"],
        "N1": ["N101", "N102", "N103", "N104"],
        "N3": ["N301", "N302"],
        "N4": ["N401", "N402", "N403", "N404"],
        "NM1": ["NM101", "NM102", "NM103", "NM104", "NM105", "NM108", "NM109"],
        "PER": ["PER01", "PER03", "PER04", "PER05", "PER06", "PER07", "PER08"],
        "DMG": ["DMG01", "DMG02", "DMG03", "DMG04"],
        "HD": ["HD01", "HD03", "HD04", "HD05"],
        "INS": ["INS01", "INS02", "INS03", "INS05", "INS06_2", "INS07", "INS08", "INS09", "INS10", "INS11", "INS12", "INS17"],
    }

    columns_to_keep_blank = [
        "ISA02", "ISA04", "BGN06", "BGN07", "BGN09", "REF03", "REF04",
        "N105", "N106", "INS04", "INS13", "INS14", "INS15", "INS16",
        "INS06_1", "NM106", "NM107", "NM110", "NM111", "PER02", "PER09",
        "N405", "N406", "DMG05", "DMG06", "DMG07", "DMG08", "DMG09",
        "HD02", "HD06", "HD07", "HD08", "HD09", "HD10", "HD11"
    ]

    def parse_segment(segment, keys):
        """Parse segment data and map to respective keys with empty strings for missing values."""
        if not segment:
            return {key: "" for key in keys}
        values = segment.split()
        return {key: values[i] if i < len(values) else "" for i, key in enumerate(keys)}

    connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=ABCCOLUMBUSSQL2;'
            'DATABASE=EDIDATABASE;'
            'UID=sa;'
            'PWD=ChangeMe#2024;'
        )
    cursor = connection.cursor()

    all_columns = sum(subfield_mappings.values(), []) + ["Date_edi"]
    column_names = ", ".join([f"[{col}]" if "-" in col else col for col in all_columns])
    placeholders = ", ".join(["?"] * len(all_columns))

    insert_query = f'''
        INSERT INTO edisegmentable ({column_names}) 
        VALUES ({placeholders})
    '''

    for item in pivot_df_data:
        try:
            parsed_data = {}
            for segment, keys in subfield_mappings.items():
                data = item.get(segment)
                if segment == "N4":
                    data_list = data.split()
                    if len(data_list[1]) > 2:
                        data_list[0] = f"{data_list[0]}_{data_list[1]}"
                        data_list.pop(1)
                    result = " ".join(data_list)
                    item[segment] = result
                if segment == "NM1":
                    data = item.get(segment)
                    data_list = data.split()
                    if len(data_list) > 4 and data_list[4] == '34':
                        data_list.insert(4, 'NA')
                    if len(data_list) > 6:
                        itemss = data_list[:-2]
                        itemss = itemss[2:]
                        new_string = ''.join(i + '_' for i in itemss)
                        new_list = [data_list[0], data_list[1], new_string, data_list[-2], data_list[-1]]
                        data_list = new_list
                    result = " ".join(data_list)
                    item[segment] = result
                if segment == "INS":
                    data = item.get(segment)
                    data_list = data.split()
                    if len(data_list) > 4 and len(data_list[4]) > 1:
                        data_list.insert(4, 'NA')
                    result = " ".join(data_list)
                    item[segment] = result
                if segment == "N1":
                    data = item.get(segment)
                    data_list = data.split()
                    if len(data_list) > 3:
                        if len(data_list[2]) > 3 and len(data_list[3]) > 3:
                            data_list[1] = f"{data_list[1]}_{data_list[2]}_{data_list[3]}"
                            data_list.pop(3)
                            data_list.pop(2)
                        else:
                            data_list[1] = f"{data_list[1]}_{data_list[2]}"
                            data_list.pop(2)
                    result = " ".join(data_list)
                    item[segment] = result

                parsed_data.update(parse_segment(item.get(segment), keys))
            
            parsed_data["Date_edi"] = item.get("Date_edi", "") 
            
            for blank_column in columns_to_keep_blank:
                parsed_data[blank_column] = ""

            values = [parsed_data.get(col, "") for col in all_columns]
            cursor.execute(insert_query, values)
            cursor.commit()
        except Exception as e:
            print(f"Error: {e}")
    connection.close()


if __name__ == "__main__":
    queue_file = "queue_file.pkl"

    while True:
        data = read_from_queue(queue_file)
        if data:
            print('Got the data')
            process = Process(target=process_data, args=(data,))
            process.start()
            process.join()
            print('insertion completed')
        else:
            print("Queue is empty. Waiting for data...")
            time.sleep(3)