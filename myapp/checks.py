import pandas as pd
import os

def perform_checks(data_df):


    def check_max_length(df, field_rules):
        errors = []

        for column, rules in field_rules.items():
            max_length = int(rules[0])

            if column in df.columns:
                for idx, value in enumerate(df[column]):
                    if pd.notna(value) and len(str(value)) > max_length:
                        errors.append(f"Row {idx + 1}, Column '{column}': Value '{value}' exceeds max length of {max_length}.")
        return errors

    def check_mandatory_fields(df, field_rules):
        errors = []

        for column, rules in field_rules.items():
            is_required = rules[1] == 'R'

            if column in df.columns:
                if is_required:
                    if not df[column].astype(str).str.strip().any(): 
                        errors.append(f"Column '{column}' is required but all values are missing or empty.")
            else:
                if is_required:
                    errors.append(f"Column '{column}' is required but missing from the dataframe.")
        return errors


    field_rules = {
        'ISA01': ['2', 'R'],
        'ISA02': ['10', 'R'],
        'ISA03': ['2', 'R'],
        'ISA04': ['10', 'R'],
        'ISA05': ['2', 'R'],
        'ISA06': ['15', 'R'],
        'ISA07': ['2', 'R'],
        'ISA08': ['15', 'R'],
        'ISA09': ['6', 'R'],
        'ISA10': ['4', 'R'],
        'ISA11': ['1', 'R'],
        'ISA12': ['5', 'R'],
        'ISA13': ['9', 'R'],
        'ISA14': ['1', 'R'],
        'ISA15': ['1', 'R'],
        'ISA16': ['1', 'R'],
        'GS01': ['2', 'R'],
        'GS02': ['15', 'R'],
        'GS03': ['15', 'R'],
        'GS04': ['8', 'R'],
        'GS05': ['4', 'R'],
        'GS06': ['15', 'R'],
        'GS07': ['1', 'R'],
        'GS08': ['12', 'R'],
        'ST01': ['3', 'R'],
        'ST02': ['9', 'R'],
        'ST03': ['35', 'R'],
        'BGN01': ['2', 'R'],
        'BGN02': ['30', 'R'],
        'BGN03': ['8', 'R'],
        'BGN04': ['8', 'R'],
        'BGN05': ['2', 'S'],
        'BGN06': ['30', 'S'],
        'BGN08': ['2', 'R'],
        'REF01': ['3', 'R'],
        'REF02': ['30', 'S'],
        'DTP01': ['3', 'R'],
        'DTP02': ['3', 'R'],
        'DTP03': ['35', 'R'],
        'QTY01': ['2', 'R'],
        'QTY02': ['15', 'R'],
        'N101': ['3', 'R'],
        'N102': ['60', 'S'],
        'N103': ['2', 'R'],
        'N104': ['80', 'R'],
        'INS01': ['1', 'R'],
        'INS02': ['2', 'R'],
        'INS03': ['3', 'R'],
        'INS04': ['2', 'S'],
        'INS05': ['1', 'R'],
        'INS06-1': ['2', 'S'],
        'INS06-2': ['2', 'S'],
        'INS07': ['2', 'S'],
        'INS08': ['2', 'S'],
        'INS09': ['2', 'S'],
        'INS10': ['2', 'S'],
        'INS11': ['3', 'S'],
        'INS12': ['2', 'S'],
        'NM101': ['3', 'R'],
        'NM102': ['1', 'R'],
        'NM103': ['35', 'R'],
        'NM104': ['25', 'R'],
        'NM105': ['10', 'S'],
        'NM106': ['1', 'S'],
        'NM107': ['1', 'S'],
        'NM108': ['2', 'R'],
        'NM109': ['80', 'R'],
        'PER01': ['2', 'R'],
        'PER03': ['2', 'R'],
        'PER04': ['80', 'R'],
        'PER05': ['2', 'S'],
        'PER06': ['80', 'S'],
        'PER07': ['2', 'S'],
        'PER08': ['80', 'S'],
        'N301': ['55', 'R'],
        'N302': ['55', 'S'],
        'N401': ['30', 'R'],
        'N402': ['30', 'R'],
        'N403': ['15', 'R'],
        'N404': ['3', 'S'],
        'DMG01': ['2', 'R'],
        'DMG02': ['8', 'R'],
        'DMG03': ['8', 'R'],
        'DMG04': ['35', 'S'],
        'DSB01': ['1', 'R'],
        'DSB07': ['2', 'S'],
        'DSB08': ['2', 'S'],
        'HD01': ['3', 'R'],
        'HD03': ['30', 'R'],
        'HD04': ['35', 'S'],
        'HD05': ['80', 'S'],
        'SE01': ['10', 'R'],
        'SE02': ['9', 'R'],
        'GE01': ['10', 'R'],
        'GE02': ['10', 'R'],
        'IEA01': ['2', 'R'],
        'IEA02': ['9', 'R'],
    }



    max_length_errors = check_max_length(data_df, field_rules)
    mandatory_field_errors = check_mandatory_fields(data_df, field_rules)

    validation_errors = max_length_errors + mandatory_field_errors

    return validation_errors