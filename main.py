import pandas as pd
from Utilities.Check import null_check, count_check, compare_tables, columnCount
from Utilities.Utils import ReadData, read_excel_data
from Configuration import config
import os
import json
from datetime import datetime

def main(index):
    # Use variables from config.py
    credential_workbook_name = config.credential_workbook_name
    source_sheet_name = config.source_sheet_name
    target_sheet_name = config.target_sheet_name

    # Read source and target data
    source_df = ReadData(credential_workbook_name, source_sheet_name)
    if source_df is None:
        raise ValueError("Failed to load source data.")

    target_df = ReadData(credential_workbook_name, target_sheet_name)
    if target_df is None:
        raise ValueError("Failed to load target data.")

    # Read the check configuration sheet
    # check_sheet_name = config.check_sheet_name


    # check_sheet = read_excel_data(credential_workbook_name, check_sheet_name)

    null_sheet_name = config.null_check_sheet
    count_sheet_name = config.count_check_sheet
    column_sheet_name = config.column_check_sheet
    compare_sheet_name = config.compare_tables_sheet

    null_check_sheet = read_excel_data(credential_workbook_name, null_sheet_name)
    count_check_sheet = read_excel_data(credential_workbook_name, count_sheet_name)
    column_check_sheet = read_excel_data(credential_workbook_name, column_sheet_name)
    compare_check_sheet = read_excel_data(credential_workbook_name, compare_sheet_name)


    null_c = null_check_sheet[f'A{index}'].value
    count_c = count_check_sheet[f'A{index}'].value
    column_c = column_check_sheet[f'A{index}'].value
    compare_t = compare_check_sheet[f'A{index}'].value

    # print(null_c+ " "+ count_c)

    # Validate that the check values are "Yes" or "No"
    valid_check_values = {"Yes", "No"}
    if null_c not in valid_check_values:
        raise ValueError(f"Invalid value for null_c: {null_c}. Expected 'Yes' or 'No'.")
    if count_c not in valid_check_values:
        raise ValueError(f"Invalid value for count_c: {count_c}. Expected 'Yes' or 'No'.")
    if column_c not in valid_check_values:
        raise ValueError(f"Invalid value for column_c: {column_c}. Expected 'Yes' or 'No'.")
    if compare_t not in valid_check_values:
        raise ValueError(f"Invalid value for compare_t: {compare_t}. Expected 'Yes' or 'No'.")

    # Read and validate null column configuration
    null_columns = null_check_sheet[f'B{index}'].value
    try:
        null_column_list = json.loads(null_columns)
        if not isinstance(null_column_list, list):
            raise ValueError("Null columns configuration must be a list.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse null columns: {e}")

    # Setup the test results directory
    test_results_dir = os.path.join(config.script_dir, 'TestResults')
    if not os.path.exists(test_results_dir):
        os.makedirs(test_results_dir)

    # Generate timestamp for output file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    output_excel_path = os.path.join(test_results_dir, f'TC_{index}_{timestamp}_comparison_results.xlsx')

    # Perform the checks and write results to the Excel file
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        if null_c == "Yes":
            null_check(source_df, target_df, null_column_list, writer=writer)
        if count_c == "Yes":
            count_check(source_df, target_df, writer=writer)
        if column_c == "Yes":
            columnCount(source_df, target_df, writer=writer)
        if compare_t == "Yes":
            sourceQuery = compare_check_sheet[f'D{index}'].value
            targetQuery = compare_check_sheet[f'E{index}'].value

            # Validate the presence of queries if required
            if sourceQuery != "n/a" and targetQuery != "n/a":
                source_df = ReadData(credential_workbook_name, source_sheet_name, sourceQuery)
                target_df = ReadData(credential_workbook_name, target_sheet_name, targetQuery)
            primaryKey = compare_check_sheet[f'B{index}'].value
            compare_table_columns = compare_check_sheet[f'C{index}'].value

            # Validate and parse compare_table_columns
            try:
                compare_table_columns_list = json.loads(compare_table_columns)
                if not isinstance(compare_table_columns_list, list):
                    raise ValueError("Comparison columns configuration must be a list.")
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to parse comparison columns: {e}")

            compare_tables(source_df, target_df, primaryKey, compare_table_columns_list, writer=writer)
#
# if __name__ == "__main__":
#     main()
