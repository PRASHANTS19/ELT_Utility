import pandas as pd
from Utilities.Check import ReadData, null_check, count_check, compare_tables, columnCount, read_excel_data
from openpyxl import load_workbook
import os
import json
from datetime import datetime

# Setup paths and file names
script_dir = os.path.dirname(os.path.abspath(__file__))
credential_workbook_name = os.path.join(script_dir, 'Credentials', 'Credentials.xlsx')
source_sheet_name = 'Source'
target_sheet_name = 'Target'

# Read source and target data
source_df = ReadData(credential_workbook_name, source_sheet_name)
if source_df is None:
    raise ValueError("Failed to load source data.")

target_df = ReadData(credential_workbook_name, target_sheet_name)
if target_df is None:
    raise ValueError("Failed to load target data.")

# Read the check configuration sheet
check_sheet_name = 'Checks'
check_sheet = read_excel_data(credential_workbook_name, check_sheet_name)

null_c = check_sheet['B2'].value
count_c = check_sheet['B3'].value
column_c = check_sheet['B4'].value
compare_t = check_sheet['B5'].value

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
null_columns = check_sheet['D2'].value
try:
    null_column_list = json.loads(null_columns)
    if not isinstance(null_column_list, list):
        raise ValueError("Null columns configuration must be a list.")
except json.JSONDecodeError as e:
    raise ValueError(f"Failed to parse null columns: {e}")

# Setup the test results directory
test_results_dir = os.path.join(script_dir, 'TestResults')
if not os.path.exists(test_results_dir):
    os.makedirs(test_results_dir)

# Generate timestamp for output file
timestamp = datetime.now().strftime('%Y%m%d_%H%M')
output_excel_path = os.path.join(test_results_dir, f'{timestamp}_comparison_results.xlsx')

# Perform the checks and write results to the Excel file
with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
    if null_c == "Yes":
        null_check(source_df, target_df, null_column_list, writer=writer)
    if count_c == "Yes":
        count_check(source_df, target_df, writer=writer)
    if column_c == "Yes":
        columnCount(source_df, target_df, writer=writer)
    if compare_t == "Yes":
        sourceQuery = check_sheet['E5'].value
        targetQuery = check_sheet['F5'].value

        # Validate the presence of queries if required
        if sourceQuery != "n/a" and targetQuery != "n/a":
            source_df = ReadData(credential_workbook_name, source_sheet_name, sourceQuery)
            target_df = ReadData(credential_workbook_name, target_sheet_name, targetQuery)
        primaryKey = check_sheet['C5'].value
        compare_table_columns = check_sheet['D5'].value

        # Validate and parse compare_table_columns
        try:
            compare_table_columns_list = json.loads(compare_table_columns)
            if not isinstance(compare_table_columns_list, list):
                raise ValueError("Comparison columns configuration must be a list.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse comparison columns: {e}")

        compare_tables(source_df, target_df, primaryKey, compare_table_columns_list, writer=writer)
