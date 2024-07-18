import pandas as pd
from Utilities.Check import ReadData, null_check, count_check, compare_tables, columnCount, read_excel_data
from openpyxl import load_workbook
import os
import json

script_dir = os.path.dirname(os.path.abspath(__file__))
credential_workbook_name = os.path.join(script_dir, 'Credentials', 'Credentials.xlsx')
source_sheet_name = 'Source'
target_sheet_name = 'Target'

source_df = ReadData(credential_workbook_name, source_sheet_name)
if source_df is None:
    print("Failed to load source data.")

target_df = ReadData(credential_workbook_name, target_sheet_name)
if target_df is None:
    print("Failed to load target data.")


check_sheet_name = 'Checks'
check_sheet = read_excel_data(credential_workbook_name, check_sheet_name)

null_c = check_sheet['B2'].value
count_c = check_sheet['B3'].value
column_c = check_sheet['B4'].value
compare_t = check_sheet['B5'].value

null_columns = check_sheet['D2'].value
# print("null_column is: ", type(null_column), null_column)
null_column_list = json.loads(null_columns)
# print("Converted null_column is: ", type(null_column_list), null_column_list)

test_results_dir = os.path.join(script_dir, 'TestResults')
if not os.path.exists(test_results_dir):
    os.makedirs(test_results_dir)

output_excel_path = os.path.join(test_results_dir, 'comparison_results.xlsx')
# add column name in null check
with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
    # Perform the checks and write results to the Excel file
    if null_c == "Yes":
        null_check(source_df, target_df, null_column_list, writer=writer)
    if count_c == "Yes":
        count_check(source_df, target_df, writer=writer)
    if column_c == "Yes":
        columnCount(source_df, target_df, writer=writer)
    if compare_t == "Yes":
        primaryKey = check_sheet['C5'].value
        compare_table_columns = check_sheet['D5'].value
        compare_table_columns_list = json.loads(compare_table_columns)
        compare_tables(source_df, target_df, primaryKey, compare_table_columns_list, writer=writer)




