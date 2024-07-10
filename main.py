from Utils import Utils
from openpyxl import load_workbook
from ReadData import ReadData

util = Utils()

Credential_workbook_name = 'Credentials.xlsx'
source_sheet_name = 'Source'
target_sheet_name = 'Target'

source_df = ReadData(Credential_workbook_name, source_sheet_name)
if source_df is not None:
    print()
else:
    print("Failed to load data.")

target_df = ReadData(Credential_workbook_name, target_sheet_name)
if target_df is not None:
    print()
else:
    print("Failed to load data.")

# add column name in null check
util.null_check(source_df, target_df, ["empid", "name"]) 
util.count_check(source_df, target_df)

# compare 
util.compare_tables(source_df, target_df, "empid" ,["name"])







