import pandas as pd
from Utilities.Check import null_check, count_check, compare_tables, columnCount
from Utilities.Utils import ReadData, read_excel_data
from openpyxl import load_workbook
import json
import config

# Use variables from config.py
check_sheet = read_excel_data(config.credential_workbook_name, config.check_sheet_name)

null_c = check_sheet['B2'].value
count_c = check_sheet['B3'].value
column_c = check_sheet['B4'].value
compare_t = check_sheet['B5'].value
null_columns = check_sheet['D2'].value

def run_main():
    import main  # Delayed import to avoid circular import
    main.main()

if __name__ == "__main__":
    run_main()
