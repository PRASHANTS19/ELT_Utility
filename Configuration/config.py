import os

# Setup paths and file names
start_test_case = 2;
end_test_case = 3


script_dir = os.path.dirname(os.path.abspath(__file__))
credential_workbook_name = os.path.join(script_dir, '', 'Credentials.xlsx')
script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'P360_Test'))
source_sheet_name = 'Source'
target_sheet_name = 'Target'
check_sheet_name = 'Checks'
null_check_sheet = 'null_check'
count_check_sheet = 'count_check'
column_check_sheet = 'column_check'
compare_tables_sheet = 'compare_tables'


print(script_dir)
