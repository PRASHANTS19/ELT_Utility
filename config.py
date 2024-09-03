import os

# Setup paths and file names
script_dir = os.path.dirname(os.path.abspath(__file__))
credential_workbook_name = os.path.join(script_dir, 'Credentials', 'Credentials.xlsx')
source_sheet_name = 'Source'
target_sheet_name = 'Target'
check_sheet_name = 'Checks'
