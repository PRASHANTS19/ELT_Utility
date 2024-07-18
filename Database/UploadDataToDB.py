import pandas as pd
import os
from database import DB
from openpyxl import load_workbook

# relative paths
from Utilities.Check import ReadData

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
excel_path = os.path.join(parent_dir, 'Credentials', 'Credentials.xlsx')
# excel_path = 'Credentials.xlsx'
sheet_name = 'UploadDataToDB'

# Function to read data from Excel sheet and handle potential errors
def read_excel_data(excel_path, sheet_name):
    try:
        wb = load_workbook(excel_path)
        sheet = wb[sheet_name]
        return sheet
    except FileNotFoundError:
        print(f"Error: The file '{excel_path}' was not found.")
        return None
    except KeyError:
        print(f"Error: The sheet '{sheet_name}' does not exist in the workbook.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading the Excel file: {e}")
        return None

# Read Excel data
sheet = read_excel_data(excel_path, sheet_name)

if sheet:
    try:
        df = ReadData(excel_path, sheet_name)

        user = sheet['B5'].value
        password = sheet['B6'].value
        host = sheet['B7'].value
        database_name = sheet['B8'].value
        table_name = sheet['B9'].value
        create_table_query = sheet['B10'].value

        utils = DB(user, password, host, database_name)
        
        # Try to connect to the database
        try:
            utils.connectDb()
        except Exception as e:
            print(f"Error while connecting to the database: {e}")
            utils = None

        # Insert data into the database if connection was successful
        if utils:
            try:
                df = utils.insertDataToDb(df, table_name, create_table_query)
            except Exception as e:
                print(f"Error while inserting data into the database: {e}")

            # Close the database connection
            try:
                utils.closeDb()
            except Exception as e:
                print(f"Error while closing the database connection: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
