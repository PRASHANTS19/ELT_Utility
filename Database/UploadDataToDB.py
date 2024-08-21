import logging
import os
from database import DB
from openpyxl import load_workbook
from Utilities.Check import ReadData

# Set up logging
logging.basicConfig(level=logging.INFO)

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
excel_path = os.path.join(parent_dir, 'Credentials', 'Credentials.xlsx')
sheet_name = 'UploadDataToDB'

def read_excel_data(excel_path, sheet_name):
    try:
        wb = load_workbook(excel_path)
        sheet = wb[sheet_name]
        return sheet
    except FileNotFoundError as e:
        logging.error(f"File '{excel_path}' was not found. Exception: {e}")
    except KeyError as e:
        logging.error(f"Sheet '{sheet_name}' does not exist. Exception: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while reading Excel file: {e}")
    return None

sheet = read_excel_data(excel_path, sheet_name)

if sheet:
    try:
        df = ReadData(excel_path, sheet_name)
        user, password, host, database_name, table_name, create_table_query = (
            sheet['B5'].value, sheet['B6'].value, sheet['B7'].value, sheet['B8'].value,
            sheet['B9'].value, sheet['B10'].value
        )

        utils = DB(user, password, host, database_name)

        try:
            utils.connectDb()
            df = utils.insertDataToDb(df, table_name, create_table_query)
        except Exception as e:
            logging.error(f"Error during database operation: {e}")
        finally:
            try:
                utils.closeDb()
            except Exception as e:
                logging.error(f"Error while closing the database: {e}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
