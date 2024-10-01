import pandas as pd
from openpyxl import load_workbook
from Utilities.Connection import DB
def ReadData(excel_path: str, sheet_name: str, query=None) -> pd.DataFrame:
    """
    Method to read data from an external source (CSV, Excel, JSON, or database) based on the configuration provided
    in an Excel file.

    1. Loads the specified Excel workbook and sheet.
    2. Validates the necessary cells to determine the data source type and path.
    3. Reads data from the specified source and returns it as a DataFrame.

    Returns:
    - pd.DataFrame or None
        A DataFrame containing the data from the specified source, or None if an error occurs.

    Exceptions Handled:
    - FileNotFoundError: Raised if the Excel file is not found at the specified path.
    - ValueError: Raised for missing or invalid data in the Excel sheet or unsupported data source types.
    - pd.errors.EmptyDataError: Raised when no data is found in the specified file.
    - Exception: Catches any other unexpected errors during execution.

    Owner: Prashant Sahu
    Date: 23-Aug-2024
    """
    try:
        # Load the workbook and check if the sheet exists
        wb = load_workbook(excel_path)
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in the Excel file '{excel_path}'.")
        sheet = wb[sheet_name]

        # Validate and read required cells
        type = sheet['B1'].value
        path = sheet['B2'].value
        dbType = sheet['B4'].value
        if type is None or path is None:
            raise ValueError("The source type or path is not specified in the Excel sheet.")
        type = type.strip()
        path = path.strip()

        df = None

        # Load the data based on the source type
        if type == 'csv':
            df = pd.read_csv(path)
        elif type == 'excel':
            df = pd.read_excel(path)
        elif type == 'json':
            df = pd.read_json(path)
        elif type == 'database':
            # Ensure database fields are populated
            user = sheet['B5'].value
            password = sheet['B6'].value
            host = sheet['B7'].value
            database_name = sheet['B8'].value
            table_name = sheet['B9'].value

            if not all([user, password, host, database_name, table_name]):
                raise ValueError("Incomplete database credentials or table information in the Excel sheet.")

            db = DB(user.strip(), password.strip(), host.strip(), database_name.strip())
            if dbType.strip() == "Mysql":
                db.connectDb()
            elif dbType.strip() == "PostgreSQL":
                db.connectDbPostgres()
            else:
                raise ValueError(f"Unsupported database type: {dbType}")

            df = db.readDatabase(table_name.strip(), query)
            db.closeDb()
        else:
            raise ValueError(f"Unsupported source type: '{type}'.")

        return df

    except FileNotFoundError as e:
        print(f"Error: The file at path '{excel_path}' was not found. {e}")
    except pd.errors.EmptyDataError as e:
        print(f"Error: No data found in the file '{excel_path}'. {e}")
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return None



def read_excel_data(excel_path, sheet_name):
    """
    Method to read and return a specific sheet from an Excel file.

    1. Attempts to load the specified Excel workbook and sheet.
    2. Returns the sheet object if successful.
    3. Handles errors gracefully and provides meaningful messages if the file or sheet is not found.
    Returns:
    - sheet: openpyxl.worksheet.worksheet.Worksheet or None
        The sheet object if found; otherwise, returns None if an error occurs.
    Exceptions Handled:
    - FileNotFoundError: Raised if the Excel file is not found at the specified path.
    - KeyError: Raised if the specified sheet name does not exist in the workbook.
    - Exception: Catches any other unexpected errors during execution.

    Owner: Prashant Sahu
    Date: 23-Aug-2024
    """
    try:
        # Load the workbook from the specified path
        wb = load_workbook(excel_path)
        # Access the specified sheet
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
