import math
import pandas as pd
from mysql.connector import Error
import numpy as np
from openpyxl import load_workbook
from Database.database import DB
import logging

def null_check(source_df, target_df, columns=None, writer=None):
    try:
        # Check if DataFrames are loaded
        if source_df is None or target_df is None:
            logging.error("DataFrames are not loaded.")
            return

        # Replace empty strings with NaN in the specified columns or entire DataFrame
        if columns:
            source_df[columns] = source_df[columns].replace('', np.nan)
            target_df[columns] = target_df[columns].replace('', np.nan)
        else:
            source_df = source_df.replace('', np.nan)
            target_df = target_df.replace('', np.nan)

        # Calculate null counts
        source_null_count = source_df[columns].isnull().sum() if columns else source_df.isnull().sum()
        target_null_count = target_df[columns].isnull().sum() if columns else target_df.isnull().sum()

        print("Null values comparison:")
        print(f"Null count in source table:\n{source_null_count} total: {source_null_count.sum()}")
        print(f"Null count in target table:\n{target_null_count} total: {target_null_count.sum()}")

        if writer:
            # Write the null count comparison to an Excel sheet
            df_null_comparison = pd.DataFrame({
                'Source Null Count': source_null_count,
                'Target Null Count': target_null_count
            })
            df_null_comparison.to_excel(writer, sheet_name='Null Check')

    except KeyError as ke:
        print(f"KeyError during null check: {ke}")
    except Exception as e:
        print(f"Error during null check: {e}")

def count_check(source_df, target_df, writer=None):
    try:
        # Calculate total rows in each DataFrame
        source_df_totalrows = len(source_df)
        target_df_totalrows = len(target_df)

        print(f"Total rows in Source table: {source_df_totalrows}")
        print(f"Total rows in Target table: {target_df_totalrows}")

        if writer:
            # Write the count check to an Excel sheet
            df_count_comparison = pd.DataFrame({
                'Source Rows': [source_df_totalrows],
                'Target Rows': [target_df_totalrows]
            })
            df_count_comparison.to_excel(writer, sheet_name='Count Check', index=False)

    except Exception as e:
        print(f"Error during count check: {e}")


def compare_tables(source_df, target_df, key_column, data_columns, writer=None):
    try:
        # Ensure the specified columns exist in both DataFrames
        columns = [key_column] + data_columns
        missing_columns = [col for col in columns if col not in source_df.columns or col not in target_df.columns]

        if missing_columns:
            raise ValueError(f"Columns {missing_columns} not found in one of the DataFrames.")

        # Fill NaN values with empty strings
        source_df = source_df[columns].fillna('')
        target_df = target_df[columns].fillna('')

        # Create tuples of the specified columns
        source_tuples = set(map(tuple, source_df.to_records(index=False)))
        target_tuples = set(map(tuple, target_df.to_records(index=False)))

        # Find common rows
        common_rows = target_tuples & source_tuples
        common_count = len(common_rows)

        print(f"Number of common rows: {common_count}")
        if common_count > 0:
            print(f"Common rows: {common_rows}")

        # Find different rows
        source_only_rows = source_tuples - target_tuples
        target_only_rows = target_tuples - source_tuples
        source_only_count = len(source_only_rows)
        target_only_count = len(target_only_rows)

        print(f"Number of rows only in source: {source_only_count}")
        if source_only_count > 0:
            print(f"Rows only in source: {source_only_rows}")
        print(f"Number of rows only in target: {target_only_count}")
        if target_only_count > 0:
            print(f"Rows only in target: {target_only_rows}")

        if writer:
            # Write the table comparison to an Excel sheet
            df_common_rows = pd.DataFrame(list(common_rows), columns=columns)
            df_common_rows.to_excel(writer, sheet_name='Common Rows', index=False)

            df_source_only_rows = pd.DataFrame(list(source_only_rows), columns=columns)
            df_source_only_rows.to_excel(writer, sheet_name='Source Only Rows', index=False)

            df_target_only_rows = pd.DataFrame(list(target_only_rows), columns=columns)
            df_target_only_rows.to_excel(writer, sheet_name='Target Only Rows', index=False)

    except ValueError as ve:
        print(f"ValueError during table comparison: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred during table comparison: {e}")


def ReadData(excel_path: str, sheet_name: str, query=None) -> pd.DataFrame:
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


def columnCount(source_df, target_df, writer=None):
    try:
        # Ensure the input dataframes are not None
        if source_df is None or target_df is None:
            raise ValueError("One or both of the input dataframes are None.")

        # Count the number of columns in both dataframes
        source_column_count = len(source_df.columns)
        target_column_count = len(target_df.columns)

        print(f"Total columns in Source: {source_column_count}")
        print(f"Total columns in Target: {target_column_count}")

        if writer:
            # Write the column count comparison to an Excel sheet
            df_column_count_comparison = pd.DataFrame({
                'Source Columns': [source_column_count],
                'Target Columns': [target_column_count]
            })
            df_column_count_comparison.to_excel(writer, sheet_name='Column Count', index=False)
    except ValueError as ve:
        print(f"ValueError during column count check: {ve}")
    except AttributeError as ae:
        print(f"AttributeError during column count check: {ae}. Ensure dataframes are properly loaded.")
    except Exception as e:
        print(f"Unexpected error during column count check: {e}")


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
