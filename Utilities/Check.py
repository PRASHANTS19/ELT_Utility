import math
import pandas as pd
from mysql.connector import Error
import numpy as np
from openpyxl import load_workbook
from Database.database import DB
import logging

def null_check(source_df, target_df, columns=None, writer=None):
    """
    Method to perform null check comparison between source and target DataFrames.

    1. Checks if the DataFrames are loaded.
    2. Replaces empty strings with NaN in the specified columns or entire DataFrame.
    3. Calculates and prints the null counts for source and target DataFrames.
    4. Optionally writes the null count comparison to an Excel sheet.
    Raises:
    - KeyError: If specified columns are not found in the DataFrame.
    - Exception: For any other general errors during execution.

    Owner: Prashant Sahu
    Date: 23-Aug-2024
    """
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
    """
    Method to compare the total row counts between source and target DataFrames.

    1. Calculates the total number of rows in the source and target DataFrames.
    2. Prints the total row count for both DataFrames.
    3. Optionally writes the row count comparison to an Excel sheet.
    Raises:
    - Exception: Catches any general errors during execution.

    Owner: Prashant Sahu
    Date: 23-Aug-2024
    """
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

def columnCount(source_df, target_df, writer=None):
    """
    Method to compare the number of columns between source and target DataFrames.

    1. Ensures that both input DataFrames are not None.
    2. Counts the number of columns in the source and target DataFrames.
    3. Prints the total column count for both DataFrames.
    4. Optionally writes the column count comparison to an Excel sheet.
    Raises:
    - ValueError: If one or both of the input DataFrames are None.
    - AttributeError: If DataFrames are not properly loaded or do not have columns attribute.
    - Exception: Catches any other unexpected errors during execution.

    Owner: Prashant Sahu
    Date: 23-Aug-2024
    """
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

def compare_tables(source_df, target_df, key_column, data_columns, writer=None):
    """
    Method to compare rows between source and target DataFrames based on a key column and specific data columns.

    1. Ensures that the specified key column and data columns exist in both DataFrames.
    2. Fills NaN values with empty strings in the specified columns.
    3. Creates tuples of the specified columns for comparison.
    4. Finds and prints the number of common rows, rows only in the source, and rows only in the target.
    5. Optionally writes the comparison results to an Excel sheet, including common rows, source-only rows, and target-only rows.
    Raises:
    - ValueError: If the specified columns are not found in one or both DataFrames.
    - Exception: Catches any other unexpected errors during execution.

    Owner: Prashant Sahu
    Date: 23-Aug-2024
    """
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





