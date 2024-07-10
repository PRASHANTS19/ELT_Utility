import pandas as pd
import mysql.connector
from mysql.connector import Error
import math
import warnings
import numpy as np




class Utils:
    def __init__(self, user=None, password=None, host=None, database_name=None):
        self.user = user
        self.password = password
        self.host = host
        self.database = database_name
        self.mycursor = None
        self.mydatabase = None

    def connectDb(self):
        try:
            self.mydatabase = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database
            )
            self.mycursor = self.mydatabase.cursor()
            if self.mydatabase.is_connected():
                print("Connected to the database successfully!")
        except Error as e:
            print(f"Error: {e}")
            self.mydatabase = None
            self.mycursor = None

    def insertDataToDb(self, df):
        try:
            if len(df) == 0:
                print("DataFrame is empty. Nothing to insert.")
                return

            self.mycursor.execute("DROP TABLE IF EXISTS employees")
            self.mycursor.execute("CREATE TABLE employees (Id INT AUTO_INCREMENT PRIMARY KEY, empid INT, name VARCHAR(255), designation VARCHAR(255))")

            df = df.where(pd.notnull(df), None)
            for _, row in df.iterrows():
                empid = row['empid'] if pd.notnull(row['empid']) else None
                name = row['name'] if pd.notnull(row['name']) else None
                designation = row['designation'] if pd.notnull(row['designation']) else None
                
                self.mycursor.execute("INSERT INTO employees (empid, name, designation) VALUES (%s, %s, %s)", (empid, name, designation))

            self.mydatabase.commit()
            print("Data inserted successfully.") 
        except Exception as e:
            print(f"Error inserting data to DB: {e}")

    def readDatabase(self, table_name)->pd.DataFrame:
        try:
            # Suppress the specific warning from pandas
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                # Read data from MySQL into a DataFrame
                df = pd.read_sql(f"SELECT * FROM {table_name}", con=self.mydatabase)
                df.replace(to_replace=[None], value=float('nan'), inplace=True)
                return df 
        except Exception as e:
            print(f"Error reading source data from DB: {e}")
            return df

    @staticmethod
    def null_check(source_df, target_df, columns=None):
        try:
            if source_df is None or target_df is None:
                print("DataFrames are not loaded.")
                return
            if columns:
                source_df[columns] = source_df[columns].replace('', np.nan)
                target_df[columns] = target_df[columns].replace('', np.nan)
            else:
                source_df = source_df.replace('', np.nan)
                target_df = target_df.replace('', np.nan)
            print("Null values comparison:")
            source_null_count = source_df[columns].isnull().sum() if columns else source_df.isnull().sum()
            target_null_count = target_df[columns].isnull().sum() if columns else target_df.isnull().sum()

            print(f"Null count in source table:\n {source_null_count} total: {source_null_count.sum()}")
            print(f"Null count in target table:\n {target_null_count}  total: {target_null_count.sum()}")
        except Exception as e:
            print(f"Error during null check: {e}")

    @staticmethod
    def count_check(source_df, target_df):
        try:
            source_df_totalrows = len(source_df)
            target_df_totalrows = len(target_df)

            print(f"Total rows in Source table:\n {source_df_totalrows}")
            print(f"Total rows in target table:\n {target_df_totalrows}")
        except Exception as e:
            print(f"Error during count check: {e}")
        
    def is_nan(self, x):
        return isinstance(x, float) and math.isnan(x)
    
    @staticmethod
    def compare_tables(source_df, target_df, key_column, data_columns):
        try:
            # Ensure the specified columns exist in both DataFrames
            columns = [key_column] + data_columns
            for col in columns:
                if col not in source_df.columns or col not in target_df.columns:
                    print(f"Column {col} not found in one of the DataFrames.")
                    return

            # Fill NaN values with empty strings
            source_df = source_df[columns].fillna('')
            target_df = target_df[columns].fillna('')
            # print("source df is: ", source_df)
            # print("Target df is: ", target_df)

            # Create tuples of the specified columns
            source_tuples = set(map(tuple, source_df.to_records(index=False)))
            target_tuples = set(map(tuple, target_df.to_records(index=False)))

            # print("source tuple is: ", source_tuples)
            # print("Target tuple is: ", target_tuples)

            # Find common rows
            common_rows = target_tuples & source_tuples
            common_count = len(common_rows)

            print(f"Number of common rows: {common_count}")
            if common_count > 0:
                print(f"Common rows: {common_rows}")
        except Exception as e:
            print(f"Error during table comparison: {e}")


    def closeDb(self):
        try:
            if self.mycursor is not None and self.mydatabase.is_connected():
                self.mycursor.close()
                self.mydatabase.close()
                print("Database connection closed.")
        except Error as e:
            print(f"Error closing the database connection: {e}")

    @staticmethod
    def columnCount(source_df, target_df):
        print("Total Coumns in Source is: ", len(source_df.columns))
        print("Total Coumns in Target is: ", len(target_df.columns))



    
    # def compare_tables(self):
        # try:
        #     target_tuples = list(self.target_df.to_records(index=False)) 
        #     source_tuples = list(self.source_df.to_records(index=False))

        #     differce_count = 0
        #     min_length = min(len(source_tuples), len(target_tuples))
        #     for i in range(min_length):
        #         target_row = target_tuples[i]
        #         source_row = source_tuples[i]

        #         for j in range(len(target_row)):
        #             if (self.is_nan(target_row[j]) and self.is_nan(source_row[j])):
        #                 continue
        #             elif target_row[j] != source_row[j]:
        #                 differce_count += 1
        #                 break

        #     differce_count += max(len(source_tuples), len(target_tuples)) - min_length

        #     print(f"Number of differing rows: {differce_count}")
        # except Exception as e:
        #     print(f"Error during table comparison: {e}")
   
    