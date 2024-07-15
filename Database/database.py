import pandas as pd
import mysql.connector
from mysql.connector import Error
import math
import warnings
import numpy as np
from openpyxl import load_workbook



class DB:
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

    def is_nan(self, x):
        return isinstance(x, float) and math.isnan(x)
    
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

    def closeDb(self):
        try:
            if self.mycursor is not None and self.mydatabase.is_connected():
                self.mycursor.close()
                self.mydatabase.close()
                print("Database connection closed.")
        except Error as e:
            print(f"Error closing the database connection: {e}")


    



    
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
   
    