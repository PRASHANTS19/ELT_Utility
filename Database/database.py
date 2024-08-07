import pandas as pd
import mysql.connector
from mysql.connector import Error
import math
import warnings
import numpy as np
from openpyxl import load_workbook
import psycopg2
from psycopg2 import OperationalError


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

    def connectDbPostgres(self):
        try:
            self.mydatabase = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database
            )
            self.mycursor = self.mydatabase.cursor()
            if self.mydatabase:
                print("Connected to the database successfully!")
        except OperationalError as e:
            print(f"Error: {e}")
            self.mydatabase = None
            self.mycursor = None

    def is_nan(self, x):
        return isinstance(x, float) and math.isnan(x)
    
    def insertDataToDb(self, df, table_name, create_table_query):
        try:
            if len(df) == 0:
                print("DataFrame is empty. Nothing to insert.")
                return

            self.mycursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.mycursor.execute(create_table_query)

            df = df.where(pd.notnull(df), None)
            # Get column names from DataFrame
            columns = df.columns.tolist()
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"


            # Insert data
            for _, row in df.iterrows():
                values = [row[col] if pd.notnull(row[col]) else None for col in columns]
                self.mycursor.execute(sql, values)

            self.mydatabase.commit()
            print("Data inserted successfully.") 
        except Exception as e:
            print(f"Error inserting data to DB: {e}")

    def readDatabase(self, table_name , query=None)->pd.DataFrame:
        try:
            # Suppress the specific warning from pandas
            df = None
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                # Read data from MySQL into a DataFrame
                if query is not None:
                    df = pd.read_sql(query, con=self.mydatabase)
                else:
                    df = pd.read_sql(f"SELECT * FROM {table_name}", con=self.mydatabase)
                df.replace(to_replace=[None], value=float('nan'), inplace=True)
                return df 
        except Exception as e:
            print(f"Error reading source data from DB: {e}")
            return df

    def closeDb(self):
        try:
            if self.mycursor:
                self.mycursor.close()
            if self.mydatabase:
                self.mydatabase.close()
            print("Database connection closed successfully!")
        except Exception as e:
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
   
    