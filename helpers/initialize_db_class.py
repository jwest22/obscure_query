import os
import duckdb
import pandas as pd
import streamlit as st
import pandas as pd
import uuid
import json
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
class CSVLoaderToDuckDB:
    def __init__(self, data_dir, db_file_path):
        self.data_dir = data_dir
        self.db_file_path = db_file_path

    def connect_db(self):
        """ Connect to a file-based DuckDB database. """
        try:
            self.conn_build = duckdb.connect(self.db_file_path)
            return True
        except Exception as e:
            st.write(f"Error: Unable to connect to database: {e}")
            return False

    def load_csv_files(self):
        """ Load all CSV files from the directory into the DuckDB database. """
        if self.connect_db():
            try:
                # Iterate over all files in the directory
                for filename in os.listdir(self.data_dir):
                    if filename.endswith(".csv"):
                        self.load_csv_file(filename)
            finally:
                self.conn_build.close()

    def load_csv_file(self, filename):
        """ Load a single CSV file into the DuckDB database. """
        try:
            # Determine table name (without '.csv')
            table_name = os.path.splitext(filename)[0]

            # Create a table in DuckDB from the DataFrame
            self.conn_build.execute(f"create or replace table {table_name} as select * from '{self.data_dir}/{filename}'")

            # Log successful loading
            st.write(f"Successfully loaded {filename} into DuckDB as table {table_name}.")
        except pd.errors.ParserError:
            st.write(f"Error: Failed to parse {filename} as CSV.")
        except Exception as e:
            st.write(f"Error: An unexpected error occurred while processing {filename}: {e}")
