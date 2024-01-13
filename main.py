import streamlit as st
import pandas as pd
import duckdb
import os

from helpers import SimilarityIndex

def load_csv_to_duckdb(data_dir, db_file_path):
    # Connect to a file-based DuckDB database
    with duckdb.connect(db_file_path) as conn_build:
        # Iterate over all files in the directory
        for filename in os.listdir(data_dir):
            if filename.endswith(".csv"):
                try:
                    # Determine table name (without '.csv')
                    table_name = os.path.splitext(filename)[0]

                    # Create a table in DuckDB from the DataFrame
                    conn_build.execute(f"CREATE TABLE {table_name} AS SELECT * FROM '{data_dir}/{filename}'")

                    # Log successful loading
                    print(f"Successfully loaded {filename} into DuckDB as table {table_name}.")
                except pd.errors.ParserError:
                    print(f"Error: Failed to parse {filename} as CSV.")
                except Exception as e:
                    print(f"Error: An unexpected error occurred while processing {filename}: {e}")

# Specify the path to your DuckDB file
db_file_path = 'demo_data.duckdb'
data_dir = 'data'

# Streamlit UI
st.title('DuckDB SQL Explorer')

if st.button("Build DuckDB DB"):
    load_csv_to_duckdb(data_dir, db_file_path)

conn_query = duckdb.connect(db_file_path)
df = conn_query.execute("select * from information_schema.tables").fetchdf()

# Display schema information
st.subheader('Database Schema')
st.dataframe(df, use_container_width=True, hide_index=True)

# SQL Query Input

st.subheader('Write SQL Query')
sql_query = st.text_area("Enter your SQL query here:", height=100)

# Display Query Results
if st.button('Run Query'):
    try:
        result = conn_query.execute(sql_query).fetchdf()
        st.dataframe(result)
    except Exception as e:
        st.error(f"An error occurred: {e}")
 
if st.button("Compute Similarity Index"):
    df_info_schema_cols = conn_query.execute("select * from information_schema.columns").fetchdf()
    
    db_similarity = SimilarityIndex('demo_data.duckdb')

    k = 128

    similarity_results = db_similarity.compute_similarity_index_for_assets(df_info_schema_cols, k, similarity_threshold=0.8)
    db_similarity.create_similarity_results_table(similarity_results)