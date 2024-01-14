import streamlit as st
import pandas as pd
import duckdb
import os

from helpers import SimilarityIndex, CardinalityIndex, RelationMap

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
                    conn_build.execute(f"create or replace table {table_name} as select * from '{data_dir}/{filename}'")

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

"---"

conn_query = duckdb.connect(db_file_path)
df = conn_query.execute("select * from information_schema.tables").fetchdf()

# Display schema information
st.subheader('Database Schema')
st.dataframe(df, use_container_width=True, hide_index=True)

# SQL Query Input

st.subheader('Write SQL Query')
sql_query = st.text_area("Enter your SQL query here:")

# Display Query Results
if st.button('Run Query'):
    try:
        result = conn_query.execute(sql_query).fetchdf()
        st.dataframe(result)
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Database utility functions        
"---"
st.write("Utilities")

col1, col2, col3, col4 = st.columns(4, gap="small")

with col1:
    if st.button("Build Database File"):
        load_csv_to_duckdb(data_dir, db_file_path)

with col2:
    if st.button("Build Similarity Index"):
        
        df_info_schema_cols = conn_query.execute("select * from information_schema.columns").fetchdf()
        db_similarity = SimilarityIndex('demo_data.duckdb')

        # Number of minhash functions
        k = 128

        similarity_results = db_similarity.compute_similarity_index_for_assets(df_info_schema_cols, k, similarity_threshold=0.8)
        similarity_index = db_similarity.create_similarity_index_table(similarity_results)

        st.write(similarity_index)

with col3:
    if st.button("Build Cardinality Index"):
        db_cardinality = CardinalityIndex('demo_data.duckdb')
        
        cardinality_index = db_cardinality.create_cardinality_table()
        st.write(cardinality_index)
        
        cardinality_update = db_cardinality.update_duckdb_table_with_cardinality()
        st.write(cardinality_update)
        
with col4:
    if st.button("Build Relation Map"):
        db_relation_map = RelationMap('demo_data.duckdb')
        relation_map = db_relation_map.create_relation_map(index_table_id='cardinality_index', similarity_table_id='similarity_index')
        st.write(relation_map)
