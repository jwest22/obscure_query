import streamlit as st
import pandas as pd
import duckdb
import os
import datetime

from streamlit import session_state as state
from helpers import SimilarityIndex, CardinalityIndex, RelationMap, callOpenAI

if 'relation_map' not in state:
    state.relation_map = None
    
if 'question' not in state:
    state.question = None
        
if 'openai_api_key' not in state:
    state.openai_api_key = None
    
if 'openai_response' not in state:
    state.openai_response = None

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
                    st.write(f"Successfully loaded {filename} into DuckDB as table {table_name}.")
                except pd.errors.ParserError:
                    st.write(f"Error: Failed to parse {filename} as CSV.")
                except Exception as e:
                    st.write(f"Error: An unexpected error occurred while processing {filename}: {e}")

# Specify the path to your DuckDB file
db_file_path = 'demo_data.duckdb'
data_dir = 'data'

# Streamlit UI
st.title('Obscura Pro Machina')
"---"

conn_query = duckdb.connect(db_file_path)
df = conn_query.execute("select * from information_schema.tables").fetchdf()
conn_query.commit()
conn_query.close()

# Display schema information
st.subheader('Database Schema')
st.dataframe(df, use_container_width=True, hide_index=True)

# SQL Query Input

st.subheader('Write SQL Query')
sql_query = st.text_area("Enter your SQL query here:")

# Display Query Results
if st.button('Run Query'):
    try:
        conn_query = duckdb.connect(db_file_path)
        result = conn_query.execute(sql_query).fetchdf()
        st.dataframe(result)
        conn_query.commit()
        conn_query.close()
        
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Database utility functions        
"---"
st.subheader("Utilities")

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

"---"
st.subheader("Relation Map")

if st.button("Serialize Relaion Map") or state.relation_map:
    
    db_relation_map = RelationMap('demo_data.duckdb')
    state.relation_map = db_relation_map.serialize_relation_map('relation_map')
        
    download_map = state.relation_map.replace('#','').replace('*','')
        
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
    file = f'relation_map_{current_time}.txt'
        
    st.download_button("Download Relation Map", data=download_map, file_name=file, mime='text')

    with st.expander('Relation Map'):
            st.markdown(state.relation_map,unsafe_allow_html=True)

    state.openai_api_key = st.text_input("Enter your OpenAI API key", type="password")

    if state.openai_api_key:
        # Set the API key as an environment variable
        os.environ['OPENAI_API_KEY'] = state.openai_api_key
        st.write('OpenAI API Key set!')
    
    state.question = st.text_input("Ask a question!")

    if state.openai_api_key and state.relation_map and state.question:
        api_call = callOpenAI()
        state.openai_response = api_call.api_call_query(state.relation_map, state.question)

        for i, choice in enumerate(state.openai_response.choices):
            query = choice.message.content
            st.write("SQL Query:")
            st.code(query, language="SQL")
