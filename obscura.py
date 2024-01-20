import streamlit as st
import pandas as pd
import duckdb
import os
import datetime

from streamlit import session_state as state
from helpers import SimilarityIndex, CardinalityIndex, RelationMap, callOpenAI, CSVLoaderToDuckDB, BigQueryHelper

# Initializing session state values for persistence between application reruns

if 'database_select' not in state:
    state.database_select = None
    
if 'database_source' not in state:
    state.database_source = None

if 'database_schema' not in state:
    state.database_schema = None
    
if 'database_connection' not in state:
    state.database_connection = None
    
if 'database_path' not in state:
    state.database_path = None

if 'relation_map' not in state:
    state.relation_map = None
    
if 'question' not in state:
    state.question = None
        
if 'openai_api_key' not in state:
    state.openai_api_key = None
    
if 'openai_response' not in state:
    state.openai_response = None

# Streamlit UI
st.title('Obscura Pro Machina')
"---"

state.database_select = st.selectbox(label='Select your database', options=('DuckDB','BigQuery'))

if state.database_select == 'DuckDB' and state.database_source != 'DuckDB':
    state.database_path = st.text_input('DuckDB File Path',value='demo_data.duckdb')
    conn_query = duckdb.connect(state.database_path)
    state.database_schema = conn_query.execute("select * from information_schema.tables").fetchdf()
    state.database_source = 'DuckDB'
    conn_query.commit()
    conn_query.close()
    
if state.database_select == 'BigQuery' and state.database_source != 'BigQuery':
    key_path = ".secrets\.bigquery-key.json"
    state.database_connection = BigQueryHelper(key_path)
    project_id = state.database_connection.get_project_id_from_key_file()
    datasets_and_tables = state.database_connection.list_datasets_and_tables(project_id)
    dataset_table_pairs = [(dataset, table) for dataset, tables in datasets_and_tables.items() for table in tables]
    state.database_schema = pd.DataFrame(dataset_table_pairs, columns=['Dataset', 'Table'])
    state.database_source = 'BigQuery'

# Display schema information
st.subheader('Database Schema')
st.dataframe(state.database_schema, use_container_width=True, hide_index=True)

# SQL Query Input

st.subheader('Write SQL Query')
sql_query = st.text_area("Enter your SQL query here:")

# Display Query Results
if st.button('Run Query'):
        try:
            if state.database_source == 'DuckDB':
                conn_query = duckdb.connect(state.database_path)
                result = conn_query.execute(sql_query).fetchdf()
                conn_query.commit()
                conn_query.close()
            elif state.database_source == 'BigQuery':
                result = state.database_connection.run_query(sql_query)
            st.dataframe(result)
        except Exception as e:
            st.error(f"An error occurred: {e}")

# Database utility functions        
"---"
st.subheader("Utilities")

col1, col2, col3, col4 = st.columns(4, gap="small")

with col1:
    if state.database_source == 'DuckDB':
        if st.button("Build Database File"):
            data_dir = 'data'
            loader = CSVLoaderToDuckDB(data_dir, state.database_path)
            loader.load_csv_files()

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
