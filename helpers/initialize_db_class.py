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

class BigQueryHelper:
    def __init__(self, key_path):
        self.key_path = key_path
        self.client = self.create_bigquery_client()

    def create_bigquery_client(self):
        client = bigquery.Client.from_service_account_json(self.key_path)
        return client

    def get_project_id_from_key_file(self):
        with open(self.key_path, 'r') as key_file:
            key_data = json.load(key_file)
            return key_data.get('project_id')

    def get_bigquery_table_to_dataframe(self, dataset_id=None, table_id=None, query=None):
        if query is None:
            query = f"SELECT * FROM `{dataset_id}.{table_id}`"
        query_job = self.client.query(query)
        df = query_job.result().to_dataframe()
        return df

    def get_index_rows(self, dataset_id, dataset_ref):
        rows = []
        tables = self.client.list_tables(dataset_ref)
        for table in tables:
            table_id = table.table_id
            table_ref = dataset_ref.table(table_id)
            table_info = self.client.get_table(table_ref)
            for col in table_info.schema:
                unique_string = f"{dataset_id}_{table_id}_{col.name}"
                unique_id = uuid.uuid5(uuid.NAMESPACE_DNS, unique_string)
                unique_id_str = str(unique_id)
                row = {
                    'uuid': unique_id_str,
                    'dataset': dataset_id,
                    'table': table_id,
                    'column': col.name,
                    'datatype': col.field_type,
                    'cardinality': None
                }
                rows.append(row)
        return rows

    def get_bigquery_assets(self, target_dataset_id=None):
        rows = []
        if target_dataset_id is not None:
            dataset_ref = self.client.dataset(target_dataset_id)
            rows.extend(self.get_index_rows(target_dataset_id, dataset_ref))
        else:
            for dataset in self.client.list_datasets():
                dataset_id = dataset.dataset_id
                dataset_ref = self.client.dataset(dataset_id)
                rows.extend(self.get_index_rows(dataset_id, dataset_ref))

        return pd.DataFrame(rows, columns=['uuid', 'dataset', 'table', 'column', 'datatype', 'cardinality'])
    
    def run_query(self, query=None):
        if query is not None:
            query_job = self.client.query(query)
            df = query_job.result().to_dataframe()
            return df

    def build_bigquery_index(self, project_id, df, dataset_id, table_id, replace):
        df = df[~((df['dataset'] == dataset_id) & (df['table'] == table_id))]
        schema = [
            SchemaField('uuid', 'STRING', mode='REQUIRED'),
            SchemaField('dataset', 'STRING', mode='REQUIRED'),
            SchemaField('table', 'STRING', mode='REQUIRED'),
            SchemaField('column', 'STRING', mode='REQUIRED'),
            SchemaField('datatype', 'STRING', mode='REQUIRED'),
            SchemaField('cardinality', 'FLOAT'),
        ]

        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = self.client.create_dataset(dataset, exists_ok=True)

        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.client.create_table(table, exists_ok=True)

        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if replace else bigquery.WriteDisposition.WRITE_APPEND
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        log = (f"Ori index built at {project_id}.{dataset_id}.{table_id}")
        
        return log

    def update_bigquery_table_with_cardinality(self, project_id, dataset_id, table_id):
        query = f"SELECT dataset, table, column FROM `{project_id}.{dataset_id}.{table_id}`"
        df = self.client.query(query).result().to_dataframe()

        for index, row in df.iterrows():
            target_dataset = row['dataset']
            target_table = row['table']
            target_column = row['column']

            sql = f"""
            UPDATE `{project_id}.{dataset_id}.{table_id}`
            SET Cardinality = (
                SELECT COUNT(DISTINCT {target_column}) / COUNT({target_column})
                FROM `{target_dataset}.{target_table}`
                WHERE {target_column} IS NOT NULL
            )
            WHERE Dataset = '{target_dataset}' AND table = '{target_table}' AND column = '{target_column}'
            """
            self.client.query(sql).result()

        log = (f"Ori index {project_id}.{dataset_id}.{table_id} has been updated with cardinality")

        return log
    
    def build_bigquery_jaccard(self, project_id, dataset_id, table_id, target_dataset_id, target_table_id, replace):
        schema = [
            SchemaField('table_a', 'STRING', mode='REQUIRED'),
            SchemaField('column_a', 'STRING', mode='REQUIRED'),
            SchemaField('table_b', 'STRING', mode='REQUIRED'),
            SchemaField('column_b', 'STRING', mode='REQUIRED'),
            SchemaField('jaccard', 'FLOAT', mode='REQUIRED'),
        ]

        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(target_table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.client.create_table(table, exists_ok=True)

        dataframe = self.get_bigquery_table_to_dataframe(dataset_id, table_id)
        results = self.compute_jaccard_index_for_assets(self.client, project_id, dataframe, target_dataset_id, 100)

        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if replace else bigquery.WriteDisposition.WRITE_APPEND
        results_df = pd.DataFrame(results)
        results_df.columns = [
            'table_a',
            'column_a',
            'table_b',
            'column_b',
            'jaccard'
        ]
        job = self.client.load_table_from_dataframe(results_df, table_ref, job_config=job_config)
        job.result()
        
        log = (f"Ori similarity index built at {project_id}.{dataset_id}.{target_table_id}")

        return log
    
    def build_bigquery_relation_map(self, project_id, dataset_id, index_table_id, jaccard_table_id, target_table_id, sim_threshold=0, replace=False):
        query = f"""
        SELECT 
            CAST(inter_a.uuid AS STRING) as left_uuid,
            CAST(inter_b.uuid AS STRING) as right_uuid,
            inter_a.cardinality as left_card,
            inter_b.cardinality as right_card,
            1 as weight,
            0 as priority
        FROM `{project_id}.{dataset_id}.{index_table_id}` inter_a
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` inter_b
        ON inter_a.table != inter_b.table
        AND inter_a.column = inter_b.column
        AND inter_a.datatype = inter_b.datatype
        UNION ALL
        SELECT
            CAST(jaccard_a.uuid AS STRING) as left_uuid,
            CAST(jaccard_b.uuid AS STRING) as right_uuid,
            jaccard_a.cardinality as left_card,
            jaccard_b.cardinality as right_card,
            jaccard.jaccard as weight,
            1 as priority
        FROM `{project_id}.{dataset_id}.{jaccard_table_id}` jaccard
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` jaccard_a
        ON jaccard.table_a = jaccard_a.table AND jaccard.column_a = jaccard_a.column
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` jaccard_b
        ON jaccard.table_b = jaccard_b.table AND jaccard.column_b = jaccard_b.column
        WHERE jaccard.jaccard > {sim_threshold}
        """

        relation = self.client.query(query).result().to_dataframe()
        
        schema = [
            SchemaField('left_uuid', 'STRING', mode='REQUIRED'),
            SchemaField('right_uuid', 'STRING', mode='REQUIRED'),
            SchemaField('left_card', 'FLOAT', mode='REQUIRED'),
            SchemaField('right_card', 'FLOAT', mode='REQUIRED'),
            SchemaField('weight', 'FLOAT', mode='REQUIRED'),
            SchemaField('priority', 'FLOAT', mode='REQUIRED'),
        ]
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(target_table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.client.create_table(table, exists_ok=replace)
        
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if replace else bigquery.WriteDisposition.WRITE_APPEND
        job = self.client.load_table_from_dataframe(relation, table_ref, job_config=job_config)
        job.result()
        log = (f"ori relation map built at {project_id}.{dataset_id}.{target_table_id}")

        return log
    
    def serialize_bigquery_relation_map(self, project_id, dataset_id, index_table_id, map_table_id):
        index_query = f"""
        SELECT 
            dataset,
            table,
            column,
            datatype
        
        FROM `{project_id}.{dataset_id}.{index_table_id}`
        """   
        
        relation_query = f"""
        SELECT 
            left_node.dataset as dataset_left,
            left_node.table as table_left,
            left_node.column as column_left,
            left_node.datatype as datatype_left,
            IF(map.left_card < 1, 'many','one') AS join_type_left,
            right_node.dataset as dataset_right,
            right_node.table as table_right,
            right_node.column as column_right,
            right_node.datatype as datatype_right,
            IF(map.right_card < 1, 'many','one') AS join_type_right
        
        FROM `{project_id}.{dataset_id}.{map_table_id}` map
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` left_node
        ON map.left_uuid = left_node.uuid
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` right_node
        ON map.right_uuid = right_node.uuid
        """
        index_map = self.client.query(index_query).result().to_dataframe()
        relation_map_enriched = self.client.query(relation_query).result().to_dataframe()
        
        # Create a schema map for tables and columns with data types
        schema_map = {}
        for _, row in index_map.iterrows():
            dataset = row[f'dataset']
            table = row[f'table']
            column = row[f'column']
            datatype = row[f'datatype']
            if dataset not in schema_map:
                schema_map[dataset] = {}
            if table not in schema_map[dataset]:
                schema_map[dataset][table] = {}
            schema_map[dataset][table][column] = datatype

        # Serialize the table schema with data types
        schema_str = "Database Schema Description:\n"
        schema_str += f"Project ID: {project_id}\n"
        for dataset, tables in schema_map.items():
            schema_str += f"Dataset: {dataset}\n"
            for table, columns in tables.items():
                schema_str += f"  Table: {table}\n    Columns:\n"
                for column, datatype in columns.items():
                    schema_str += f"      {column} ({datatype})\n"

        # Serialize the DataFrame to a human-readable schema map
        schema_map_str = "Relations:\n"
        for index, row in relation_map_enriched.iterrows():
            schema_map_str += (
                f"  {row['table_left']}.{row['column_left']} references {row['table_right']}.{row['column_right']} forming a {row['join_type_left']}-to-{row['join_type_right']} relationship between {project_id}.{row['dataset_left']}.{row['table_left']} and {project_id}.{row['dataset_right']}.{row['table_right']}.\n"
            )
            
        return schema_str + schema_map_str

    def compute_jaccard_index_for_assets(self, project_id, dataframe, dataset, k):
        selected_columns_df = dataframe[['table', 'column', 'datatype']]
        column_pairs = set()
        jaccard_results = []

        for index1, row1 in selected_columns_df.iterrows():
            for index2 in range(index1 + 1, len(selected_columns_df)):
                row2 = selected_columns_df.iloc[index2]
                table1, col1, type1 = row1['table'], row1['column'], row1['datatype']
                table2, col2, type2 = row2['table'], row2['column'], row2['datatype']

                # Ensure datatypes are the same and tables are different
                if type1 == type2 and table1 != table2:
                    pair = (table1, col1, table2, col2)

                    # Avoid processing the same pair again
                    if pair not in column_pairs:
                        column_pairs.add(pair)

                        # Query to compute Jaccard index
                        query = f"""
                        WITH minhash_A AS (
                            SELECT DISTINCT FARM_FINGERPRINT(TO_JSON_STRING(t.{col1})) AS h
                            FROM {project_id}.{dataset}.{table1} AS t
                            ORDER BY h
                            LIMIT {k}
                        ),
                        minhash_B AS (
                            SELECT DISTINCT FARM_FINGERPRINT(TO_JSON_STRING(t.{col2})) AS h
                            FROM {project_id}.{dataset}.{table2} AS t
                            ORDER BY h
                            LIMIT {k}
                        )
                        SELECT COUNT(*) / {k} AS APPROXIMATE_JACCARD_INDEX
                        FROM minhash_A
                        INNER JOIN minhash_B ON minhash_A.h = minhash_B.h
                        """
                        query_result = self.client.query(query).result()
                        jaccard_index = list(query_result)[0].APPROXIMATE_JACCARD_INDEX

                        jaccard_results.append((table1, col1, table2, col2, jaccard_index))

        return jaccard_results

    def list_datasets_and_tables(self, project_id):
        """
        Returns a dictionary with datasets as keys and lists of tables as values for the specified project_id.

        :param project_id: The ID of the BigQuery project
        :return: A dictionary where each key is a dataset name and each value is a list of table names
        """
        datasets_and_tables = {}

        # List all datasets in the project
        try:
            datasets = self.client.list_datasets(project=project_id)  # Specify the project
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                full_dataset_id = f"{project_id}.{dataset_id}"  # Construct the full dataset ID
                tables = self.client.list_tables(full_dataset_id)  # List tables in the dataset

                # Collect all table names in the current dataset
                table_names = [table.table_id for table in tables]
                datasets_and_tables[dataset_id] = table_names

        except Exception as e:
            print(f"Error occurred while listing datasets and tables: {e}")
            return {}

        return datasets_and_tables
