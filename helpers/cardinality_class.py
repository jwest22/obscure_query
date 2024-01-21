import duckdb
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound

from .utility_class import BigQueryHelper
class DuckDBCardinalityIndex:
    def __init__(self, db_path):
        """
        Initialize the CardinalityIndex class, which connects to a DuckDB database to manage cardinality information.

        Args:
            db_path (str): Path to the DuckDB database file.
        """
        
        self.db_path = db_path

    def create_cardinality_table(self):
        """
        Creates a table in the DuckDB database named 'cardinality_index'. This table will store cardinality information 
        for each column in the database, helping to understand the uniqueness of data in each column.

        Returns:
            str: A log message indicating the success or failure of the operation.
        """
        conn = duckdb.connect(database=self.db_path, read_only=False)
        
        log = ""
        
        try:
            create_table_sql = """
                create or replace table cardinality_index as

                    select
                        table_name,
                        column_name,
                        data_type,
                        null::float AS cardinality
                    
                    from information_schema.columns
            """
            
            conn.execute(create_table_sql)
            
            log = log + ("Cardinality table successful")
            
        except Exception as e:
            
            log = log + (f"Cardinality table error: {e}")
            
        conn.commit()
        conn.close()

        return log

    def update_duckdb_table_with_cardinality(self):
        """
        Updates the 'cardinality_index' table with cardinality information for each column in the database.
        Cardinality is calculated as the ratio of distinct values to total non-null values for a given column.

        Returns:
            str: A log message indicating the success or failure of the operation.
        """
        
        conn = duckdb.connect(database=self.db_path, read_only=False)
        
        query = f"select table_name, column_name from cardinality_index"
        
        df = conn.execute(query).fetchdf()
        
        log = ""
        
        try:
            
            for index, row in df.iterrows():
                target_table = row['table_name']
                target_column = row['column_name']

                sql = f"""
                update cardinality_index
                set cardinality = (
                    select count(distinct \"{target_column}\") * 1.0 / count(\"{target_column}\")
                    from {target_table}
                    where \"{target_column}\" is not null
                )
                where table_name = '{target_table}' and column_name = '{target_column}'
                """
                
                conn.execute(sql)

            log = log + ("Update cardinality successful")
            
        except Exception as e:
            
            log = log + (f"Update cardinality error: {e}")

        conn.commit()
        conn.close()
        
        return log

class BigQueryCardinalityIndex:
    def __init__(self, key_path):
        self.key_path = key_path
        self.bigquery_helper = BigQueryHelper(key_path)

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

        dataset_ref = self.bigquery_helper.client.dataset(dataset_id)

        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.bigquery_helper.client.create_table(table, exists_ok=True)

        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if replace else bigquery.WriteDisposition.WRITE_APPEND
        job = self.bigquery_helper.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        log = (f"Cardinality index initialized at {project_id}.{dataset_id}.{table_id}")
        
        return log

    def update_bigquery_table_with_cardinality(self, project_id, dataset_id, table_id):
        query = f"SELECT dataset, table, column FROM `{project_id}.{dataset_id}.{table_id}`"
        df = self.bigquery_helper.client.query(query).result().to_dataframe()

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
            self.bigquery_helper.client.query(sql).result()

        log = (f"Cardinality index at {project_id}.{dataset_id}.{table_id} successfully populated")

        return log
