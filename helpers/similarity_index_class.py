from datasketch import MinHash
import pandas as pd
import duckdb
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound

from .utility_class import BigQueryHelper
class DuckDBSimilarityIndex:
    def __init__(self, db_path):
        """
        Initialize the SimilarityIndex class with the path to the DuckDB database.

        Args:
        db_path (str): Path to the DuckDB database file.
        """
        self.db_path = db_path

    def get_column_values(self, table_name, column_name):
        """
        Fetches the values of a specific column from a table in the DuckDB database.

        Args:
        table_name (str): Name of the table.
        column_name (str): Name of the column.

        Returns:
        pandas.Series: Series containing the values from the specified column.
        """
        
        conn = duckdb.connect(self.db_path)
        
        query = f"SELECT {column_name} FROM {table_name}"
        
        try:
            result = conn.execute(query).fetchdf()
            
            return result[column_name]
        
        except Exception as e:
            print(f"An error occurred: {e}")
            
            return pd.Series()
        
        finally:
            conn.close()

    def compute_minhash(self, values, num_perm=128):
        """
        Computes the MinHash of a collection of values.

        Args:
        values (iterable): Iterable of values to compute the MinHash.
        num_perm (int): Number of permutations used in MinHash calculation.

        Returns:
        MinHash: MinHash object representing the MinHash of the given values.
        """
        
        m = MinHash(num_perm=num_perm)
        
        for v in values:
            m.update(str(v).encode('utf8'))
            
        return m

    def compute_similarity_index_minhash(self, minhash1, minhash2):
        """
        Computes the Jaccard similarity index between two MinHash objects.

        Args:
        minhash1 (MinHash): The first MinHash object.
        minhash2 (MinHash): The second MinHash object.

        Returns:
        float: Jaccard similarity index between the two MinHash objects.
        """
        return minhash1.jaccard(minhash2)

    def compute_similarity_index_for_assets(self, dataframe, k, similarity_threshold=0.7):
        """
        Computes similarity indices for all unique pairs of columns in a dataframe that have the same data type and 
        appends only those with a similarity index above a specified threshold.

        Args:
        dataframe (pandas.DataFrame): DataFrame containing 'table_name', 'column_name', and 'data_type' columns.
        k (int): Number of permutations used in MinHash calculation.
        similarity_threshold (float): Minimum similarity index threshold for the results to be appended.

        Returns:
        list of tuples: Each tuple contains (table1, column1, table2, column2, similarity_index).
        """
        filtered_df = dataframe[dataframe['table_name'] != 'similarity_index']
        selected_columns_df = filtered_df[['table_name', 'column_name', 'data_type']]

        column_pairs = []
        for index1, row1 in selected_columns_df.iterrows():
            for index2, row2 in selected_columns_df.iterrows():
                if index1 != index2:
                    table1, col1, type1 = row1['table_name'], row1['column_name'], row1['data_type']
                    table2, col2, type2 = row2['table_name'], row2['column_name'], row2['data_type']

                    if type1 == type2 and table1 != table2 and (table2, col2, table1, col1) not in column_pairs:
                        column_pairs.append((table1, col1, table2, col2))

        similarity_df = []
        for table1, col1, table2, col2 in column_pairs:
            values1 = self.get_column_values(table1, col1) 
            values2 = self.get_column_values(table2, col2)

            minhash1 = self.compute_minhash(values1, num_perm=k)
            minhash2 = self.compute_minhash(values2, num_perm=k)

            similarity_index = self.compute_similarity_index_minhash(minhash1, minhash2)
            if similarity_index >= similarity_threshold:
                similarity_df.append((table1, col1, table2, col2, similarity_index))

        return similarity_df

    def create_similarity_index_table(self, similarity_index):
        """
        Creates a table in the DuckDB database and inserts the similarity results.

        Args:
        similarity_index (list of tuples): The similarity results to be stored, where each tuple is 
                                            (table1, column1, table2, column2, similarity_index).
        """
        conn = duckdb.connect(self.db_path)
        
        log = ""
        
        try:
            create_table_query = """
                create or replace table similarity_index (
                    table1 varchar,
                    column1 varchar,
                    table2 varchar,
                    column2 varchar,
                    similarity_index double
                )
                """
            conn.execute(create_table_query)
        except Exception as e:
            log = log + (f"Similarity index table error: {e}")

        try:
            # Insert the data into the table
            insert_query = "insert into similarity_index (table1, column1, table2, column2, similarity_index) values (?, ?, ?, ?, ?)"
            for result in similarity_index:
                conn.execute(insert_query, result)

            log = log + ("Similarity index table insert successful")
        except Exception as e:
            log = log + (f"Similarity index table insert error: {e}")         
            
        conn.commit()
        conn.close()

        return log

class BigQuerySimilarityIndex:
    
    def __init__(self, key_path):
        self.key_path = key_path
        self.bigquery_helper = BigQueryHelper(key_path)

    def build_bigquery_jaccard(self, project_id, dataset_id, table_id, target_dataset_id, target_table_id, k, replace):
        schema = [
            SchemaField('table_a', 'STRING', mode='REQUIRED'),
            SchemaField('column_a', 'STRING', mode='REQUIRED'),
            SchemaField('table_b', 'STRING', mode='REQUIRED'),
            SchemaField('column_b', 'STRING', mode='REQUIRED'),
            SchemaField('jaccard', 'FLOAT', mode='REQUIRED'),
        ]

        dataset_ref = self.bigquery_helper.client.dataset(dataset_id)
        table_ref = dataset_ref.table(target_table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.bigquery_helper.client.create_table(table, exists_ok=True)

        dataframe = self.bigquery_helper.get_bigquery_table_to_dataframe(dataset_id, table_id)
        results = self.compute_jaccard_index_for_assets(project_id, dataframe, target_dataset_id, k)

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
        job = self.bigquery_helper.client.load_table_from_dataframe(results_df, table_ref, job_config=job_config)
        job.result()
        
        log = (f"Similarity index built at {project_id}.{dataset_id}.{target_table_id}")

        return log

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
                        query_result = self.bigquery_helper.client.query(query).result()
                        jaccard_index = list(query_result)[0].APPROXIMATE_JACCARD_INDEX

                        jaccard_results.append((table1, col1, table2, col2, jaccard_index))

        return jaccard_results
