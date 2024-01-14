from datasketch import MinHash
import pandas as pd
import duckdb

class SimilarityIndex:
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
                    table1 VARCHAR,
                    column1 VARCHAR,
                    table2 VARCHAR,
                    column2 VARCHAR,
                    similarity_index DOUBLE
                )
                """
            conn.execute(create_table_query)
        except Exception as e:
            log = log + (f"similarity_index table error: {e}")

        try:
            # Insert the data into the table
            insert_query = "insert into similarity_index (table1, column1, table2, column2, similarity_index) values (?, ?, ?, ?, ?)"
            for result in similarity_index:
                conn.execute(insert_query, result)
                print(f"Inserted {result}")

            log = log + ("similarity_index table insert successful")
        except Exception as e:
            log = log + (f"similarity_index table insert error: {e}")         
            
        conn.commit()
        conn.close()

        return log
