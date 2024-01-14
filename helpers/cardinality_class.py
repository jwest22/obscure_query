import duckdb
import pandas as pd

class CardinalityIndex:
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
