import duckdb
import pandas as pd

class CardinalityIndex:
    def __init__(self, db_path):
        self.conn = duckdb.connect(database=db_path, read_only=False)

    def create_cardinality_table(self):
        try:
            create_table_sql = """
                create table cardinality_index as

                    select
                        table_name,
                        column_name,
                        data_type,
                        null::float AS cardinality
                    
                    from information_schema.columns
            """
            self.conn.execute(create_table_sql)
            log = ("Successfully built cardinality_index.")
        except Exception as e:
            log = (f"Error: An unexpected error occurred while creating the cardinality_index: {e}")

        return log

    def update_duckdb_table_with_cardinality(self):
        query = f"select table_name, column_name from cardinality_index"
        df = self.conn.execute(query).fetchdf()

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
                self.conn.execute(sql)

            log = ("cardinality_index has been updated with cardinality")
        except Exception as e:
            log = (f"Error: An unexpected error occurred while updating the cardinality_index: {e}")

        return log
