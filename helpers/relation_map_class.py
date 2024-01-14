import duckdb

class RelationMap:
    def __init__(self, db_path):
        """
        Initialize the RelationMap class with the path to the DuckDB database.

        Args:
        db_path (str): Path to the DuckDB database file.
        """
        self.db_path = db_path
        
    def create_relation_map(self, index_table_id, similarity_table_id, target_table_id = 'relation_map'):

        conn = duckdb.connect(self.db_path)

        query = f"""
        select 
            cast(inter_a.table_name as string) as left_table_name,
            cast(inter_b.table_name as string) as right_table_name,
            cast(inter_a.column_name as string) as left_column_name,
            cast(inter_b.column_name as string) as right_column_name,
            inter_a.cardinality as left_card,
            inter_b.cardinality as right_card,
            1 as weight,
            0 as priority
            
        from {index_table_id} inter_a
        inner join {index_table_id} inter_b
            on inter_a.table_name != inter_b.table_name
            and inter_a.column_name = inter_b.column_name
            and inter_a.data_type = inter_b.data_type
            
        where inter_a.column_name != ('id') and inter_b.column_name != ('id')
            
        union all
        
        select
            cast(similarity_a.table_name as string) as left_table_name,
            cast(similarity_b.table_name as string) as right_table_name,
            cast(similarity_a.column_name as string) as left_column_name,
            cast(similarity_b.column_name as string) as right_column_name,
            similarity_a.cardinality as left_card,
            similarity_b.cardinality as right_card,
            similarity.similarity_index as weight,
            1 as priority
            
        from {similarity_table_id} similarity
        inner join {index_table_id} similarity_a
            on similarity.table1 = similarity_a.table_name and similarity.column1 = similarity_a.column_name
        inner join {index_table_id} similarity_b
            on similarity.table2 = similarity_b.table_name and similarity.column2 = similarity_b.column_name
            
        where similarity_a.cardinality >= 0.5
            and similarity_b.cardinality >= 0.5
            and similarity_a.column_name != ('id') and similarity_b.column_name != ('id')

        """
        log = ""
        
        try:
            relation = conn.execute(query).fetchdf()

            # Handling the creation of a new table or appending to an existing one
            
            conn.register('relation_df', relation)
            conn.execute(f"create or replace table {target_table_id} as select * from relation_df")

            log = log + ("Relation map successful")
        
        except Exception as e:
            log = log + (f"Relation map error: {e}")
        
        return log
