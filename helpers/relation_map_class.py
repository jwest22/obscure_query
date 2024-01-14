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
            cast(inter_a.table_name as string) as table_name_left,
            cast(inter_b.table_name as string) as table_name_right,
            cast(inter_a.column_name as string) as column_name_left,
            cast(inter_b.column_name as string) as column_name_right,
            cast(inter_a.data_type as string) as data_type_left,
            cast(inter_b.data_type as string) as data_type_right,
            inter_a.cardinality as cardinality_left,
            inter_b.cardinality as cardinality_right,
            1 as weight,
            0 as priority
            
        from {index_table_id} inter_a
        inner join {index_table_id} inter_b
            on inter_a.table_name != inter_b.table_name
            and inter_a.column_name = inter_b.column_name
            and inter_a.data_type = inter_b.data_type
            
        where (inter_a.column_name != ('id') and inter_b.column_name != ('id'))
            and (inter_a.column_name not like ('%deleted%') and inter_b.column_name not like ('%deleted%'))
            and inter_a.data_type != 'DOUBLE'
            and inter_b.data_type != 'DOUBLE'
            
        union all
        
        select
            cast(similarity_a.table_name as string) as table_name_left,
            cast(similarity_b.table_name as string) as table_name_right,
            cast(similarity_a.column_name as string) as column_name_left,
            cast(similarity_b.column_name as string) as column_name_right,
            cast(similarity_a.data_type as string) as data_type_left,
            cast(similarity_b.data_type as string) as data_type_right,
            similarity_a.cardinality as cardinality_left,
            similarity_b.cardinality as cardinality_right,
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

    def serialize_relation_map(self, map_table_id):
        
        conn = duckdb.connect(self.db_path)
        
        index_query = f"""
            select 
                table_name,
                column_name,
                data_type
            from information_schema.columns
            
            where table_name not in ('similarity_index','cardinality_index','relation_map')
        """ 
        relation_query = f"""
            select 
                table_name_left,
                column_name_left,
                data_type_left,
                if(cardinality_left < 1, 'many','one') as join_type_left,
                
                table_name_right,
                column_name_right,
                data_type_right,
                if(cardinality_right < 1, 'many','one') as join_type_right
                
            from {map_table_id}
            
            where cardinality_left = 1 or cardinality_right = 1
        """
        
        # Execute queries with DuckDB
        index_map = conn.execute(index_query).fetchdf()
        relation_map_enriched = conn.execute(relation_query).fetchdf()
        
        # Create a schema map for tables and columns with data types
        schema_map = {}
        for _, row in index_map.iterrows():
            table_name = row['table_name']
            column_name = row['column_name']
            data_type = row['data_type']
            if table_name not in schema_map:
                schema_map[table_name] = {}
            schema_map[table_name][column_name] = data_type

        # Serialize the table schema with data types
        schema_str = "## Database Schema Description\n"
        for table_name, columns in schema_map.items():
            schema_str += f"### Table: {table_name}\n#### Columns:\n"
            for column_name, data_type in columns.items():
                schema_str += f"- **{column_name}**: *{data_type}*\n"

        # Serialize the DataFrame to a human-readable schema map
        schema_map_str = "## Relations\n"
        for index, row in relation_map_enriched.iterrows():
            schema_map_str += (
                f"- **{row['table_name_left']}.{row['column_name_left']}** references **{row['table_name_right']}.{row['column_name_right']}** forming a **{row['join_type_left']}**-to-**{row['join_type_right']}** relationship.\n"
            )
            
        return schema_str + schema_map_str
