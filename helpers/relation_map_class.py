import duckdb
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField

from .utility_class import BigQueryHelper
class DuckDBRelationMap:
    def __init__(self, db_path):
        """
        Initialize the RelationMap class with the path to the DuckDB database.

        Args:
        db_path (str): Path to the DuckDB database file.
        """
        self.db_path = db_path
        
    def create_relation_map(self, index_table_id, similarity_table_id, target_table_id = 'relation_map'):
        """
        Creates a relation map in the DuckDB database. This map represents relationships between tables based on
        column similarities and other criteria.

        Args:
            index_table_id (str): Identifier for the index table used to build relations.
            similarity_table_id (str): Identifier for the similarity table used to build relations.
            target_table_id (str, optional): Name of the target table where the relation map will be stored. 
                                             Defaults to 'relation_map'.

        Returns:
            str: A log message indicating the success or failure of the operation.
        """

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
        """
        Serializes the relation map into a human-readable format, including a description of the database schema
        and the relationships between tables.

        Args:
            map_table_id (str): Identifier for the map table that contains the relationship data.

        Returns:
            str: A string representation of the database schema and the relation map.
        """
        
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

class BigQueryRelationMap:
    def __init__(self, key_path):
        self.key_path = key_path
        self.bigquery_helper = BigQueryHelper(key_path)
        
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

        relation = self.bigquery_helper.client.query(query).result().to_dataframe()
        
        schema = [
            SchemaField('left_uuid', 'STRING', mode='REQUIRED'),
            SchemaField('right_uuid', 'STRING', mode='REQUIRED'),
            SchemaField('left_card', 'FLOAT', mode='REQUIRED'),
            SchemaField('right_card', 'FLOAT', mode='REQUIRED'),
            SchemaField('weight', 'FLOAT', mode='REQUIRED'),
            SchemaField('priority', 'FLOAT', mode='REQUIRED'),
        ]
        dataset_ref = self.bigquery_helper.client.dataset(dataset_id)
        table_ref = dataset_ref.table(target_table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.bigquery_helper.client.create_table(table, exists_ok=replace)
        
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if replace else bigquery.WriteDisposition.WRITE_APPEND
        job = self.bigquery_helper.client.load_table_from_dataframe(relation, table_ref, job_config=job_config)
        job.result()
        log = (f"Relation map built at {project_id}.{dataset_id}.{target_table_id}")

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
            left_node.table as table_name_left,
            left_node.column as column_name_left,
            left_node.datatype as datatype_left,
            IF(map.left_card < 1, 'many','one') AS join_type_left,
            right_node.dataset as dataset_right,
            right_node.table as table_name_right,
            right_node.column as column_name_right,
            right_node.datatype as datatype_right,
            IF(map.right_card < 1, 'many','one') AS join_type_right
        
        FROM `{project_id}.{dataset_id}.{map_table_id}` map
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` left_node
        ON map.left_uuid = left_node.uuid
        INNER JOIN `{project_id}.{dataset_id}.{index_table_id}` right_node
        ON map.right_uuid = right_node.uuid
        """
        index_map = self.bigquery_helper.client.query(index_query).result().to_dataframe()
        relation_map_enriched = self.bigquery_helper.client.query(relation_query).result().to_dataframe()
        
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
