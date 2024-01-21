import pandas as pd
import uuid
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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

    def get_bigquery_table_to_dataframe(self, dataset_id, table_id):
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
