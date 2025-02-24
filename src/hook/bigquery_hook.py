from google.cloud import bigquery

class BigQueryHook:
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)

    def get_client(self):
        """Retorna o cliente BigQuery."""
        return self.client

    def get_dataset_ref(self):
        """Retorna a referência do dataset."""
        return f"{self.project_id}.{self.dataset_id}"
    
    def get_table(self, table_ref, schema):
        """Retorna a tabela no BigQuery."""
        return bigquery.Table(table_ref, schema)
