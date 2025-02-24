from hook.bigquery_hook import BigQueryHook

class BigQueryBase:
    def __init__(self, project_id, dataset_id):
        self.bq_hook = BigQueryHook(project_id, dataset_id)
        self.client = self.bq_hook.get_client()
        self.dataset_ref = self.bq_hook.get_dataset_ref()

    def create_table(self, table_name, schema):
        """Cria uma tabela no BigQuery se não existir."""
        table_ref = f"{self.dataset_ref}.{table_name}"
        table = self.bq_hook.get_table(table_ref, schema=schema)
        self.client.create_table(table, exists_ok=True)
        print(f"Tabela {table_name} criada/verificada com sucesso.")

    def drop_table(self, table_name):
        """Remove uma tabela do BigQuery."""
        query = f"DROP TABLE IF EXISTS `{self.dataset_ref}.{table_name}`"
        self._execute_query(query, f"Tabela {table_name} removida com sucesso.")

    def truncate_table(self, table_name):
        """Trunca uma tabela no BigQuery."""
        query = f"TRUNCATE TABLE `{self.dataset_ref}.{table_name}`"
        self._execute_query(query, f"Tabela {table_name} truncada com sucesso.")

    def insert_data(self, table_name, dataframe):
        """Insere dados em uma tabela do BigQuery."""
        table_ref = f"{self.dataset_ref}.{table_name}"
        job = self.client.load_table_from_dataframe(dataframe, table_ref)
        job.result()
        print(f"Dados inseridos na tabela {table_name}.")

    def list_tables(self):
        """Retorna uma lista com todas as tabelas do dataset no BigQuery."""
        tables = list(self.client.list_tables(self.dataset_ref))
        return [table.table_id for table in tables]

    def _execute_query(self, query, success_message):
        """Executa uma query no BigQuery e exibe mensagem de sucesso."""
        job = self.client.query(query)
        job.result()
        print(success_message)
