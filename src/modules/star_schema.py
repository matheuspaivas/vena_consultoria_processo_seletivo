import pandas as pd
from modules.bigquery_base import BigQueryBase
from modules.storage_base import StorageBase

class StarSchemaBuilder:
    def __init__(self, project_id, dataset_id, bucket_name):
        self.bigquery = BigQueryBase(project_id, dataset_id)
        self.storage = StorageBase(project_id, bucket_name)

    def build_star_schema(self):
        """Lê arquivos Parquet do bucket e cria o star-schema no BigQuery."""
        print("Listando arquivos .parquet no bucket processed/...")
        files = self.storage.list_files(prefix="processed/", file_format=".parquet")

        if not files:
            print("Nenhum arquivo encontrado.\n")
            return
        
        for file in files:
            print({file})
            
        print(f"\n")

        df = pd.concat([self.storage.read_file_from_bucket(file) for file in files], ignore_index=True)
        print(f"Tamanho do DataFrame consolidado: {df.shape}\n")
        print(f"\n---------------------------------------\n")

        self._create_dimensions_and_fact(df)

    def _create_dimensions_and_fact(self, df):
        """Cria as dimensões e a tabela fato no BigQuery."""
        dim_conta = self._generate_dimension(df, "Nome", "dim_conta", "IdConta")
        dim_unidade = self._generate_dimension(df, "Unidade", "dim_unidade", "IdUnidade")
        dim_tipo = self._generate_dimension(df, "Tipo", "dim_tipo", "IdTipo")
        dim_calendario = self._create_dim_calendario(df)

        self._create_fact_table(df, dim_conta, dim_unidade, dim_tipo, dim_calendario)

        print("Star Schema carregado com sucesso!\n\n")

    def _generate_dimension(self, df, column_name, table_name, id_column):
        """Gera e insere uma dimensão incremental no BigQuery."""
        print(f"Criando Dimensão: {table_name}.")
        unique_values = df[[column_name]].drop_duplicates().reset_index(drop=True)
        unique_values.insert(0, id_column, range(1, len(unique_values) + 1))

        self.bigquery.truncate_table(table_name)
        self.bigquery.insert_data(table_name, unique_values)

        print(f"Dimensão {table_name} criada com sucesso.")
        print(f"\n---------------------------------------\n")
        return unique_values

    def _create_dim_calendario(self, df):
        """Cria a dimensão Calendário."""
        print(f"Criando Dimensão: dim_calendario.")
        dim_calendario = df[["DataRef"]].drop_duplicates().copy()
        dim_calendario["DiaDaSemana"] = pd.to_datetime(dim_calendario["DataRef"]).dt.dayofweek
        dim_calendario["Mes"] = pd.to_datetime(dim_calendario["DataRef"]).dt.month
        dim_calendario["Ano"] = pd.to_datetime(dim_calendario["DataRef"]).dt.year
        dim_calendario.insert(0, "IdCalendario", range(1, len(dim_calendario) + 1))

        self.bigquery.truncate_table("dim_calendario")
        self.bigquery.insert_data("dim_calendario", dim_calendario)

        print(f"Dimensão dim_calendário criada com sucesso.")
        print(f"\n---------------------------------------\n")
        return dim_calendario

    def _create_fact_table(self, df, dim_conta, dim_unidade, dim_tipo, dim_calendario):
        """Cria a tabela fato a partir das dimensões."""
        print(f"Criando da Tabela Fato.")
        df_fato = df.merge(dim_conta, on="Nome").merge(dim_unidade, on="Unidade").merge(dim_tipo, on="Tipo").merge(dim_calendario, on="DataRef")

        df_fato = df_fato[["IdConta_y", "IdUnidade", "IdTipo", "IdCalendario", "DataRef", "Valor"]]
        df_fato = df_fato.rename(columns={"IdConta_y": "IdConta"})

        print(f"Tamanho do DataFrame da Tabela Fato: {df_fato.shape}")

        self.bigquery.truncate_table("fato")
        self.bigquery.insert_data("fato", df_fato)

        print("Fato criada com sucesso.\n")
        print(f"\n---------------------------------------\n")
