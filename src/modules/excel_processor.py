import pandas as pd
from modules.storage_base import StorageBase

class ExcelProcessor:
    def __init__(self, project_id, bucket_name):
        self.storage = StorageBase(project_id, bucket_name)

    def process_file(self, folder_prefix):
        """Lê, transforma e retorna uma lista de DataFrames processados para todos os arquivos no bucket."""
        print("Listando arquivos .xlsx no bucket rawdata/...")
        files = self.storage.list_files(prefix=folder_prefix, file_format=".xlsx")

        for file in files:
            print({file})

        print(f"\n---------------------------------------\n")
        
        if not files:
            print("Nenhum arquivo .xlsx encontrado para processamento.\n")

        for file_name in files:
            print(f"Processando: {file_name}")
            xls = self.storage.read_file_from_bucket(file_name)
            if not isinstance(xls, pd.ExcelFile):
                print(f"Erro ao processar {file_name}: formato inválido.")
                continue

            df = xls.parse(sheet_name=xls.sheet_names[0])
            df_processed = self._normalize_data(df)
            print(f"Tamanho do dataframe: {df_processed.shape}")

            # Salvar no bucket no formato Parquet
            output_name = file_name.replace("rawdata", "processed").replace(".xlsx", ".parquet")
            self.storage.save_file_to_bucket(df_processed, output_name)

    def _normalize_data(self, df):
        """Normaliza os dados transformando colunas em linhas."""
        df = df.copy()

        colunas_fixas = ["Nome", "Total"]
        colunas_para_normalizar = [col for col in df.columns if col not in colunas_fixas]

        df_melted = df.melt(id_vars=["Nome"], value_vars=colunas_para_normalizar, var_name="Mes_Unidade", value_name="Valor")

        df_melted["Valor"] = pd.to_numeric(df_melted["Valor"], errors="coerce")

        df_melted["DataRef"] = self._extract_date(df_melted["Mes_Unidade"])
        df_melted["Unidade"] = self._extract_unit(df_melted["Mes_Unidade"])
        df_melted["IdConta"] = self._extract_account_id(df_melted["Nome"])
        df_melted["Tipo"] = df_melted["IdConta"].apply(self._identify_type, args=(df_melted["IdConta"].tolist(),))

        df_melted["Nome"] = df_melted["Nome"].str.strip()
        df_melted = self._add_category_flags(df_melted)

        return df_melted[["Nome", "Valor", "DataRef", "Unidade", "IdConta", "Conta", "Grupo", "Subgrupo", "Tipo"]]

    def _extract_date(self, column):
        """Extrai a data de referência do formato MM/YYYY."""
        return pd.to_datetime(column.str.extract(r"(\d{2}/\d{4})")[0], format="%m/%Y", errors="coerce")

    def _extract_unit(self, column):
        """Extrai a unidade do nome do mês/unidade."""
        return (
            column.str.replace(r"\d{2}/\d{4}", "", regex=True)
                  .str.replace(r"\(\s*LOJA\s*(\d+)\s*\)", r"Unidade \1", regex=True)
                  .str.strip()
        )

    def _extract_account_id(self, column):
        """Extrai o ID da conta do nome da conta."""
        return column.str.extract(r"^\s*([\d.]+)\s*-\s*")[0]

    def _identify_type(self, id_conta, id_list):
        """Determina se é Grupo, Subgrupo ou Conta baseado no ID."""
        if pd.isna(id_conta):
            return "Desconhecido"

        id_conta = str(id_conta)
        if "." not in id_conta:
            return "Grupo"

        id_conta_prefix = id_conta + "."
        return "Conta" if not any(str(i).startswith(id_conta_prefix) for i in id_list if pd.notna(i)) else "Subgrupo"

    def _add_category_flags(self, df):
        """Adiciona colunas booleanas para Categoria, Grupo e Subgrupo."""
        df["Conta"] = df["Tipo"] == "Conta"
        df["Grupo"] = df["Tipo"] == "Grupo"
        df["Subgrupo"] = df["Tipo"] == "Subgrupo"
        return df
