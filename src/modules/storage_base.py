import io
import os
import pandas as pd
from hook.storage_hook import StorageHook
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env, se existir
load_dotenv()

class StorageBase:
    def __init__(self, project_id, bucket_name):
        self.storage_hook = StorageHook(project_id, bucket_name)
        self.bucket = self.storage_hook.get_bucket()

    def list_files(self, prefix="", file_format=""):
        """Lista arquivos no bucket, podendo filtrar por prefixo e formato."""
        blobs = self.bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs if blob.name.endswith(file_format)]

    def read_file_from_bucket(self, file_name):
        """Lê um arquivo do bucket, suportando .xlsx e .parquet."""
        blob = self.bucket.blob(file_name)
        file_content = blob.download_as_bytes()

        if file_name.endswith(".xlsx"):
            return pd.ExcelFile(io.BytesIO(file_content))
        elif file_name.endswith(".parquet"):
            return pd.read_parquet(io.BytesIO(file_content))
        else:
            raise ValueError("Formato de arquivo não suportado")

    def save_file_to_bucket(self, df, file_name):
        """Salva um DataFrame no bucket em formato .xlsx ou .parquet."""
        output_path = f"{file_name}"
        blob = self.bucket.blob(output_path)
        file_ext = os.path.splitext(file_name)[1]

        buffer = io.BytesIO()
        if file_ext == ".xlsx":
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False)
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif file_ext == ".parquet":
            df.to_parquet(buffer, engine="pyarrow")
            content_type = "application/octet-stream"
        else:
            raise ValueError("Formato de arquivo não suportado")

        blob.upload_from_string(buffer.getvalue(), content_type=content_type)
        print(f"Arquivo salvo no bucket: {output_path}")
        print(f"\n---------------------------------------\n")

    def delete_processed_files(self):
        """Deleta todos os arquivos na pasta 'processed' dentro do bucket."""
        blobs = self.bucket.list_blobs(prefix="processed/")
        for blob in blobs:
            blob.delete()
            print(f"Arquivo deletado: {blob.name}")
        print("Todos os arquivos da pasta '/processed' foram deletados.")
