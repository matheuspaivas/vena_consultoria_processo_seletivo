from modules.excel_processor import ExcelProcessor
from modules.star_schema import StarSchemaBuilder

class Pipeline:
    def __init__(self, project_id, bucket_name, dataset_id):
        self.processor = ExcelProcessor(project_id, bucket_name)
        self.star_schema = StarSchemaBuilder(project_id, dataset_id, bucket_name)

    def run(self):
        """Executa todo o pipeline de processamento."""
        print("\nProcessando arquivos .xlsx no bucket rawdata/...\n")
        self.processor.process_file("rawdata/")

        print("Criando e carregando o Star Schema...\n")
        self.star_schema.build_star_schema()

        print("Pipeline concluído!\n")
        print("\n-------------------------------\n\n")

