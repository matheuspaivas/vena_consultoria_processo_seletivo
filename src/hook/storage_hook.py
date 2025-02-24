from google.cloud import storage

class StorageHook:
    def __init__(self, project_id, bucket_name):
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def get_bucket(self):
        """Retorna o bucket do GCP Storage."""
        return self.bucket
