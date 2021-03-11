from google.cloud import storage

from cshift.client_service_common import config as csc_config
from .gcs_path import GcsPath

class Dao:
    def __init__(self):
        self.gcs_client = storage.Client(project=csc_config.PROJECT)

    def download(self, *args, **kwargs):
        raise NotImplementedError()

    def upload(self, *args, **kwargs):
        raise NotImplementedError()

    def _download_bytes_from_gcs(self, gcs_path: GcsPath) -> bytes:
        bucket = self.gcs_client.get_bucket(gcs_path.bucket)
        blob = storage.Blob(gcs_path.path_ext, bucket)
        return blob.download_as_string()

    def _upload_bytes_to_gcs(self, bytes: bytes, gcs_path: GcsPath) -> None:
        bucket = self.gcs_client.get_bucket(gcs_path.bucket)
        blob = storage.Blob(gcs_path.path_ext, bucket)
        blob.upload_from_string(bytes)