from google.cloud import datastore, storage

from cshift.client_service_common import config as csc_config
from cshift import enums
from cshift.proto import cshift_pb2 as pb2

from .gcs_path import GcsPath

class ArtifactDao:
    def __init__(self, spec: pb2.ArtifactSpec):
        self.datastore_client = datastore.Client(project=csc_config.PROJECT)
        self.gcs_client = storage.Client(project=csc_config.PROJECT)
        self.spec = spec
        self.gcs_path = GcsPath.from_message(spec.gcs_path)

    def download(self) -> bytes:
        return self._download_bytes_from_gcs()

    def upload(self, bytes: bytes) -> None:
        self._upload_bytes_to_gcs(bytes=bytes)
        self._put_ref_to_datastore()

    def _put_ref_to_datastore(self) -> None:
        key = self.datastore_client.key(
            enums.DatastoreEntityKind.ARTIFACT,
            self.gcs_path.path_ext)
        entity = datastore.Entity(key)
        entity['bucket'] = self.gcs_path.bucket
        entity['path_ext'] = self.gcs_path.path_ext
        self.datastore_client.put(entity)

    def _download_bytes_from_gcs(self) -> bytes:
        bucket = self.gcs_client.get_bucket(self.gcs_path.bucket)
        blob = storage.Blob(self.gcs_path.path_ext, bucket)
        return blob.download_as_string()

    def _upload_bytes_to_gcs(self, bytes: bytes) -> None:
        bucket = self.gcs_client.get_bucket(self.gcs_path.bucket)
        blob = storage.Blob(self.gcs_path.path_ext, bucket)
        blob.upload_from_string(bytes)