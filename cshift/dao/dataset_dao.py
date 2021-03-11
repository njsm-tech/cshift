from google.cloud import datastore

from cshift.client_service_common import config as csc_config
from cshift import enums
from cshift.proto import cshift_pb2 as pb2

from .dao import Dao
from .gcs_path import GcsPath

class DatasetDao(Dao):
    def __init__(self, spec: pb2.DatasetSpec):
        super().__init__()
        self.spec = spec
        self.gcs_path = GcsPath.from_message(spec.gcs_path)
        self.datastore_client = datastore.Client(project=csc_config.PROJECT)

    def download(self) -> bytes:
        return self._download_bytes_from_gcs(self.gcs_path)

    def upload(self, bytes: bytes, gcs_path: GcsPath) -> None:
        self._upload_bytes_to_gcs(bytes=bytes, gcs_path=gcs_path)
        self._put_ref_to_datastore(gcs_path=gcs_path)

    def _put_ref_to_datastore(self, gcs_path: GcsPath) -> None:
        key = self.datastore_client.Key(
            enums.DatastoreEntityKind.DATASET,
            gcs_path.path_ext)
        entity = self.datastore_client.Entity(key)
        entity['bucket'] = gcs_path.bucket
        entity['path_ext'] = gcs_path.path_ext
        self.datastore_client.put(entity)
