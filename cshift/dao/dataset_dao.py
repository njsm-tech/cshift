from google.cloud import datastore, storage

from cshift.client_service_common import config as csc_config
from cshift.proto import cshift_pb2 as pb2

from .dao import Dao
from .gcs_path import GcsPath

gcs_client = storage.Client(project=csc_config.PROJECT)

class DatasetDao(Dao):
    def __init__(self, spec: pb2.DatasetSpec):
        self.spec = spec
        self.gcs_path = GcsPath.from_message(spec.gcs_path)

    def download(self) -> bytes:
        return self._download_bytes()

    def upload(self, bytes: bytes = None, ref: str = None) -> None:
        self._upload_bytes(bytes=bytes, ref=ref)
        self._put_ref_to_datastore(ref=ref)

    def _download_bytes(self) -> bytes:


    def _put_ref_to_datastore(self, ref: str) -> None:
        pass

    def _upload_bytes(self, bytes: bytes, ref: str) -> None:
        pass
