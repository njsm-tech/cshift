from __future__ import annotations

from io import BytesIO

from google.cloud import datastore, storage
import pandas as pd

from cshift.client_service_common import config as csc_config
from cshift import enums
from cshift.proto import cshift_pb2 as pb2

from .artifact_gcs_path import ArtifactGcsPath

class Artifact:
    def __init__(self, spec: pb2.ArtifactSpec):
        self.datastore_client = datastore.Client(project=csc_config.PROJECT)
        self.gcs_client = storage.Client(project=csc_config.PROJECT)
        self.spec = spec
        self.gcs_path = ArtifactGcsPath.from_message(spec.gcs_path)

    @classmethod
    def new_spec(cls,
                 name: str = None,
                 version: str = None,
                 artifact_type: pb2.ArtifactType = None,
                 gcs_path: pb2.ArtifactGcsPath = None,
                 deserialized_type: pb2.ArtifactDeserializedType = None,
                 serialization_format: pb2.ArtifactSerializationFormat = None
                 ) -> pb2.ArtifactSpec:
        return pb2.ArtifactSpec(
            name=name,
            version=version,
            artifact_type=artifact_type,
            gcs_path=gcs_path,
            deserialized_type=deserialized_type,
            serialization_format=serialization_format)

    @classmethod
    def new(cls, **kwargs) -> Artifact:
        spec = cls.new_spec(**kwargs)
        return cls(spec=spec)

    @staticmethod
    def dataframe_from_parquet_bytes(parquet_bytes: bytes) -> pd.DataFrame:
        buffer = BytesIO(parquet_bytes)
        return pd.read_parquet(buffer)

    @staticmethod
    def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
        buffer = BytesIO()
        df.columns = [str(c) for c in df.columns]
        df.to_parquet(buffer)
        return buffer.getvalue()

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