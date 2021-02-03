from __future__ import annotations

import pandas as pd

from cshift.client_service_common import config as csc_config
from cshift.dao.artifact import Artifact
from cshift.proto import cshift_pb2 as pb2

class Result:
    def __init__(self,
                 df: pd.DataFrame = None,
                 comparison_spec: pb2.ComparisonSpec = None):
        self.df = df
        self.comparison_spec = comparison_spec
        self.name = self.comparison_spec.metadata.name + '-result'
        self.id = self.comparison_spec.metadata.id + '-result'

        artifact_gcs_path = pb2.ArtifactGcsPath(
            bucket=csc_config.RESULTS_BUCKET,
            username='',
            project=csc_config.PROJECT,
            artifact_type=pb2.ArtifactType.RESULT,
            artifact_name=self.name,
            artifact_version=None
        )
        self.artifact_spec = pb2.ArtifactSpec(
            name=self.name,
            version=None,
            artifact_type=pb2.ArtifactType.RESULT,
            gcs_path=artifact_gcs_path,
            deserialized_type=pb2.ArtifactDeserializedType.PANDAS_DATAFRAME,
            serialization_format=pb2.ArtifactSerializationFormat.PARQUET)
        self.artifact = Artifact(spec=self.artifact_spec)
        self.metadata = pb2.CShiftMetadata(
            name=self.name,
            id=self.id,
            version=None)

        self.spec = pb2.ResultSpec(
            metadata=self.metadata,
            comparison_spec=self.comparison_spec,
            artifact_spec=self.artifact_spec)

    def get(self) -> None:
        self.df = self.artifact.download()

    def record(self) -> None:
        bytes = Artifact.dataframe_to_parquet_bytes(self.df)
        self.artifact.upload(bytes=bytes)

    def to_message(self) -> pb2.ResultSpec:
        return pb2.ResultSpec(
            metadata=self.metadata,
            comparison_spec=self.comparison_spec,
            artifact_spec=self.artifact.spec)