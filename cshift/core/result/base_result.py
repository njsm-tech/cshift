from __future__ import annotations

from cshift.client_service_common import config as csc_config
from cshift.dao.artifact import Artifact
from cshift.dao.artifact_gcs_path import ArtifactGcsPath
from cshift.proto import cshift_pb2 as pb2

class BaseResult:
    def __init__(self,
                 username: str = None,
                 comparison_pipeline_spec: pb2.ComparisonPipelineSpec = None,
                 **kwargs):
        self.comparison_pipeline_spec = comparison_pipeline_spec
        self.name = BaseResult.make_name(comparison_pipeline_spec)
        _bucket = csc_config.RESULTS_BUCKET
        _path_ext = "{username}/{path_prefix}/{artifact_id}".format(
            username=username,
            path_prefix=csc_config.RESULTS_PATH_PREFIX,
            artifact_id=self.name)
        self.gcs_path = ArtifactGcsPath(
            bucket=_bucket,
            path_ext=_path_ext)

        artifact_spec = pb2.ArtifactSpec(
            name=self.name,
            artifact_type=pb2.ArtifactType.RESULT,
            gcs_path=self.gcs_path.to_message())
        self.artifact = Artifact(artifact_spec)

        self.spec = pb2.ResultSpec(
            comparison_pipeline_spec=self.comparison_pipeline_spec,
            artifact_spec=artifact_spec)

    @classmethod
    def make_name(cls,
                   comparison_pipeline_spec: pb2.ComparisonPipelineSpec
                   ) -> str:
        return "{}-{}".format(
            comparison_pipeline_spec.name,
            cls.__name__.lower())

    def get(self) -> BaseResult:
        '''Get the result from database'''
        raise NotImplementedError()

    def record(self) -> None:
        '''Put the result in the database.'''
        raise NotImplementedError()