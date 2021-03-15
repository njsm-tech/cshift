from cshift.client_service_common import api_paths
from cshift.proto import enums_pb2, messages_pb2

from .client_comparison_pipeline import ClientComparisonPipeline
from .client_object import ClientObject

class ClientResult(ClientObject):
    def __init__(self,
                 comparison_pipeline: ClientComparisonPipeline,
                 **kwargs):
        super().__init__(**kwargs)
        self.comparison_pipeline = comparison_pipeline
        artifact_spec = messages_pb2.ArtifactSpec(
            artifact_type=enums_pb2.ArtifactType.RESULT)
        self.spec = messages_pb2.ResultSpec(
            comparison_pipeline_spec=comparison_pipeline.spec,
            artifact_spec=artifact_spec)

    def wait(self):
        # poll main service periodically to see if result is available yet
        pass

    def get(self):
        return self.request_get(url=api_paths.GET_RESULT, spec=self.spec)