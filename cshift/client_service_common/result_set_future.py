from __future__ import annotations

from cshift import client
from cshift.client_service_common import api_paths
from cshift.dao.job import Job
from cshift.proto import cshift_pb2 as pb2

class ResultSetFuture:
    def __init__(self,
                 job: Job,
                 comparison_pipeline_spec: pb2.ComparisonPipelineSpec = None,
                 comparison_set_spec: pb2.ComparisonSetSpec = None):
        self.job = job
        if comparison_pipeline_spec is not None:
            self.spec = comparison_pipeline_spec
        if comparison_set_spec is not None:
            self.spec = comparison_set_spec

    def get(self) -> client.client_result_set.ClientResultSet:
        poll_res = client.client_object.ClientObject.request_get(
            url=api_paths.POLL_RESULT,
            spec=self.spec)
        print(poll_res)
        return poll_res
