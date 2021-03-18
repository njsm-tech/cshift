from __future__ import annotations

from cshift.client.client_object import ClientObject
from cshift.client.client_result_set import ClientResultSet
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

    def get(self) -> ClientResultSet:
        poll_res = ClientObject.request_get(
            url=api_paths.POLL_RESULT,
            spec=self.spec)
        while poll_res
