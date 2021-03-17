from __future__ import annotations

from cshift.dao.job import Job
from cshift.proto import cshift_pb2 as pb2

class ResultSetFuture:
    def __init__(self,
                 job: Job,
                 comparison_pipeline_spec: pb2.ComparisonPipelineSpec):
        self.job = job
        self.comparison_pipeline_spec = comparison_pipeline_spec