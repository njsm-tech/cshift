from __future__ import annotations

from cshift import client
from cshift.client_service_common import api_paths
from cshift.dao.job import Job
from cshift.proto import cshift_pb2 as pb2

class ResultSetFuture:
    def __init__(self,
                 job: Job,
                 spec: pb2.ResultSetSpec):
        self.job = job
        self.spec = spec

    def get(self) -> client.client_result_set.ClientResultSet:
        poll_res = self.poll()
        return poll_res

    def poll(self):
        poll_res = client.client_object.ClientObject.request_get(
            url=api_paths.POLL_RESULT,
            spec=self.spec)
        return poll_res