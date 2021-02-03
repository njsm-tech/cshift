import time

from cshift.client_service_common import api_paths
from cshift import enums
from cshift.proto import cshift_pb2 as pb2

from .client_comparison import ClientComparison
from .client_object import ClientObject

WAIT_SLEEP = 2

class ClientResult(ClientObject):
    def __init__(self,
                 comparison: ClientComparison,
                 **kwargs):
        super().__init__(**kwargs)
        self.comparison = comparison
        artifact_spec = pb2.ArtifactSpec(
            artifact_type=pb2.ArtifactType.RESULT)
        self.spec = pb2.ResultSpec(
            comparison_pipeline_spec=comparison.spec,
            artifact_spec=artifact_spec)

    def _get_wait(self):
        # TODO: fix this
        # poll main service periodically to see if result is available yet
        res = self.request_get(url=api_paths.POLL_RESULT, spec=self.spec)
        print(res)
        res = enums.ResponseCode.from_value(res)
        while res == enums.ResponseCode.TASK_QUEUED:
            time.sleep(WAIT_SLEEP)
            res = self.request_get(url=api_paths.POLL_RESULT, spec=self.spec)
        if res == enums.ResponseCode.FAILED:
            raise Exception('Task failed')
        elif res == enums.ResponseCode.SUCCESS:
            return self.get(wait=False)
        else:
            raise Exception('Response not recognized %s' % res)

    def poll(self):
        return self.request_get(url=api_paths.POLL_RESULT, spec=self.spec)

    def get(self, wait=True):
        if wait:
            return self._get_wait()
        return self.request_get(url=api_paths.GET_RESULT, spec=self.spec)