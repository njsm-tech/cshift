from cshift.client_service_common import api_paths
from cshift.proto import cshift_pb2 as pb2

from .client_config import ClientConfig
from .client_object import ClientObject
from .client_dataset import ClientDataset

class ClientModel(ClientObject):
    def __init__(self,
                 name: str = None,
                 config: ClientConfig = None,
                 training_set: ClientDataset = None):
        super().__init__(config=config)
        self.spec = pb2.ModelSpec(
            name=name,
            training_set_spec=training_set.spec)

    def register(self):
        return self.request_post(
            url=api_paths.REGISTER_MODEL,
            spec=self.spec)