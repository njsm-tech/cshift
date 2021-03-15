import requests

from cshift.client_service_common.api_paths import main_service_urlify

from .client_config import ClientConfig

PROTOBUF_HEADER = {'Content-Type': 'application/protobuf'}

class ClientObject:
    def __init__(self, config: ClientConfig = None):
        self.config = config

    def request_get(self, url, spec):
        return requests.get(
            url=main_service_urlify(url),
            headers=PROTOBUF_HEADER,
            data=spec.SerializeToString())

    def request_post(self, url, spec):
        return requests.post(
            url=main_service_urlify(url),
            headers=PROTOBUF_HEADER,
            data=spec.SerializeToString())
