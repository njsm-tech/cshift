import requests

from cshift.client_service_common.api_paths import main_service_urlify

PROTOBUF_HEADER = {'Content-Type': 'application/protobuf'}

class ClientObject:
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
