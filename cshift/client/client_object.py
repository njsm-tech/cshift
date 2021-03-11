import requests

from cshift.client_service_common.api_paths import main_service_urlify

class ClientObject:
    def _req(self, type_, url, spec):
        if type_ == 'post':
            return self._post(url, spec)

    def _post(self, url, spec):
        return requests.post(
            url=main_service_urlify(url),
            headers={'Content-Type': 'application/protobuf'},
            data=spec.SerializeToString())
