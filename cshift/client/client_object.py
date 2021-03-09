import requests

class ClientObject:
    def _req(self, type_, url, spec):
        if type_ == 'post':
            return self._post(url, spec)

    def _post(self, url, spec):
        return requests.post(
            url=url,
            headers={'Content-Type': 'application/protobuf'},
            data=spec.SerializeToString())
