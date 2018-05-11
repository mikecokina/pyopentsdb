import json


class MockRequests(object):
    def __init__(self):
        self._status_code = 200
        self._content = list()

    @property
    def status_code(self):
        return self._status_code

    @property
    def content(self):
        return self._content

    @status_code.setter
    def status_code(self, status_code):
        self._status_code = status_code

    @content.setter
    def content(self, content):
        self._content = content


def mock_requests_get(url, data=None, *args, **kwargs):
    mr = MockRequests()

    content = bytes(json.dumps({
        'url': url if url else None,
        'data': data if data else None
    }).encode())

    mr.status_code = 200
    mr.content = content

    return mr
