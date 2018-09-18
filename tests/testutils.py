import json
from pytsdb import errors

ADHOC_HOSTS = ['localhost', 'myqeb.com', 'web.kim.com']
ADHOC_PORTS = ['201', 22222, 693]
ADHOC_PROTOCOLS = ['http', 'https', 'ftp']


class MockRequests(object):
    def __init__(self):
        self._status_code = 200
        self._content = list()
        self._url = ''

    @property
    def status_code(self):
        return self._status_code

    @property
    def content(self):
        return self._content

    @property
    def url(self):
        return self._url

    @status_code.setter
    def status_code(self, status_code):
        self._status_code = status_code

    @content.setter
    def content(self, content):
        self._content = content

    @url.setter
    def url(self, url):
        self._url = url


def get_mock_requests_post(response_content=None, status_code=None):
    def mock_requests_post(url, *args, **kwargs):
        mr = MockRequests()
        content = response_content
        if content is None:
            content = list()
            content.append(url)

        code = status_code
        if status_code is None:
            code = 200

        mr.url = url
        mr.status_code = code
        mr.content = bytes(json.dumps(content).encode())

        return mr
    return mock_requests_post


def mock_tsdb_connection_error_post(url, *args, **kwargs):
    raise errors.TsdbConnectionError("Cannot connect")


def mock_unexpected_error_post(url, *args, **kwargs):
    raise Exception("Unexpected error")


def get_mock_requests_get(response_content=None, status_code=None):
    def mock_requests_get(url, *args, **kwargs):

        mr = MockRequests()
        content = response_content
        if content is None:
            content = list()
            content.append(url)

        code = status_code
        if status_code is None:
            code = 200

        mr.url = url
        mr.status_code = code
        mr.content = bytes(json.dumps(content).encode())

        return mr
    return mock_requests_get


def mock_tsdb_error_get(url, *args, **kwargs):
    raise errors.TsdbError("Cannot connect")


def mock_unexpected_error_get(url, *args, **kwargs):
    raise Exception("Unexpected error")
