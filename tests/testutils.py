import json
from pyopentsdb import errors

from pyopentsdb import tsdb

ADHOC_HOSTS = ['localhost', 'myqeb.com', 'web.kim.com']
ADHOC_PORTS = ['201', 22222, 693]
ADHOC_PROTOCOLS = ['http', 'https', 'ftp']
ADHOC_HOSTS = ["{}://{}:{}/".format(a, b, c) for a, b, c in zip(ADHOC_PROTOCOLS, ADHOC_HOSTS, ADHOC_PORTS)]


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


def get_mock_utils_requests_post(requests_kwargs):
    def mock_utils_request_post(url, r_session, **kwargs):
        _ = kwargs.pop("data")
        not_poped = [kwarg_key for kwarg_key in kwargs if kwarg_key not in requests_kwargs]
        return dict(status=False) if not_poped else dict(status=True)
    return mock_utils_request_post


class GeneralUrlTestCase(object):
    def test_url(self, endpoint, exec_fn, **kwargs):
        for host in ADHOC_HOSTS:
            expected_url = '{}{}'.format(host[:-1], endpoint)
            c = tsdb.tsdb_connection(host)
            _exec_fn = getattr(c, exec_fn)
            mock_return_value = _exec_fn(**kwargs)
            self.assertTrue(mock_return_value[-1] == expected_url)
