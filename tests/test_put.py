import unittest
from unittest import mock

from tests.testutils import GeneralUrlTestCase
from tests.testutils import get_mock_requests_post


class TsdbSerializersTestCase(unittest.TestCase):
    __TEST_PUT_SINGLE__ = {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846400,
        "value": 18,
        "tags": {
            "host": "web01",
            "dc": "lga"
        }
    }

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(None))
    def test_url(self, _):
        GeneralUrlTestCase.test_url(self, "/api/put/", "put", data=TsdbSerializersTestCase.__TEST_PUT_SINGLE__)
