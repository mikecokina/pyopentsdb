import unittest
from unittest import mock

from tests.testutils import get_mock_requests_get
from tests.testutils import GeneralUrlTestCase


class TsdbSerializersTestCase(unittest.TestCase):
    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url(self, _):
        GeneralUrlTestCase.test_url(self, "/api/serializers/", "serializers")
