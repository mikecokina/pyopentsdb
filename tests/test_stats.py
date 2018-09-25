import unittest
from unittest import mock

from tests.testutils import get_mock_requests_get
from tests.testutils import GeneralUrlTestCase


class TsdbStatsTestCase(unittest.TestCase):
    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url_stats(self, _):
        GeneralUrlTestCase.test_url(self, "/api/stats/", "stats")

    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url_jvm_stats(self, _):
        GeneralUrlTestCase.test_url(self, "/api/stats/jvm/", "jvm_stats")

    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url_query_stats(self, _):
        GeneralUrlTestCase.test_url(self, "/api/stats/query/", "query_stats")

    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url_region_clients_stats(self, _):
        GeneralUrlTestCase.test_url(self, "/api/stats/region_clients/", "region_clients")

    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url_threads_stats(self, _):
        GeneralUrlTestCase.test_url(self, "/api/stats/threads/", "threads")

