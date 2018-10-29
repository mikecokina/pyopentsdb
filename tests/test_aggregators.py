import unittest
from pyopentsdb import tsdb
from pyopentsdb import errors
from unittest import mock
from tests.testutils import get_mock_requests_get, mock_tsdb_error_get, mock_unexpected_error_get
from tests.testutils import GeneralUrlTestCase


class AggregatorsTestCase(unittest.TestCase):
    __TEST_AGGREGATORS__ = ["mult", "p90", "zimsum", "mimmax", "sum", "p50", "none", "p95", "ep99r7"]

    def setUp(self):
        self._host = 'mockhttp://localhost:5896/'
        self._c = tsdb.tsdb_connection(self._host)

    @mock.patch('requests.Session.get', side_effect=get_mock_requests_get(None))
    def test_url(self, _):
        GeneralUrlTestCase.test_url(self, "/api/aggregators/", "aggregators")

    @mock.patch('requests.Session.get', side_effect=get_mock_requests_get(__TEST_AGGREGATORS__))
    def test_aggregators(self, _):
        response = self._c.aggregators()
        self.assertEqual(sorted(response), sorted(AggregatorsTestCase.__TEST_AGGREGATORS__))

    @mock.patch('requests.Session.get', side_effect=get_mock_requests_get(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=403))
    def test_aggregators_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('requests.Session.get', side_effect=get_mock_requests_get(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=400))
    def test_aggregators_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.ArgumentError))

    @mock.patch('requests.Session.get', side_effect=mock_tsdb_error_get)
    def test_aggregators_tsdberror(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.TsdbError))

    @mock.patch('requests.Session.get', side_effect=mock_unexpected_error_get)
    def test_aggregators_unexpectederror(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))
