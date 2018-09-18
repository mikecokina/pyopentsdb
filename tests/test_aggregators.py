import unittest
from pytsdb import tsdb
from pytsdb import errors
from unittest import mock
from tests.testutils import get_mock_requests_get, mock_tsdb_error_get, mock_unexpected_error_get
from tests.testutils import ADHOC_PROTOCOLS, ADHOC_PORTS, ADHOC_HOSTS


class AggregatorsTestCase(unittest.TestCase):
    __TEST_AGGREGATORS__ = ["mult", "p90", "zimsum", "mimmax", "sum", "p50", "none", "p95", "ep99r7"]

    def setUp(self):
        self._host = 'localhost'
        self._port = 5896
        self._protocol = 'mockhttp'

        self._c = tsdb.tsdb_connection(self._host, self._port, protocol=self._protocol)

    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url(self, _):
        hosts = ADHOC_HOSTS
        ports = ADHOC_PORTS
        protocols = ADHOC_PROTOCOLS

        for a, b, c in zip(hosts, ports, protocols):
            expected_url = '{}://{}:{}/api/aggregators/'.format(c, a, b)
            self._c = tsdb.tsdb_connection(a, b, protocol=c)
            mock_return_value = self._c.aggregators()
            self.assertTrue(mock_return_value[-1] == expected_url)

    @mock.patch('requests.get', side_effect=get_mock_requests_get(__TEST_AGGREGATORS__))
    def test_aggregators(self, _):
        response = self._c.aggregators()
        # [:-1] to avoid comparing an url adhoc appended in testutils mock
        self.assertEqual(sorted(response), sorted(AggregatorsTestCase.__TEST_AGGREGATORS__))

    @mock.patch('requests.get', side_effect=get_mock_requests_get(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=403))
    def test_aggregators_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('requests.get', side_effect=get_mock_requests_get(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=400))
    def test_aggregators_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.ArgumentError))

    @mock.patch('requests.get', side_effect=mock_tsdb_error_get)
    def test_aggregators_tsdberror(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.TsdbError))

    @mock.patch('requests.get', side_effect=mock_unexpected_error_get)
    def test_aggregators_unexpectederror(self, _):
        with self.assertRaises(Exception) as context:
            self._c.aggregators()
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))
