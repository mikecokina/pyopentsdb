import unittest
from unittest import mock
import datetime

from pytsdb import tsdb
from pytsdb import errors
from pytsdb.query import tsdb_query_metrics_validation

from tests.testutils import get_mock_requests_post, mock_tsdb_connection_error_post, mock_unexpected_error_post
from tests.testutils import ADHOC_HOSTS, ADHOC_PORTS, ADHOC_PROTOCOLS

# todo: unify repeating test


class QueryTestCase(unittest.TestCase):

    __TEST_RESPONSE__ = [
        {
            "metric": "sys.unix.v0",
            "tags": {
                "host": "localhost",
                "os": "ubuntu"
            },
            "aggregateTags": [],
            "dps": {
                "1527804000": 4963.0,
                "1527804600": 4992.0,
                "1527805200": 5033.0,
                "1527805800": 4989.0,
                "1527806400": 4980.0,
                "1527807000": 4974.0,
                "1527807600": 4992.0,
                "1527808200": 4995.0,
                "1527808800": 5042.0
            }
        }
    ]

    __ADHOC__QUERY_PARAMS = {
        "start": datetime.datetime(2018, 1, 1),
        "metrics": [
            {
                "aggregator": "none",
                "metric": "sys.unix.v0",
            }
        ]
    }

    def setUp(self):
        self._host = 'localhost'
        self._port = 5896
        self._protocol = 'mockhttp'

        self._c = tsdb.tsdb_connection(self._host, self._port, protocol=self._protocol)

    @mock.patch('requests.post', side_effect=get_mock_requests_post(None))
    def test_url(self, _):
        hosts = ADHOC_HOSTS
        ports = ADHOC_PORTS
        protocols = ADHOC_PROTOCOLS

        for a, b, c in zip(hosts, ports, protocols):
            expected_url = '{}://{}:{}/api/query/'.format(c, a, b)
            self._c = tsdb.tsdb_connection(a, b, protocol=c)
            mock_return_value = self._c.query(** QueryTestCase.__ADHOC__QUERY_PARAMS)
            # url is added ad hoc in testutils
            self.assertTrue(mock_return_value[-1] == expected_url)

    @mock.patch('requests.post', side_effect=get_mock_requests_post(__TEST_RESPONSE__))
    def test_query(self, _):
        response = self._c.query(**QueryTestCase.__ADHOC__QUERY_PARAMS)
        self.assertEqual(response, QueryTestCase.__TEST_RESPONSE__)

    @mock.patch('requests.post', side_effect=get_mock_requests_post(None))
    def test_query_missing_arquments(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query()
        self.assertTrue(isinstance(context.exception, errors.MissingArgumentError))
        self.assertTrue("required" in str(context.exception))

    def test_tsdb_query_metrics_validation(self):
        valid_json_metrics = [
            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                    }
                ]
            },
            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                        "tags": {
                            "host": "*",
                            "dc": "lga"
                        }
                    }
                ]
            },

            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                    },
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v1",
                    }
                ]
            },
            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                        "filters": [
                            {
                                "tagk": "host",
                                "filter": "*",
                                "group_by": True,
                                "type": "wildcard"
                            }
                        ],
                    }
                ]
            },
        ]

        invalid_json_metrics = [
            {
                "notmetricsarg": [{}]
            },

            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                    },
                    {
                        "aggregator": "none",
                    }
                ]
            },

            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                        "filters": [
                            {
                                "tagk": "host",
                                "type": "wildcard"
                            }
                        ]
                    }
                ]
            },
            {
                "metrics": [
                    {
                        "aggregator": "none",
                        "metric": "sys.unix.v0",
                        "filters": [
                            {
                                "filter": "*",
                                "type": "wildcard"
                            }
                        ]
                    }
                ]
            }
        ]

        for valid in valid_json_metrics:
            with self.assertRaises(Exception) as context:
                with self.assertRaises(Exception):
                    tsdb_query_metrics_validation(**valid)
            self.assertTrue("Exception not raised" in str(context.exception))

        for invalid in invalid_json_metrics:
            with self.assertRaises(Exception) as context:
                tsdb_query_metrics_validation(**invalid)
            self.assertTrue(isinstance(context.exception, errors.MissingArgumentError))
            self.assertTrue("Missing argument" in str(context.exception))

    @mock.patch('requests.post', side_effect=get_mock_requests_post(None))
    def test_query_missing_start(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query()
        self.assertTrue(isinstance(context.exception, errors.MissingArgumentError))

    @mock.patch('requests.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=403))
    def test_query_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('requests.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=400))
    def test_query_400(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.ArgumentError))

    @mock.patch('requests.post', side_effect=mock_tsdb_connection_error_post)
    def test_query_tsdb_error(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.TsdbConnectionError))

    @mock.patch('requests.post', side_effect=mock_unexpected_error_post)
    def test_aggregators_unexpected_error(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))


class MultiQueryTestCase(unittest.TestCase):
    pass

