import unittest
from unittest import mock
import datetime

from pyopentsdb import tsdb
from pyopentsdb import errors
from pyopentsdb.query import tsdb_query_metrics_validation

from tests.testutils import get_mock_requests_post, mock_tsdb_connection_error_post, mock_unexpected_error_post
from tests.testutils import GeneralUrlTestCase, get_mock_utils_requests_post

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
        self._host = 'mockhttp://localhost:5896/'

        self._c = tsdb.tsdb_connection(self._host)

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(None))
    def test_url(self, _):
        GeneralUrlTestCase.test_url(self, "/api/query/", "query", **QueryTestCase.__ADHOC__QUERY_PARAMS)

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(__TEST_RESPONSE__))
    def test_query(self, _):
        response = self._c.query(**QueryTestCase.__ADHOC__QUERY_PARAMS)
        self.assertEqual(response, QueryTestCase.__TEST_RESPONSE__)

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(None))
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

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(None))
    def test_query_missing_start(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query()
        self.assertTrue(isinstance(context.exception, errors.MissingArgumentError))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=402))
    def test_query_402(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "403 Forbidden"}}, status_code=403))
    def test_query_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.ForbiddenError))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=400))
    def test_query_400(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.ArgumentError))

    @mock.patch('requests.Session.post', side_effect=mock_tsdb_connection_error_post)
    def test_query_tsdb_error(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.TsdbConnectionError))

    @mock.patch('requests.Session.post', side_effect=mock_unexpected_error_post)
    def test_aggregators_unexpected_error(self, _):
        with self.assertRaises(Exception) as context:
            self._c.query(**self.__ADHOC__QUERY_PARAMS)
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('pyopentsdb.query.request_post', side_effect=get_mock_utils_requests_post(
        requests_kwargs=["headers", "cookies", "timeout"]))
    def test_query_pop_arguments(self, _):
        query_dict = {
            "start": datetime.datetime(2010, 1, 1), "end": datetime.datetime(2010, 1, 1),
            "ms": False, "show_tsuids": False, "no_annotations": False,
            "global_annotations": False, "show_summary": False, "show_stats": True, "show_query": False,
            "delete": False, "timezone": "UTC", "use_calendar": False,
            "metrics": [{"aggregator": "none", "metric": "metric"}]
        }
        requests_kwargs = {"headers": {}, "cookies": {}, "timeout": {}}
        response = self._c.query(**dict(**query_dict, **requests_kwargs))
        self.assertTrue(response["status"])


