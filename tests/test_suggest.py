import unittest
from pyopentsdb import tsdb
from pyopentsdb import errors
from unittest import mock
from tests.testutils import get_mock_requests_post, mock_tsdb_connection_error_post, mock_unexpected_error_post
from tests.testutils import GeneralUrlTestCase, get_mock_utils_requests_post


class SuggestTestCase(unittest.TestCase):

    __TEST_RESPONSE__ = [
        "sys.unix.v0",
        "sys.linux.v0",
        "sys.win.v0",
    ]

    def setUp(self):
        self._host = 'mockhttp://localhost:5896/'
        self._c = tsdb.tsdb_connection(self._host)

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(None))
    def test_url(self, _):
        GeneralUrlTestCase.test_url(self, "/api/suggest/", "suggest", type="metric")

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(__TEST_RESPONSE__))
    def test_suggest(self, _):
        return_value = self._c.suggest(type="metric")
        self.assertEqual(sorted(return_value), sorted(SuggestTestCase.__TEST_RESPONSE__))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(None))
    def test_suggest_missing_type(self, _):
        with self.assertRaises(Exception) as context:
            self._c.suggest()
        self.assertTrue(isinstance(context.exception, errors.MissingArgumentError))

    @mock.patch('requests.Session.post', side_effect=mock_tsdb_connection_error_post)
    def test_suggest_tsdberror(self, _):
        with self.assertRaises(Exception) as context:
            self._c.suggest(type="metric")
        self.assertTrue(isinstance(context.exception, errors.TsdbConnectionError))

    @mock.patch('requests.Session.post', side_effect=mock_unexpected_error_post)
    def test_suggest_unexpectederror(self, _):
        with self.assertRaises(Exception) as context:
            self._c.suggest(type="metric")
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=402))
    def test_suggest_402(self, _):
        with self.assertRaises(Exception) as context:
            self._c.suggest(type="metric")
        self.assertTrue(isinstance(context.exception, errors.UncaughtError))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "403 Forbidden"}}, status_code=403))
    def test_suggest_403(self, _):
        with self.assertRaises(Exception) as context:
            self._c.suggest(type="metric")
        self.assertTrue(isinstance(context.exception, errors.ForbiddenError))

    @mock.patch('requests.Session.post', side_effect=get_mock_requests_post(
        response_content={"error": {"message": "Response code differ 200"}}, status_code=400))
    def test_suggest_400(self, _):
        with self.assertRaises(Exception) as context:
            self._c.suggest(type="metric")
        self.assertTrue(isinstance(context.exception, errors.ArgumentError))

    @mock.patch('pyopentsdb.utils.request_post', side_effect=get_mock_utils_requests_post(
        requests_kwargs=["headers", "cookies", "timeout"]))
    def test_suggest_pop_arguments(self, _):
        requests_kwargs = {"headers": {}, "cookies": {}, "timeout": {}}
        response = self._c.suggest(type="metric", q="query", max=1000, **requests_kwargs)
        self.assertTrue(response["status"])
