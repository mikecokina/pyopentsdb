import unittest
from unittest import mock

from pyopentsdb import tsdb
from tests.testutils import GeneralUrlTestCase
from tests.testutils import get_mock_requests_get


class TsdbVersionTestCase(unittest.TestCase):
    __TEST_VERSION__ = {
        "short_revision": "",
        "repo": "/root/rpmbuild/BUILD/opentsdb-2.4.0RC2",
        "host": "centos.localhost",
        "version": "2.4.0RC2",
        "full_revision": "",
        "repo_status": "MODIFIED",
        "user": "root",
        "branch": "",
        "timestamp": "1507524798"
    }

    def setUp(self):
        self._host = 'localhost'
        self._port = 5896
        self._protocol = 'mockhttp'

        self._c = tsdb.tsdb_connection(self._host, self._port, protocol=self._protocol)

    @mock.patch('requests.get', side_effect=get_mock_requests_get(None))
    def test_url(self, _):
        GeneralUrlTestCase.test_url(self, "/api/version/", "version")

    @mock.patch('requests.get', side_effect=get_mock_requests_get(__TEST_VERSION__))
    def test_version(self, _):
        return_value = self._c.version()
        self.assertEqual(sorted(return_value), sorted(TsdbVersionTestCase.__TEST_VERSION__))
