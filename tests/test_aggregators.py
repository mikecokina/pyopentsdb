import unittest
import json
from pytsdb import pytsdb
from unittest import mock
from tests.testutils import mock_requests_get


class AggregatorsTestCase(unittest.TestCase):

    def setUp(self):
        self._host = 'localhost'
        self._port = 5896
        self._protocol = 'mockhttp'

        self._c = pytsdb.connect(self._host, self._port, protocol=self._protocol)

    @mock.patch('requests.get', side_effect=mock_requests_get)
    def test_url(self, _):
        hosts = ['localhost', 'myqeb.com', 'web.kim.com']
        ports = ['201', 22222, 693]
        protocols = ['http', 'https', 'ftp']

        for a, b, c in zip(hosts, ports, protocols):
            expected_url = '{}://{}:{}/api/aggregators/'.format(c, a, b)
            self._c = pytsdb.connect(a, b, protocol=c)
            mock_return_value = self._c.aggregators()

            self.assertTrue(isinstance(mock_return_value, dict))
            self.assertTrue(mock_return_value.get('url') == expected_url)
