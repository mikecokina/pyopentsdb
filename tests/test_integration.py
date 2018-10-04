import unittest
from datetime import datetime
import time

from pyopentsdb import tsdb

SLEEP_TIME = 15
HOST = "localhost"
PORT = 4242
PROTOCOL = "http"
TSDB_CONNECTION = tsdb.tsdb_connection(HOST, PORT, protocol=PROTOCOL, timeout=10)


def ping():
    try:
        TSDB_CONNECTION.version()
        return True
    except Exception as e:
        return False


@unittest.skipIf(not ping(), "no tsdb running at localhost:4242")
class TsdbIntegrationTestCase(unittest.TestCase):
    __SINGLE_METRIC_NAME__ = "integration_test_metric.sys.cpu.nice"
    __MULTIPLE_METRIC_NAME__ = "integration_test_metric.sys.mem.size"
    __DELETE_METRIC_NAME__ = "integration_test_metric.sys.hdd.left"

    __TEST_PUT_SINGLE__ = {"metric": __SINGLE_METRIC_NAME__, "timestamp": 1514764800, "value": 18,
                           "tags": {"host": "web01", "dc": "lga"}
                           }

    __TEST_PUT_MULTIPLE__ = [{
        "metric": __MULTIPLE_METRIC_NAME__, "timestamp": 1514764800, "value": 512,
        "tags": {"host": "web02", "dc": "lga"}
    },
        {"metric": __MULTIPLE_METRIC_NAME__, "timestamp": 1514851200, "value": 4096,
         "tags": {"host": "web02", "dc": "lga"}
         }
    ]

    __TEST_DELETE__ = [{
        "metric": __DELETE_METRIC_NAME__, "timestamp": 1514764800, "value": 100,
        "tags": {"host": "web03"}
    },
        {"metric": __DELETE_METRIC_NAME__, "timestamp": 1514851200, "value": 250,
         "tags": {"host": "web03"}
         }
    ]

    def setUp(self):
        TSDB_CONNECTION.put(data=TsdbIntegrationTestCase.__TEST_PUT_SINGLE__)
        TSDB_CONNECTION.put(data=TsdbIntegrationTestCase.__TEST_PUT_MULTIPLE__)
        TSDB_CONNECTION.put(data=TsdbIntegrationTestCase.__TEST_DELETE__)

    def test_query_single(self):
        query_kwargs = {
            "start": datetime(2018, 1, 1),
            "ms": False,
            "metrics": [{
                "metric": TsdbIntegrationTestCase.__SINGLE_METRIC_NAME__,
                "aggregator": "none",
            }]
        }

        data, attempt = None, 0
        while not data and attempt < 2:
            time.sleep(SLEEP_TIME)
            data = TSDB_CONNECTION.query(**query_kwargs)
            attempt += 1

        self.assertEqual({'1514764800': 18}, data[0]["dps"])

    def test_query_multiple(self):
        query_kwargs = {
            "start": datetime(2018, 1, 1),
            "ms": False,
            "metrics": [{
                "metric": TsdbIntegrationTestCase.__MULTIPLE_METRIC_NAME__,
                "aggregator": "none",
            }]
        }

        data, attempt = None, 0
        while not data and attempt < 2:
            time.sleep(SLEEP_TIME)
            data = TSDB_CONNECTION.query(**query_kwargs)
            attempt += 1

        self.assertEqual(data[0]["metric"], TsdbIntegrationTestCase.__MULTIPLE_METRIC_NAME__)
        tss, vals = ["1514764800", "1514851200"], [512, 4096]

        for ts, val in data[0]["dps"].items():
            self.assertTrue(ts in tss and val in vals)

    def test_delete_query(self):
        query_kwargs = {
            "start": datetime(2018, 1, 1),
            "end": datetime(2018, 2, 1),
            "ms": False,
            "delete": True,
            "metrics": [{
                "metric": TsdbIntegrationTestCase.__DELETE_METRIC_NAME__,
                "aggregator": "none",
            }]
        }

        TSDB_CONNECTION.query(**query_kwargs)
        time.sleep(SLEEP_TIME)
        query_kwargs.pop("delete")

        data, attempt = True, 0
        while data and attempt < 2:
            time.sleep(SLEEP_TIME)
            data = TSDB_CONNECTION.query(**query_kwargs)
            attempt += 1
        self.assertTrue(len(data) == 1)
