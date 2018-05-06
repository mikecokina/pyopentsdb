from pytsdb import pytsdb
from _datetime import datetime


c = pytsdb.connect(host='158.197.204.202', port='4242')

data = [
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846401,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846403,
        "value": 9,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]

start = 1346846401
# start = datetime(2016, 1, 1, 12, 21, 5, 34)
c.query(metric="sys.cpu.nice", start=start, end=1346846404, tags={'host': 'web02', 'dc': 'lga'}, aggregator='none', ms_resolution=True)
