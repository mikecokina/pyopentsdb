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

start = datetime(2000, 1, 1, 23, 0, 0, 0)
end = datetime(2018, 1, 1, 0, 0, 0, 0)
metric_filters=[
    {
        "type": "wildcard",
        "tagk": "host",
        "filter": "web02*",
        "groupBy": True
    }
]


c.query(metric="sys.cpu.nice", metric_filters=metric_filters, tsuids=["000001000001000003000002000002"], start=start, end=end, tags={'dc': 'lga'}, aggregator='none', ms=True, show_tsuids=True)

print(c.filters())


