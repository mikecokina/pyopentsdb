from pytsdb import pytsdb
from datetime import datetime


c = pytsdb.connect(host='158.197.204.202', port='4242')

data = [
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846409,
        "value": 31,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    # {
    #     "metric": "sys.cpu.nice",
    #     "timestamp": 1346846403,
    #     "value": 9,
    #     "tags": {
    #        "host": "web02",
    #        "dc": "lga"
    #     }
    # }
]
# c.put(data=data, details=True)

start = datetime(2000, 1, 1, 23, 0, 0, 0)
end = datetime(2018, 1, 1, 0, 0, 0, 0)
filters = [
    {
        "type": "literal_or",
        "tagk": "host",
        "filter": "web01",
        "groupBy": False
    },
]

tags = {
    "host": "*",
    "dc": "lga"
}

tsuids = ["000001000001000003000002000002", "000001000001000001000002000002"]
tsuids = []

metric = "sys.cpu.nice"
# metric = None

rate_options = {
    # 'counter': True,
    # 'counterMax': 0,
    # 'resetValue': 1,
    'dropResets': True,
}

c.query(metric=metric, tsuids=tsuids, rate=True, rate_options=rate_options,
        start=start, end=end, tags={"host": "web01"},
        aggregator='sum', ms=True, show_tsuids=False)

