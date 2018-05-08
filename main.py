from pytsdb import pytsdb
from datetime import datetime
import json


c = pytsdb.connect(host='158.197.204.202', port='4242')

data = [
    # {
    #     "metric": "sys.cpu.nice",
    #     "timestamp": 1346846409,
    #     "value": 31,
    #     "tags": {
    #        "host": "web01",
    #        "dc": "lga"
    #     }
    # },
    {
        "metric": "sys.cpu.nice",
        "timestamp": 1346846403,
        "value": 8,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
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
    # 'dropResets': True,
}

# d = c.query(metric=metric, tsuids=tsuids, rate=False,
#             start=start, end=end, tags={"host": "web02"},
#             aggregator='sum', ms=True, show_tsuids=False)

# dd = c.delete(metric=metric, tsuids=tsuids, rate=False,
#               start=start, end=end, tags={"host": "web02"},
#               aggregator='sum', ms=True, show_tsuids=False)


# print(d[0]['dps'])

print(json.dumps(c.version(), indent=4))

