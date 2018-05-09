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
        "metric": "etc.cpu.nice",
        "timestamp": 1346846404,
        "value": 6,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]
# c.put(data=data, details=True)
# exit()

start = datetime(2000, 1, 1, 23, 0, 0, 0)
end = datetime(2018, 1, 1, 0, 0, 0, 0)
filters = [
    {
        'id': 'x',
        'tags': [{
            "type": "literal_or",
            "tagk": "host",
            "filter": "web01",
            "groupBy": False
        }]
    },
    {
        'id': 'y',
        'tags': [{
            "type": "literal_or",
            "tagk": "host",
            "filter": "web02",
            "groupBy": False
        }]
    },
]

tags = {
    "host": "*",
    "dc": "lga"
}


metric = "sys.cpu.nice"
# metric = None

rate_options = {
    # 'counter': True,
    # 'counterMax': 0,
    # 'resetValue': 1,
    # 'dropResets': True,
}

# filters = [
#      {
#         "type": "wildcard",
#         "type": "literal_or",
#         "tagk": "host",
#         "tagk": "host",
#         "filter": "web02*",
#         "filter": "web01",
#         "groupBy": True,
#         "groupBy": False
#     },
# ]

# d = c.query(metric=metric, tsuids=tsuids, rate=False,
#             start=start, end=end, tags={"host": "web02"},
#             aggregator='sum', ms=True, show_tsuids=False, filters=filters)

# dd = c.delete(metric=metric, tsuids=tsuids, rate=False,
#               start=start, end=end, tags={"host": "web02"},
#               aggregator='sum', ms=True, show_tsuids=False)


# print(d[0]['dps'])

# print(json.dumps(c.version(), indent=4))
# print(c.metrics(regxp='(etc).*'))
# print(json.dumps(c.serializers(), indent=4))

filters = [
    {
        'id': 'x',
        'tags': [{
            "type": "literal_or",
            "tagk": "host",
            "filter": "web01",
            "groupBy": False
        }]
    },
    {
        'id': 'y',
        'tags': [{
            "type": "literal_or",
            "tagk": "host",
            "filter": "web02",
            "groupBy": False
        }]
    },
]


metrics = [
       {
           "id": "a",
           "metric": "etc.cpu.nice",
           "filter": "y",
           "fillPolicy": {"policy": "NaN"}
       },
       {
           "id": "b",
           "metric": "sys.cpu.nice",
           "filter": "x",
           "fillPolicy": {"policy": "NaN"}
       }
   ]


expressions = [
       {
           "id": "e1",
           "expr": "a + b"
       }

]

outputs =[
      {"id": "e1"},
    ]

print(json.dumps(c.query_exp(
    start=datetime(1999, 1, 1),
    aggregator='none',
    end=datetime(2018, 1, 1),
    filters=filters,
    metrics=metrics,
    expressions=expressions,
    outputs=outputs
), indent=4))
