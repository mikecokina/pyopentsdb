from pytsdb import pytsdb

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


c.put(data=data, details=True)
