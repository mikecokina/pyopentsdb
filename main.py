from pytsdb import pytsdb
from pprint import pprint

c = pytsdb.connect(host='158.197.204.202', port='4242')
agg = c.get_aggregators()
print(agg)

conf = c.get_config()
pprint(conf)