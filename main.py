from pytsdb import pytsdb

c = pytsdb.connect(host='158.197.204.202', port='4242')
agg = c.get_aggregators()
print(agg)
