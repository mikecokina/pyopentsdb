from pyopentsdb import tsdb


host = "localhost",
port = 4242
protocol = 'http'

c = tsdb.tsdb_connection(host, port, protocol)
