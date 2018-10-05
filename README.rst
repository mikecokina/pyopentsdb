Pyopentsdb
==========

Python adapter for OpenTSDB HTTP APIs

Usage:
------
**Python code**::

     >> from pyopentsdb import tsdb
     >> host = "localhost"
     >> port = 4242
     >> protocol = "http"
     >> timeout = 100
     >> params = dict(type="metrics", q="sys.host.cpu", max=10000)
     >> tsdb_connection = tsdb.tsdb_connection(host, port, protocol=protocol, timeout=timeout)
     >> tsdb_connection.suggest(**params)
