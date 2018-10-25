Pyopentsdb
==========

Python adapter for OpenTSDB HTTP APIs

Usage:
------
**Python code**::
from pyopentsdb import tsdb


     >> from pyopentsdb import tsdb
     >> host = "https://server.domain/opentsdb/"
     >> cookie = {'key': 'value'}
     >> headers = {'Content-Type': 'application/json'}

     >> tsdb_connection = tsdb.tsdb_connection(host)
     >> tsdb_connection.suggest(type="metrics", q="sys.host.cpu", max=10000, cookies=cookie, headers=headers)
