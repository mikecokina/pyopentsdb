Pytsdb
========

Python adapter for OpenTSDB HTTP APIs

Usage:
------
**Python code**::

     >> host = "localhost"
     >> port = 4242
     >> protocol = "http"
     >> timetout = 100
     >> tsdb_connection = tsdb.tsdb_connection(host, port, protocol, timeout=timeout)
     >> tsdb_connection.suggest(**params)
