from pytsdb import aggregators


class TsdbConnector(object):
    def __init__(self, host, port, *args, **kwargs):
        self._host = host
        self._port = port
        self._protocol = kwargs.get('protocol', 'http')

    def get_aggregators(self):
        return aggregators.aggregators(self._host, self._port, self._protocol)

def connect(host, port, *args, **kwargs):
    return TsdbConnector(host, port, args, kwargs)
