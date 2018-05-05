from pytsdb import aggregators
from pytsdb import config


class TsdbConnector(object):
    def __init__(self, host, port, *args, **kwargs):
        self._host = host
        self._port = port
        self._protocol = kwargs.get('protocol', 'http')
        self._config = self.parameters_serializer()

    def get_aggregators(self):
        return aggregators.aggregators(**self._config)

    def get_config(self):
        return config.tsdb_configuration(**self._config)

    def parameters_serializer(self):
        return {
            'host': self._host,
            'port': self._port,
            'protocol': self._protocol
        }


def connect(host, port, *args, **kwargs):
    return TsdbConnector(host, port, args, kwargs)

