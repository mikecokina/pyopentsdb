from pytsdb import aggregators
from pytsdb import config
from pytsdb import put
from pytsdb import query
import warnings
from datetime import datetime


class TsdbConnector(object):
    def __init__(self, host, port, **kwargs):
        self._host = host
        self._port = port
        self._protocol = kwargs.get('protocol', 'http')
        self._config = self.parameters_serializer()

    def aggregators(self):
        """
        This endpoint simply lists the names of implemented aggregation functions used in timeseries queries.
        :return:  json (list)
        """
        return aggregators.aggregators(**self._config)

    def config(self):
        """
        This endpoint returns information about the running configuration of the TSD.
        It is read only and cannot be used to set configuration options.
        :return: json
        """
        return config.tsdb_configuration(**self._config)

    def put(self, data, **kwargs):
        """
        Metric has to be created or enable tsd.core.auto_create_metrics

        :param data: json like dict or list of json like dicts
        :param kwargs: dict
        :return: requests response
        """
        params = self._config
        summary = kwargs.get('summary', False)
        details = kwargs.get('details', False)
        sync = kwargs.get('sync', False)
        sync_timeout = kwargs.get('sync_timeout', 0) if sync else False

        params.update(
            {
                'data': data,
                'summary': summary,
                'details': details,
                'sync': sync,
                'sync_timeout': sync_timeout,
            }
        )

        if isinstance(sync_timeout, int) and not sync:
            warnings.warn('Argument sync_timeout has no effect without sync set to True')

        return put.put(**params)

    def query(self, metric, start, end, tags=None, aggregator="sum", downsample=None, ms_resolution=True):
        if tags is None:
            tags = {}
        params = self._config
        start = start.timestamp() if isinstance(start, datetime) else start
        end = end.timestamp() if isinstance(end, datetime) else end

        params.update(
            {
                'metric': metric,
                'start': start,
                'end': end,
                'tags': tags,
                'aggregator': aggregator,
                'downsample': downsample,
                'ms_resolution': ms_resolution,

            }
        )

        return query.query(**params)

    def parameters_serializer(self):
        return {
            'host': self._host,
            'port': self._port,
            'protocol': self._protocol
        }


def connect(host, port, **kwargs):
    return TsdbConnector(host, port, **kwargs)

