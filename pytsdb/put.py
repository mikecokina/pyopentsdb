from pytsdb.utils import request_post
from pytsdb import errors


def validate_put_data(data):
    if isinstance(data, dict):
        data = [data]

    for d in data:
        if not d.get('metric') or not not d.get('timestamp') or not not d.get('value') or not not d.get('tags'):
            raise errors.MissingArgumentError("Missing argument/s in put data")


def put(host, port, protocol, timeout, data, **kwargs):
    """
    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple; requests.request timeout
    :param data: list of dicts or dict
    :param kwargs: see bellow
        :**kwargs options**:
               * **summary** * --  bool;
                    Whether or not to return summary information
               * **details** * --  bool;
                    Whether or not to return detailed information
               * **sync** * --  bool;
                    Whether or not to wait for the data to be flushed to storage before returning the results.
               * **sync_timeout** * --  int;
                    A timeout, in milliseconds, to wait for the data to be flushed to storage before
                    returning with an error. When a timeout occurs, using the details flag will tell
                    how many data points failed and how many succeeded. sync must also be given for this
                    to take effect. A value of 0 means the write will not timeout.
    :return: dict
    """

    validate_put_data(data)
    summary = kwargs.get('summary', False)
    details = kwargs.get('details', False)
    sync = kwargs.get('sync', False)
    sync_timeout = kwargs.get('sync_timeout', 0) if sync else False
    url = api_url(host, port, protocol, summary, details, sync, sync_timeout)

    return request_post(url, data, timeout)


def api_url(host, port, protocol, summary, details, sync, sync_timeout):
    url = '{}://{}:{}/api/put/'.format(protocol, host, port)
    url, previous = (''.join([url, '?summary']), True) if summary else (url, False)

    if details:
        url, previous = (''.join([url, '?details']), True) if not previous else (''.join([url, '&details']), True)
    if sync:
        url, previous = (''.join([url, '?sync']), True) if not previous else (''.join([url, '&sync']), True)
    if isinstance(sync_timeout, int) and not isinstance(sync_timeout, bool):
        url = ''.join([url, '?sync_timeout={}'.format(sync_timeout)]) \
            if not previous \
            else ''.join([url, '&sync_timeout={}'.format(sync_timeout)])
    return url
