import requests
import json


def put(host, port, protocol, data, summary, details, sync, sync_timeout):
    """

    :param host: str
    :param port: str
    :param protocol: str
    :param data: list of dicts or dict
    :param summary: bool, Whether or not to return summary information
    :param details: bool, Whether or not to return detailed information
    :param sync: bool, Whether or not to wait for the data to be flushed to storage before returning the results.
    :param sync_timeout: int, A timeout, in milliseconds, to wait for the data to be flushed to storage before
                                returning with an error. When a timeout occurs, using the details flag will tell
                                how many data points failed and how many succeeded. sync must also be given for this
                                to take effect. A value of 0 means the write will not timeout.

    If both detailed and summary are present in a query string, the API will respond with detailed information.

    Standard status code is 204
    Standard status code with details is 200 (request._content example: b'{"success":2,"failed":0,"errors":[]}')

    :return: requests response
    """

    url = api_url(host, port, protocol, summary, details, sync, sync_timeout)
    response = requests.post(url, data=json.dumps(data))
    return response


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
