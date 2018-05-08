import requests
import json
from pytsdb import errors

def aggregators(host, port, protocol):
    """
    Return all available tsdb aggregators

    :param host: str
    :param port: str
    :param protocol: str
    :return: list
    """
    url = api_url(host, port, protocol)
    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    if response.status_code in [200]:
        return json.loads(response.content.decode())


def api_url(host, port, protocol):
    """
    Make api url for aggregators

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/aggregators/'.format(protocol, host, port)
