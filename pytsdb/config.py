import requests
import json
from pytsdb import errors

def tsdb_configuration(host, port, protocol):
    url = api_url(host, port, protocol, pointer='CONFIG')

    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    # todo: single function handle request response across entire project
    if response.status_code in [200]:
        return json.loads(response.content.decode())
    elif response.status_code in [400]:
        raise errors.TsdbQueryError(json.dumps(json.loads(response.content.decode()), indent=4))

    raise Exception('unknown error')


def filters(host, port, protocol):
    url = api_url(host, port, protocol, pointer='FILTERS')

    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    if response.status_code in [200]:
        return json.loads(response.content.decode())
    elif response.status_code in [400]:
        raise errors.TsdbQueryError(json.dumps(json.loads(response.content.decode()), indent=4))

    # todo: change uknown error to error w/ more informative value
    raise Exception('unknown error')


def api_url(host, port, protocol, pointer):
    """
    Make api url to obtain configuration

    :param host: str
    :param port: str
    :param protocol: str
    :param pointer: str
    :return: str
    """
    if pointer == 'CONFIG':
        return '{}://{}:{}/api/config/'.format(protocol, host, port)
    elif pointer == 'FILTERS':
        return '{}://{}:{}/api/config/filters/'.format(protocol, host, port)
