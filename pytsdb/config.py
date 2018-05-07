import requests
import json

def tsdb_configuration(host, port, protocol):
    url = api_url(host, port, protocol, pointer='CONFIG')
    response = requests.get(url)

    if response.status_code in [200]:
        return json.loads(response.content.decode())

    raise Exception('unknown error')


def filters(host, port, protocol):
    url = api_url(host, port, protocol, pointer='FILTERS')
    response = requests.get(url)

    if response.status_code in [200]:
        return json.loads(response.content.decode())

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
