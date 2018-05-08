from pytsdb import request


def tsdb_configuration(host, port, protocol):
    url = api_url(host, port, protocol, pointer='CONFIG')
    return request.generic_request(url)


def filters(host, port, protocol):
    url = api_url(host, port, protocol, pointer='FILTERS')
    return request.generic_request(url)


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
