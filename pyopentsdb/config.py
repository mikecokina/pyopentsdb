from pyopentsdb import utils


def tsdb_configuration(host, port, protocol, timeout):
    url = api_url(host, port, protocol, pointer='CONFIG')
    return utils.request_get(url, timeout)


def filters(host, port, protocol, timeout):
    """
    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: dict
    """
    url = api_url(host, port, protocol, pointer='FILTERS')
    return utils.request_get(url, timeout)


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
