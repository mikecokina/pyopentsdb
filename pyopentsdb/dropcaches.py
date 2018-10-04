from pyopentsdb import utils


def dropcaches(host, port, protocol, timeout):
    """
    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: dict
    """
    url = api_url(host, port, protocol)
    return utils.request_get(url, timeout)


def api_url(host, port, protocol):
    """
    Make api url for dropcaches

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/dropcaches/'.format(protocol, host, port)
