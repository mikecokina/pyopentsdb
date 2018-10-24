from pyopentsdb import utils


def dropcaches(host, port, protocol):
    """
    :param host: str
    :param port: str
    :param protocol: str
    :return: dict
    """
    url = api_url(host, port, protocol)
    return utils.request_get(url)


def api_url(host, port, protocol):
    """
    Make api url for dropcaches

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/dropcaches/'.format(protocol, host, port)
