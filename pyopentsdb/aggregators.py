from pyopentsdb import utils


def aggregators(host, port, protocol, timeout):
    """
    Return all available tsdb aggregators
    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: list
    """
    url = api_url(host, port, protocol)
    return utils.request_get(url, timeout)


def api_url(host, port, protocol):
    """
    Make api url for aggregators
    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/aggregators/'.format(protocol, host, port)
