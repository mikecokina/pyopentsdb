from pyopentsdb import utils


def aggregators(host, port, protocol):
    """
    Return all available tsdb aggregators
    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: list
    """
    url = api_url(host, port, protocol)
    return utils.request_get(url)


def api_url(host, port, protocol):
    """
    Make api url for aggregators
    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}/api/aggregators/'.format(utils.get_basic_url(host=host, port=port, protocol=protocol))
