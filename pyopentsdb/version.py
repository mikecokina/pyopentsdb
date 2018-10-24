from pyopentsdb import utils


def version(host, port, protocol):
    """
    This endpoint returns information about the running version of OpenTSDB.

    :param host: str
    :param port: str
    :param protocol: str
    :return: json
    """
    url = api_url(host, port, protocol)
    return utils.request_get(url)


def api_url(host, port, protocol):
    """
    Make api url to obtain version info

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    # """
    return '{}://{}:{}/api/version/'.format(protocol, host, port)
