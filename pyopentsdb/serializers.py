from pyopentsdb import utils


def serializers(host, port, protocol):
    """

    :param host: str
    :param port: str
    :param protocol: str
    :return: json
    """
    url = api_url(host, port, protocol)
    return utils.request_get(url)


def api_url(host, port, protocol):
    """
    Make api url to obtain configuration

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/serializers/'.format(protocol, host, port)
