from pyopentsdb import utils


def aggregators(host, r_session, **kwargs):
    """
    Return all available tsdb aggregators
    :param r_session: requests.Session
    :param host: str
    :return: list
    """
    url = api_url(host)
    return utils.request_get(url, r_session, **kwargs)


def api_url(host):
    """
    Make api url for aggregators
    :param host: str
    :return: str
    """
    return '{}/api/aggregators/'.format(host)
