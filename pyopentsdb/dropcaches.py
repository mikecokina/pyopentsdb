from pyopentsdb import utils


def dropcaches(host, r_session, **kwargs):
    """
    :param host: str
    :param r_session: requests.Session
    :return: dict
    """
    return utils.request_get(api_url(host), r_session, **kwargs)


def api_url(host):
    """
    Make api url for dropcaches

    :param host: str
    :return: str
    """
    return '{}/api/dropcaches/'.format(host)
