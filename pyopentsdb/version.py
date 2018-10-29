from pyopentsdb import utils


def version(host, r_session, **kwargs):
    """
    This endpoint returns information about the running version of OpenTSDB.

    :param host: str
    :param r_session: requests.Session
    :return: json
    """
    return utils.request_get(api_url(host), r_session, **kwargs)


def api_url(host):
    """
    Make api url to obtain version info

    :param host: str
    :return: str
    # """
    return '{}/api/version/'.format(host)
