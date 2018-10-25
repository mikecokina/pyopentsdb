from pyopentsdb import utils


def tsdb_configuration(host, r_session, **kwargs):
    return utils.request_get(api_url(host, pointer='CONFIG'), r_session, **kwargs)


def filters(host, r_session, **kwargs):
    """
    :param host: str
    :param r_session: requests.Session
    :return: dict
    """
    return utils.request_get(api_url(host, pointer='FILTERS'), r_session, **kwargs)


def api_url(host, pointer):
    """
    Make api url to obtain configuration

    :param host: str
    :param pointer: str
    :return: str
    """
    if pointer == 'CONFIG':
        return '{}/api/config/'.format(host)
    elif pointer == 'FILTERS':
        return '{}/api/config/filters/'.format(host)
