from pyopentsdb import utils
from pyopentsdb.conf import ConfigPointer


def tsdb_configuration(host, r_session, **kwargs):
    return utils.request_get(api_url(host, pointer=ConfigPointer.CONFIG), r_session, **kwargs)


def filters(host, r_session, **kwargs):
    """
    :param host: str
    :param r_session: requests.Session
    :return: dict
    """
    return utils.request_get(api_url(host, pointer=ConfigPointer.FILTERS), r_session, **kwargs)


def api_url(host, pointer):
    """
    Make api url to obtain configuration

    :param host: str
    :param pointer: str
    :return: str
    """
    if pointer == ConfigPointer.CONFIG:
        return '{}/api/config/'.format(host)
    elif pointer == ConfigPointer.FILTERS:
        return '{}/api/config/filters/'.format(host)
