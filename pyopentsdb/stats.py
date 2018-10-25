from pyopentsdb import utils


def stats(host, r_session, **kwargs):
    """
    This endpoint provides a list of statistics for the running TSD.
    Sub endpoints return details about other TSD components such as the JVM,
    thread states or storage client. All statistics are read only.

    :param host: str
    :param r_session: requests.Session
    :return: dict
    """
    return utils.request_get(api_url(host, pointer='STATS'), r_session, **kwargs)


def jvm(host, r_session, **kwargs):
    """
    The threads endpoint is used for debugging the TSD's JVM process and includes
    stats about the garbage collector, system load and memory usage. (v2.2)

    :param host: str
    :param r_session: requests.Session
    :return: dict
    """
    return utils.request_get(api_url(host, pointer='JVM'), r_session, **kwargs)


def query(host, r_session, **kwargs):
    """
    This endpoint can be used for tracking and troubleshooting queries executed
    against a TSD. It maintains an unbounded list of currently executing
    queries as well as a list of up to 256 completed queries (rotating the oldest
    queries out of memory). Information about each query includes the
    original query, request headers, response code, timing and an exception
    if thrown. (v2.2)

    :param host: str
    :param r_session: requests.Session
    :return: json
    """
    return utils.request_get(api_url(host, pointer='QUERY'), r_session, **kwargs)


def region_clients(host, r_session, **kwargs):
    """
    Returns information about the various HBase region server clients in AsyncHBase.
    This helps to identify issues with a particular region server. (v2.2)

    :param host: str
    :param r_session: requests.Session
    :return: json
    """
    return utils.request_get(api_url(host, pointer='REGION_CLIENTS'), r_session, **kwargs)


def threads(host, r_session, **kwargs):
    """
    The threads endpoint is used for debugging the TSD and providing insight
    into the state and execution of various threads without having to resort
    to a JStack trace. (v2.2)

    :param host: str
    :param r_session: requests.Session
    :return: json
    """
    return utils.request_get(api_url(host, pointer='THREADS'), r_session, **kwargs)


def api_url(host, pointer):
    """
    Make api url to obtain configuration

    :param host: str
    :param pointer: str
    :return: str
    """
    if pointer == 'STATS':
        return '{}/api/stats/'.format(host)
    elif pointer == 'JVM':
        return '{}/api/stats/jvm/'.format(host)
    elif pointer == 'QUERY':
        return '{}/api/stats/query/'.format(host)
    elif pointer == 'REGION_CLIENTS':
        return '{}/api/stats/region_clients/'.format(host)
    elif pointer == 'THREADS':
        return '{}/api/stats/threads/'.format(host)
