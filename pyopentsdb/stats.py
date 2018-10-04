from pyopentsdb import utils


def stats(host, port, protocol, timeout):
    """
    This endpoint provides a list of statistics for the running TSD.
    Sub endpoints return details about other TSD components such as the JVM,
    thread states or storage client. All statistics are read only.

    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: dict
    """
    url = api_url(host, port, protocol, pointer='STATS')
    return utils.request_get(url, timeout)


def jvm(host, port, protocol, timeout):
    """
    The threads endpoint is used for debugging the TSD's JVM process and includes
    stats about the garbage collector, system load and memory usage. (v2.2)

    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: dict
    """

    url = api_url(host, port, protocol, pointer='JVM')
    return utils.request_get(url, timeout)


def query(host, port, protocol, timeout):
    """
    This endpoint can be used for tracking and troubleshooting queries executed
    against a TSD. It maintains an unbounded list of currently executing
    queries as well as a list of up to 256 completed queries (rotating the oldest
    queries out of memory). Information about each query includes the
    original query, request headers, response code, timing and an exception
    if thrown. (v2.2)

    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: json
    """
    url = api_url(host, port, protocol, pointer='QUERY')
    return utils.request_get(url, timeout)


def region_clients(host, port, protocol, timeout):
    """
    Returns information about the various HBase region server clients in AsyncHBase.
    This helps to identify issues with a particular region server. (v2.2)

    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: json
    """
    url = api_url(host, port, protocol, pointer='REGION_CLIENTS')
    return utils.request_get(url, timeout)


def threads(host, port, protocol, timeout):
    """
    The threads endpoint is used for debugging the TSD and providing insight
    into the state and execution of various threads without having to resort
    to a JStack trace. (v2.2)

    :param host: str
    :param port: str
    :param protocol: str
    :param timeout: int/float/tuple
    :return: json
    """
    url = api_url(host, port, protocol, pointer='THREADS')
    return utils.request_get(url, timeout)


def api_url(host, port, protocol, pointer):
    """
    Make api url to obtain configuration

    :param host: str
    :param port: str
    :param protocol: str
    :param pointer: str
    :return: str
    # """
    if pointer == 'STATS':
        return '{}://{}:{}/api/stats/'.format(protocol, host, port)
    elif pointer == 'JVM':
        return '{}://{}:{}/api/stats/jvm/'.format(protocol, host, port)
    elif pointer == 'QUERY':
        return '{}://{}:{}/api/stats/query/'.format(protocol, host, port)
    elif pointer == 'REGION_CLIENTS':
        return '{}://{}:{}/api/stats/region_clients/'.format(protocol, host, port)
    elif pointer == 'THREADS':
        return '{}://{}:{}/api/stats/threads/'.format(protocol, host, port)
