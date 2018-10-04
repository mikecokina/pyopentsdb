import re

from pyopentsdb import errors
from pyopentsdb import utils


def suggest(host, port, protocol, timeout, **kwargs):
    url = api_url(host, port, protocol)
    try:
        t = kwargs['type']
    except KeyError:
        raise errors.MissingArgumentError("'type' is a required argument")

    params = dict()
    params.update(
        {
            'type': t,
            'q': kwargs.get('q', ''),
            'max': int(kwargs.get('max', 25))
        }
    )

    return utils.request_post(url, params, timeout)


def metrics(host, port, protocol, timeout, **kwargs):
    kwargs.update({
        'type': 'metrics',
        'q': kwargs.get('q', ''),
        'max': kwargs.get('max', 10000),
    })

    metric_list = suggest(host, port, protocol, timeout, **kwargs)
    regexp = kwargs.get('regexp')
    if regexp:
        metric_list = [metric for metric in metric_list if re.search(re.compile(regexp), str(metric), flags=0)]
    return metric_list


def api_url(host, port, protocol):
    """
    Make api url to obtain configuration
    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/suggest/'.format(protocol, host, port)
