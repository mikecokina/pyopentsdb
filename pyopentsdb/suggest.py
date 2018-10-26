import re

from pyopentsdb import errors
from pyopentsdb import utils


def suggest(host, r_session, **kwargs):
    try:
        t = kwargs.pop('type')
    except KeyError:
        raise errors.MissingArgumentError("'type' is a required argument")
    kwargs.update(
        {
            "data":
                {
                    'type': t,
                    'q': kwargs.pop('q', ''),
                    'max': int(kwargs.pop('max', 25))
                }
            }
        )
    return utils.request_post(api_url(host), r_session, **kwargs)


def metrics(host, r_session, **kwargs):
    kwargs.pop("type", None)
    regexp = kwargs.pop('regexp', None)

    kwargs.update({
        'type': 'metrics',
        'q': kwargs.pop('q', ''),
        'max': kwargs.pop('max', 10000),
    })

    metric_list = suggest(host, r_session, **kwargs)
    if regexp:
        metric_list = [metric for metric in metric_list if re.search(re.compile(regexp), str(metric), flags=0)]
    return metric_list


def api_url(host):
    """
    Make api url to obtain configuration
    :param host: str
    :return: str
    """
    return '{}/api/suggest/'.format(host)
