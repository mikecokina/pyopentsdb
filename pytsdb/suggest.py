import requests
import json
from pytsdb import errors
import re


def suggest(host, port, protocol, **kwargs):
    url = api_url(host, port, protocol)

    try:
        t = kwargs['type']
    except KeyError:
        raise KeyError('type is a required argument')

    params = dict()
    params.update(
        {
            'type': t,
            'q': kwargs.get('q', ''),
            'max': int(kwargs.get('max', 25))
        }
    )

    try:
        response = requests.post(url, json.dumps(params))
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    if response.status_code in [200]:
        return json.loads(response.content.decode())
    elif response.status_code in [400]:
        raise errors.TsdbQueryError(json.dumps(json.loads(response.content.decode()), indent=4))


def metrics(host, port, protocol, **kwargs):
    """
    give a list of available metrics

    :param host: str
    :param port: str
    :param protocol: str
    :param kwargs: dict
    :return: json
    """

    kwargs.update({
        'type': 'metrics',
        'q': '',
        'max': kwargs.get('max', 10000),
    })

    metric_list = suggest(host, port, protocol, **kwargs)
    regxp = kwargs.get('regxp')

    if regxp:
        metric_list = [metric for metric in metric_list if re.search(re.compile(regxp), str(metric), flags=0)]
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
