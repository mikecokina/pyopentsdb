import requests
import json
from pytsdb import errors


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

def api_url(host, port, protocol):
    """
    Make api url to obtain configuration

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/suggest/'.format(protocol, host, port)
