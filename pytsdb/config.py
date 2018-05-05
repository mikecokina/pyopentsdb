import requests
import json

def tsdb_configuration(host, port, protocol):
    url = api_url(host, port, protocol)
    response = requests.get(url)

    if response.status_code in [200, 201]:
        return json.loads(response.content.decode())

def api_url(host, port, protocol):
    """
    Make api url to obtain configuration

    :param host: str
    :param port: str
    :param protocol: str
    :return: str
    """
    return '{}://{}:{}/api/config/'.format(protocol, host, port)