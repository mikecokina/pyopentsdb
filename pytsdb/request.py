import requests
import json
from pytsdb import errors


def generic_request(url):
    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    if response.status_code in [200]:
        return json.loads(response.content.decode())
    elif response.status_code in [400]:
        raise errors.TsdbQueryError(json.dumps(json.loads(response.content.decode()), indent=4))

    raise Exception('unknown error')
