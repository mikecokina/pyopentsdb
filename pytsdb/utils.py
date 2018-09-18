import time
import json
from pytsdb import errors
import requests


def make_response_message(code, message):
    return dict(
        code=int(code),
        message=message if isinstance(message, (list, dict)) else str(message)
    )


def try_contact_tsdb(exec_fn, max_attempts=3, timeout=2):
    __MAX_TSDB_CONTACT_ATTEMPTS__ = max_attempts
    __TIMEOUT__ = timeout
    n_run = 0
    while n_run < __MAX_TSDB_CONTACT_ATTEMPTS__:
        try:
            result = exec_fn()
            if result is None:
                time.sleep(__TIMEOUT__)
                continue
            return result
        except requests.exceptions.ConnectionError:
            time.sleep(__TIMEOUT__)
            continue
        finally:
            n_run += 1
    raise errors.TsdbConnectionError("Cannot connect to TSDB host")


def request(requests_fn):
    try:
        response = try_contact_tsdb(exec_fn=requests_fn)
        if response.status_code in [200]:
            return json.loads(response.content.decode())
        elif response.status_code in [204]:
            return None
        elif response.status_code in [400]:
            raise errors.ArgumentError(json.loads(response.content.decode())["error"]["message"])
        raise errors.UncaughtError(json.loads(response.content.decode()) if response.content else 'Unknown error')
    except errors.ArgumentError:
        raise
    except errors.TsdbConnectionError:
        raise
    except Exception as e:
        raise errors.UncaughtError(str(e))


def request_post(url, params, timeout):
    return request(lambda: requests.post(url, json.dumps(params), timeout=timeout))


def request_get(url, timeout):
    return request(lambda: requests.get(url, timeout=timeout))
