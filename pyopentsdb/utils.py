import time
import json
from pyopentsdb import errors
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
    result = None
    while n_run < __MAX_TSDB_CONTACT_ATTEMPTS__:
        try:
            result = exec_fn()
            if result is None:
                time.sleep(__TIMEOUT__)
                continue
            elif result.status_code in [424]:
                raise errors.FailedDependency
            return result
        except (requests.exceptions.ConnectionError, errors.FailedDependency) as e:
            if 'SSLError' in str(e):
                raise errors.SSLError(str(e))

            time.sleep(__TIMEOUT__)
            continue
        finally:
            n_run += 1

    if result is not None:
        return result
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
        elif response.status_code in [403]:
            raise errors.ForbiddenError(response.content.decode())
        elif response.status_code in [424]:
            raise errors.FailedDependency(json.loads(response.content.decode())["error"]["message"])
        raise errors.UncaughtError(json.loads(response.content.decode()) if response.content else 'Unknown error')
    except errors.ArgumentError:
        raise
    except errors.TsdbConnectionError:
        raise
    except errors.ForbiddenError:
        raise
    except Exception as e:
        raise errors.UncaughtError(str(e))


def request_post(url, r_session, **kwargs):
    data = json.dumps(kwargs.pop('data', dict()))
    return request(lambda: r_session.post(url, data=data, **kwargs))


def request_get(url, r_session, **kwargs):
    return request(lambda: r_session.get(url, **kwargs))
