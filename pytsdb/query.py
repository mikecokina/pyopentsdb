import requests
import json
from pytsdb import errors


def query(host, port, protocol, **kwargs):
    """

    :param host: str
    :param port: str
    :param protocol: str
    :param kwargs: dict

    :return: json
    """
    try:
        start = kwargs['start']
    except KeyError:
        raise KeyError('start is a required argument')

    try:
        aggregator = kwargs['aggregator']
    except KeyError:
        raise KeyError('aggregator is a required argument')

    end = kwargs.get('end') or None
    ms_resolution = bool(kwargs.get('ms', False))
    show_tsuids = bool(kwargs.get('show_tsuids', False))
    no_annotations = bool(kwargs.get('no_annotations', False))
    global_annotations = bool(kwargs.get('global_annotations', False))
    show_summary = bool(kwargs.get('show_summary', False))
    show_stats = bool(kwargs.get('show_stats', False))
    show_query = bool(kwargs.get('show_query', False))
    delete_match = bool(kwargs.get('delete', False))
    timezone = kwargs.get('timezone', 'UTC')
    use_calendar = bool(kwargs.get('use_calendar', False))

    # basic
    params = {
        'start': '{}'.format(int(start.timestamp())),
        'msResolution': ms_resolution,
        'showTSUIDs': show_tsuids,
        'noAnnotations': no_annotations,
        'globalAnnotations': global_annotations,
        'showSummary': show_summary,
        'showStats': show_stats,
        'showQuery': show_query,
        'delete': delete_match,
        'timezone': timezone,
        'useCalendar': use_calendar,
        'queries': list(),
    }

    if end:
        params.update({'end': int(end.timestamp())})

    q, mq, tq = dict(), dict(), dict()
    queries = list()

    q.update({'aggregator': aggregator})
    q.update({'explicitTags': bool(kwargs.get('explicit_tags', False))})
    q.update({'rate': bool(kwargs.get('rate', False))})
    # todo: check, whether rateOptions is working or not
    if kwargs.get('rate_options'):
        q.update({'rateOptions': kwargs.get('rate_options')})

    if kwargs.get('tags'):
        q.update({'tags': kwargs.get('tags')})

    if kwargs.get('filters'):
        q.update({'filters': kwargs.get('filters')})

    if kwargs.get('downsample'):
        q.update({'downsample': kwargs.get('downsample')})

    if kwargs.get('metric'):
        mq = q.copy()
        mq.update(
            {
                'metric': kwargs.get('metric'),
            }
        )
        queries.append(mq)

    if kwargs.get('tsuids'):
        tq = q.copy()
        tq.update(
            {
                'tsuids': kwargs['tsuids'],
            }
        )
        queries.append(tq)

    params.update({'queries': queries})

    url = api_url(host, port, protocol)
    try:
        response = requests.post(url, json.dumps(params))
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    if response.status_code in [200]:
        return json.loads(response.content.decode())
    elif response.status_code in [400]:
        raise errors.TsdbQueryError(json.dumps(json.loads(response.content.decode()), indent=4))


def delete(host, port, protocol, **kwargs):
    """

    :param host: str
    :param port: str
    :param protocol: str
    :param kwargs: dict

    :return: json
    """

    kwargs.update({'delete': True})
    return query(host, port, protocol, **kwargs)


def api_url(host, port, protocol):
    return '{}://{}:{}/api/query/'.format(protocol, host, port)
