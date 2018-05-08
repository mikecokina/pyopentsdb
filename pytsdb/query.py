import requests
import json
from pytsdb import errors
import warnings


def query(host, port, protocol, **kwargs):
    """

    :param host: str
    :param port: str
    :param protocol: str
    :param kwargs: dict

    :return: json
    """
    # todo: double check all required params in all objects

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

    if delete_match:
        warnings.warn('To data deletion tsd.http.query.allow_delete has to be set')

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

    url = api_url(host, port, protocol, pointer='QUERY')
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


def exp(host, port, protocol, **kwargs):

    q = dict()

    # time JSON
    time_json = dict()

    try:
        start = kwargs['start']
    except KeyError:
        raise KeyError('start is a required argument')

    try:
        aggregator = kwargs['aggregator']
    except KeyError:
        raise KeyError('aggregator is a required argument')

    time_json.update({
        'start': start.timestamp(),
        'aggregator': aggregator,
    })

    if kwargs.get('end'):
        time_json.update({'end': kwargs.get('end').timestamp()})

    if kwargs.get('downsampler'):
        # required params in donwsampler object
        if not kwargs['downsampler'].get('interval') or not kwargs['downsampler'].get('aggregator'):
            raise KeyError('Reqiured parameters interval and aggregator in downsampler object')

        if kwargs['downsampler'].get('fillPolicy'):
            # required parameter in downsampler.fillPolicy object
            if not kwargs['downsampler']['fillPolicy'].get('policy'):
                raise KeyError('Reqiured parameter policy in downsampler.fillPolicy object')

        time_json.update({'downsampler': kwargs.get('downsampler')})

    if kwargs.get('rate'):
        time_json.update({'rate': bool(kwargs.get('rate'))})

    q.update({'time': time_json})

    # filters JSON
    filters_json = dict()

    if kwargs.get('filters'):
        # required param in filters object
        if not kwargs['filters'].get('id'):
            raise KeyError('Missing required parameter id in filters object')
        filters_json.update({'id': kwargs['filters'].get('id')})

        if kwargs['filters'].get('tags'):
            # required param in filters.tags objects
            for tags_object in kwargs['filters']['tags']:
                if not tags_object.get('type') or not tags_object.get('tagk') or not tags_object.get('filter'):
                    raise KeyError('Missing parameter type, tagk or filter in filters.tags object')
        filters_json.update({'tags': kwargs['filters'].get('tags')})

    q.update({'filters': [filters_json]})
    return q



















def api_url(host, port, protocol, pointer):
    if pointer == 'QUERYe':
        return '{}://{}:{}/api/query/'.format(protocol, host, port)
    elif pointer == 'EXP':
        return '{}://{}:{}/api/query/exp/'.format(protocol, host, port)
