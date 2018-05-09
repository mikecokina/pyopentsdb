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

    try:
        start = kwargs['start']
    except KeyError:
        raise errors.MissingArgumentsError('start is a required argument')

    try:
        aggregator = kwargs['aggregator']
    except KeyError:
        raise errors.MissingArgumentsError('aggregator is a required argument')

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
        for filter_object in kwargs.get('filters'):
            if not filter_object.get('type') or not filter_object.get('tagk') or not filter_object.get('filter'):
                raise errors.MissingArgumentsError('Missing argument type, tagk or filter in filters item object')

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

    if not queries:
        raise errors.MissingArgumentsError('Missing metrics or tsuids to resolve')

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
    """

    :param host: str
    :param port: str
    :param protocol: str
    :param kwargs: json
    :return:
    """

    warnings.warn('It seems there is still present bug similar to https://github.com/OpenTSDB/opentsdb/issues/817'
                  'Please, avoid expressions containig previous expression like [{"id": "e", "expr": "a + b"}, '
                  '{"id": "e1", "expr": "e + b"}]. It can leads to query with no response.')

    q = dict()

    # time JSON
    time_json = dict()

    try:
        start = kwargs['start']
    except KeyError:
        raise errors.MissingArgumentsError('start is a required argument')

    try:
        aggregator = kwargs['aggregator']
    except KeyError:
        raise errors.MissingArgumentsError('aggregator is a required argument')

    time_json.update({
        'start': int(start.timestamp()),
        'aggregator': aggregator,
    })

    if kwargs.get('end'):
        time_json.update({'end': int(kwargs.get('end').timestamp())})

    if kwargs.get('downsampler'):
        # required params in donwsampler object
        if not kwargs['downsampler'].get('interval') or not kwargs['downsampler'].get('aggregator'):
            raise errors.MissingArgumentsError('Reqiured arguments interval and aggregator in downsampler object')

        if kwargs['downsampler'].get('fillPolicy'):
            # required argument in downsampler.fillPolicy object
            if not kwargs['downsampler']['fillPolicy'].get('policy'):
                raise errors.MissingArgumentsError('Reqiured argument policy in downsampler.fillPolicy object')

        time_json.update({'downsampler': kwargs.get('downsampler')})

    if kwargs.get('rate'):
        time_json.update({'rate': bool(kwargs.get('rate'))})

    q.update({'time': time_json})

    # filters JSON
    if not kwargs.get('filters'):
        raise errors.MissingArgumentsError('At least one filter must be specified (for now) '
                                           'with at least an aggregation function supplied.')
    for filter_object in kwargs.get('filters'):
        # required param in filters object
        if not filter_object.get('id'):
            raise errors.MissingArgumentsError('Missing required argument id in filters object')
        if filter_object.get('tags'):
            # required param in filters.tags objects
            for tags_object in filter_object['tags']:
                if not tags_object.get('type') or not tags_object.get('tagk') or not tags_object.get('filter'):
                    raise errors.MissingArgumentsError('Missing argument type, tagk or '
                                                       'filter in filters.tags object')
    q.update({'filters': kwargs.get('filters')})

    # metrics JSON
    if not kwargs.get('metrics'):
        raise errors.MissingArgumentsError('There must be at least one metric specified.')

    for metric_object in kwargs.get('metrics'):
        # reqired arguments in metric object
        if not metric_object.get('id') or not metric_object.get('filter') or not metric_object.get('metric'):
            raise errors.MissingArgumentsError('Missing id, filter or metric argument in metric object')

        if metric_object.get('fillPolicy'):
            # required argument in metric object fillPolicy
            if not metric_object['fillPolicy'].get('policy'):
                raise errors.MissingArgumentsError('Reqiured argument policy in downsampler.fillPolicy object')
    q.update({'metrics': kwargs.get('metrics')})

    # todo: check wether contained filter id in metrics match any filter id from filters

    # expressions JSON
    # todo: check self-reference expressions and reaise error due to warning in this function
    if not kwargs.get('expressions'):
        raise errors.MissingArgumentsError('At least on expression over the metrics is required')

    for expression_object in kwargs.get('expressions'):
        # reqired arguments in expression object
        if not expression_object.get('id') or not expression_object.get('expr'):
            raise errors.MissingArgumentsError('Missing id or expr argument in expression object')

        if expression_object.get('fillPolicy'):
            if not expression_object['fillPolicy'].get('policy'):
                raise errors.MissingArgumentsError('Reqiured argument policy in one of the expression fillPolicy object')

        if expression_object.get('join'):
            if not expression_object['join'].get('operator'):
                raise errors.MissingArgumentsError('Missign argument operator in expession.join object')

    q.update({'expressions': kwargs.get('expressions')})

    # outputs JSON
    if kwargs.get('outputs'):
        for output_object in kwargs.get('outputs'):
            if not output_object.get('id'):
                raise errors.MissingArgumentsError('Missing argument id in outputs object')
        q.update({'outputs': kwargs.get('outputs')})

    url = api_url(host, port, protocol, pointer='EXP')

    try:
        response = requests.post(url, json.dumps(q))
    except requests.exceptions.ConnectionError:
        raise errors.TsdbConnectionError('Cannot connect to host')

    if response.status_code in [200]:
        return json.loads(response.content.decode())
    elif response.status_code in [400]:
        raise errors.TsdbQueryError(json.dumps(json.loads(response.content.decode()), indent=4))


def api_url(host, port, protocol, pointer):
    if pointer == 'QUERY':
        return '{}://{}:{}/api/query/'.format(protocol, host, port)
    elif pointer == 'EXP':
        return '{}://{}:{}/api/query/exp/'.format(protocol, host, port)
