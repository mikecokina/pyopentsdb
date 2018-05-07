import requests
import json


def query(host, port, protocol, **kwargs):
    try:
        start = kwargs['start']
    except KeyError:
        raise KeyError('start is a required argument')

    try:
        end = kwargs['end']
    except KeyError:
        raise KeyError('end is a required argument')

    try:
        aggregator = kwargs['aggregator']
    except KeyError:
        raise KeyError('aggregator is a required argument')

    ms_resolution = kwargs.get('ms', False)
    show_tsuids = kwargs.get('show_tsuids', False)
    no_annotations = kwargs.get('no_annotations', False)
    global_annotations = kwargs.get('global_annotations', False)
    show_summary = kwargs.get('show_summary', False)
    show_stats = kwargs.get('show_stats', False)
    show_query = kwargs.get('show_query', False)
    delete_match = kwargs.get('delete', False)
    timezone = kwargs.get('timezone', 'UTC')
    use_calendar = kwargs.get('use_calendar', False)

    # basic
    params = {
        'start': '{}'.format(int(start.timestamp())),
        'end': '{}'.format(int(end.timestamp())),
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
        'queries': [],
    }

    m_query, tsuids_query = dict(), dict()
    queries, metric_filters, tsuids_filters = list(), list(), list()

    if 'metric' in kwargs.keys():
        m_query.update(
            {
                'aggregator': aggregator,
                'metric': kwargs['metric'],
                "filters": [],
            }
        )

        queries.append(m_query)

    if 'tsuids' in kwargs.keys():
        tsuids_query.update(
            {
                'aggregator': aggregator,
                'tsuids': kwargs['tsuids'],
                "filters": [],

                # "filters": [
                #     {
                #         "type": "wildcard",
                #         "tagk": "host",
                #         "filter": "web02*",
                #         "groupBy": True
                #     }]
            }
        )

        queries.append(tsuids_query)

    params.update({'queries': queries})

    print(json.dumps(params, indent=4))

    url = api_url(host, port, protocol)
    response = requests.post(url, json.dumps(params))

    if response.status_code in [200]:
        return json.loads(response.content.decode())
    pass


def delete():
    pass


def api_url(host, port, protocol):
    return '{}://{}:{}/api/query/'.format(protocol, host, port)
