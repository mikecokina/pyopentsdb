import requests
import json


def query(host, port, protocol, **kwargs):
    """

    :param host:
    :param port:
    :param protocol:
    :param kwargs: dict
           start:
                    The start time for the query. This can be a relative or absolute timestamp.
                    See Querying or Reading Data for details.

           end:
                    An end time for the query. If not supplied, the TSD will assume the local system time on the server.
                    This may be a relative or absolute timestamp. See Querying or Reading Data for details.

           metric:
                    The name of a metric stored in the system

           aggregator:
                    The name of an aggregation function to use. See /api/aggregators

           ms:
                    Whether or not to output data point timestamps in milliseconds or seconds.
                    The msResolution flag is recommended. If this flag is not provided and there
                    are multiple data points within a second, those data points will be down sampled
                    using the query's aggregation function.

           show_tsuids:
                    Whether or not to output the TSUIDs associated with timeseries in the results.
                    If multiple time series were aggregated into one set, multiple TSUIDs will be
                    returned in a sorted manner


           no_annotations:
                    Whether or not to return annotations with a query. The default is to return annotations
                    for the requested timespan but this flag can disable the return. This affects both local
                    and global notes and overrides globalAnnotations

           global_annotations:
                    Whether or not the query should retrieve global annotations for the requested timespan

           show_summary:
                    Whether or not to show a summary of timings surrounding the query in the results. This creates
                    another object in the map that is unlike the data point objects. See Query Details and Stats

           show_stats:
                    Whether or not to show detailed timings surrounding the query in the results. This creates
                    another object in the map that is unlike the data point objects. See Query Details and Stats

           show_query:
                    Whether or not to return the original sub query with the query results. If the request
                    contains many sub queries then this is a good way to determine which results belong to which
                    sub query. Note that in the case of a * or wildcard query, this can produce
                    a lot of duplicate output.

           delete:
                    Can be passed to the JSON with a POST to delete any data points that match the given query.

           timezone:
                    An optional timezone for calendar-based downsampling. Must be a valid timezone database name
                    supported by the JRE installed on the TSD server.

           use_calendar:
                    Whether or not use the calendar based on the given timezone for downsampling intervals

    :return:
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

    if end:
        params.update({'end': int(end.timestamp())})

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
