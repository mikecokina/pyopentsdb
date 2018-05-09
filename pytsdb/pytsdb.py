from pytsdb import aggregators
from pytsdb import config
from pytsdb import put
from pytsdb import query
from pytsdb import stats
from pytsdb import version
from pytsdb import suggest
from pytsdb import dropcaches
from pytsdb import serializers
import warnings


class TsdbConnector(object):
    def __init__(self, host, port, **kwargs):
        self._host = host
        self._port = port
        self._protocol = kwargs.get('protocol', 'http')
        self._config = self._parameters_serializer()

    def aggregators(self):
        """
        This endpoint simply lists the names of implemented aggregation functions used in timeseries queries.

        :return:  json (list)
        """
        return aggregators.aggregators(**self._config)

    def config(self):
        """
        This endpoint returns information about the running configuration of the TSD.
        It is read only and cannot be used to set configuration options.

        :return: json
        """
        return config.tsdb_configuration(**self._config)

    def put(self, data, **kwargs):
        """
        Metric has to be created or enable tsd.core.auto_create_metrics

        :param data: list of dicts or dict
        :param kwargs: dict
               summary: bool
                    Whether or not to return summary information

               details: bool
                    Whether or not to return detailed information
               sync: bool
                    Whether or not to wait for the data to be flushed to storage before returning the results.

               sync_timeout: int
                    A timeout, in milliseconds, to wait for the data to be flushed to storage before
                    returning with an error. When a timeout occurs, using the details flag will tell
                    how many data points failed and how many succeeded. sync must also be given for this
                    to take effect. A value of 0 means the write will not timeout.

        If both detailed and summary are present in a query string, the API will respond with detailed information.

        Standard status code is 204
        Standard status code with details is 200 (request._content example: b'{"success":2,"failed":0,"errors":[]}')

        :return: requests response
        """
        params = self._config
        summary = kwargs.get('summary', False)
        details = kwargs.get('details', False)
        sync = kwargs.get('sync', False)
        sync_timeout = kwargs.get('sync_timeout', 0) if sync else False

        params.update(
            {
                'data': data,
                'summary': summary,
                'details': details,
                'sync': sync,
                'sync_timeout': sync_timeout,
            }
        )

        if isinstance(sync_timeout, int) and not isinstance(sync_timeout, bool) and not sync:
            warnings.warn('Argument sync_timeout has no effect without sync set to True')

        return put.put(**params)

    def query(self, **kwargs):
        """
        Enables extracting data from the storage system in various formats determined by
        the serializer selected. Queries can be submitted via the 1.0 query string format
        or body content.

        :param kwargs: dict
               start: datetime
                        The start time for the query. This can be a relative or absolute timestamp.
                        See Querying or Reading Data for details.

               end: datetime
                        An end time for the query. If not supplied, the TSD will assume the local system time on the server.
                        This may be a relative or absolute timestamp. See Querying or Reading Data for details.

               metric: str
                        The name of a metric stored in the system

               aggregator: str
                        The name of an aggregation function to use. See /api/aggregators

               ms: bool
                        Whether or not to output data point timestamps in milliseconds or seconds.
                        The msResolution flag is recommended. If this flag is not provided and there
                        are multiple data points within a second, those data points will be down sampled
                        using the query's aggregation function.

               show_tsuids: bool
                        Whether or not to output the TSUIDs associated with timeseries in the results.
                        If multiple time series were aggregated into one set, multiple TSUIDs will be
                        returned in a sorted manner


               no_annotations: bool
                        Whether or not to return annotations with a query. The default is to return annotations
                        for the requested timespan but this flag can disable the return. This affects both local
                        and global notes and overrides globalAnnotations

               global_annotations: bool
                        Whether or not the query should retrieve global annotations for the requested timespan

               show_summary: bool
                        Whether or not to show a summary of timings surrounding the query in the results. This creates
                        another object in the map that is unlike the data point objects. See Query Details and Stats

               show_stats: bool
                        Whether or not to show detailed timings surrounding the query in the results. This creates
                        another object in the map that is unlike the data point objects. See Query Details and Stats

               show_query: bool
                        Whether or not to return the original sub query with the query results. If the request
                        contains many sub queries then this is a good way to determine which results belong to which
                        sub query. Note that in the case of a * or wildcard query, this can produce
                        a lot of duplicate output.

               delete: bool
                        Can be passed to the JSON with a POST to delete any data points that match the given query.

               timezone: str
                        An optional timezone for calendar-based downsampling. Must be a valid timezone database name
                        supported by the JRE installed on the TSD server.

               use_calendar: bool
                        Whether or not use the calendar based on the given timezone for downsampling intervals

               filters: json
                        type: str
                            The name of the filter to invoke. See /api/config/filters

                        tagk: str
                            The tag key to invoke the filter on

                        filter:
                            The filter expression to evaluate and depends on the filter being used

                        groupBy: bool
                            Whether or not to group the results by each value matched by the filter.
                            By default all values matching the filter will be aggregated into a single series.

                        example:
                            filters = [
                            {
                                "type": "wildcard",
                                "tagk": "host",
                                "filter": "web01*",
                                "groupBy": False
                            }]

               tags: dict
                        To drill down to specific timeseries or group results by tag, supply one or more map values
                        in the same format as the query string. Tags are converted to filters in 2.2.
                        See the notes below about conversions. Note that if no tags are specified, all metrics
                        in the system will be aggregated into the results. Deprecated in 2.2

                        example:
                            {tagk: tagv}

               explicit_tags: bool
                        Returns the series that include only the tag keys provided in the filters.

               downsample: str
                        An optional downsampling function to reduce the amount of data returned.

                        examples:
                            1h-sum
                            30m-avg-nan
                            24h-max-zero
                            1dc-sum
                            0all-sum

                        pattern:
                            <Size><Units>-<Aggregator>-<Fill Policy>

               rate: bool
                        Whether or not the data should be converted into deltas before returning.
                        This is useful if the metric is a continuously incrementing counter and you
                        want to view the rate of change between data points.

               rate_options: json
                        counter: bool
                            Whether or not the underlying data is a monotonically increasing counter that may roll over

                        counterMax: int
                            A positive integer representing the maximum value for the counter.

                        resetValue: int
                            An optional value that, when exceeded, will cause the aggregator to return a 0 instead
                            of the calculated rate. Useful when data sources are frequently reset to avoid spurious spikes.

                        dropResets: bool
                            Whether or not to simply drop rolled-over or reset data points.


        :return: json
        """
        return query.query(self._host, self._port, self._protocol, **kwargs)

    def query_exp(self, **kwargs):
        """
        This endpoint allows for querying data using expressions. The query is broken up into different sections.

        Two set operations (or Joins) are allowed. The union of all time series ore the intersection.

        For example we can compute "a + b" with a group by on the host field. Both metrics queried alone
        would emit a time series per host, e.g. maybe one for "web01", "web02" and "web03". Lets say
        metric "a" has values for all 3 hosts but metric "b" is missing "web03".

        With the intersection operator, the expression will effectively add "a.web01 + b.web01" and "a.web02 + b.web02"
        but will skip emitting anything for "web03". Be aware of this if you see fewer outputs that you expected
        or you see errors about no series available after intersection.

        With the union operator the expression will add the web01 and web02 series but for metric "b",
        it will substitute the metric's fill policy value for the results.


        :param kwargs: dict
               start: datetime
                    The start time for the query. This may be relative, absolute human readable or absolute Unix Epoch.

               aggregator: str
                    The global aggregation function to use for all metrics. It may be overridden on a per metric basis.

               end: datetime
                    The end time for the query. If left out, the end is now

               downsampler: json like
                    Reduces the number of data points returned. The format is defined below

                        interval: str
                            A downsampling interval, i.e. what time span to rollup raw
                            values into. The format is <#><unit>, e.g. 15m

                        aggregator: str
                            The aggregation function to use for reducing the data points

                        fillPolicy: json like
                            A policy to use for filling buckets that are missing data points

                                policy: str
                                    The name of a policy to use. The values are listed in the table below

                                value: double
                                    For scalar fills, an optional value that can be used during substitution

                    example:
                        downsampler = {
                            "interval":"15m",
                            "aggregator":"max"
                        }

               filters: json
                    Filters are for selecting various time series based on the tag keys and values.
                    At least one filter must be specified (for now) with at least an aggregation
                    function supplied. Fields include:

                        id: str
                            A unique ID for the filter. Cannot be the same as any metric or expression ID

                        tags: json
                            A list of filters on tag values

                                type; str
                                    The name of the filter from the API

                                tagk: str
                                    The tag key name such as host or colo that we filter on

                                filter: str
                                    The value to filter on. This depends on the filter in use. See the API for details

                                groupBy: bool
                                    Whether or not to group results by the tag values matching this filter.
                                    E.g. grouping by host will return one result per host. Not grouping by host
                                    would aggregate (using the aggregation function) all results for the metric
                                    into one series

                    example:
                        filters = [
                            {
                                'id': 'x',
                                'tags': [{
                                    "type": "literal_or",
                                    "tagk": "host",
                                    "filter": "web02",
                                    "groupBy": True
                                }]
                            },
                            {
                                'id': 'y',
                                'tags': [{
                                    "type": "literal_or",
                                    "tagk": "host",
                                    "filter": "web02",
                                    "groupBy": True
                                }]
                            },
                        ]

               metrics: json
                    The metrics list determines which metrics are included in the expression.
                    There must be at least one metric.

                    id: str
                        A unique ID for the metric. This MUST be a simple string, no punctuation or spaces

                    filter: str
                        The filter id to use when fetching this metric. It must match a filter in the filters array

                    metric: str
                        The name of a metric in OpenTSDB

                    aggregator: str
                        An optional aggregation function to overload the global function in time for just this metric

                    fillPolicy: json
                        If downsampling is not used, this can be included to determine what to emit in calculations.
                        It will also override the downsampling policy (for more information see downsampler)

                    example:
                        metrics = [
                           {
                               "id": "a",
                               "metric": "etc.cpu.nice",
                               "filter": "y",
                               "fillPolicy": {"policy": "NaN"}
                           },
                           {
                               "id": "b",
                               "metric": "sys.cpu.nice",
                               "filter": "x",
                               "fillPolicy": {"policy": "NaN"}
                           }
                       ]

               expressions: json
                    A list of one or more expressions over the metrics. The variables in an expression MUST refer
                    to either a metric ID field or an expression ID field. Nested expressions are supported
                    but exceptions will be thrown if a self reference or circular dependency is detected.
                    So far only basic operations are supported such as addition, subtraction, multiplication,
                    division, modulo

                    id: str
                        A unique ID for the expression

                    expr: str
                        The expression to execute

                    join: str
                        The set operation or "join" to perform for series across sets.

                            operator: str
                                The operator to use, either union or intersection

                            useQueryTags: bool
                                Whether or not to use just the tags explicitly defined
                                in the filters when computing the join keys

                            includeAggTags: bool
                                Whether or not to include the tag keys that were aggregated
                                out of a series in the join key

                    fillPolicy: json
                        An optional fill policy for the expression when it is used in a nested
                        expression and doesn't have a value (for more information see downsampler)

                    example:
                        expressions = [
                               {
                                   "id": "e1",
                                   "expr": "a + b",
                                   "join": {
                                        "operator": "union"
                                   }
                               }
                        ]

               outputs: json
                    These determine the output behavior and allow you to eliminate some expressions from
                    the results or include the raw metrics. By default, if this section is missing,
                    all expressions and only the expressions will be serialized. The field is a list of one or more
                    output objects. More fields will be added later with flags to affect the output.

                    id: str
                        The ID of the metric or expression

                    alias: str
                        An optional descriptive name for series

                    example:
                        outputs =[
                          {"id": "e1"},
                        ]

               rate: bool
                    Whether or not to calculate all metrics as rates, i.e. value per second.
                    This is computed before expressions.

        :return: json
        """
        return query.exp(self._host, self._port, self._protocol, **kwargs)

    def query_gexp(self, **kwargs):
        return query.gexp(self._host, self._port, self._protocol, **kwargs)

    def query_last(self, **kwargs):
        return query.last(self._host, self._port, self._protocol, **kwargs)

    def delete(self, **kwargs):
        """
        Enables deletion of data matching query. Use same the kwargs used for query

        :param kwargs: dict
        :return: json
        """
        return query.delete(self._host, self._port, self._protocol, **kwargs)

    def filters(self):
        """
        This endpoint lists the various filters loaded by the TSD and some information about how to use them.

        :return: json
        """
        return config.filters(**self._config)

    def stats(self):
        """
        This endpoint provides a list of statistics for the running TSD.
        Sub endpoints return details about other TSD components such as the JVM,
        thread states or storage client. All statistics are read only.

        :return: json
        """
        return stats.stats(**self._config)

    def jvm_stats(self):
        """
        The threads endpoint is used for debugging the TSD's JVM process and includes
        stats about the garbage collector, system load and memory usage. (v2.2)

        :return: json
        """
        return stats.jvm(**self._config)

    def query_stats(self):
        """
        This endpoint can be used for tracking and troubleshooting queries executed
        against a TSD. It maintains an unbounded list of currently executing
        queries as well as a list of up to 256 completed queries (rotating the oldest
        queries out of memory). Information about each query includes the
        original query, request headers, response code, timing and an exception
        if thrown. (v2.2)
        :return: json
        """
        return stats.query(**self._config)

    def region_clients(self):
        """
        Returns information about the various HBase region server clients in AsyncHBase.
        This helps to identify issues with a particular region server. (v2.2)

        :return json
        """
        return stats.region_clients(**self._config)

    def threads(self):
        """
        The threads endpoint is used for debugging the TSD and providing insight
        into the state and execution of various threads without having to resort
        to a JStack trace. (v2.2)

        :return: json
        """
        return stats.threads(**self._config)

    def version(self):
        """
        This endpoint returns information about the running version of OpenTSDB.

        :return: json
        """
        return version.version(**self._config)

    def suggest(self, **kwargs):
        """
        This endpoint provides a means of implementing an "auto-complete" call that can be accessed
        repeatedly as a user types a request in a GUI. It does not offer full text searching or wildcards,
        rather it simply matches the entire string passed in the query on the first characters
        of the stored data. For example, passing a query of type=metrics&q=sys will return the top 25
        metrics in the system that start with sys. Matching is case sensitive, so sys will not match System.CPU.
        Results are sorted alphabetically.

        :param kwargs: dict
               type: str
                    The type of data to auto complete on. Must be one of the following: metrics, tagk or tagv

               q:
                    A string to match on for the given type

               max:
                    The maximum number of suggested results to return. Must be greater than 0

        :return: json
        """
        return suggest.suggest(self._host, self._port, self._protocol, **kwargs)

    def metrics(self, **kwargs):
        """

        Helper function that give a list of available metrics

        :param kwargs: dict
               max:
                    The maximum number of suggested results to return. Must be greater than 0

               regxp:
                    Regex pattern to matrics have to satisfied

        :return: json
        """
        return suggest.metrics(self._host, self._port, self._protocol, **kwargs)

    def dropcaches(self):
        """
        This endpoint purges the in-memory data cached in OpenTSDB. This includes all UID to name
        and name to UID maps for metrics, tag names and tag values.

        :return: json
        """
        return dropcaches.dropcaches(**self._config)

    def serializers(self):
        """
        This endpoint lists the serializer plugins loaded by the running TSD. Information
        given includes the name, implemented methods, content types and methods

        :return: json
        """

        return serializers.serializers(**self._config)

    def _parameters_serializer(self):
        return {
            'host': self._host,
            'port': self._port,
            'protocol': self._protocol
        }


def connect(host, port, **kwargs):
    return TsdbConnector(host, port, **kwargs)
