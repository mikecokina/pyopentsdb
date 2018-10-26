import requests

from pyopentsdb import query
from pyopentsdb import suggest
from pyopentsdb import aggregators
from pyopentsdb import dropcaches
from pyopentsdb import config
from pyopentsdb import stats
from pyopentsdb import version
from pyopentsdb import serializers
from pyopentsdb import put


class TsdbConnector(object):
    def __init__(self, host, **kwargs):
        """
        :param port: int (mandatory); OpenTSDB instance port
        :param kwargs: see bellow
        :**kwargs options**:
        """
        self._host = str(host)[:-1] if str(host).endswith('/') else str(host)
        self._session = kwargs.pop("session", requests.Session())
        self._config = self._parameters_serializer()

    def _parameters_serializer(self):
        return {
            'host': self._host,
            'session': self._session
        }

    def suggest(self, **kwargs):
        """
        This endpoint provides a means of implementing an "auto-complete" call that can be accessed
        repeatedly as a user types a request in a GUI. It does not offer full text searching or wildcards,
        rather it simply matches the entire string passed in the query on the first characters
        of the stored data. For example, passing a query of type=metrics&q=sys will return the top 25
        metrics in the system that start with sys. Matching is case sensitive, so sys will not match System.CPU.
        Results are sorted alphabetically.

        :param kwargs: see bellow
        :**kwargs options**:
               * **type** * -- str;
                    The type of data to auto complete on. Must be one of the following: metrics, tagk or tagv
               * **q** * -- str;
                    A string to match on for the given type
               * **max** * -- int;
                    The maximum number of suggested results to return. Must be greater than 0
               * **<requests.request kwargs>** *
        :return: dict
        """
        return suggest.suggest(self._host, self._session, **kwargs)

    def metrics(self, **kwargs):
        """
        Helper function that give a list of available metrics
        :param kwargs: see bellow
        :**kwargs options**:
               * **max** * -- int;
                    The maximum number of suggested results to return. Must be greater than 0
               * **regxp** * -- str;
                    Regex pattern to matrics have to satisfied
               * **q** * -- str;
                    A metric to match on
               * **<requests.request kwargs>** *
        :return: dict
        """
        return suggest.metrics(self._host, self._session, **kwargs)

    def config(self, **kwargs):
        """
        This endpoint returns information about the running configuration of the TSD.
        It is read only and cannot be used to set configuration options.
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return config.tsdb_configuration(self._host, self._session, **kwargs)

    def filters(self, **kwargs):
        """
        This endpoint lists the various filters loaded by the TSD and some information about how to use them.
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return config.filters(self._host, self._session, **kwargs)

    def query(self, **kwargs):
        """
        Enables extracting data from the storage system in various formats determined by
        the serializer selected. Queries can be submitted via the 1.0 query string format
        or body content.

        :param kwargs: see bellow
        :**kwargs options**:
               * **start** * -- datetime;
                        The start time for the query. This can be a relative or absolute timestamp.
                        See Querying or Reading Data for details.
               * **end** * -- datetime;
                        An end time for the query. If not supplied, the TSD will assume the local system time on the server.
                        This may be a relative or absolute timestamp. See Querying or Reading Data for details.
               * **metrics** * -- dict;
                        The full name of a metric is supplied along with an optional list of tags.
                        This is optimized for aggregating multiple time series into one result.
                        aggregator: str
                                The name of an aggregation function to use. See /api/aggregators
                        metric: str
                                The name of a metric stored in the system
                        rate: bool
                                Whether or not the data should be converted into deltas before returning.
                                This is useful if the metric is a continuously incrementing counter and you
                                want to view the rate of change between data points.
                       rateOptions: dict
                                counter: bool
                                        Whether or not the underlying data is a monotonically increasing
                                        counter that may roll over
                                counterMax: int
                                        A positive integer representing the maximum value for the counter.
                                resetValue: int
                                        An optional value that, when exceeded, will cause the aggregator
                                        to return a 0 instead of the calculated rate. Useful when data sources
                                        are frequently reset to avoid spurious spikes.
                                dropResets: bool
                                        Whether or not to simply drop rolled-over or reset data points.
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
                       tags: dict
                                To drill down to specific timeseries or group results by tag, supply one or more map values
                                in the same format as the query string. Tags are converted to filters in 2.2.
                                See the notes below about conversions. Note that if no tags are specified, all metrics
                                in the system will be aggregated into the results. Deprecated in 2.2
                                example:
                                    {tagk: tagv}
                       filters: dict
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
                       explicitTags: bool
                                Returns the series that include only the tag keys provided in the filters.
               * **ms** * -- bool;
                        Whether or not to output data point timestamps in milliseconds or seconds.
                        The msResolution flag is recommended. If this flag is not provided and there
                        are multiple data points within a second, those data points will be down sampled
                        using the query's aggregation function.
               * **show_tsuids** * -- bool;
                        Whether or not to output the TSUIDs associated with timeseries in the results.
                        If multiple time series were aggregated into one set, multiple TSUIDs will be
                        returned in a sorted manner
               * **no_annotations** * -- bool;
                        Whether or not to return annotations with a query. The default is to return annotations
                        for the requested timespan but this flag can disable the return. This affects both local
                        and global notes and overrides globalAnnotations
               * **global_annotations** * -- bool;
                        Whether or not the query should retrieve global annotations for the requested timespan
               * **show_summary** * -- bool;
                        Whether or not to show a summary of timings surrounding the query in the results. This creates
                        another object in the map that is unlike the data point objects. See Query Details and Stats
               * **show_stats** * -- bool;
                        Whether or not to show detailed timings surrounding the query in the results. This creates
                        another object in the map that is unlike the data point objects. See Query Details and Stats
               * **show_query** * -- bool;
                        Whether or not to return the original sub query with the query results. If the request
                        contains many sub queries then this is a good way to determine which results belong to which
                        sub query. Note that in the case of a * or wildcard query, this can produce
                        a lot of duplicate output.
               * **delete** * -- bool;
                        Can be passed to the JSON with a POST to delete any data points that match the given query.
               * **timezone** * -- str;
                        An optional timezone for calendar-based downsampling. Must be a valid timezone database name
                        supported by the JRE installed on the TSD server.
               * **use_calendar** * -- bool;
                        Whether or not use the calendar based on the given timezone for downsampling intervals
               * **<requests.request kwargs>** *
        :return: dict
        """

        return query.query(self._host, self._session, **kwargs)

    def multiquery(self, query_chunks, **kwargs):
        """
        Enables issuing multiple queries in parallel

        :param query_chunks: list; list of json serializable dicts representing OpenTSDB query
        :param kwargs: see bellow
        :**kwargs options**:
            * **max_tsdb_concurrency** -- int (optional), default=40;
                        Maximum number of concurrency threads hitting OpenTSDB api
            * **<requests.request kwargs>** *
        :return: dict
        """
        return query.multiquery(self._host, self._session,  query_chunks, **kwargs)

    def aggregators(self, **kwargs):
        """
        This endpoint simply lists the names of implemented aggregation functions used in timeseries queries.
        :return:  dict
        """
        return aggregators.aggregators(self._host, self._session, **kwargs)

    def dropcaches(self, **kwargs):
        """
        This endpoint purges the in-memory data cached in OpenTSDB. This includes all UID to name
        and name to UID maps for metrics, tag names and tag values.
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return dropcaches.dropcaches(self._host, self._session, **kwargs)

    def serializers(self, **kwargs):
        """
        This endpoint lists the serializer plugins loaded by the running TSD. Information
        given includes the name, implemented methods, content types and methods
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return serializers.serializers(self._host, self._session, **kwargs)

    def stats(self, **kwargs):
        """
        This endpoint provides a list of statistics for the running TSD.
        Sub endpoints return details about other TSD components such as the JVM,
        thread states or storage client. All statistics are read only.
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return stats.stats(self._host, self._session, **kwargs)

    def jvm_stats(self, **kwargs):
        """
        The threads endpoint is used for debugging the TSD's JVM process and includes
        stats about the garbage collector, system load and memory usage. (v2.2)
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return stats.jvm(self._host, self._session, **kwargs)

    def query_stats(self, **kwargs):
        """
        This endpoint can be used for tracking and troubleshooting queries executed
        against a TSD. It maintains an unbounded list of currently executing
        queries as well as a list of up to 256 completed queries (rotating the oldest
        queries out of memory). Information about each query includes the
        original query, request headers, response code, timing and an exception
        if thrown. (v2.2)
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return stats.query(self._host, self._session, **kwargs)

    def region_clients(self, **kwargs):
        """
        Returns information about the various HBase region server clients in AsyncHBase.
        This helps t identify issues with a particular region server. (v2.2)
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return dict
        """
        return stats.region_clients(self._host, self._session, **kwargs)

    def threads(self, **kwargs):
        """
        The threads endpoint is used for debugging the TSD and providing insight
        into the state and execution of various threads without having to resort
        to a JStack trace. (v2.2)
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return stats.threads(self._host, self._session, **kwargs)

    def version(self, **kwargs):
        """
        This endpoint returns information about the running version of OpenTSDB.
        :**kwargs options**:
            * **<requests.request kwargs>** *
        :return: dict
        """
        return version.version(self._host, self._session, **kwargs)

    def put(self, data, **kwargs):
        """
        Metric has to be created or enable tsd.core.auto_create_metrics
        :param data: list of dicts or dict
        :param kwargs: see bellow
        :**kwargs options**:
               * **summary** * --  bool;
                    Whether or not to return summary information
               * **details** * --  bool;
                    Whether or not to return detailed information
               * **sync** * --  bool;
                    Whether or not to wait for the data to be flushed to storage before returning the results.
               * **sync_timeout** * --  int;
                    A timeout, in milliseconds, to wait for the data to be flushed to storage before
                    returning with an error. When a timeout occurs, using the details flag will tell
                    how many data points failed and how many succeeded. sync must also be given for this
                    to take effect. A value of 0 means the write will not timeout.
               * **<requests.request kwargs>** *
        If both detailed and summary are present in a query string, the API will respond with detailed information.
        Standard status code is 204
        Standard status code with details is 200 (request._content example: b'{"success":2,"failed":0,"errors":[]}')
        :return: dict
        """
        return put.put(self._host, self._session, data, **kwargs)

    def close(self):
        self._session.close()


def tsdb_connection(host, **kwargs):
    __TSDB_CONNECTION__ = TsdbConnector(
        host=host,
        **kwargs
    )
    return __TSDB_CONNECTION__
