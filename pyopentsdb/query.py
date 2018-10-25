from queue import Queue
from queue import Empty
from threading import Thread

from pyopentsdb import errors
from pyopentsdb.utils import request_post


class IterableQueue(object):
    """ Transform standard python Queue instance to iterable one"""

    def __init__(self, source_queue):
        """

        :param source_queue: queue.Queue, (mandatory)
        """
        self.source_queue = source_queue

    def __iter__(self):
        while True:
            try:
                yield self.source_queue.get_nowait()
            except Empty:
                return
            

def tsdb_query_metrics_validation(**kwargs):
    """
    looking for metric and all related and required arguments in kwargs specified in OpenTSDB http api

    :param kwargs: dict
    :return:
    """

    # tsdb query kwargs have to contain 'metrics' argument
    if not kwargs.get('metrics'):
        raise errors.MissingArgumentError("Missing argument 'metrics' in query")

    # metrics can contain more than one metric in list
    for metric_object in kwargs['metrics']:
        # each metric in metrics has to specify aggregator function
        if not metric_object.get('metric') or not metric_object.get('aggregator'):
            raise errors.MissingArgumentError("Missing argument 'metric' or 'aggregator' in metrics object")

        # each metric can contain filters
        if metric_object.get('filters'):
            for metric_filter in metric_object['filters']:
                # if filter is presented , it has contain 'type', 'tagk' and 'filter' (filter definition)
                if not metric_filter.get('type') or not metric_filter.get('tagk') or \
                        metric_filter.get('filter') is None:
                    raise errors.MissingArgumentError(
                        "Missing argument 'type', 'tagk' or 'filter' in filters object")


def query(host, r_session, **kwargs):
    """
    :param host: str
    :param r_session: requests.Session
    :param kwargs: dict
    :return: dict
    """

    # todo: make sure kwargs of tsdb are not colliding kwargs of requests

    try:
        start = kwargs.pop('start')
    except KeyError:
        raise errors.MissingArgumentError("'start' is a required argument")

    try:
        tsdb_query_metrics_validation(**kwargs)
    except errors.MissingArgumentError as e:
        raise errors.MissingArgumentError(str(e))

    # general driven arguments
    end = kwargs.pop('end', None)
    ms_resolution = bool(kwargs.pop('ms', False))
    show_tsuids = bool(kwargs.pop('show_tsuids', False))
    no_annotations = bool(kwargs.pop('no_annotations', False))
    global_annotations = bool(kwargs.pop('global_annotations', False))
    show_summary = bool(kwargs.pop('show_summary', False))
    show_stats = bool(kwargs.pop('show_stats', False))
    show_query = bool(kwargs.pop('show_query', False))
    delete_match = bool(kwargs.pop('delete', False))
    timezone = kwargs.pop('timezone', 'UTC')
    use_calendar = bool(kwargs.pop('use_calendar', False))

    queries = kwargs.pop('metrics')

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
    params.update({'queries': queries})
    kwargs.update(dict(data=params))
    return request_post(api_url(host, pointer='QUERY'), r_session, **kwargs)


def multiquery(host, r_session, query_chunks, max_tsdb_concurrency=40, **kwargs):
    """
    OpenTSDB /api/query/ concurrency wrapper

    :param host: str (mandatory); OpenTSDB host
    :param r_session: requests.Session
    :param query_chunks: list (mandatory); list of json serializable dicts representing OpenTSDB query
    :param max_tsdb_concurrency: int (optional), default=40; maximum number of concurrency
                                                            threads hitting OpenTSDB api
    :return: dict; json serializable
    """

    __WORKER_RUN__ = True

    # todo: optimize, in case one of worker fail, terminate execution
    def tsdb_worker():
        while __WORKER_RUN__:
            query_kwargs = query_queue.get()

            if query_kwargs == "TERMINATOR":
                break

            # if tehre is already at least one (just one) error in queue, terminate all running threads
            # it is uselles and time consuming to finished rest of queries, if one of them fail
            if not error_queue.empty():
                break

            try:
                result = query(host, r_session, **dict(**query_kwargs, **kwargs))
                result_queue.put(result)
            except Exception as we:
                error_queue.put(we)
                break

    n_threads = min(len(query_chunks), max_tsdb_concurrency)
    query_queue = Queue(maxsize=len(query_chunks) + n_threads)
    result_queue = Queue(maxsize=len(query_chunks) + n_threads)
    error_queue = Queue()

    threads = list()
    try:
        for q in query_chunks:
            # valiate all queries in query_chunks
            tsdb_query_metrics_validation(**q)
            # add query kwargs to queue for future execution in threads
            query_queue.put(q)

        for _ in range(n_threads):
            query_queue.put("TERMINATOR")

        for _ in range(n_threads):
            t = Thread(target=tsdb_worker)
            threads.append(t)
            t.daemon = True
            t.start()

        for t in threads:
            t.join()

    except KeyboardInterrupt:
        raise
    finally:
        __WORKER_RUN__ = False

    if not error_queue.empty():
        # if not empty, error_queue has to contain exception from tsdb_worker
        raise error_queue.get()

    if result_queue.qsize() != len(query_chunks):
        # this statement is probably not necessary
        raise errors.TsdbError("Number of queries and responses is not the same")

    # make sure any other kind of response code won't be propagated to this place and will be catched and processed
    # in previous part of code
    return sum([val for val in IterableQueue(result_queue)], list())


def api_url(host, pointer):
    if pointer == 'QUERY':
        return '{}/api/query/'.format(host)
