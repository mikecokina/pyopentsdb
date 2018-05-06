import requests
import json


def query(host, port, protocol, metric, start, end, tags, aggregator, downsample, ms_resolution):

    q = "{aggregator}:{downsample}{metric}{{{tags}}}".format(
        aggregator=aggregator,
        downsample=downsample + '-avg:' if downsample else '',
        metric=metric,
        tags=','.join("{}={}".format(k, v) for k, v in tags.items())
    )

    q_json = {
        'ms': ms_resolution,
        'start': '{0:.3f}'.format(start),
        'end': '{0:.3f}'.format(end),
        'm': q
    }

    url = api_url(host, port, protocol)
    response = requests.get(url, q_json)
    pass


def delete():
    pass


def api_url(host, port, protocol):
    return '{}://{}:{}/api/query/'.format(protocol, host, port)
