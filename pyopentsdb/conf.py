from enum import Enum


class StatsPointer(Enum):
    STATS = 1
    JVM = 2
    QUERY = 3
    REGION_CLIENTS = 4
    THREADS = 5


class ConfigPointer(Enum):
    CONFIG = 1
    FILTERS = 2


class QueryPointer(Enum):
    QUERY = 1


BASIC_TSDB_QUERY = {
        'start': None,
        'end': None,
        'msResolution': False,
        'showTSUIDs': False,
        'noAnnotations': False,
        'globalAnnotations': False,
        'showSummary': False,
        'showStats': False,
        'showQuery': False,
        'delete': False,
        'timezone': 'UTC',
        'useCalendar': False,
        'queries': list(),
    }



