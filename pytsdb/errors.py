class TsdbError(Exception):
    """ TsdbError - raise on any another error related to TSDB """
    pass


class TsdbConnectionError(TsdbError):
    """ TsdbConnectionError - raise on connection error """
    pass


class TsdbQueryError(TsdbError):
    """ TsdbQueryError - raise on any query error """
    pass


class ArgumentError(TsdbError):
    """ ArgumentsError """
    pass


class MissingArgumentError(ArgumentError):
    """ MissingArgumentsError - raise when argument in json schema is missing """
    pass


class UncaughtError(TsdbError):
    """ UncaughError - raise as last possible/unknown error """
    pass
