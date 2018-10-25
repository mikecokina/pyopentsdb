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


class FailedDependency(TsdbError):
    """
    FailedDependency - raise when method could not be performed on the resource because the
    requested action depended on another action and that action failed (e.g. full query)
    """
    pass


class SSLError(TsdbError):
    """
    SSLError - raise when problem with SSL
    """
    pass


class ForbiddenError(TsdbError):
    """
    ForbiddenError - raise when server return 403
    """
    pass
