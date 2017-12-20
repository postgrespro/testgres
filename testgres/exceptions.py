# coding: utf-8


class TestgresException(Exception):
    """
    Base exception
    """

    pass


class ExecUtilException(TestgresException):
    """
    Stores exit code
    """

    def __init__(self, message, exit_code=0):
        super(ExecUtilException, self).__init__(message)
        self.exit_code = exit_code


class ClusterTestgresException(TestgresException):
    pass


class QueryException(TestgresException):
    pass


class TimeoutException(TestgresException):
    pass


class StartNodeException(TestgresException):
    pass


class InitNodeException(TestgresException):
    pass


class BackupException(TestgresException):
    pass


class CatchUpException(TestgresException):
    pass
