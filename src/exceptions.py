# coding: utf-8

import six

from testgres.operations.exceptions import TestgresException
from testgres.operations.exceptions import ExecUtilException
from testgres.operations.exceptions import InvalidOperationException


class PortForException(TestgresException):
    pass


@six.python_2_unicode_compatible
class QueryException(TestgresException):
    def __init__(self, message=None, query=None):
        super(QueryException, self).__init__(message)

        self.message = message
        self.query = query

    def __str__(self):
        msg = []

        if self.message:
            msg.append(self.message)

        if self.query:
            msg.append(u'Query: {}'.format(self.query))

        return six.text_type('\n').join(msg)


class TimeoutException(QueryException):
    pass


class CatchUpException(QueryException):
    pass


@six.python_2_unicode_compatible
class StartNodeException(TestgresException):
    def __init__(self, message=None, files=None):
        super(StartNodeException, self).__init__(message)

        self.message = message
        self.files = files

    def __str__(self):
        msg = []

        if self.message:
            msg.append(self.message)

        for f, lines in self.files or []:
            msg.append(u'{}\n----\n{}\n'.format(f, lines))

        return six.text_type('\n').join(msg)


class InitNodeException(TestgresException):
    pass


class BackupException(TestgresException):
    pass


assert ExecUtilException.__name__ == "ExecUtilException"
assert InvalidOperationException.__name__ == "InvalidOperationException"
