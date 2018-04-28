# coding: utf-8

import six


class TestgresException(Exception):
    pass


@six.python_2_unicode_compatible
class ExecUtilException(TestgresException):
    def __init__(self, message=None, command=None, exit_code=0, out=None):
        super(ExecUtilException, self).__init__(message)

        self.message = message
        self.command = command
        self.exit_code = exit_code
        self.out = out

    def __str__(self):
        msg = []

        if self.message:
            msg.append(self.message)

        if self.command:
            msg.append(u'Command: {}'.format(self.command))

        if self.exit_code:
            msg.append(u'Exit code: {}'.format(self.exit_code))

        if self.out:
            msg.append(u'----\n{}'.format(self.out))

        return six.text_type('\n').join(msg)


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
