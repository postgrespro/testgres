# coding: utf-8

import six
import typing

from testgres.operations.exceptions import TestgresException
from testgres.operations.exceptions import ExecUtilException
from testgres.operations.exceptions import InvalidOperationException


class PortForException(TestgresException):
    _message: typing.Optional[str]

    def __init__(
        self,
        message: typing.Optional[str] = None,
    ):
        assert message is None or type(message) == str  # noqa: E721
        super().__init__(message)
        self._message = message
        return

    @property
    def message(self) -> str:
        assert self._message is None or type(self._message) == str  # noqa: E721
        if self._message is None:
            return ""
        return self._message

    def __repr__(self) -> str:
        args = []

        if self._message is not None:
            args.append(("message", self._message))

        result = "{}(".format(type(self).__name__)
        sep = ""
        for a in args:
            result += sep + a[0] + "=" + repr(a[1])
            sep = ", "
            continue
        result += ")"
        return result


@six.python_2_unicode_compatible
class QueryException(TestgresException):
    _description: typing.Optional[str]
    _query: typing.Optional[str]

    def __init__(
        self,
        message: typing.Optional[str] = None,
        query: typing.Optional[str] = None
    ):
        assert message is None or type(message) == str  # noqa: E721
        assert query is None or type(query) == str  # noqa: E721

        super().__init__(message)

        self._description = message
        self._query = query
        return

    @property
    def message(self) -> str:
        assert self._description is None or type(self._description) == str  # noqa: E721
        assert self._query is None or type(self._query) == str  # noqa: E721

        msg = []

        if self._description:
            msg.append(self._description)

        if self._query:
            msg.append(u'Query: {}'.format(self._query))

        r = six.text_type('\n').join(msg)
        assert type(r) == str  # noqa: E721
        return r

    @property
    def description(self) -> typing.Optional[str]:
        assert self._description is None or type(self._description) == str  # noqa: E721
        return self._description

    @property
    def query(self) -> typing.Optional[str]:
        assert self._query is None or type(self._query) == str  # noqa: E721
        return self._query

    def __repr__(self) -> str:
        args = []

        if self._description is not None:
            args.append(("message", self._description))

        if self._query is not None:
            args.append(("query", self._query))

        result = "{}(".format(type(self).__name__)
        sep = ""
        for a in args:
            result += sep + a[0] + "=" + repr(a[1])
            sep = ", "
            continue
        result += ")"
        return result


class QueryTimeoutException(QueryException):
    def __init__(
        self,
        message: typing.Optional[str] = None,
        query: typing.Optional[str] = None
    ):
        assert message is None or type(message) == str  # noqa: E721
        assert query is None or type(query) == str  # noqa: E721

        super().__init__(message, query)
        return


# [2026-01-10] To backward compatibility.
TimeoutException = QueryTimeoutException


# [2026-01-10] It inherits TestgresException now, not QueryException
class CatchUpException(TestgresException):
    _message: typing.Optional[str]

    def __init__(
        self,
        message: typing.Optional[str] = None,
    ):
        assert message is None or type(message) == str  # noqa: E721
        super().__init__(message)
        self._message = message
        return

    @property
    def message(self) -> str:
        assert self._message is None or type(self._message) == str  # noqa: E721
        if self._message is None:
            return ""
        return self._message

    def __repr__(self) -> str:
        args = []

        if self._message is not None:
            args.append(("message", self._message))

        result = "{}(".format(type(self).__name__)
        sep = ""
        for a in args:
            result += sep + a[0] + "=" + repr(a[1])
            sep = ", "
            continue
        result += ")"
        return result


@six.python_2_unicode_compatible
class StartNodeException(TestgresException):
    _description: typing.Optional[str]
    _files: typing.Optional[typing.Iterable]

    def __init__(
        self,
        message: typing.Optional[str] = None,
        files: typing.Optional[typing.Iterable] = None
    ):
        assert message is None or type(message) == str  # noqa: E721
        assert files is None or isinstance(files, typing.Iterable)

        super().__init__(message)

        self._description = message
        self._files = files
        return

    @property
    def message(self) -> str:
        assert self._description is None or type(self._description) == str  # noqa: E721
        assert self._files is None or isinstance(self._files, typing.Iterable)

        msg = []

        if self._description:
            msg.append(self._description)

        for f, lines in self._files or []:
            assert type(f) == str  # noqa: E721
            assert type(lines) in [str, bytes]  # noqa: E721
            msg.append(u'{}\n----\n{}\n'.format(f, lines))

        return six.text_type('\n').join(msg)

    @property
    def description(self) -> typing.Optional[str]:
        assert self._description is None or type(self._description) == str  # noqa: E721
        return self._description

    @property
    def files(self) -> typing.Optional[typing.Iterable]:
        assert self._files is None or isinstance(self._files, typing.Iterable)
        return self._files

    def __repr__(self) -> str:
        args = []

        if self._description is not None:
            args.append(("message", self._description))

        if self._files is not None:
            args.append(("files", self._files))

        result = "{}(".format(type(self).__name__)
        sep = ""
        for a in args:
            result += sep + a[0] + "=" + repr(a[1])
            sep = ", "
            continue
        result += ")"
        return result


class InitNodeException(TestgresException):
    _message: typing.Optional[str]

    def __init__(
        self,
        message: typing.Optional[str] = None,
    ):
        assert message is None or type(message) == str  # noqa: E721
        super().__init__(message)
        self._message = message
        return

    @property
    def message(self) -> str:
        assert self._message is None or type(self._message) == str  # noqa: E721
        if self._message is None:
            return ""
        return self._message

    def __repr__(self) -> str:
        args = []

        if self._message is not None:
            args.append(("message", self._message))

        result = "{}(".format(type(self).__name__)
        sep = ""
        for a in args:
            result += sep + a[0] + "=" + repr(a[1])
            sep = ", "
            continue
        result += ")"
        return result


class BackupException(TestgresException):
    _message: typing.Optional[str]

    def __init__(
        self,
        message: typing.Optional[str] = None,
    ):
        assert message is None or type(message) == str  # noqa: E721
        super().__init__(message)
        self._message = message
        return

    @property
    def message(self) -> str:
        assert self._message is None or type(self._message) == str  # noqa: E721
        if self._message is None:
            return ""
        return self._message

    def __repr__(self) -> str:
        args = []

        if self._message is not None:
            args.append(("message", self._message))

        result = "{}(".format(type(self).__name__)
        sep = ""
        for a in args:
            result += sep + a[0] + "=" + repr(a[1])
            sep = ", "
            continue
        result += ")"
        return result


assert ExecUtilException.__name__ == "ExecUtilException"
assert InvalidOperationException.__name__ == "InvalidOperationException"
