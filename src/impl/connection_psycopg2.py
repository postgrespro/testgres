# coding: utf-8
# we support both pg8000 and psycopg2

from ..enums import IsolationLevel

from ..defaults import \
    default_dbname, \
    default_username2

from ..exceptions import QueryException
from ..exceptions import NodeConnectionException
from ..exceptions import NodeConnectionInterfaceException
from ..exceptions import NodeConnectionDatabaseException
from ..exceptions import NodeConnectionDataException
from ..exceptions import NodeConnectionOperationalException
from ..exceptions import NodeConnectionIntegrityException
from ..exceptions import NodeConnectionInternalException
from ..exceptions import NodeConnectionProgrammingException
from ..exceptions import NodeConnectionNotSupportedException
from ..exceptions import NodeConnectionTransactionRollbackException
from ..exceptions import NodeConnectionQueryCanceledException

import typing

import psycopg2 as pglib
from psycopg2._psycopg import connection as PSYCOPG2__Connection
from psycopg2._psycopg import cursor as PSYCOPG2__Cursor
from psycopg2._psycopg import QueryCanceledError as PSYCOPG2__QueryCanceledError
from psycopg2._psycopg import TransactionRollbackError as PSYCOPG2__TransactionRollbackError


class NodeConnection:
    """
    Transaction wrapper returned by Node
    """
    def __init__(
        self,
        node,
        dbname=None,
        username=None,
        password=None,
        autocommit=False,
    ):
        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username2(node.os_ops)

        self._node = node

        c = __class__._exec_dblib(
            pglib.connect,
            database=dbname,
            user=username,
            password=password,
            host=node.host,
            port=node.port,
        )
        assert isinstance(c, PSYCOPG2__Connection)

        self._connection = c

        self._connection.autocommit = autocommit

        c = self.connection.cursor()
        assert isinstance(c, PSYCOPG2__Cursor)
        self._cursor = c

    @property
    def node(self):
        return self._node

    @property
    def connection(self):
        return self._connection

    @property
    def pid(self):
        r = self.execute("select pg_catalog.pg_backend_pid()")
        assert r is not None
        return r[0][0]

    @property
    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return

    def begin(self, isolation_level=IsolationLevel.ReadCommitted):
        # Check if level isn't an IsolationLevel
        if not isinstance(isolation_level, IsolationLevel):
            # Get name of isolation level
            level_str = str(isolation_level).lower()

            # Validate level string
            try:
                isolation_level = IsolationLevel(level_str)
            except ValueError:
                error = 'Invalid isolation level "{}"'
                raise QueryException(error.format(level_str))

        # Set isolation level
        cmd_f = 'SET TRANSACTION ISOLATION LEVEL {}'
        cmd = cmd_f.format(isolation_level.value)

        __class__._exec_dblib(
            self.cursor.execute,
            cmd,
        )
        return self

    def commit(self):
        __class__._exec_dblib(
            self.connection.commit,
        )
        return self

    def rollback(self):
        __class__._exec_dblib(
            self.connection.rollback,
        )

        return self

    def execute(self, query, *args):
        __class__._exec_dblib(
            self.cursor.execute,
            query,
            args,
        )

        if self.cursor.description is None:
            return None

        # note: pg8000 might return tuples
        res = __class__._exec_dblib(
            self.cursor.fetchall,
        )
        assert type(res) is list
        return res

    def close(self):
        __class__._exec_dblib(
            self.cursor.close,
        )
        __class__._exec_dblib(
            self.connection.close,
        )
        return

    @staticmethod
    def _exec_dblib(
        func: typing.Callable,
        *args,
        **kwargs,
    ) -> typing.Any:
        try:
            r = func(*args, **kwargs)
        except Exception as e:
            __class__._map_pglib_exception(e)

        return r

    @staticmethod
    def _map_pglib_exception(exc: Exception) -> typing.NoReturn:
        if isinstance(exc, PSYCOPG2__QueryCanceledError):
            raise NodeConnectionQueryCanceledException(str(exc)) from exc

        if isinstance(exc, PSYCOPG2__TransactionRollbackError):
            raise NodeConnectionTransactionRollbackException(str(exc)) from exc

        if isinstance(exc, pglib.NotSupportedError):
            raise NodeConnectionNotSupportedException(str(exc)) from exc

        if isinstance(exc, pglib.InternalError):
            raise NodeConnectionInternalException(str(exc)) from exc

        if isinstance(exc, pglib.ProgrammingError):
            raise NodeConnectionProgrammingException(str(exc)) from exc

        if isinstance(exc, pglib.IntegrityError):
            raise NodeConnectionIntegrityException(str(exc)) from exc

        if isinstance(exc, pglib.OperationalError):
            raise NodeConnectionOperationalException(str(exc)) from exc

        if isinstance(exc, pglib.DataError):
            raise NodeConnectionDataException(str(exc)) from exc

        if isinstance(exc, pglib.DatabaseError):
            raise NodeConnectionDatabaseException(str(exc)) from exc

        if isinstance(exc, pglib.InterfaceError):
            raise NodeConnectionInterfaceException(str(exc)) from exc

        if isinstance(exc, pglib.Error):
            raise NodeConnectionException(str(exc)) from exc

        raise exc
