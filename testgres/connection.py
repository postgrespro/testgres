# coding: utf-8

# we support both pg8000 and psycopg2
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

from enum import Enum

from .exceptions import QueryException
from .utils import default_username as _default_username

# export these exceptions
InternalError = pglib.InternalError
ProgrammingError = pglib.ProgrammingError


class IsolationLevel(Enum):
    """
    Transaction isolation level for NodeConnection
    """

    ReadUncommitted, ReadCommitted, RepeatableRead, Serializable = range(4)


class NodeConnection(object):
    """
    Transaction wrapper returned by Node
    """

    def __init__(self,
                 parent_node,
                 dbname,
                 host="127.0.0.1",
                 username=None,
                 password=None):

        # Use default user if not specified
        username = username or _default_username()

        self.parent_node = parent_node

        self.connection = pglib.connect(
            database=dbname,
            user=username,
            port=parent_node.port,
            host=host,
            password=password)

        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def begin(self, isolation_level=IsolationLevel.ReadCommitted):
        # yapf: disable
        levels = [
            'read uncommitted',
            'read committed',
            'repeatable read',
            'serializable'
        ]

        # Check if level is an IsolationLevel
        if (isinstance(isolation_level, IsolationLevel)):

            # Get index of isolation level
            level_idx = isolation_level.value
            assert(level_idx in range(4))

            # Replace isolation level with its name
            isolation_level = levels[level_idx]

        else:
            # Get name of isolation level
            level_str = str(isolation_level).lower()

            # Validate level string
            if level_str not in levels:
                error = 'Invalid isolation level "{}"'
                raise QueryException(error.format(level_str))

            # Replace isolation level with its name
            isolation_level = level_str

        # Set isolation level
        cmd = 'SET TRANSACTION ISOLATION LEVEL {}'
        self.cursor.execute(cmd.format(isolation_level))

        return self

    def commit(self):
        self.connection.commit()

        return self

    def rollback(self):
        self.connection.rollback()

        return self

    def execute(self, query, *args):
        self.cursor.execute(query, args)

        try:
            res = self.cursor.fetchall()

            # pg8000 might return tuples
            if isinstance(res, tuple):
                res = [tuple(t) for t in res]

            return res
        except Exception:
            return None

    def close(self):
        self.cursor.close()
        self.connection.close()
