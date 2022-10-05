# coding: utf-8

# we support both pg8000 and psycopg2
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

from .enums import IsolationLevel

from .defaults import \
    default_dbname, \
    default_username

from .exceptions import QueryException

# export some exceptions
DatabaseError = pglib.DatabaseError
InternalError = pglib.InternalError
ProgrammingError = pglib.ProgrammingError
OperationalError = pglib.OperationalError


class NodeConnection(object):
    """
    Transaction wrapper returned by Node
    """
    def __init__(self,
                 node,
                 dbname=None,
                 username=None,
                 password=None,
                 autocommit=False):

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        self._node = node

        self._connection = pglib.connect(database=dbname,
                                         user=username,
                                         password=password,
                                         host=node.host,
                                         port=node.port)

        self._connection.autocommit = autocommit
        self._cursor = self.connection.cursor()

    @property
    def node(self):
        return self._node

    @property
    def connection(self):
        return self._connection

    @property
    def pid(self):
        return self.execute("select pg_catalog.pg_backend_pid()")[0][0]

    @property
    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

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
        cmd = 'SET TRANSACTION ISOLATION LEVEL {}'
        self.cursor.execute(cmd.format(isolation_level.value))

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
