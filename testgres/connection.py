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
        """
        Initialize a database.

        Args:
            self: (todo): write your description
            node: (todo): write your description
            dbname: (str): write your description
            username: (str): write your description
            password: (str): write your description
            autocommit: (todo): write your description
        """

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        self._node = node

        self._connection = pglib.connect(
            database=dbname,
            user=username,
            password=password,
            host=node.host,
            port=node.port)

        self._connection.autocommit = autocommit
        self._cursor = self.connection.cursor()

    @property
    def node(self):
        """
        Returns the node as a node.

        Args:
            self: (todo): write your description
        """
        return self._node

    @property
    def connection(self):
        """
        Get a connection object.

        Args:
            self: (todo): write your description
        """
        return self._connection

    @property
    def pid(self):
        """
        Get the pid.

        Args:
            self: (todo): write your description
        """
        return self.execute("select pg_catalog.pg_backend_pid()")[0][0]

    @property
    def cursor(self):
        """
        Return the cursor.

        Args:
            self: (todo): write your description
        """
        return self._cursor

    def __enter__(self):
        """
        Decor function.

        Args:
            self: (todo): write your description
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        Triggers the given callable traceback.

        Args:
            self: (todo): write your description
            type: (todo): write your description
            value: (todo): write your description
            traceback: (todo): write your description
        """
        self.close()

    def begin(self, isolation_level=IsolationLevel.ReadCommitted):
        """
        Initialize the level.

        Args:
            self: (todo): write your description
            isolation_level: (str): write your description
            IsolationLevel: (str): write your description
            ReadCommitted: (bool): write your description
        """
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
        """
        Commit the current transaction.

        Args:
            self: (todo): write your description
        """
        self.connection.commit()

        return self

    def rollback(self):
        """
        Roll back the database.

        Args:
            self: (todo): write your description
        """
        self.connection.rollback()

        return self

    def execute(self, query, *args):
        """
        Execute a query and return the results.

        Args:
            self: (todo): write your description
            query: (str): write your description
        """
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
        """
        Close the connection.

        Args:
            self: (todo): write your description
        """
        self.cursor.close()
        self.connection.close()
