# coding: utf-8
# we support both pg8000 and psycopg2

# to backward compatibility
from .exceptions import NodeConnectionDatabaseException as DatabaseError
from .exceptions import NodeConnectionOperationalException as OperationalError
from .exceptions import NodeConnectionInternalException as InternalError
from .exceptions import NodeConnectionProgrammingException as ProgrammingError

try:
    from .impl.connection_psycopg2 import NodeConnection, pglib
except ImportError:
    try:
        from .impl.connection_pg8000 import NodeConnection, pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

assert DatabaseError.__name__ == "NodeConnectionDatabaseException"
assert OperationalError.__name__ == "NodeConnectionOperationalException"
assert InternalError.__name__ == "NodeConnectionInternalException"
assert ProgrammingError.__name__ == "NodeConnectionProgrammingException"
assert NodeConnection.__name__ == "NodeConnection"
assert pglib is not None
