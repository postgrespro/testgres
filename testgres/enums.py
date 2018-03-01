from enum import Enum, IntEnum


class XLogMethod(Enum):
    """
    Available WAL methods for NodeBackup
    """

    none = 'none'
    fetch = 'fetch'
    stream = 'stream'


class IsolationLevel(Enum):
    """
    Transaction isolation level for NodeConnection
    """

    ReadUncommitted = 'read uncommitted'
    ReadCommitted = 'read committed'
    RepeatableRead = 'repeatable read'
    Serializable = 'serializable'


class NodeStatus(IntEnum):
    """
    Status of a PostgresNode
    """

    Running, Stopped, Uninitialized = range(3)

    # for Python 3.x
    def __bool__(self):
        return self == NodeStatus.Running

    # for Python 2.x
    __nonzero__ = __bool__
