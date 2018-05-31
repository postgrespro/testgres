from enum import Enum, IntEnum
from six import iteritems


class XLogMethod(Enum):
    """
    Available WAL methods for :class:`.NodeBackup`
    """

    none = 'none'
    fetch = 'fetch'
    stream = 'stream'


class IsolationLevel(Enum):
    """
    Transaction isolation level for :class:`.NodeConnection`
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


class ProcessType(Enum):
    """
    Types of processes
    """

    AutovacuumLauncher = 'autovacuum launcher'
    BackgroundWriter = 'background writer'
    Checkpointer = 'checkpointer'
    LogicalReplicationLauncher = 'logical replication launcher'
    Startup = 'startup'
    StatsCollector = 'stats collector'
    WalReceiver = 'wal receiver'
    WalSender = 'wal sender'
    WalWriter = 'wal writer'

    # special value
    Unknown = 'unknown'

    @staticmethod
    def from_process(process):
        # legacy names for older releases of PG
        alternative_names = {
            ProcessType.LogicalReplicationLauncher: [
                'logical replication worker'
            ],
            ProcessType.BackgroundWriter: [
                'writer'
            ],
        }  # yapf: disable

        # we deliberately cut special words and spaces
        cmdline = ''.join(process.cmdline()) \
                    .replace('postgres:', '', 1) \
                    .replace('bgworker:', '', 1) \
                    .replace(' ', '')

        for ptype in ProcessType:
            if cmdline.startswith(ptype.value.replace(' ', '')):
                return ptype

        for ptype, names in iteritems(alternative_names):
            for name in names:
                if cmdline.startswith(name.replace(' ', '')):
                    return ptype

        # default
        return ProcessType.Unknown


class DumpFormat(Enum):
    """
    Available dump formats
    """

    Plain = 'plain'
    Custom = 'custom'
    Directory = 'directory'
    Tar = 'tar'
