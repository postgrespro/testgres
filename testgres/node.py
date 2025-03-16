# coding: utf-8
import logging  # noqa: F401
import os
import random  # noqa: F401
import signal  # noqa: F401
import subprocess  # noqa: F401
import threading  # noqa: F401
import tempfile
import platform
from queue import Queue  # noqa: F401

import time  # noqa: F401

try:
    from collections.abc import Iterable  # noqa: F401
except ImportError:
    from collections import Iterable  # noqa: F401

# we support both pg8000 and psycopg2
from .node_base import pglib

from six import raise_from, iteritems, text_type   # noqa: F401

from .node_base import \
    PostgresNode_Base, \
    ProcessProxy   # noqa: F401

from .enums import \
    NodeStatus, \
    ProcessType, \
    DumpFormat  # noqa: F401

from .cache import cached_initdb  # noqa: F401

from .config import testgres_config

from .connection import NodeConnection  # noqa: F401

from .consts import \
    DATA_DIR, \
    LOGS_DIR, \
    TMP_NODE, \
    TMP_DUMP, \
    PG_CONF_FILE, \
    PG_AUTO_CONF_FILE, \
    HBA_CONF_FILE, \
    RECOVERY_CONF_FILE, \
    PG_LOG_FILE, \
    UTILS_LOG_FILE, \
    PG_CTL__STATUS__OK, \
    PG_CTL__STATUS__NODE_IS_STOPPED, \
    PG_CTL__STATUS__BAD_DATADIR  # noqa: F401

from .consts import \
    MAX_LOGICAL_REPLICATION_WORKERS, \
    MAX_REPLICATION_SLOTS, \
    MAX_WORKER_PROCESSES, \
    MAX_WAL_SENDERS, \
    WAL_KEEP_SEGMENTS, \
    WAL_KEEP_SIZE  # noqa: F401

from .decorators import \
    method_decorator, \
    positional_args_hack  # noqa: F401

from .defaults import \
    default_dbname, \
    generate_app_name  # noqa: F401

from .exceptions import \
    CatchUpException,   \
    ExecUtilException,  \
    QueryException,     \
    StartNodeException, \
    TimeoutException,   \
    InitNodeException,  \
    TestgresException,  \
    BackupException,    \
    InvalidOperationException  # noqa: F401

from .logger import TestgresLogger  # noqa: F401

from .pubsub import Publication, Subscription  # noqa: F401

from .standby import First  # noqa: F401

from . import utils  # noqa: F401

from .utils import \
    PgVer, \
    eprint, \
    get_bin_path2, \
    get_pg_version, \
    execute_utility2, \
    options_string, \
    clean_on_error  # noqa: F401

from .backup import NodeBackup  # noqa: F401

from .operations.os_ops import ConnectionParams
from .operations.os_ops import OsOperations
from .operations.local_ops import LocalOperations
from .operations.remote_ops import RemoteOperations

InternalError = pglib.InternalError
ProgrammingError = pglib.ProgrammingError
OperationalError = pglib.OperationalError


class PostgresNode(PostgresNode_Base):
    def __init__(self, name=None, base_dir=None, port=None, conn_params: ConnectionParams = ConnectionParams(),
                 bin_dir=None, prefix=None):
        """
        PostgresNode constructor.

        Args:
            name: node's application name.
            port: port to accept connections.
            base_dir: path to node's data directory.
            bin_dir: path to node's binary directory.
        """

        os_ops = __class__._get_os_ops(conn_params)
        assert os_ops is not None
        assert isinstance(os_ops, OsOperations)

        super().__init__(os_ops, name=name, port=port, bin_dir=bin_dir, prefix=prefix)

        if base_dir:
            self._base_dir = base_dir
        else:
            self._base_dir = os_ops.mkdtemp(prefix=self._prefix or TMP_NODE)
    
    @staticmethod
    def _get_os_ops(conn_params: ConnectionParams) -> OsOperations:
        assert type(conn_params) == ConnectionParams  # noqa: E721

        if testgres_config.os_ops:
            return testgres_config.os_ops

        if conn_params.ssh_key:
            return RemoteOperations(conn_params)

        return LocalOperations(conn_params)

    @property
    def base_dir(self):
        # NOTE: it's safe to create a new dir
        if not self.os_ops.path_exists(self._base_dir):
            self.os_ops.makedirs(self._base_dir)

        return self._base_dir

    @property
    def logs_dir(self):
        path = os.path.join(self.base_dir, LOGS_DIR)

        # NOTE: it's safe to create a new dir
        if not self.os_ops.path_exists(path):
            self.os_ops.makedirs(path)

        return path

    @property
    def data_dir(self):
        # NOTE: we can't run initdb without user's args
        return os.path.join(self.base_dir, DATA_DIR)

    @property
    def utils_log_file(self):
        return os.path.join(self.logs_dir, UTILS_LOG_FILE)

    @property
    def pg_log_file(self):
        return os.path.join(self.logs_dir, PG_LOG_FILE)

    # deprecated
    @property
    def utils_log_name(self):
        return self.utils_log_file

    # deprecated
    @property
    def pg_log_name(self):
        return self.pg_log_file


class NodeApp:

    def __init__(self, test_path=None, nodes_to_cleanup=None, os_ops=LocalOperations()):
        if test_path:
            if os.path.isabs(test_path):
                self.test_path = test_path
            else:
                self.test_path = os.path.join(os_ops.cwd(), test_path)
        else:
            self.test_path = os_ops.cwd()
        self.nodes_to_cleanup = nodes_to_cleanup if nodes_to_cleanup else []
        self.os_ops = os_ops

    def make_empty(
            self,
            base_dir=None,
            port=None,
            bin_dir=None):
        real_base_dir = os.path.join(self.test_path, base_dir)
        self.os_ops.rmdirs(real_base_dir, ignore_errors=True)
        self.os_ops.makedirs(real_base_dir)

        node = PostgresNode(base_dir=real_base_dir, port=port, bin_dir=bin_dir)
        node.should_rm_dirs = True
        self.nodes_to_cleanup.append(node)

        return node

    def make_simple(
            self,
            base_dir=None,
            port=None,
            set_replication=False,
            ptrack_enable=False,
            initdb_params=[],
            pg_options={},
            checksum=True,
            bin_dir=None):
        assert type(pg_options) == dict  # noqa: E721

        if checksum and '--data-checksums' not in initdb_params:
            initdb_params.append('--data-checksums')
        node = self.make_empty(base_dir, port, bin_dir=bin_dir)
        node.init(
            initdb_params=initdb_params, allow_streaming=set_replication)

        # set major version
        pg_version_file = self.os_ops.read(os.path.join(node.data_dir, 'PG_VERSION'))
        node.major_version_str = str(pg_version_file.rstrip())
        node.major_version = float(node.major_version_str)

        # Set default parameters
        options = {
            'max_connections': 100,
            'shared_buffers': '10MB',
            'fsync': 'off',
            'wal_level': 'logical',
            'hot_standby': 'off',
            'log_line_prefix': '%t [%p]: [%l-1] ',
            'log_statement': 'none',
            'log_duration': 'on',
            'log_min_duration_statement': 0,
            'log_connections': 'on',
            'log_disconnections': 'on',
            'restart_after_crash': 'off',
            'autovacuum': 'off',
            # unix_socket_directories will be defined later
        }

        # Allow replication in pg_hba.conf
        if set_replication:
            options['max_wal_senders'] = 10

        if ptrack_enable:
            options['ptrack.map_size'] = '1'
            options['shared_preload_libraries'] = 'ptrack'

        if node.major_version >= 13:
            options['wal_keep_size'] = '200MB'
        else:
            options['wal_keep_segments'] = '12'

        # Apply given parameters
        for option_name, option_value in iteritems(pg_options):
            options[option_name] = option_value

        # Define delayed propertyes
        if not ("unix_socket_directories" in options.keys()):
            options["unix_socket_directories"] = __class__._gettempdir_for_socket()

        # Set config values
        node.set_auto_conf(options)

        # kludge for testgres
        # https://github.com/postgrespro/testgres/issues/54
        # for PG >= 13 remove 'wal_keep_segments' parameter
        if node.major_version >= 13:
            node.set_auto_conf({}, 'postgresql.conf', ['wal_keep_segments'])

        return node

    @staticmethod
    def _gettempdir_for_socket():
        platform_system_name = platform.system().lower()

        if platform_system_name == "windows":
            return __class__._gettempdir()

        #
        # [2025-02-17] Hot fix.
        #
        # Let's use hard coded path as Postgres likes.
        #
        # pg_config_manual.h:
        #
        # #ifndef WIN32
        # #define DEFAULT_PGSOCKET_DIR  "/tmp"
        # #else
        # #define DEFAULT_PGSOCKET_DIR ""
        # #endif
        #
        # On the altlinux-10 tempfile.gettempdir() may return
        # the path to "private" temp directiry - "/temp/.private/<username>/"
        #
        # But Postgres want to find a socket file in "/tmp" (see above).
        #

        return "/tmp"

    @staticmethod
    def _gettempdir():
        v = tempfile.gettempdir()

        #
        # Paranoid checks
        #
        if type(v) != str:  # noqa: E721
            __class__._raise_bugcheck("tempfile.gettempdir returned a value with type {0}.".format(type(v).__name__))

        if v == "":
            __class__._raise_bugcheck("tempfile.gettempdir returned an empty string.")

        if not os.path.exists(v):
            __class__._raise_bugcheck("tempfile.gettempdir returned a not exist path [{0}].".format(v))

        # OK
        return v

    @staticmethod
    def _raise_bugcheck(msg):
        assert type(msg) == str  # noqa: E721
        assert msg != ""
        raise Exception("[BUG CHECK] " + msg)
