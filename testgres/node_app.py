from .node import OsOperations
from .node import LocalOperations
from .node import PostgresNode

import os
import platform
import tempfile


class NodeApp:

    def __init__(self, test_path=None, nodes_to_cleanup=None, os_ops=None):
        assert os_ops is None or isinstance(os_ops, OsOperations)

        if os_ops is None:
            os_ops = LocalOperations.get_single_instance()

        assert isinstance(os_ops, OsOperations)

        if test_path:
            if os.path.isabs(test_path):
                self.test_path = test_path
            else:
                self.test_path = os_ops.build_path(os_ops.cwd(), test_path)
        else:
            self.test_path = os_ops.cwd()
        self.nodes_to_cleanup = nodes_to_cleanup if nodes_to_cleanup else []
        self.os_ops = os_ops

    def make_empty(
            self,
            base_dir=None,
            port=None,
            bin_dir=None):
        real_base_dir = self.os_ops.build_path(self.test_path, base_dir)
        self.os_ops.rmdirs(real_base_dir, ignore_errors=True)
        self.os_ops.makedirs(real_base_dir)

        node = PostgresNode(base_dir=real_base_dir, port=port, bin_dir=bin_dir)
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
        pg_version_file = self.os_ops.read(self.os_ops.build_path(node.data_dir, 'PG_VERSION'))
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
        for option_name, option_value in pg_options.items():
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
