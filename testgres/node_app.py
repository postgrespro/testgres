from .node import OsOperations
from .node import LocalOperations
from .node import PostgresNode
from .node import PortManager

import os
import platform
import tempfile
import typing


T_DICT_STR_STR = typing.Dict[str, str]
T_LIST_STR = typing.List[str]


class NodeApp:
    _test_path: str
    _os_ops: OsOperations
    _port_manager: PortManager
    _nodes_to_cleanup: typing.List[PostgresNode]

    def __init__(
        self,
        test_path: typing.Optional[str] = None,
        nodes_to_cleanup: typing.Optional[list] = None,
        os_ops: typing.Optional[OsOperations] = None,
        port_manager: typing.Optional[PortManager] = None,
    ):
        assert test_path is None or type(test_path) == str  # noqa: E721
        assert os_ops is None or isinstance(os_ops, OsOperations)
        assert port_manager is None or isinstance(port_manager, PortManager)

        if os_ops is None:
            os_ops = LocalOperations.get_single_instance()

        assert isinstance(os_ops, OsOperations)
        self._os_ops = os_ops
        self._port_manager = port_manager

        if test_path is None:
            self._test_path = os_ops.cwd()
        elif os.path.isabs(test_path):
            self._test_path = test_path
        else:
            self._test_path = os_ops.build_path(os_ops.cwd(), test_path)

        if nodes_to_cleanup is None:
            self._nodes_to_cleanup = []
        else:
            self._nodes_to_cleanup = nodes_to_cleanup

    @property
    def test_path(self) -> str:
        assert type(self._test_path) == str  # noqa: E721
        return self._test_path

    @property
    def os_ops(self) -> OsOperations:
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops

    @property
    def port_manager(self) -> PortManager:
        assert self._port_manager is None or isinstance(self._port_manager, PortManager)
        return self._port_manager

    @property
    def nodes_to_cleanup(self) -> typing.List[PostgresNode]:
        assert type(self._nodes_to_cleanup) == list  # noqa: E721
        return self._nodes_to_cleanup

    def make_empty(
            self,
            base_dir: str,
            port: typing.Optional[int] = None,
            bin_dir: typing.Optional[str] = None
    ) -> PostgresNode:
        assert type(base_dir) == str  # noqa: E721
        assert port is None or type(port) == int  # noqa: E721
        assert bin_dir is None or type(bin_dir) == str  # noqa: E721

        assert isinstance(self._os_ops, OsOperations)
        assert type(self._test_path) == str  # noqa: E721

        if base_dir is None:
            raise ValueError("Argument 'base_dir' is not defined.")

        if base_dir == "":
            raise ValueError("Argument 'base_dir' is empty.")

        real_base_dir = self._os_ops.build_path(self._test_path, base_dir)
        self._os_ops.rmdirs(real_base_dir, ignore_errors=True)
        self._os_ops.makedirs(real_base_dir)

        port_manager: PortManager = None

        if port is None:
            port_manager = self._port_manager

        node = PostgresNode(
            base_dir=real_base_dir,
            port=port,
            bin_dir=bin_dir,
            os_ops=self._os_ops,
            port_manager=port_manager
        )

        try:
            assert type(self._nodes_to_cleanup) == list  # noqa: E721
            self._nodes_to_cleanup.append(node)
        except:  # noqa: E722
            node.cleanup(release_resources=True)
            raise

        return node

    def make_simple(
            self,
            base_dir: str,
            port: typing.Optional[int] = None,
            set_replication: bool = False,
            ptrack_enable: bool = False,
            initdb_params: typing.Optional[T_LIST_STR] = None,
            pg_options: typing.Optional[T_DICT_STR_STR] = None,
            checksum: bool = True,
            bin_dir: typing.Optional[str] = None
    ) -> PostgresNode:
        assert type(base_dir) == str  # noqa: E721
        assert port is None or type(port) == int  # noqa: E721
        assert type(set_replication) == bool  # noqa: E721
        assert type(ptrack_enable) == bool  # noqa: E721
        assert initdb_params is None or type(initdb_params) == list  # noqa: E721
        assert pg_options is None or type(pg_options) == dict  # noqa: E721
        assert type(checksum) == bool  # noqa: E721
        assert bin_dir is None or type(bin_dir) == str  # noqa: E721

        node = self.make_empty(
            base_dir,
            port,
            bin_dir=bin_dir
        )

        final_initdb_params = initdb_params

        if checksum:
            final_initdb_params = __class__._paramlist_append_is_not_exist(
                initdb_params,
                final_initdb_params,
                '--data-checksums'
            )
            assert final_initdb_params is not None
            assert '--data-checksums' in final_initdb_params

        node.init(
            initdb_params=final_initdb_params,
            allow_streaming=set_replication
        )

        # set major version
        pg_version_file = self._os_ops.read(self._os_ops.build_path(node.data_dir, 'PG_VERSION'))
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
        if pg_options is not None:
            assert type(pg_options) == dict  # noqa: E721
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
    def _paramlist_has_param(
        params: typing.Optional[T_LIST_STR],
        param: str
    ) -> bool:
        assert type(param) == str  # noqa: E721

        if params is None:
            return False

        assert type(params) == list  # noqa: E721

        if param in params:
            return True

        return False

    @staticmethod
    def _paramlist_append(
        user_params: typing.Optional[T_LIST_STR],
        updated_params: typing.Optional[T_LIST_STR],
        param: str,
    ) -> T_LIST_STR:
        assert user_params is None or type(user_params) == list  # noqa: E721
        assert updated_params is None or type(updated_params) == list  # noqa: E721
        assert type(param) == str  # noqa: E721

        if updated_params is None:
            if user_params is None:
                return [param]

            return [*user_params, param]

        assert updated_params is not None
        if updated_params is user_params:
            return [*user_params, param]

        updated_params.append(param)
        return updated_params

    @staticmethod
    def _paramlist_append_is_not_exist(
        user_params: typing.Optional[T_LIST_STR],
        updated_params: typing.Optional[T_LIST_STR],
        param: str,
    ) -> typing.Optional[T_LIST_STR]:
        if __class__._paramlist_has_param(updated_params, param):
            return updated_params
        return __class__._paramlist_append(user_params, updated_params, param)

    @staticmethod
    def _gettempdir_for_socket() -> str:
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
    def _gettempdir() -> str:
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
