from src import PostgresNode
from src import PortManager
from src import OsOperations
from src import NodeStatus
from src.node import PostgresNodeLogReader

from tests.helpers.utils import Utils as HelperUtils
from tests.helpers.utils import T_WAIT_TIME

from tests.helpers.global_data import PostgresNodeService

import typing


class PostgresNodeUtils:
    class PostgresNodeUtilsException(Exception):
        pass

    class PortConflictNodeException(PostgresNodeUtilsException):
        _data_dir: str
        _port: int

        def __init__(self, data_dir: str, port: int):
            assert type(data_dir) == str  # noqa: E721
            assert type(port) == int  # noqa: E721

            super().__init__()

            self._data_dir = data_dir
            self._port = port
            return

        @property
        def data_dir(self) -> str:
            assert type(self._data_dir) == str  # noqa: E721
            return self._data_dir

        @property
        def port(self) -> int:
            assert type(self._port) == int  # noqa: E721
            return self._port

        @property
        def message(self) -> str:
            assert type(self._data_dir) == str  # noqa: E721
            assert type(self._port) == int  # noqa: E721

            r = "PostgresNode [data:{}][port: {}] conflicts with port of another instance.".format(
                self._data_dir,
                self._port,
            )
            assert type(r) == str  # noqa: E721
            return r

        def __str__(self) -> str:
            r = self.message
            assert type(r) == str  # noqa: E721
            return r

        def __repr__(self) -> str:
            # It must be overrided!
            assert type(self) == __class__  # noqa: E721
            r = "{}({}, {})".format(
                __class__.__name__,
                repr(self._data_dir),
                repr(self._port),
            )
            assert type(r) == str  # noqa: E721
            return r

    # --------------------------------------------------------------------
    class StartNodeException(PostgresNodeUtilsException):
        _data_dir: str
        _files: typing.Optional[typing.Iterable]

        def __init__(
            self,
            data_dir: str,
            files: typing.Optional[typing.Iterable] = None
        ):
            assert type(data_dir) == str  # noqa: E721
            assert files is None or isinstance(files, typing.Iterable)

            super().__init__()

            self._data_dir = data_dir
            self._files = files
            return

        @property
        def message(self) -> str:
            assert self._data_dir is None or type(self._data_dir) == str  # noqa: E721
            assert self._files is None or isinstance(self._files, typing.Iterable)

            msg_parts = []

            msg_parts.append("PostgresNode [data_dir: {}] is not started.".format(
                self._data_dir
            ))

            for f, lines in self._files or []:
                assert type(f) == str  # noqa: E721
                assert type(lines) in [str, bytes]  # noqa: E721
                msg_parts.append(u'{}\n----\n{}\n'.format(f, lines))

            return "\n".join(msg_parts)

        @property
        def data_dir(self) -> typing.Optional[str]:
            assert type(self._data_dir) == str  # noqa: E721
            return self._data_dir

        @property
        def files(self) -> typing.Optional[typing.Iterable]:
            assert self._files is None or isinstance(self._files, typing.Iterable)
            return self._files

        def __repr__(self) -> str:
            assert type(self._data_dir) == str  # noqa: E721
            assert self._files is None or isinstance(self._files, typing.Iterable)

            r = "{}({}, {})".format(
                __class__.__name__,
                repr(self._data_dir),
                repr(self._files),
            )
            assert type(r) == str  # noqa: E721
            return r

    # --------------------------------------------------------------------
    @staticmethod
    def get_node(
        node_svc: PostgresNodeService,
        name: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        port_manager: typing.Optional[PortManager] = None
    ) -> PostgresNode:
        assert isinstance(node_svc, PostgresNodeService)
        assert isinstance(node_svc.os_ops, OsOperations)
        assert isinstance(node_svc.port_manager, PortManager)

        if port_manager is None:
            port_manager = node_svc.port_manager

        return PostgresNode(
            name,
            port=port,
            os_ops=node_svc.os_ops,
            port_manager=port_manager if port is None else None
        )

    # --------------------------------------------------------------------
    @staticmethod
    def wait_for_running_state(
        node: PostgresNode,
        node_log_reader: PostgresNodeLogReader,
        timeout: T_WAIT_TIME,
    ):
        assert type(node) == PostgresNode  # noqa: E721
        assert type(node_log_reader) == PostgresNodeLogReader  # noqa: E721
        assert type(timeout) in [int, float]
        assert node_log_reader._node is node
        assert timeout > 0

        for _ in HelperUtils.WaitUntil(
            timeout=timeout
        ):
            s = node.status()

            if s == NodeStatus.Running:
                return

            assert s == NodeStatus.Stopped

            blocks = node_log_reader.read()
            assert type(blocks) == list  # noqa: E721

            for block in blocks:
                assert type(block) == PostgresNodeLogReader.LogDataBlock  # noqa: E721

                if 'Is another postmaster already running on port' in block.data:
                    raise __class__.PortConflictNodeException(node.data_dir, node.port)

                if 'database system is shut down' in block.data:
                    raise __class__.StartNodeException(
                        node.data_dir,
                        node._collect_special_files(),
                    )
            continue
