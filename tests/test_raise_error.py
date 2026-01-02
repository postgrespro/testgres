from src import InvalidOperationException
from src import NodeStatus
from src.raise_error import RaiseError

import pytest
import typing


class TestRaiseError:
    class tagData001__NodeErr_CantEnumerateChildProcesses:
        node_status: NodeStatus
        node_pid: typing.Optional[int]
        expected_msg: str

        def __init__(
            self,
            node_status: NodeStatus,
            node_pid: typing.Optional[int],
            expected_msg: str,
        ):
            assert type(node_status) == NodeStatus  # noqa: E721
            assert node_pid is None or type(node_pid) == int  # noqa: E721
            assert type(expected_msg) == str  # noqa: E721
            self.node_status = node_status
            self.node_pid = node_pid
            self.expected_msg = expected_msg
            return

        @property
        def sign(self) -> str:
            assert type(self.node_status) == NodeStatus  # noqa: E721
            assert self.node_pid is None or type(self.node_pid) == int  # noqa: E721

            msg = "status: {}; pid: {}".format(
                self.node_status,
                self.node_pid,
            )
            return msg

    sm_Data001: list[tagData001__NodeErr_CantEnumerateChildProcesses] = [
        tagData001__NodeErr_CantEnumerateChildProcesses(
            NodeStatus.Uninitialized,
            None,
            "Can't enumerate node child processes. Node is not initialized.",
        ),
        tagData001__NodeErr_CantEnumerateChildProcesses(
            NodeStatus.Stopped,
            None,
            "Can't enumerate node child processes. Node is not running.",
        ),
    ]

    @pytest.fixture(
        params=sm_Data001,
        ids=[x.sign for x in sm_Data001],
    )
    def data001(self, request: pytest.FixtureRequest) -> tagData001__NodeErr_CantEnumerateChildProcesses:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "tagData001__NodeErr_CantEnumerateChildProcesses"
        return request.param

    def test_001__node_err__cant_enumerate_child_processes(
        self,
        data001: tagData001__NodeErr_CantEnumerateChildProcesses,
    ):
        assert type(data001) == __class__.tagData001__NodeErr_CantEnumerateChildProcesses  # noqa: E721

        with pytest.raises(expected_exception=InvalidOperationException) as x:
            RaiseError.node_err__cant_enumerate_child_processes(
                data001.node_status,
                data001.node_pid,
            )

        assert x is not None
        assert str(x.value) == data001.expected_msg
        return
