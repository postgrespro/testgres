from src import InvalidOperationException
from src import NodeStatus
from src.raise_error import RaiseError

import pytest
import typing


class TestRaiseError:
    class tagTestData001:
        node_status: NodeStatus
        expected_msg: str

        def __init__(
            self,
            node_status: NodeStatus,
            expected_msg: str,
        ):
            assert type(node_status) == NodeStatus  # noqa: E721
            assert type(expected_msg) == str  # noqa: E721
            self.node_status = node_status
            self.expected_msg = expected_msg
            return

        @property
        def sign(self) -> str:
            assert type(self.node_status) == NodeStatus  # noqa: E721

            msg = "status: {}".format(self.node_status)
            return msg

    sm_Data001: typing.List[tagTestData001] = [
        tagTestData001(
            NodeStatus.Uninitialized,
            "Can't enumerate node child processes. Node is not initialized.",
        ),
        tagTestData001(
            NodeStatus.Stopped,
            "Can't enumerate node child processes. Node is not running.",
        ),
    ]

    @pytest.fixture(
        params=sm_Data001,
        ids=[x.sign for x in sm_Data001],
    )
    def data001(self, request: pytest.FixtureRequest) -> tagTestData001:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "tagTestData001"
        return request.param

    def test_001__node_err__cant_enumerate_child_processes(
        self,
        data001: tagTestData001,
    ):
        assert type(data001) == __class__.tagTestData001  # noqa: E721

        with pytest.raises(expected_exception=InvalidOperationException) as x:
            RaiseError.node_err__cant_enumerate_child_processes(
                data001.node_status
            )

        assert x is not None
        assert str(x.value) == data001.expected_msg
        return

    sm_Data002: typing.List[tagTestData001] = [
        tagTestData001(
            NodeStatus.Uninitialized,
            "Can't kill server process. Node is not initialized.",
        ),
        tagTestData001(
            NodeStatus.Stopped,
            "Can't kill server process. Node is not running.",
        ),
    ]

    @pytest.fixture(
        params=sm_Data002,
        ids=[x.sign for x in sm_Data002],
    )
    def data002(self, request: pytest.FixtureRequest) -> tagTestData001:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "tagTestData001"
        return request.param

    def test_002__node_err__cant_kill(
        self,
        data002: tagTestData001,
    ):
        assert type(data002) == __class__.tagTestData001  # noqa: E721

        with pytest.raises(expected_exception=InvalidOperationException) as x:
            RaiseError.node_err__cant_kill(
                data002.node_status
            )

        assert x is not None
        assert str(x.value) == data002.expected_msg
        return
