from .enums import NodeStatus

import typing


class PostgresNodeState:
    node_status: NodeStatus
    pid: typing.Optional[int]

    def __init__(
        self,
        node_status: NodeStatus,
        pid: typing.Optional[int],
    ):
        assert type(node_status) is NodeStatus
        assert pid is None or type(pid) is int

        self.node_status = node_status
        self.pid = pid
        return
