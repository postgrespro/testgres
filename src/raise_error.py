from .exceptions import InvalidOperationException
from .enums import NodeStatus

import typing


class RaiseError:
    @staticmethod
    def pg_ctl_returns_an_empty_string(_params):
        errLines = []
        errLines.append("Utility pg_ctl returns an empty string.")
        errLines.append("Command line is {0}".format(_params))
        raise RuntimeError("\n".join(errLines))

    @staticmethod
    def pg_ctl_returns_an_unexpected_string(out, _params):
        errLines = []
        errLines.append("Utility pg_ctl returns an unexpected string:")
        errLines.append(out)
        errLines.append("------------")
        errLines.append("Command line is {0}".format(_params))
        raise RuntimeError("\n".join(errLines))

    @staticmethod
    def pg_ctl_returns_a_zero_pid(out, _params):
        errLines = []
        errLines.append("Utility pg_ctl returns a zero pid. Output string is:")
        errLines.append(out)
        errLines.append("------------")
        errLines.append("Command line is {0}".format(_params))
        raise RuntimeError("\n".join(errLines))

    @staticmethod
    def node_err__cant_enumerate_child_processes(
        node_status: NodeStatus
    ):
        assert type(node_status) == NodeStatus  # noqa: E721

        msg = "Can't enumerate node child processes. {}.".format(
            __class__._map_node_status_to_reason(
                node_status,
                None,
            )
        )

        raise InvalidOperationException(msg)

    @staticmethod
    def node_err__cant_kill(
        node_status: NodeStatus
    ):
        assert type(node_status) == NodeStatus  # noqa: E721

        msg = "Can't kill server process. {}.".format(
            __class__._map_node_status_to_reason(
                node_status,
                None,
            )
        )

        raise InvalidOperationException(msg)

    @staticmethod
    def _map_node_status_to_reason(
        node_status: NodeStatus,
        node_pid: typing.Optional[int],
    ) -> str:
        assert type(node_status) == NodeStatus  # noqa: E721
        assert node_pid is None or type(node_pid) == int  # noqa: E721

        if node_status == NodeStatus.Uninitialized:
            return "Node is not initialized"

        if node_status == NodeStatus.Stopped:
            return "Node is not running"

        if node_status == NodeStatus.Running:
            return "Node is running (pid: {})".format(
                node_pid
            )

        # assert False
        return "Node has unknown status {}".format(
            node_status
        )
