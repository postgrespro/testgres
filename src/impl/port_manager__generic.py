from testgres.operations.os_ops import OsOperations

from ..port_manager import PortManager
from ..exceptions import PortForException

import threading
import random
import typing
import logging


class PortManager__Generic(PortManager):
    _C_MIN_PORT_NUMBER = 1024
    _C_MAX_PORT_NUMBER = 65535

    _os_ops: OsOperations
    _guard: object
    # TODO: is there better to use bitmap fot _available_ports?
    _available_ports: typing.Set[int]
    _reserved_ports: typing.Set[int]

    def __init__(self, os_ops: OsOperations):
        assert __class__._C_MIN_PORT_NUMBER <= __class__._C_MAX_PORT_NUMBER

        assert os_ops is not None
        assert isinstance(os_ops, OsOperations)
        self._os_ops = os_ops
        self._guard = threading.Lock()

        self._available_ports = set(
            range(__class__._C_MIN_PORT_NUMBER, __class__._C_MAX_PORT_NUMBER + 1)
        )
        assert len(self._available_ports) == (
            (__class__._C_MAX_PORT_NUMBER - __class__._C_MIN_PORT_NUMBER) + 1
        )
        self._reserved_ports = set()
        return

    def reserve_port(self) -> int:
        assert self._guard is not None
        assert type(self._available_ports) == set  # noqa: E721t
        assert type(self._reserved_ports) == set  # noqa: E721

        with self._guard:
            t = tuple(self._available_ports)
            assert len(t) == len(self._available_ports)
            sampled_ports = random.sample(t, min(len(t), 100))
            t = None

            for port in sampled_ports:
                assert type(port) == int  # noqa: E721
                assert not (port in self._reserved_ports)
                assert port in self._available_ports

                assert port >= __class__._C_MIN_PORT_NUMBER
                assert port <= __class__._C_MAX_PORT_NUMBER

                if not self._os_ops.is_port_free(port):
                    continue

                self._reserved_ports.add(port)
                self._available_ports.discard(port)
                assert port in self._reserved_ports
                assert not (port in self._available_ports)
                __class__.helper__send_debug_msg("Port {} is reserved.", port)
                return port

        raise PortForException("Can't select a port.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        assert number >= __class__._C_MIN_PORT_NUMBER
        assert number <= __class__._C_MAX_PORT_NUMBER

        assert self._guard is not None
        assert type(self._reserved_ports) == set  # noqa: E721

        with self._guard:
            assert number in self._reserved_ports
            assert not (number in self._available_ports)
            self._available_ports.add(number)
            self._reserved_ports.discard(number)
            assert not (number in self._reserved_ports)
            assert number in self._available_ports
            __class__.helper__send_debug_msg("Port {} is released.", number)
        return

    @staticmethod
    def helper__send_debug_msg(msg_template: str, *args) -> None:
        assert msg_template is not None
        assert args is not None
        assert type(msg_template) == str  # noqa: E721
        assert type(args) == tuple  # noqa: E721
        assert msg_template != ""
        s = "[port manager] "
        s += msg_template.format(*args)
        logging.debug(s)
