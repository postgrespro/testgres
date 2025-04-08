from .operations.os_ops import OsOperations

from .exceptions import PortForException

from . import utils

import threading
import random
import typing


class PortManager:
    def __init__(self):
        super().__init__()

    def reserve_port(self) -> int:
        raise NotImplementedError("PortManager::reserve_port is not implemented.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        raise NotImplementedError("PortManager::release_port is not implemented.")


class PortManager__ThisHost(PortManager):
    sm_single_instance: PortManager = None
    sm_single_instance_guard = threading.Lock()

    def __init__(self):
        pass

    def __new__(cls) -> PortManager:
        assert __class__ == PortManager__ThisHost
        assert __class__.sm_single_instance_guard is not None

        if __class__.sm_single_instance is None:
            with __class__.sm_single_instance_guard:
                __class__.sm_single_instance = super().__new__(cls)
        assert __class__.sm_single_instance
        assert type(__class__.sm_single_instance) == __class__  # noqa: E721
        return __class__.sm_single_instance

    def reserve_port(self) -> int:
        return utils.reserve_port()

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        return utils.release_port(number)


class PortManager__Generic(PortManager):
    _os_ops: OsOperations
    _guard: object
    # TODO: is there better to use bitmap fot _available_ports?
    _available_ports: typing.Set[int]
    _reserved_ports: typing.Set[int]

    def __init__(self, os_ops: OsOperations):
        assert os_ops is not None
        assert isinstance(os_ops, OsOperations)
        self._os_ops = os_ops
        self._guard = threading.Lock()
        self._available_ports: typing.Set[int] = set(range(1024, 65535))
        self._reserved_ports: typing.Set[int] = set()

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
                assert not (port in self._reserved_ports)
                assert port in self._available_ports

                if not self._os_ops.is_port_free(port):
                    continue

                self._reserved_ports.add(port)
                self._available_ports.discard(port)
                assert port in self._reserved_ports
                assert not (port in self._available_ports)
                return port

        raise PortForException("Can't select a port.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721

        assert self._guard is not None
        assert type(self._reserved_ports) == set  # noqa: E721

        with self._guard:
            assert number in self._reserved_ports
            assert not (number in self._available_ports)
            self._available_ports.add(number)
            self._reserved_ports.discard(number)
            assert not (number in self._reserved_ports)
            assert number in self._available_ports
