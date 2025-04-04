from .operations.os_ops import OsOperations

from .helpers.port_manager import PortForException

from . import utils

import threading
import random

class PostgresNodePortManager:
    def __init__(self):
        super().__init__()

    def reserve_port(self) -> int:
        raise NotImplementedError("PostgresNodePortManager::reserve_port is not implemented.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        raise NotImplementedError("PostgresNodePortManager::release_port is not implemented.")


class PostgresNodePortManager__ThisHost(PostgresNodePortManager):
    sm_single_instance: PostgresNodePortManager = None
    sm_single_instance_guard = threading.Lock()

    def __init__(self):
        pass

    def __new__(cls) -> PostgresNodePortManager:
        assert __class__ == PostgresNodePortManager__ThisHost
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


class PostgresNodePortManager__Generic(PostgresNodePortManager):
    _os_ops: OsOperations
    _allocated_ports_guard: object
    _allocated_ports: set[int]

    def __init__(self, os_ops: OsOperations):
        assert os_ops is not None
        assert isinstance(os_ops, OsOperations)
        self._os_ops = os_ops
        self._allocated_ports_guard = threading.Lock()
        self._allocated_ports = set[int]()

    def reserve_port(self) -> int:
        ports = set(range(1024, 65535))
        assert type(ports) == set  # noqa: E721

        assert self._allocated_ports_guard is not None
        assert type(self._allocated_ports) == set  # noqa: E721

        with self._allocated_ports_guard:
            ports.difference_update(self._allocated_ports)

            sampled_ports = random.sample(tuple(ports), min(len(ports), 100))

            for port in sampled_ports:
                assert not (port in self._allocated_ports)

                if not self._os_ops.is_port_free(port):
                    continue

                self._allocated_ports.add(port)
                return port

        raise PortForException("Can't select a port")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721

        assert self._allocated_ports_guard is not None
        assert type(self._allocated_ports) == set  # noqa: E721

        with self._allocated_ports_guard:
            assert number in self._allocated_ports
            self._allocated_ports.discard(number)
