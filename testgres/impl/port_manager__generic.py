from ..operations.os_ops import OsOperations

from ..port_manager import PortManager
from ..exceptions import PortForException
from .. import consts

import os
import threading
import random
import typing


class PortManager__Generic(PortManager):
    _os_ops: OsOperations
    _guard: object
    # TODO: is there better to use bitmap fot _available_ports?
    _available_ports: typing.Set[int]
    _reserved_ports: typing.Set[int]

    _lock_dir: str

    def __init__(self, os_ops: OsOperations):
        assert os_ops is not None
        assert isinstance(os_ops, OsOperations)
        self._os_ops = os_ops
        self._guard = threading.Lock()
        self._available_ports: typing.Set[int] = set(range(1024, 65535))
        self._reserved_ports: typing.Set[int] = set()

        temp_dir = os_ops.get_tempdir()
        assert type(temp_dir) == str  # noqa: E721
        self._lock_dir = os.path.join(temp_dir, consts.TMP_TESTGRES_PORTS)
        assert type(self._lock_dir) == str  # noqa: E721
        os_ops.makedirs(self._lock_dir)

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

                try:
                    lock_path = self.helper__make_lock_path(port)
                    self._os_ops.makedir(lock_path)
                except:  # noqa: 722
                    continue

                assert self._os_ops.path_exists(lock_path)

                try:
                    self._reserved_ports.add(port)
                except:  # noqa: 722
                    assert not (port in self._reserved_ports)
                    self._os_ops.rmdir(lock_path)
                    raise

                assert port in self._reserved_ports
                self._available_ports.discard(port)
                assert not (port in self._available_ports)
                return port

        raise PortForException("Can't select a port.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721

        assert self._guard is not None
        assert type(self._reserved_ports) == set  # noqa: E721

        lock_path = self.helper__make_lock_path(number)

        with self._guard:
            assert number in self._reserved_ports
            assert not (number in self._available_ports)
            self._available_ports.add(number)
            self._reserved_ports.discard(number)
            assert not (number in self._reserved_ports)
            assert number in self._available_ports

            assert isinstance(self._os_ops, OsOperations)
            assert self._os_ops.path_exists(lock_path)
            self._os_ops.rmdir(lock_path)

        return

    def helper__make_lock_path(self, port_number: int) -> str:
        assert type(port_number) == int  # noqa: E721
        assert type(self._lock_dir) == str  # noqa: E721

        result = os.path.join(self._lock_dir, str(port_number) + ".lock")
        assert type(result) == str  # noqa: E721
        return result
