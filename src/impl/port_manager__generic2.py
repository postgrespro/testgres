from testgres.operations.os_ops import OsOperations

from ..port_manager import PortManager
from ..exceptions import PortForException
from .. import consts

import threading
import random
import typing
import logging


class OsLockFsObj:
    _os_ops: typing.Optional[OsOperations]
    _path: typing.Optional[str]

    def __init__(self, os_ops: OsOperations, path: str):
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        os_ops.makedir(path)  # throw

        self._os_ops = os_ops
        self._path = path
        return

    def release(self) -> None:
        assert type(self._path) is str
        assert isinstance(self._os_ops, OsOperations)
        assert self._os_ops.path_exists(self._path)

        self._os_ops.rmdir(self._path)  # throw

        self._path = None
        self._os_ops = None
        return


class PortManager__Generic2(PortManager):
    _C_MIN_PORT_NUMBER = 1024
    _C_MAX_PORT_NUMBER = 65535

    _os_ops: OsOperations
    _guard: typing.Any

    # TODO: is there better to use bitmap fot _available_ports?
    _available_ports: typing.Set[int]
    _reserved_ports: typing.Dict[int, OsLockFsObj]

    _lock_dir: typing.Optional[str]

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
        self._reserved_ports = dict()
        self._lock_dir = None
        return

    def reserve_port(self) -> int:
        assert self._guard is not None
        assert type(self._available_ports) is set
        assert type(self._reserved_ports) is dict
        assert isinstance(self._os_ops, OsOperations)

        with self._guard:
            if self._lock_dir is None:
                temp_dir = self._os_ops.get_tempdir()
                assert type(temp_dir) is str
                lock_dir = self._os_ops.build_path(temp_dir, consts.TMP_TESTGRES_PORTS)
                assert type(lock_dir) is str
                self._os_ops.makedirs(lock_dir)
                self._lock_dir = lock_dir

            assert self._lock_dir is not None
            assert type(self._lock_dir) is str

            t = tuple(self._available_ports)
            assert len(t) == len(self._available_ports)
            sampled_ports = random.sample(t, min(len(t), 100))
            t = None

            for port in sampled_ports:
                assert type(port) is int
                assert not (port in self._reserved_ports)
                assert port in self._available_ports

                assert port >= __class__._C_MIN_PORT_NUMBER
                assert port <= __class__._C_MAX_PORT_NUMBER

                if not self._os_ops.is_port_free(port):
                    continue

                try:
                    lock_path = self.helper__make_lock_path(port)
                    lock_obj = OsLockFsObj(self._os_ops, lock_path)  # raise
                except:  # noqa: 722
                    continue

                assert isinstance(lock_obj, OsLockFsObj)
                assert self._os_ops.path_exists(lock_path)

                try:
                    self._reserved_ports[port] = lock_obj
                except:  # noqa: 722
                    assert not (port in self._reserved_ports)
                    lock_obj.release()
                    raise

                self._available_ports.discard(port)
                assert port in self._reserved_ports
                assert not (port in self._available_ports)
                __class__.helper__send_debug_msg("Port {} is reserved.", port)
                return port

        raise PortForException("Can't select a port.")

    def release_port(self, number: int) -> None:
        assert type(number) is int
        assert number >= __class__._C_MIN_PORT_NUMBER
        assert number <= __class__._C_MAX_PORT_NUMBER

        assert self._guard is not None
        assert type(self._reserved_ports) is dict

        with self._guard:
            assert number in self._reserved_ports
            assert not (number in self._available_ports)
            self._available_ports.add(number)
            lock_obj = self._reserved_ports.pop(number)
            assert not (number in self._reserved_ports)
            assert number in self._available_ports
            assert isinstance(lock_obj, OsLockFsObj)
            lock_obj.release()
            __class__.helper__send_debug_msg("Port {} is released.", number)
        return

    @staticmethod
    def helper__send_debug_msg(msg_template: str, *args) -> None:
        assert msg_template is not None
        assert args is not None
        assert type(msg_template) is str
        assert type(args) is tuple
        assert msg_template != ""
        s = "[port manager] "
        s += msg_template.format(*args)
        logging.debug(s)

    def helper__make_lock_path(self, port_number: int) -> str:
        assert type(port_number) is int
        # You have to call the reserve_port at first!
        assert type(self._lock_dir) is str

        result = self._os_ops.build_path(self._lock_dir, str(port_number) + ".lock")
        assert type(result) is str
        return result
