from ..operations.os_ops import OsOperations
from ..operations.os_ops import OsLockObj

from ..port_manager import PortManager
from ..exceptions import PortForException
from .. import consts

import os
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
    _reserved_ports: typing.Dict[int, OsLockObj]

    _lock_dir: str

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

    def reserve_port(self) -> int:
        assert self._guard is not None
        assert type(self._available_ports) == set  # noqa: E721t
        assert type(self._reserved_ports) == dict  # noqa: E721
        assert isinstance(self._os_ops, OsOperations)

        with self._guard:
            if self._lock_dir is None:
                temp_dir = self._os_ops.get_tempdir()
                assert type(temp_dir) == str  # noqa: E721
                lock_dir = os.path.join(temp_dir, consts.TMP_TESTGRES_PORTS)
                assert type(lock_dir) == str  # noqa: E721
                self._os_ops.makedirs(lock_dir)
                self._lock_dir = lock_dir

            assert self._lock_dir is not None
            assert type(self._lock_dir) == str  # noqa: E721

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

                try:
                    lock_path = self.helper__make_lock_path(port)
                    lock_obj = self._os_ops.create_lock_fs_obj(lock_path)  # raise
                except:  # noqa: 722
                    continue

                assert isinstance(lock_obj, OsLockObj)
                assert self._os_ops.path_exists(lock_path)

                try:
                    self._reserved_ports[port] = lock_obj
                except:  # noqa: 722
                    assert not (port in self._reserved_ports)
                    lock_obj.release()
                    raise

                assert port in self._reserved_ports
                self._available_ports.discard(port)
                assert not (port in self._available_ports)
                __class__.helper__send_debug_msg("Port {} is reserved.".format(port))
                return port

        raise PortForException("Can't select a port.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        assert number >= __class__._C_MIN_PORT_NUMBER
        assert number <= __class__._C_MAX_PORT_NUMBER

        assert self._guard is not None
        assert type(self._reserved_ports) == dict  # noqa: E721

        with self._guard:
            assert number in self._reserved_ports
            assert not (number in self._available_ports)
            self._available_ports.add(number)
            lock_obj = self._reserved_ports.pop(number)
            assert not (number in self._reserved_ports)
            assert number in self._available_ports
            assert isinstance(lock_obj, OsLockObj)
            lock_obj.release()
            __class__.helper__send_debug_msg("Port {} is released.", number)
        return

    @staticmethod
    def helper__send_debug_msg(msg_template: str, *args) -> None:
        assert msg_template is not None
        assert str is not None
        assert type(msg_template) == str  # noqa: E721
        assert type(args) == tuple  # noqa: E721
        assert msg_template != ""
        s = "[port manager] "
        s += msg_template.format(*args)
        logging.debug(s)

    def helper__make_lock_path(self, port_number: int) -> str:
        assert type(port_number) == int  # noqa: E721
        # You have to call the reserve_port at first!
        assert type(self._lock_dir) == str  # noqa: E721

        result = os.path.join(self._lock_dir, str(port_number) + ".lock")
        assert type(result) == str  # noqa: E721
        return result
