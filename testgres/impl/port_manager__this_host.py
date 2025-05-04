from ..port_manager import PortManager

from .. import utils

import threading


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
