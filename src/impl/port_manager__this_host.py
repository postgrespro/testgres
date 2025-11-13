from ..port_manager import PortManager

from .. import utils

import threading


class PortManager__ThisHost(PortManager):
    sm_single_instance: PortManager = None
    sm_single_instance_guard = threading.Lock()

    @staticmethod
    def get_single_instance() -> PortManager:
        assert __class__ == PortManager__ThisHost
        assert __class__.sm_single_instance_guard is not None

        if __class__.sm_single_instance is not None:
            assert type(__class__.sm_single_instance) == __class__  # noqa: E721
            return __class__.sm_single_instance

        with __class__.sm_single_instance_guard:
            if __class__.sm_single_instance is None:
                __class__.sm_single_instance = __class__()
        assert __class__.sm_single_instance is not None
        assert type(__class__.sm_single_instance) == __class__  # noqa: E721
        return __class__.sm_single_instance

    def reserve_port(self) -> int:
        return utils.reserve_port()

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        return utils.release_port(number)
