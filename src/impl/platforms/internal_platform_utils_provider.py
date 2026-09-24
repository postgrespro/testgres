from .internal_platform_utils_factory import create_internal_platform_utils
from .internal_platform_utils_factory import InternalPlatformUtils

from testgres.operations.os_ops import OsOperations

import typing


class InternalPlaformUtilsProvider:
    T_PLATFORM_UTILS = InternalPlatformUtils

    _os_ops: OsOperations
    _platform_utils: typing.Optional[T_PLATFORM_UTILS] = None

    def __init__(
        self,
        os_ops: OsOperations,
    ):
        assert isinstance(os_ops, OsOperations)
        self._os_ops = os_ops
        self._platform_utils = None
        return

    def get(self) -> T_PLATFORM_UTILS:
        if self._platform_utils is None:
            self._platform_utils = create_internal_platform_utils(
                self._os_ops,
            )
            assert isinstance(self._platform_utils, __class__.T_PLATFORM_UTILS)

        assert isinstance(self._platform_utils, __class__.T_PLATFORM_UTILS)
        return self._platform_utils
