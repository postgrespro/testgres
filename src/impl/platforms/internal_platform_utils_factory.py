from .internal_platform_utils import InternalPlatformUtils

from testgres.operations.os_ops import OsOperations


def create_internal_platform_utils(
    os_ops: OsOperations
) -> InternalPlatformUtils:
    assert isinstance(os_ops, OsOperations)

    platform_name = os_ops.get_platform()
    assert type(platform_name) is str

    if platform_name == "linux":
        from .linux import internal_platform_utils as x
        return x.InternalPlatformUtils()

    if platform_name == "win32":
        from .win32 import internal_platform_utils as x
        return x.InternalPlatformUtils()

    # not implemented
    return InternalPlatformUtils()
