from __future__ import annotations

from .. import internal_platform_utils as base
from testgres.operations.os_ops import OsOperations
import typing


class InternalPlatformUtils(base.InternalPlatformUtils):
    def FindPostmaster(
        self,
        os_ops: OsOperations,
        bin_dir: str,
        data_dir: str
    ) -> InternalPlatformUtils.FindPostmasterResult:
        assert isinstance(os_ops, OsOperations)
        assert type(bin_dir) is str
        assert type(data_dir) is str
        return __class__.FindPostmasterResult.create_not_implemented()

    def ProcessIsZombi_soft_check(
        self,
        os_ops: OsOperations,
        pid: int,
    ) -> typing.Optional[bool]:
        assert isinstance(os_ops, OsOperations)
        assert type(pid) is int
        return None
