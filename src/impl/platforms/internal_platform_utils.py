from __future__ import annotations

import enum
import typing

from testgres.operations.os_ops import OsOperations


class InternalPlatformUtils:
    class FindPostmasterResultCode(enum.Enum):
        ok = 0
        not_found = 1,
        not_implemented = 2
        many_processes = 3
        has_problems = 4

    class FindPostmasterResult:
        code: InternalPlatformUtils.FindPostmasterResultCode
        pid: typing.Optional[int]

        def __init__(
            self,
            code: InternalPlatformUtils.FindPostmasterResultCode,
            pid: typing.Optional[int]
        ):
            assert type(code) == InternalPlatformUtils.FindPostmasterResultCode  # noqa: E721
            assert pid is None or type(pid) == int  # noqa: E721
            self.code = code
            self.pid = pid
            return

        @staticmethod
        def create_ok(pid: int) -> InternalPlatformUtils.FindPostmasterResult:
            assert type(pid) == int  # noqa: E721
            return __class__(InternalPlatformUtils.FindPostmasterResultCode.ok, pid)

        @staticmethod
        def create_not_found() -> InternalPlatformUtils.FindPostmasterResult:
            return __class__(InternalPlatformUtils.FindPostmasterResultCode.not_found, None)

        @staticmethod
        def create_not_implemented() -> InternalPlatformUtils.FindPostmasterResult:
            return __class__(InternalPlatformUtils.FindPostmasterResultCode.not_implemented, None)

        @staticmethod
        def create_many_processes() -> InternalPlatformUtils.FindPostmasterResult:
            return __class__(InternalPlatformUtils.FindPostmasterResultCode.many_processes, None)

        @staticmethod
        def create_has_problems() -> InternalPlatformUtils.FindPostmasterResult:
            return __class__(InternalPlatformUtils.FindPostmasterResultCode.has_problems, None)

    def FindPostmaster(
        self,
        os_ops: OsOperations,
        bin_dir: str,
        data_dir: str
    ) -> FindPostmasterResult:
        assert isinstance(os_ops, OsOperations)
        assert type(bin_dir) == str  # noqa: E721
        assert type(data_dir) == str  # noqa: E721
        raise NotImplementedError("InternalPlatformUtils::FindPostmaster is not implemented.")
