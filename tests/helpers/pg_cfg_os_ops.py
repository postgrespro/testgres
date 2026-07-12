from __future__ import annotations

from testgres.postgres_configuration.os.abstract import configuration_os_ops as abs_pg_cfg_os_ops
from testgres.operations.os_ops import OsOperations

import datetime
import typing


class PgCfgOsFile(abs_pg_cfg_os_ops.ConfigurationOsFile):
    class tagData:
        os_ops: OsOperations
        file_path: str
        encoding: str
        next_line: int
        file_lines: typing.Optional[typing.List[str]]

        def __init__(
            self,
            os_ops: OsOperations,
            file_path: str,
            encoding: str,
        ):
            assert isinstance(os_ops, OsOperations)
            assert type(file_path) is str
            assert type(encoding) is str

            self.os_ops = os_ops
            self.file_path = file_path
            self.encoding = encoding
            self.file_lines = None
            self.next_line = 0
            self.file_lines = None
            return

    # --------------------------------------------------------------------
    _data: typing.Optional[tagData]

    # --------------------------------------------------------------------
    def __init__(
        self,
        os_ops: OsOperations,
        file_path: str,
        encoding: str,
    ):
        assert isinstance(os_ops, OsOperations)
        assert type(file_path) is str

        super().__init__()

        self._data = __class__.tagData(
            os_ops,
            file_path,
            encoding,
        )
        return

    # --------------------------------------------------------------------
    def __enter__(self) -> PgCfgOsFile:
        assert isinstance(self._data, __class__.tagData)
        return self

    # --------------------------------------------------------------------
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._data is not None:
            self.Close()

        assert self._data is None
        return False

    # --------------------------------------------------------------------
    @property
    def Name(self) -> str:
        assert type(self._data) is __class__.tagData
        assert type(self._data.file_path) is str
        return self._data.file_path

    # --------------------------------------------------------------------
    @property
    def IsClosed(self) -> bool:
        return self._data is None

    # --------------------------------------------------------------------
    def ReadLine(self) -> typing.Optional[str]:
        assert type(self._data) is __class__.tagData
        assert isinstance(self._data.os_ops, OsOperations)

        if self._data.file_lines is None:
            assert self._data.next_line == 0
            content = self._data.os_ops.read(
                self._data.file_path,
                encoding=self._data.encoding,
                binary=False,
            )
            assert type(content) is str
            self._data.file_lines = content.splitlines(keepends=True)

        assert type(self._data.file_lines) is list

        assert self._data.next_line <= len(self._data.file_lines)

        if self._data.next_line == len(self._data.file_lines):
            return None

        r = self._data.file_lines[self._data.next_line]
        assert type(r) is str

        self._data.next_line = self._data.next_line + 1
        return r

    # --------------------------------------------------------------------
    def Overwrite(self, text: str) -> None:
        assert type(self._data) is __class__.tagData
        assert isinstance(self._data.os_ops, OsOperations)

        self._data.os_ops.write(
            self._data.file_path,
            text,
            truncate=True,
            encoding=self._data.encoding,
            binary=False,
        )
        return

    # --------------------------------------------------------------------
    def Close(self) -> None:
        assert type(self._data) is __class__.tagData
        self._data = None
        return

    # --------------------------------------------------------------------
    def GetModificationTS(self) -> datetime.datetime:
        assert type(self._data) is __class__.tagData
        assert isinstance(self._data.os_ops, OsOperations)

        stat = self._data.os_ops.get_file_stat(self._data.file_path)
        assert stat is not None
        assert type(stat) is dict
        assert OsOperations.C_FILE_STAT_PROP__MTIME in stat

        mtime = stat[OsOperations.C_FILE_STAT_PROP__MTIME]
        assert mtime is not None
        assert type(mtime) is datetime.datetime
        return mtime


class PgCfgOsOps(abs_pg_cfg_os_ops.ConfigurationOsOps):
    _os_ops: OsOperations
    _file_encoding: str

    def __init__(
        self,
        os_ops: OsOperations,
        file_encoding: str,
    ):
        assert isinstance(os_ops, OsOperations)
        assert type(file_encoding) is str
        self._os_ops = os_ops
        self._file_encoding = file_encoding
        return

    def Path_IsAbs(self, a: str) -> bool:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.is_abs_path(a)

    def Path_Join(self, a: str, *p: str) -> str:
        assert type(a) is str
        assert type(p) is tuple
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.build_path(a, *p)

    def Path_NormPath(self, a: str) -> str:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.get_path_normpath(a)

    def Path_AbsPath(self, a: str) -> str:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.get_abs_path(a)

    def Path_NormCase(self, a: str) -> str:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.get_path_normcase(a)

    def Path_DirName(self, a: str) -> str:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.get_dirname(a)

    def Path_BaseName(self, a: str) -> str:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        return self._os_ops.get_path_basename(a)

    def Remove(self, a: str) -> None:
        assert type(a) is str
        assert isinstance(self._os_ops, OsOperations)
        self._os_ops.remove_file(a)
        return

    def OpenFileToRead(self, filePath: str) -> abs_pg_cfg_os_ops.ConfigurationOsFile:
        assert type(filePath) is str
        assert isinstance(self._os_ops, OsOperations)
        f = PgCfgOsFile(self._os_ops, filePath, self._file_encoding)
        return f

    def OpenFileToWrite(self, filePath: str) -> abs_pg_cfg_os_ops.ConfigurationOsFile:
        assert type(filePath) is str
        assert isinstance(self._os_ops, OsOperations)
        f = PgCfgOsFile(self._os_ops, filePath, self._file_encoding)
        return f

    def CreateFile(self, filePath: str) -> abs_pg_cfg_os_ops.ConfigurationOsFile:
        assert type(filePath) is str
        assert isinstance(self._os_ops, OsOperations)
        f = PgCfgOsFile(self._os_ops, filePath, self._file_encoding)
        self._os_ops.create_file(filePath)
        return f
