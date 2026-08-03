# coding: utf-8

from . import internal_utils

import typing

from testgres.operations.os_ops import OsOperations


class FileLineReader:
    _os_ops: OsOperations
    _file_name: str
    _file_encoding: str
    _file_pos: int
    _buffer_pos: int
    _buffer: bytes

    # --------------------------------------------------------------------
    def __init__(
        self,
        os_ops: OsOperations,
        file_name: str,
        file_encoding: str = "utf-8",
        file_pos: int = 0
    ):
        assert isinstance(os_ops, OsOperations)
        assert type(file_encoding) is str
        self._os_ops = os_ops
        self._file_name = file_name
        self._file_encoding = file_encoding
        self._file_pos = file_pos
        self._buffer_pos = 0
        self._buffer = internal_utils.read_line_to_pos__bin(
            os_ops,
            file_name,
            file_pos,
        )
        return

    # interface ----------------------------------------------------------
    def read_line(self) -> typing.Optional[str]:
        assert isinstance(self._os_ops, OsOperations)
        assert type(self._buffer_pos) is int
        assert type(self._buffer) is bytes
        assert self._buffer_pos >= 0
        assert self._buffer_pos <= len(self._buffer)

        scan_pos = self._buffer_pos

        while True:
            sz1 = len(self._buffer)

            if scan_pos == sz1:
                block = self._os_ops.read_binary(
                    self._file_name,
                    self._file_pos,
                )
                assert type(block) is bytes
                self._buffer += block
                self._file_pos += len(block)

            x = self._buffer.find(b'\n', scan_pos)

            sz2 = len(self._buffer)

            if x == -1:
                if scan_pos == sz2:
                    return None

                if self._buffer_pos == 0:
                    scan_pos = sz2
                else:
                    assert self._buffer_pos > 0
                    self._buffer = self._buffer[self._buffer_pos:]
                    scan_pos = sz2 - self._buffer_pos
                    self._buffer_pos = 0
                continue

            assert x >= 0
            assert x < sz2

            b = self._buffer[self._buffer_pos:(x+1)]

            s = b.decode(self._file_encoding)

            self._buffer_pos = x + 1
            return s
