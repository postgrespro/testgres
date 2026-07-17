from __future__ import annotations

from ....helpers.global_data import OsOpsDescrs
from ....helpers.global_data import OsOpsDescr
from ....helpers.global_data import OsOperations

from src.node import PostgresNodeLogReader

import pytest
import typing
import random


class TestSetM001__helper_create_log_info:
    sm_os_ops_descrs: typing.List[OsOpsDescr] = [
        OsOpsDescrs.sm_local_os_ops_descr,
        OsOpsDescrs.sm_remote_os_ops_descr
    ]

    @pytest.fixture(
        params=[
            pytest.param(
                descr,
                id=descr.sign,
            )
            for descr in sm_os_ops_descrs
        ],
    )
    def os_ops_descr(self, request: pytest.FixtureRequest) -> OsOpsDescr:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, OsOpsDescr)
        return request.param

    def test_001__common(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        # Scenario 1: The log file ends with a normal line feed
        filename = os_ops.mkstemp("data_for_create_log_info")
        C_DATA1 = b"Line 1\nLine 2\n"
        os_ops.write(filename, C_DATA1, binary=True, truncate=True)

        log_info1 = PostgresNodeLogReader._create_log_info(os_ops, filename, find_line_start=True)
        # Since the file ends with \n, the tail must be empty!
        assert log_info1.tail == b""
        assert log_info1.position == len(C_DATA1)

        # Scenario 2: The log file contains an unterminated line (our UTF-8 trap)
        C_DATA2 = b"Line 1\nLine 2\nIncomplete UTF8 \xd0"
        os_ops.write(filename, C_DATA2, binary=True, truncate=True)

        log_info2 = PostgresNodeLogReader._create_log_info(os_ops, filename, find_line_start=True)
        # The tail must contain exactly the piece after the last \n
        assert log_info2.tail == b"Incomplete UTF8 \xd0"
        assert log_info2.position == len(C_DATA2)

        # Scenario 3: The file has no line breaks at all (one long line)
        C_DATA3 = b"Just one long line without newlines"
        os_ops.write(filename, C_DATA3, binary=True, truncate=True)

        log_info3 = PostgresNodeLogReader._create_log_info(os_ops, filename, find_line_start=True)
        # Should take the entire file in tail, and set the position to the file size
        assert log_info3.tail == b"Just one long line without newlines"
        assert log_info3.position == len(C_DATA3)

        # 4. Large data (two segments)
        allowed_bytes = bytes([b for b in range(256) if b != 10])
        C_DATA4 = bytes(random.choices(allowed_bytes, k=5000))
        os_ops.write(filename, C_DATA4, binary=True, truncate=True)

        log_info = PostgresNodeLogReader._create_log_info(os_ops, filename, find_line_start=True)
        assert log_info.tail == C_DATA4
        assert log_info.position == len(C_DATA4)

        # 5. Large data (many segments)
        allowed_bytes = bytes([b for b in range(256) if b != 10])
        C_DATA5 = bytes(random.choices(allowed_bytes, k=999983))
        os_ops.write(filename, C_DATA5, binary=True, truncate=True)

        log_info = PostgresNodeLogReader._create_log_info(os_ops, filename, find_line_start=True)
        assert log_info.tail == C_DATA5
        assert log_info.position == len(C_DATA5)

        # 6. Large data (first_line + many segments)
        allowed_bytes = bytes([b for b in range(256) if b != 10])
        os_ops.write(filename, b'abcd\n', binary=True, truncate=True)
        os_ops.write(filename, C_DATA5, binary=True, truncate=False)

        log_info = PostgresNodeLogReader._create_log_info(os_ops, filename, find_line_start=True)
        assert log_info.tail == C_DATA5
        assert log_info.position == 5 + len(C_DATA5)

        # Cleanup
        os_ops.remove_file(filename)
        return
