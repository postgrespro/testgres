from __future__ import annotations

from ...helpers.global_data import OsOpsDescrs
from ...helpers.global_data import OsOpsDescr
from ...helpers.global_data import OsOperations

from src.impl.file_line_reader import FileLineReader

import pytest
import typing
import dataclasses
import logging


class TestFileLineReader:
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

    # --------------------------------------------------------------------
    @dataclasses.dataclass
    class tagStep:
        write_data: bytes
        read_lines: typing.List[typing.Optional[str]]

    sm_Steps001: typing.List[tagStep] = [
        tagStep(
            b"",
            [None, None, None]
        ),
        tagStep(
            b"a",
            [None, None]
        ),
        tagStep(
            b"b",
            [None, None]
        ),
        tagStep(
            b"c\n",
            ["abc\n", None]
        ),
        tagStep(
            b"d",
            [None, None]
        ),
        tagStep(
            b"efg\n1\n\n3",
            ["defg\n", "1\n", "\n", None, None]
        ),
        tagStep(
            b" \n  1\n",
            ["3 \n", "  1\n", None, None]
        ),
        # russian text ma: b'\xd0\xbc\xd0\xb0'
        tagStep(
            b'\xd0',
            [None]
        ),
        tagStep(
            b'\xbc',
            [None]
        ),
        tagStep(
            b'\xd0',
            [None]
        ),
        tagStep(
            b'\xb0',
            [None]
        ),
        tagStep(
            b'\n',  # FINISH
            ["\u043c\u0430\n", None]
        ),

    ]

    # --------------------------------------------------------------------
    def test_001__common(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        tmpdir = os_ops.mkdtemp()
        filename = os_ops.build_path(tmpdir, "my.log")
        assert not os_ops.path_exists(filename)

        os_ops.touch(filename)
        assert os_ops.path_exists(filename)
        assert os_ops.get_file_size(filename) == 0

        file_line_reader = FileLineReader(
            os_ops,
            filename,
            file_encoding="utf-8",
        )

        # -----------------------
        nStep = 0

        for step in __class__.sm_Steps001:
            nStep += 1

            logging.info("-------------------- step: {}".format(nStep))

            logging.info("write: {}".format(step.write_data))
            os_ops.write(filename, step.write_data, binary=True)

            nRead = 0
            for expected_line in step.read_lines:
                nRead += 1
                logging.info("read [{}]. expected line is {!r}".format(
                    nRead,
                    expected_line,
                ))
                actual_line = file_line_reader.read_line()

                if actual_line != expected_line:
                    err_msg = "Read bad line {!r}. Expected {!r}".format(
                        actual_line,
                        expected_line,
                    )
                    raise RuntimeError(err_msg)
                continue
            continue

        assert file_line_reader.read_line() is None

        os_ops.rmdirs(tmpdir)
        assert not os_ops.path_exists(tmpdir)
        return
