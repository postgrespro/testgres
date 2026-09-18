from tests.helpers.global_data import OsOpsDescr
from tests.helpers.global_data import OsOpsDescrs
from tests.helpers.global_data import OsOperations

from testgres.operations.exceptions import ExecUtilException

from src.utils import execute_utility2

import pytest
import typing


class TestUtils__command_execute2:
    sm_os_ops_descrs: typing.List[OsOpsDescr] = [
        OsOpsDescrs.sm_local_os_ops_descr,
        OsOpsDescrs.sm_remote_os_ops_descr
    ]

    @pytest.fixture(
        params=[descr.os_ops for descr in sm_os_ops_descrs],
        ids=[descr.sign for descr in sm_os_ops_descrs]
    )
    def os_ops(self, request: pytest.FixtureRequest) -> OsOperations:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, OsOperations)
        return request.param

    def test_execute_utility2__log(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        log_file: typing.Optional[str] = None

        try:
            C_OUT_DATA = "AAAA"

            log_file = os_ops.mkstemp("testgres--")
            assert os_ops.path_exists(log_file)

            os_ops.write(
                log_file,
                C_OUT_DATA + "\n",
                truncate=False,
                binary=False,
            )

            cmd = ["sh", "-c", "echo BBBB"]

            r = execute_utility2(
                os_ops,
                cmd,
                logfile=log_file,
            )

            assert type(r) is str
            assert r == "BBBB\n"

            assert os_ops.path_exists(log_file)

            log_content = os_ops.read(
                log_file,
                binary=False,
            )

            expected_content_lines = [
                C_OUT_DATA,
                "sh -c 'echo BBBB'",
                "# BBBB",
                "",
            ]

            expected_content_s = "\n".join(expected_content_lines)

            assert log_content == expected_content_s
        finally:
            if log_file is not None:
                assert type(log_file) is str
                os_ops.remove_file(log_file)

        assert type(log_file) is str
        assert not os_ops.path_exists(log_file)
        return

    def test_execute_utility3__error__check_false(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        cmd = ["sh", "-c", "nonexistent_command"]

        r = execute_utility2(
            os_ops,
            cmd,
            verbose=True,
            ignore_errors=True,
        )

        assert type(r) is tuple
        assert len(r) == 3
        assert type(r[0]) is int
        assert type(r[1]) is str
        assert type(r[2]) is str

        assert r[0] == 127
        assert r[1] == ""
        assert "nonexistent_command" in r[2]
        assert "not found" in r[2]
        return

    def test_execute_utility3__error__check_true(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        cmd = ["sh", "-c", "nonexistent_command"]

        with pytest.raises(expected_exception=ExecUtilException) as x:
            execute_utility2(
                os_ops,
                cmd,
                ignore_errors=False,
            )

        assert type(x.value) is ExecUtilException
        assert type(x.value.exit_code) is int
        assert x.value.exit_code == 127

        assert type(x.value.message) is str
        assert type(x.value.out) is str
        assert type(x.value.error) is str

        assert x.value.message.startswith("Utility exited with non-zero code (127). Error:")
        assert "nonexistent_command" in x.value.message
        assert "not found" in x.value.message
        assert "nonexistent_command" in x.value.error
        assert "not found" in x.value.error
        return
