from __future__ import annotations

from tests.helpers.global_data import OsOpsDescrs
from tests.helpers.global_data import OsOperations
from tests.helpers.run_conditions import RunConditions

from src.exceptions import ExecUtilException

import pytest
import subprocess
import logging
import typing
import io


class TestSet001__Reseach:
    def test_000__zombie_file_via_python__linux(
        self,
    ):
        os_ops = OsOpsDescrs.sm_local_os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_darwin()
        RunConditions.skip_if_windows()

        with pytest.raises(expected_exception=FileNotFoundError):
            open("/proc/111892/stat")

        return

    def test_001__zombie_file_via_local_os_ops__linux(
        self,
    ):
        os_ops = OsOpsDescrs.sm_local_os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_darwin()
        RunConditions.skip_if_windows()

        with pytest.raises(expected_exception=FileNotFoundError):
            os_ops.read_binary("/proc/111892/stat", 0)

        return

    def test_002__zombie_file_via_remote_os_ops__linux(
        self,
    ):
        os_ops = OsOpsDescrs.sm_remote_os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_darwin()
        RunConditions.skip_if_windows()

        with pytest.raises(expected_exception=ExecUtilException) as x:
            os_ops.read_binary("/proc/111892/stat", 0)

        assert type(x.value) is ExecUtilException
        assert x.value.exit_code == 1
        return

    def test_003__process_lookup_error_race_condition__linux(
        self,
    ):
        RunConditions.skip_if_darwin()
        RunConditions.skip_if_windows()

        proc: typing.Optional[subprocess.Popen] = None
        file: typing.Optional[io.IOBase] = None

        try:
            # 1: Run a long-running background process
            proc = subprocess.Popen(
                ["sleep", "120"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            pid = proc.pid

            logging.info("Process is created. PID={}.".format(
                pid,
            ))

            proc_stat_file = f"/proc/{pid}/stat"

            # 2. Successfully open the descriptor file while the process is still alive
            logging.info("Open file [{}].".format(
                proc_stat_file,
            ))

            file = open(proc_stat_file, "rb")

            logging.info("File [{}] is open.".format(
                proc_stat_file,
            ))

            # 3. Hard kill the process (SIGKILL) and wait for the Linux kernel to clean up its structures
            logging.info("Kill process.")
            proc.kill()
            logging.info("Wait process.")
            proc.wait()

            # 4. We try to read from an already open file.
            # We expect the Linux kernel to return ESRCH (Errno 3), and Python to throw a ProcessLookupError
            logging.info("Try to read stat file.")
            with pytest.raises(expected_exception=ProcessLookupError) as x:
                file.read()

            logging.info("OK. Exception {} is catched.".format(
                type(x.value).__name__,
            ))

            assert type(x.value) is ProcessLookupError
        finally:
            if file:
                file.close()

            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait()
        return
