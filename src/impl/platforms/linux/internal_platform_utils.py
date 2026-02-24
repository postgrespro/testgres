from __future__ import annotations

from .. import internal_platform_utils as base
from ... import internal_utils

from testgres.operations.os_ops import OsOperations
from testgres.operations.exceptions import ExecUtilException

import re
import shlex


class InternalPlatformUtils(base.InternalPlatformUtils):
    C_BASH_EXE = "/bin/bash"

    sm_exec_env = {
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
    }

    # --------------------------------------------------------------------
    def FindPostmaster(
        self,
        os_ops: OsOperations,
        bin_dir: str,
        data_dir: str
    ) -> InternalPlatformUtils.FindPostmasterResult:
        assert isinstance(os_ops, OsOperations)
        assert type(bin_dir) is str
        assert type(data_dir) is str
        assert type(__class__.C_BASH_EXE) is str
        assert type(__class__.sm_exec_env) is dict
        assert len(__class__.C_BASH_EXE) > 0
        assert len(bin_dir) > 0
        assert len(data_dir) > 0

        pg_path_e = re.escape(os_ops.build_path(bin_dir, "postgres"))
        data_dir_e = re.escape(data_dir)

        assert type(pg_path_e) is str
        assert type(data_dir_e) is str

        regexp = r"^\s*[0-9]+\s+" + pg_path_e + r"(\s+.*)?\s+\-[D]\s+" + data_dir_e + r"(\s+.*)?"

        cmd = [
            __class__.C_BASH_EXE,
            "-c",
            "ps -ewwo \"pid=,args=\" | grep -E " + shlex.quote(regexp),
        ]

        exit_status, output_b, error_b = os_ops.exec_command(
            cmd=cmd,
            ignore_errors=True,
            verbose=True,
            exec_env=__class__.sm_exec_env,
        )

        assert type(output_b) is bytes
        assert type(error_b) is bytes

        output = output_b.decode("utf-8")
        error = error_b.decode("utf-8")

        assert type(output) is str
        assert type(error) is str

        if exit_status == 1:
            return __class__.FindPostmasterResult.create_not_found()

        if exit_status != 0:
            errMsg = f"test command returned an unexpected exit code: {exit_status}"
            raise ExecUtilException(
                message=errMsg,
                command=cmd,
                exit_code=exit_status,
                out=output,
                error=error,
            )

        lines = output.splitlines()
        assert type(lines) is list

        if len(lines) == 0:
            return __class__.FindPostmasterResult.create_not_found()

        if len(lines) > 1:
            msgs = []
            msgs.append("Many processes like a postmaster are found: {}.".format(len(lines)))

            for i in range(len(lines)):
                assert type(lines[i]) is str
                lines.append("[{}] '{}'".format(i, lines[i]))
                continue

            internal_utils.send_log_debug("\n".join(lines))
            return __class__.FindPostmasterResult.create_many_processes()

        def is_space_or_tab(ch) -> bool:
            assert type(ch) is str
            return ch == " " or ch == "\t"

        line = lines[0]
        start = 0
        while start < len(line) and is_space_or_tab(line[start]):
            start += 1

        pos = start
        while pos < len(line) and line[pos].isnumeric():
            pos += 1

        if pos == start:
            return __class__.FindPostmasterResult.create_has_problems()

        if pos != len(line) and not line[pos].isspace():
            return __class__.FindPostmasterResult.create_has_problems()

        pid = int(line[start:pos])
        assert type(pid) is int

        return __class__.FindPostmasterResult.create_ok(pid)
