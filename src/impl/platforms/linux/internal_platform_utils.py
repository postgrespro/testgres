from __future__ import annotations

from .. import internal_platform_utils as base
from ... import internal_utils

from testgres.operations.os_ops import OsOperations
from testgres.operations.exceptions import ExecUtilException

import re
import shlex


class InternalPlatformUtils(base.InternalPlatformUtils):
    def FindPostmaster(
        self,
        os_ops: OsOperations,
        bin_dir: str,
        data_dir: str
    ) -> InternalPlatformUtils.FindPostmasterResult:
        assert isinstance(os_ops, OsOperations)
        assert type(bin_dir) == str  # noqa: E721
        assert type(data_dir) == str  # noqa: E721

        pg_path_e = re.escape(os_ops.build_path(bin_dir, "postgres"))
        data_dir_e = re.escape(data_dir)

        assert type(pg_path_e) == str  # noqa: E721
        assert type(data_dir_e) == str  # noqa: E721

        exec_env = {
            "LANG": "en_US.UTF-8",
            "LC_ALL": "en_US.UTF-8",
        }

        regexp = r"^\s*[0-9]+\s+" + pg_path_e + r"(\s+.*)?\s+\-[D]\s+" + data_dir_e + r"(\s+.*)?"

        cmd = [
            "/bin/bash",
            "-c",
            "ps -ewwo \"pid=,args=\" | grep -E " + shlex.quote(regexp),
        ]

        exit_status, output_b, error_b = os_ops.exec_command(
            cmd=cmd,
            ignore_errors=True,
            verbose=True,
            exec_env=exec_env,
        )

        assert type(output_b) == bytes  # noqa: E721
        assert type(error_b) == bytes  # noqa: E721

        output = output_b.decode("utf-8")
        error = error_b.decode("utf-8")

        assert type(output) == str  # noqa: E721
        assert type(error) == str  # noqa: E721

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
        assert type(lines) == list  # noqa: E721

        if len(lines) == 0:
            return __class__.FindPostmasterResult.create_not_found()

        if len(lines) > 1:
            msgs = []
            msgs.append("Many processes like a postmaster are found: {}.".format(len(lines)))

            for i in range(len(lines)):
                assert type(lines[i]) == str  # noqa: E721
                lines.append("[{}] '{}'".format(i, lines[i]))
                continue

            internal_utils.send_log_debug("\n".join(lines))
            return __class__.FindPostmasterResult.create_many_processes()

        def is_space_or_tab(ch) -> bool:
            assert type(ch) == str  # noqa: E721
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
        assert type(pid) == int  # noqa: E721

        return __class__.FindPostmasterResult.create_ok(pid)
