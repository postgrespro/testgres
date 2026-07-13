from __future__ import annotations

from .. import internal_platform_utils as base
from ... import internal_utils

from testgres.operations.os_ops import OsOperations
from testgres.operations.exceptions import ExecUtilException

import re
import shlex
import typing
import time


class InternalPlatformUtils(base.InternalPlatformUtils):
    C_MAX_FIND_POSTMASTER_ATTEMPTS = 5
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

        failures: typing.List[Exception] = []

        postmaster_pid: typing.Optional[int] = None

        nAttempts = 0

        while True:
            nAttempts += 1

            try:
                postmaster_pid = __class__._FindPostmaster(
                    os_ops,
                    bin_dir,
                    data_dir,
                )
            except Exception as e:
                failures.append(e)

                log_msg = "FindPostmaster (bin_dir={!r}, data_dir={!r}) detects a problem. Exception {}:\n{}".format(
                    bin_dir,
                    data_dir,
                    type(e).__name__,
                    e,
                )
                internal_utils.send_log_debug(log_msg)

                if nAttempts < __class__.C_MAX_FIND_POSTMASTER_ATTEMPTS:
                    time.sleep(0.05)
                    continue

                __class__._find_postmaster__throw_error__fail(
                    bin_dir=bin_dir,
                    data_dir=data_dir,
                    failures=failures,
                )

            break

        if postmaster_pid is None:
            return InternalPlatformUtils.FindPostmasterResult.create_not_found()

        assert type(postmaster_pid) is int

        return InternalPlatformUtils.FindPostmasterResult.create_ok(postmaster_pid)

    # --------------------------------------------------------------------
    @staticmethod
    def _FindPostmaster(
        os_ops: OsOperations,
        bin_dir: str,
        data_dir: str
    ) -> typing.Optional[int]:
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

        regexp = r"^\s*[0-9]+\s+[0-9]+\s+" + pg_path_e + r"(\s+.*)?\s+\-[D]\s+" + data_dir_e + r"(\s+.*)?"

        cmd = [
            __class__.C_BASH_EXE,
            "-c",
            "ps -ewwo \"pid=,ppid=,args=\" | grep -E " + shlex.quote(regexp),
           ]

        exec_r = os_ops.exec_command(
            cmd=cmd,
            ignore_errors=True,
            verbose=True,
            exec_env=__class__.sm_exec_env,
        )

        assert type(exec_r) is tuple
        assert len(exec_r) == 3

        exit_status, output_b, error_b = exec_r

        assert type(exit_status) is int
        assert type(output_b) is bytes
        assert type(error_b) is bytes

        if exit_status == 1:
            return None

        output = output_b.decode("utf-8")
        error = error_b.decode("utf-8")

        assert type(output) is str
        assert type(error) is str

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
            # ACHTUNG!
            raise RuntimeError("Command returns 0 error code without output.")

        # parse result lines
        pid_to_ppid: __class__.T_PID_TO_PPID = {}

        for i_line in range(len(lines)):
            assert type(lines[i_line]) is str

            parts = lines[i_line].split()
            assert type(parts) is list

            if len(parts) < 2:
                __class__._find_postmaster__throw_error__bad_line_format(
                    lines,
                    i_line,
                    "no usefull data",
                )

            if not parts[0].isdigit():
                __class__._find_postmaster__throw_error__bad_line_format(
                    lines,
                    i_line,
                    "bad pid",
                )

            if not parts[1].isdigit():
                __class__._find_postmaster__throw_error__bad_line_format(
                    lines,
                    i_line,
                    "bad ppid",
                )

            pid = int(parts[0])
            ppid = int(parts[1])

            if pid not in pid_to_ppid:
                pid_to_ppid[pid] = ppid
                continue

            other_ppid = pid_to_ppid[pid]
            assert type(other_ppid) is int

            if ppid == other_ppid:
                log_msg = "FindPostmaster (data_dir={!r}) get pid ({}) with ppid ({}) more than one time.".format(
                    data_dir,
                    pid,
                    ppid,
                )
                internal_utils.send_log_debug(log_msg)
                continue

            # ACTUNG ppid is changed --> restart
            __class__._find_postmaster__throw_error__ppid_is_changed(
                pid,
                ppid,
                other_ppid,
                lines,
            )

        assert len(pid_to_ppid) <= len(lines)

        true_postmasters = [
            pid for pid, ppid in pid_to_ppid.items()
            if ppid not in pid_to_ppid
        ]

        if len(true_postmasters) == 0:
            __class__._find_postmaster__throw_error__cycle(
                pid_to_ppid,
            )

        if len(true_postmasters) == 1:
            true_pid = true_postmasters[0]

            if len(pid_to_ppid) > 1:
                msg = "Many processes like a postmaster for data dir [{}] are found ({}).".format(
                    data_dir,
                    len(true_postmasters),
                )

                msg += " List (ppid->pid): {}.".format(
                    __class__._make_text_from_pid_to_ppid(pid_to_ppid),
                )

                msg += " True postmaster PID is {}.".format(true_pid)
                internal_utils.send_log_debug(msg)

            return true_pid

        assert len(true_postmasters) > 1

        __class__._find_postmaster__throw_error__many_postmasters(
            true_postmasters,
            pid_to_ppid,
        )

    def ProcessIsZombi_soft_check(
        self,
        os_ops: OsOperations,
        pid: int,
    ) -> typing.Optional[bool]:
        assert isinstance(os_ops, OsOperations)
        assert type(pid) is int

        proc_stat_file = os_ops.build_path("/proc", str(pid), "stat")

        if not os_ops.path_exists(proc_stat_file):
            return False

        result: typing.Optional[bool] = None

        try:
            # Read one line from /proc/PID/stat
            stat_content = os_ops.read_binary(proc_stat_file, 0).decode("utf-8", errors="ignore")

            # We look for the closing parenthesis of the process name to ensure that
            # we start from it and not depend on spaces inside the parentheses!
            r_paren_idx = stat_content.rfind(")")

            if r_paren_idx == -1:
                pass
            elif len(stat_content) <= r_paren_idx + 2:
                pass
            else:
                # The status goes exactly one space after the closing bracket
                assert (r_paren_idx + 2) < len(stat_content)
                proc_status = stat_content[r_paren_idx + 2]
                result = proc_status == "Z"
        except Exception as e:
            # If the file disappeared right during reading, it means the process is completely erased
            if __class__._is_file_not_found_exception(e):
                result = False

        return result

    @staticmethod
    def _is_file_not_found_exception(e: Exception) -> bool:
        if isinstance(e, FileNotFoundError):
            return True

        if isinstance(e, ExecUtilException):
            if e.exit_code == 2:
                return True

        return False

    T_PID_TO_PPID = typing.Dict[int, int]

    @staticmethod
    def _make_text_from_pid_to_ppid(pid_to_ppid: T_PID_TO_PPID) -> str:
        result = ""
        sep = ""
        for pid, ppid in pid_to_ppid.items():
            result += sep + " {}->{}".format(ppid, pid)
            sep = ", "
            continue
        return result

    @staticmethod
    def _find_postmaster__throw_error__bad_line_format(
        lines: typing.List[str],
        i_line: int,
        hint: str,
    ) -> typing.NoReturn:
        assert type(lines) is list
        assert type(i_line) is int
        assert type(hint) is int

        error_lines: typing.List[str] = []
        error_lines.append(
            "Line {} has bad format. Hint: {}.".format(
                i_line + 1,
                hint,
            ),
        )
        error_lines.append(
            "Problem line is:"
        )
        error_lines.append(
            " " + repr(lines[i_line]),
        )
        error_lines.append(
            "All the lines is:",
        )
        for i in range(len(lines)):
            error_lines.append(
                " {}. {!r}".format(i + 1, lines[i]),
            )
            continue

        raise RuntimeError("\n".join(error_lines))

    @staticmethod
    def _find_postmaster__throw_error__ppid_is_changed(
        pid: int,
        ppid: int,
        other_ppid: int,
        lines: typing.List[str],
    ) -> typing.NoReturn:
        assert type(pid) is int
        assert type(ppid) is int
        assert type(other_ppid) is int
        assert type(lines) is list

        error_lines: typing.List[str] = []
        error_lines.append(
            "Parent of process ({}) is changed from {} to {}.".format(
                pid,
                other_ppid,
                ppid,
            ),
        )
        error_lines.append(
            "All the lines is:",
        )
        for i in range(len(lines)):
            error_lines.append(
                " {}. {!r}".format(i + 1, lines[i]),
            )
            continue

        raise RuntimeError("\n".join(error_lines))

    @staticmethod
    def _find_postmaster__throw_error__cycle(
        pid_to_ppid: T_PID_TO_PPID,
    ) -> typing.NoReturn:
        msg = "Cycle in processes postgres process tree. "

        msg += " List (ppid->pid): {},".format(
            __class__._make_text_from_pid_to_ppid(pid_to_ppid),
        )

        msg += " List size is {}.".format(
            len(pid_to_ppid),
        )
        raise RuntimeError(msg)

    @staticmethod
    def _find_postmaster__throw_error__many_postmasters(
        postmaster_pids: typing.List[int],
        pid_to_ppid: T_PID_TO_PPID,
    ) -> typing.NoReturn:
        msg = "Many processes like a postmaster are found ({}): {}.".format(
            len(postmaster_pids),
            ", ".join(map(str, postmaster_pids)),
        )

        msg += " Trees (ppid->pid): {}.".format(
            __class__._make_text_from_pid_to_ppid(pid_to_ppid),
        )

        msg += " Total process count is {}.".format(len(pid_to_ppid))
        raise RuntimeError(msg)

    @staticmethod
    def _find_postmaster__throw_error__fail(
        bin_dir: str,
        data_dir: str,
        failures: typing.List[Exception],
    ) -> typing.NoReturn:
        assert type(bin_dir) is str
        assert type(data_dir) is str
        assert type(failures) is list

        err_msg = "FindPostmater did {} attempts and has not gotten a stable result. List of failures:\n"

        n = 0
        sep = ""
        for e in failures:
            assert isinstance(e, Exception)

            n += 1
            err_msg += sep
            err_msg += "Failure #{}. Exception ({}):\n{}".format(
                n,
                type(e).__name__,
                str(e),
            )
            sep = "\n"
            continue

        raise RuntimeError(err_msg)
