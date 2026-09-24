from __future__ import annotations

from . import internal_utils
from . platforms.internal_platform_utils_provider import InternalPlaformUtilsProvider

from .. import consts
from ..node_state import PostgresNodeState
from ..enums import NodeStatus
from ..raise_error import RaiseError
from ..exceptions import ExecUtilException

from testgres.operations.os_ops import OsOperations

import typing
import time


class InternalNodeUtils:
    T_PLATFORM_UTILS = InternalPlaformUtilsProvider.T_PLATFORM_UTILS

    @staticmethod
    def get_pg_node_state(
        os_ops: OsOperations,
        bin_dir: str,
        data_dir: str,
        utils_log_file: typing.Optional[str],
    ) -> PostgresNodeState:
        assert isinstance(os_ops, OsOperations)
        assert type(bin_dir) is str
        assert type(data_dir) is str
        assert utils_log_file is None or type(utils_log_file) is str

        C_MAX_ATTEMPTS = 3
        C_SLEEP_TIME1 = 1
        C_SLEEP_TIME_MULT = 2

        pg_ctl_params = [
            os_ops.build_path(bin_dir, consts.BINARY_NAME__PG_CTL),
            "-D",
            data_dir,
            "status",
        ]

        attempt = 0
        sleep_time = C_SLEEP_TIME1

        platform_utils_provider = InternalPlaformUtilsProvider(
            os_ops,
        )

        while True:
            assert type(attempt) is int
            assert attempt >= 0
            assert attempt < C_MAX_ATTEMPTS

            attempt += 1

            if attempt > 1:
                internal_utils.send_log_debug("Sleep {} second(s) before an attempt #{}".format(
                    sleep_time,
                    attempt,
                ))
                time.sleep(sleep_time)
                sleep_time = sleep_time * C_SLEEP_TIME_MULT

            exec_r = internal_utils.execute_utility3(
                os_ops,
                pg_ctl_params,
                utils_log_file,
                check=False,
            )

            status_code = exec_r.returncode
            out = exec_r.stdout
            error = exec_r.stderr

            assert type(status_code) is int
            assert type(out) is str
            assert type(error) is str

            # -----------------
            if status_code == consts.PG_CTL__STATUS__NODE_IS_STOPPED:
                return PostgresNodeState(NodeStatus.Stopped, None)

            # -----------------
            if status_code == consts.PG_CTL__STATUS__BAD_DATADIR:
                return PostgresNodeState(NodeStatus.Uninitialized, None)

            # -----------------
            if status_code == consts.PG_CTL__STATUS__OK:
                pid = __class__._parse_pid(
                    out,
                    pg_ctl_params,
                )
                assert type(pid) is int
                assert pid != 0

                # ----------------- detect zombie
                if platform_utils_provider.get().ProcessIsZombie_soft_check(os_ops, pid) is True:
                    internal_utils.send_log_debug("Postmaster process {} is a zombie.".format(
                        pid,
                    ))
                    return PostgresNodeState(NodeStatus.Zombie, pid)

                # -----------------
                return PostgresNodeState(NodeStatus.Running, pid)

            assert status_code != consts.PG_CTL__STATUS__OK

            errMsg = "Getting of a node status [data_dir is {0}] failed.".format(
                data_dir,
            )

            e1 = ExecUtilException(
                message=errMsg,
                command=pg_ctl_params,
                exit_code=status_code,
                out=out,
                error=error,
            )

            if status_code == consts.PG_CTL__STATUS__FAILED:
                internal_utils.send_log_debug(
                    "pg_ctl fails with an error: {}".format(
                        exec_r.stderr,
                    ))

                try:
                    find_postmaster_r = platform_utils_provider.get().FindPostmaster(
                        os_ops,
                        bin_dir,
                        data_dir,
                    )
                except Exception as e2:
                    raise e2 from e1

                assert type(find_postmaster_r) is __class__.T_PLATFORM_UTILS.FindPostmasterResult

                if find_postmaster_r.code == __class__.T_PLATFORM_UTILS.FindPostmasterResultCode.ok:
                    # Postmaster is alive. Let's wait a few seconds and check its status again.
                    internal_utils.send_log_debug(
                        "Postmaster is found and has PID {}.".format(
                            find_postmaster_r.pid,
                        ))

                    if attempt < C_MAX_ATTEMPTS:
                        continue

            raise e1

    @staticmethod
    def _parse_pid(
        out: str,
        pg_ctl_params,
    ) -> int:
        assert type(out) is str

        if out == "":
            RaiseError.pg_ctl_returns_an_empty_string(
                pg_ctl_params,
            )

        C_PID_PREFIX = "(PID: "

        i = out.find(C_PID_PREFIX)

        if i == -1:
            RaiseError.pg_ctl_returns_an_unexpected_string(
                out,
                pg_ctl_params,
            )

        assert i > 0
        assert i < len(out)
        assert len(C_PID_PREFIX) <= len(out)
        assert i <= len(out) - len(C_PID_PREFIX)

        i += len(C_PID_PREFIX)
        start_pid_s = i

        while True:
            if i == len(out):
                RaiseError.pg_ctl_returns_an_unexpected_string(
                    out,
                    pg_ctl_params,
                )

            ch = out[i]

            if ch == ")":
                break

            if ch.isdigit():
                i += 1
                continue

            RaiseError.pg_ctl_returns_an_unexpected_string(
                out,
                pg_ctl_params,
            )
            assert False

        if i == start_pid_s:
            RaiseError.pg_ctl_returns_an_unexpected_string(
                out,
                pg_ctl_params,
            )

        # TODO: Let's verify a length of pid string.

        pid = int(out[start_pid_s:i])

        if pid == 0:
            RaiseError.pg_ctl_returns_a_zero_pid(
                out,
                pg_ctl_params,
            )

        assert pid != 0

        return pid
