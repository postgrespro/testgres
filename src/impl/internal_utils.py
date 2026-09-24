from testgres.operations.os_ops import OsOperations
from testgres.operations.os_ops import OsCommandResult
from testgres.operations.types import T_OS_CMD
from testgres.operations.types import T_OS_EXEC_ENV
from testgres.operations.helpers import Helpers as OsHelpers

from ..exceptions import ExecUtilException

import logging
import typing


def send_log(level: int, msg: str) -> None:
    assert type(level) is int
    assert type(msg) is str

    return logging.log(level, "[testgres] " + msg)


def send_log_info(msg: str) -> None:
    assert type(msg) is str

    return send_log(logging.INFO, msg)


def send_log_debug(msg: str) -> None:
    assert type(msg) is str

    return send_log(logging.DEBUG, msg)


def read_line_to_pos__bin(
    os_ops: OsOperations,
    filename: str,
    position: int,
) -> bytes:
    assert type(filename) is str
    assert type(position) is int
    assert len(filename) > 0
    assert position >= 0
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)

    if position == 0:
        return b''

    assert position > 0

    read_position = position
    result_blocks: typing.List[bytes] = []

    C_BACK_READ_BLOCK_SIZE = 4096

    while read_position > 0:
        if read_position < C_BACK_READ_BLOCK_SIZE:
            block_sz = read_position
        else:
            block_sz = C_BACK_READ_BLOCK_SIZE

        assert block_sz > 0
        assert block_sz <= C_BACK_READ_BLOCK_SIZE

        read_position -= block_sz

        assert read_position < position
        assert read_position >= 0

        block = os_ops.read_binary(filename, read_position, block_sz)

        assert type(block) is bytes

        if len(block) != block_sz:
            err_msg = "[BUG CHECK] Readed block has bad size ({}). Expected size is ({}). File name {}.".format(
                len(block),
                block_sz,
                filename,
            )
            raise RuntimeError(err_msg)

        assert len(block) == block_sz

        x = block.rfind(b"\n", 0, block_sz)

        if x == -1:
            result_blocks.append(block)
            continue

        if x == block_sz - 1:
            break

        block = block[x + 1:]
        result_blocks.append(block)
        break

    result = b''.join(reversed(result_blocks))
    assert type(result) is bytes
    assert len(result) <= (position - read_position)
    return result


def execute_utility3(
    os_ops: OsOperations,
    args: T_OS_CMD,
    logfile: typing.Optional[str] = None,
    check: bool = True,
    exec_env: typing.Optional[T_OS_EXEC_ENV] = None,
) -> OsCommandResult:
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)
    assert type(check) is bool
    assert exec_env is None or type(exec_env) is dict

    exec_r = os_ops.run(
        args,
        check=check,
        encoding=OsHelpers.GetDefaultEncoding(),
        exec_env=exec_env,
    )

    assert type(exec_r) is OsCommandResult

    # write new log entry if possible
    if logfile:
        try:
            log_lines = [
                os_ops.join_command_arguments(args),
            ]

            if exec_r.stdout is None:
                log_lines.append("# #NONE#")
            else:
                # comment-out lines
                assert type(exec_r.stdout) is str
                log_lines += ['# ' + line for line in exec_r.stdout.splitlines()]

            log_lines.append("")

            os_ops.write(
                filename=logfile,
                data="\n".join(log_lines),
                truncate=False,
            )
        except IOError:
            raise ExecUtilException(
                "Problem with writing to logfile `{}` during run command `{}`".format(
                    logfile,
                    args,
                ))

    return exec_r
