from testgres.operations.os_ops import OsOperations

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
