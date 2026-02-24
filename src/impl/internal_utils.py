import logging


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
