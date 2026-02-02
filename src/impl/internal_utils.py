import logging


def send_log(level: int, msg: str) -> None:
    assert type(level) == int  # noqa: E721
    assert type(msg) == str  # noqa: E721

    return logging.log(level, "[testgres] " + msg)


def send_log_info(msg: str) -> None:
    assert type(msg) == str  # noqa: E721

    return send_log(logging.INFO, msg)


def send_log_debug(msg: str) -> None:
    assert type(msg) == str  # noqa: E721

    return send_log(logging.DEBUG, msg)
