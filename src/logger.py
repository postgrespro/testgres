# coding: utf-8

from .config import testgres_config as tconf
from .exceptions import ExecUtilException
from .impl.file_line_reader import FileLineReader

from testgres.operations.os_ops import OsOperations

import logging
import threading
import time
import typing


class TestgresLogger(threading.Thread):
    _C_SLEEP_MIN = 0.01
    _C_SLEEP_MAX = 60

    """
    Helper class to implement reading from log files.
    """
    def __init__(
        self,
        node_name: str,
        log_file_name: str,
        log_file_encoding: str = "utf-8",
        os_ops: typing.Optional[OsOperations] = None,
    ):
        assert type(node_name) is str
        assert type(log_file_name) is str
        assert type(log_file_encoding) is str
        assert os_ops is None or isinstance(os_ops, OsOperations)

        threading.Thread.__init__(self)

        if os_ops is None:
            os_ops = tconf.os_ops

        assert os_ops is None or isinstance(os_ops, OsOperations)

        self._os_ops = os_ops
        self._node_name = node_name
        self._log_file_name = log_file_name
        self._log_file_encoding = log_file_encoding
        self._stop_event = threading.Event()
        self._logger = logging.getLogger(node_name)
        self._logger.setLevel(logging.INFO)
        return

    def run(self):
        # open log file for reading
        file_line_reader = FileLineReader(
            self._os_ops,
            self._log_file_name,
            self._log_file_encoding,
        )

        sleep_time = __class__._C_SLEEP_MIN

        try:
            # work until we're asked to stop
            while not self._stop_event.is_set():
                line = None

                while True:
                    try:
                        line = file_line_reader.read_line()  # raise
                    except Exception as e:
                        if __class__._is_file_not_found_exception(e):
                            if not self._os_ops.path_exists(self._log_file_name):
                                break
                        raise
                    break

                if line is None:
                    time.sleep(sleep_time)
                    sleep_time = min(__class__._C_SLEEP_MAX, 2 * sleep_time)
                    continue

                assert type(line) is str

                sleep_time = __class__._C_SLEEP_MIN

                # do we have new lines?
                line = line.strip()

                extra = {'node': self._node_name}
                self._logger.info(line, extra=extra)
                continue
        except Exception as e:
            self._logger.error(e)
            raise
        finally:
            # don't forget to clear event
            # [2026-07-10] legacy cargo cult, thread is single-use only.
            # self._stop_event.clear()
            pass
        return

    def stop(self, wait=True):
        self._stop_event.set()

        if wait:
            self.join()
        return

    @staticmethod
    def _is_file_not_found_exception(e: Exception) -> bool:
        if isinstance(e, FileNotFoundError):
            return True

        if isinstance(e, ExecUtilException):
            if e.exit_code == 2:
                return True

        return False
