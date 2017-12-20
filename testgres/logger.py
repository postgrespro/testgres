# coding: utf-8

import logging
import select
import threading
import time


class TestgresLogger(threading.Thread):
    """
    Helper class to implement reading from postgresql.log
    """

    def __init__(self, node_name, log_file_name):
        threading.Thread.__init__(self)

        self._node_name = node_name
        self._log_file_name = log_file_name
        self._stop_event = threading.Event()
        self._logger = logging.getLogger(node_name)
        self._logger.setLevel(logging.INFO)

    def run(self):
        # open log file for reading
        with open(self._log_file_name, 'r') as fd:
            # work until we're asked to stop
            while not self._stop_event.is_set():
                # do we have new lines?
                if fd in select.select([fd], [], [], 0)[0]:
                    for line in fd.readlines():
                        line = line.strip()
                        if line:
                            extra = {'node': self._node_name}
                            self._logger.info(line, extra=extra)
                else:
                    time.sleep(0.1)

            # don't forget to clear event
            self._stop_event.clear()

    def stop(self, wait=True):
        self._stop_event.set()

        if wait:
            self.join()
