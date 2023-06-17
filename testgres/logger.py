# coding: utf-8

import logging
import select
import threading
import time

# create logger
log = logging.getLogger('Testgres')

if not log.handlers:
    log.setLevel(logging.WARN)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARN)
    # create formatter
    formatter = logging.Formatter('\n%(asctime)s - %(name)s[%(levelname)s]: %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    log.addHandler(ch)


class TestgresLogger(threading.Thread):
    """
    Helper class to implement reading from log files.
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
                sleep_time = 0.1
                new_lines = False

                # do we have new lines?
                if fd in select.select([fd], [], [], 0)[0]:
                    for line in fd.readlines():
                        line = line.strip()
                        if line:
                            new_lines = True
                            extra = {'node': self._node_name}
                            self._logger.info(line, extra=extra)

                if not new_lines:
                    time.sleep(sleep_time)

            # don't forget to clear event
            self._stop_event.clear()

    def stop(self, wait=True):
        self._stop_event.set()

        if wait:
            self.join()
