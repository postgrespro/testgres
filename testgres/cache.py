# coding: utf-8

import io
import os

from shutil import copytree
from six import raise_from

from .config import testgres_config

from .consts import XLOG_CONTROL_FILE

from .defaults import generate_system_id

from .exceptions import \
    InitNodeException, \
    ExecUtilException

from .utils import \
    get_bin_path, \
    execute_utility


def cached_initdb(data_dir, logfile=None, params=None):
    """
    Perform initdb or use cached node files.
    """
    def call_initdb(initdb_dir, log=None):
        try:
            _params = [get_bin_path("initdb"), "-D", initdb_dir, "-N"]
            execute_utility(_params + (params or []), log)
        except ExecUtilException as e:
            raise_from(InitNodeException("Failed to run initdb"), e)

    if params or not testgres_config.cache_initdb:
        call_initdb(data_dir, logfile)
    else:
        # Fetch cached initdb dir
        cached_data_dir = testgres_config.cached_initdb_dir

        # Initialize cached initdb
        if not os.path.exists(cached_data_dir) or \
           not os.listdir(cached_data_dir):
            call_initdb(cached_data_dir)

        try:
            # Copy cached initdb to current data dir
            copytree(cached_data_dir, data_dir)

            # Assign this node a unique system id if asked to
            if testgres_config.cached_initdb_unique:
                # XXX: write new unique system id to control file
                # Some users might rely upon unique system ids, but
                # our initdb caching mechanism breaks this contract.
                pg_control = os.path.join(data_dir, XLOG_CONTROL_FILE)
                with io.open(pg_control, "r+b") as f:
                    f.write(generate_system_id())    # overwrite id

                # XXX: build new WAL segment with our system id
                _params = [get_bin_path("pg_resetwal"), "-D", data_dir, "-f"]
                execute_utility(_params, logfile)

        except ExecUtilException as e:
            msg = "Failed to reset WAL for system id"
            raise_from(InitNodeException(msg), e)

        except Exception as e:
            raise_from(InitNodeException("Failed to spawn a node"), e)
