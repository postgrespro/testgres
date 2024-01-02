# coding: utf-8

import os

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

from .operations.local_ops import LocalOperations
from .operations.os_ops import OsOperations


def cached_initdb(data_dir, logfile=None, params=None, os_ops: OsOperations = LocalOperations(), bin_path=None, cached=True):
    """
    Perform initdb or use cached node files.
    """

    def call_initdb(initdb_dir, log=logfile):
        try:
            initdb_path = os.path.join(bin_path, 'initdb') if bin_path else get_bin_path("initdb")
            _params = [initdb_path, "-D", initdb_dir, "-N"]
            execute_utility(_params + (params or []), log)
        except ExecUtilException as e:
            raise_from(InitNodeException("Failed to run initdb"), e)

    if params or not testgres_config.cache_initdb or not cached:
        call_initdb(data_dir, logfile)
    else:
        # Fetch cached initdb dir
        cached_data_dir = testgres_config.cached_initdb_dir

        # Initialize cached initdb

        if not os_ops.path_exists(cached_data_dir) or \
                not os_ops.listdir(cached_data_dir):
            call_initdb(cached_data_dir)

        try:
            # Copy cached initdb to current data dir
            os_ops.copytree(cached_data_dir, data_dir)

            # Assign this node a unique system id if asked to
            if testgres_config.cached_initdb_unique:
                # XXX: write new unique system id to control file
                # Some users might rely upon unique system ids, but
                # our initdb caching mechanism breaks this contract.
                pg_control = os.path.join(data_dir, XLOG_CONTROL_FILE)
                system_id = generate_system_id()
                cur_pg_control = os_ops.read(pg_control, binary=True)
                new_pg_control = system_id + cur_pg_control[len(system_id):]
                os_ops.write(pg_control, new_pg_control, truncate=True, binary=True, read_and_write=True)

                # XXX: build new WAL segment with our system id
                _params = [get_bin_path("pg_resetwal"), "-D", data_dir, "-f"]
                execute_utility(_params, logfile)

        except ExecUtilException as e:
            msg = "Failed to reset WAL for system id"
            raise_from(InitNodeException(msg), e)

        except Exception as e:
            raise_from(InitNodeException("Failed to spawn a node"), e)
