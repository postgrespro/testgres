# coding: utf-8

import os
import shutil

from six import raise_from

from .config import testgres_config

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
            shutil.copytree(cached_data_dir, data_dir)
        except Exception as e:
            raise_from(InitNodeException("Failed to spawn a node"), e)
