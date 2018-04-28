# coding: utf-8

import atexit
import os
import shutil
import tempfile

from six import raise_from

from .config import TestgresConfig

from .exceptions import \
    InitNodeException, \
    ExecUtilException

from .utils import \
    get_bin_path, \
    execute_utility


def cached_initdb(data_dir, initdb_logfile, initdb_params=[]):
    """
    Perform initdb or use cached node files.
    """

    def call_initdb(initdb_dir):
        try:
            _params = [get_bin_path("initdb"), "-D", initdb_dir, "-N"]
            execute_utility(_params + initdb_params, initdb_logfile)
        except ExecUtilException as e:
            raise_from(InitNodeException("Failed to run initdb"), e)

    def rm_cached_data_dir(cached_data_dir):
        shutil.rmtree(cached_data_dir, ignore_errors=True)

    # Call initdb if we have custom params or shouldn't cache it
    if initdb_params or not TestgresConfig.cache_initdb:
        call_initdb(data_dir)
    else:
        # Set default temp dir for cached initdb
        if TestgresConfig.cached_initdb_dir is None:

            # Create default temp dir
            TestgresConfig.cached_initdb_dir = tempfile.mkdtemp()

            # Schedule cleanup
            atexit.register(rm_cached_data_dir,
                            TestgresConfig.cached_initdb_dir)

        # Fetch cached initdb dir
        cached_data_dir = TestgresConfig.cached_initdb_dir

        # Initialize cached initdb
        if not os.listdir(cached_data_dir):
            call_initdb(cached_data_dir)

        try:
            # Copy cached initdb to current data dir
            shutil.copytree(cached_data_dir, data_dir)
        except Exception as e:
            raise_from(InitNodeException("Failed to copy files"), e)
