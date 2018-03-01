# coding: utf-8

import atexit
import copy
import tempfile

from contextlib import contextmanager
from shutil import rmtree
from tempfile import mkdtemp

from .consts import TMP_CACHE


class GlobalConfig(object):
    """
    Global config (override default settings).

    Attributes:
        cache_initdb:           shall we use cached initdb instance?
        cached_initdb_dir:      shall we create a temp dir for cached initdb?
        cached_initdb_unique:   shall we assign new node a unique system id?

        cache_pg_config:        shall we cache pg_config results?

        temp_dir:               base temp dir for nodes with default 'base_dir'.

        use_python_logging:     use python logging configuration for all nodes.
        error_log_lines:        N of log lines to be shown in exception (0=inf).

        node_cleanup_full:          shall we remove EVERYTHING (including logs)?
        node_cleanup_on_good_exit:  remove base_dir on nominal __exit__().
        node_cleanup_on_bad_exit:   remove base_dir on __exit__() via exception.

    NOTE: attributes must not be callable or begin with __.
    """

    cache_initdb = True
    _cached_initdb_dir = None
    cached_initdb_unique = False

    cache_pg_config = True

    use_python_logging = False
    error_log_lines = 20

    node_cleanup_full = True
    node_cleanup_on_good_exit = True
    node_cleanup_on_bad_exit = False

    @property
    def cached_initdb_dir(self):
        return self._cached_initdb_dir

    @cached_initdb_dir.setter
    def cached_initdb_dir(self, value):
        self._cached_initdb_dir = value

        if value:
            cached_initdb_dirs.add(value)

    @property
    def temp_dir(self):
        return tempfile.tempdir

    @temp_dir.setter
    def temp_dir(self, value):
        tempfile.tempdir = value

    def __init__(self, **options):
        self.update(options)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setattr__(self, name, value):
        if name not in self.keys():
            raise TypeError('Unknown option {}'.format(name))

        super(GlobalConfig, self).__setattr__(name, value)

    def keys(self):
        keys = []

        for key in dir(GlobalConfig):
            if not key.startswith('__') and not callable(self[key]):
                keys.append(key)

        return keys

    def items(self):
        return ((key, self[key]) for key in self.keys())

    def update(self, config):
        for key, value in config.items():
            self[key] = value

        return self

    def copy(self):
        return copy.copy(self)


# cached dirs to be removed
cached_initdb_dirs = set()

# default config object
testgres_config = GlobalConfig()

# NOTE: for compatibility
TestgresConfig = testgres_config

# stack of GlobalConfigs
config_stack = []


@atexit.register
def _rm_cached_initdb_dirs():
    for d in cached_initdb_dirs:
        rmtree(d, ignore_errors=True)


def push_config(**options):
    """
    Permanently set custom GlobalConfig options
    and put previous settings on top of stack.
    """

    # push current config to stack
    config_stack.append(testgres_config.copy())

    return testgres_config.update(options)


def pop_config():
    """
    Set previous GlobalConfig options from stack.
    """

    if len(config_stack) == 0:
        raise IndexError('Reached initial config')

    # restore popped config
    return testgres_config.update(config_stack.pop())


@contextmanager
def scoped_config(**options):
    """
    Temporarily set custom GlobalConfig options for this context.

    Example:
        >>> with scoped_config(cache_initdb=False):
        ...     with get_new_node().init().start() as node:
        ...         print(node.execute('select 1'))
    """

    try:
        # set a new config with options
        config = push_config(**options)

        # return it
        yield config
    finally:
        # restore previous config
        pop_config()


def configure_testgres(**options):
    """
    Adjust current global options.
    Look at GlobalConfig to learn what can be set.
    """

    testgres_config.update(options)


# NOTE: assign initial cached dir for initdb
testgres_config.cached_initdb_dir = mkdtemp(prefix=TMP_CACHE)
