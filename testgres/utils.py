# coding: utf-8

from __future__ import division

import functools
import io
import os
import port_for
import six
import subprocess

from distutils.version import LooseVersion

from .config import TestgresConfig
from .exceptions import ExecUtilException

# rows returned by PG_CONFIG
_pg_config_data = {}

# ports used by nodes
bound_ports = set()


def reserve_port():
    """
    Generate a new port and add it to 'bound_ports'.
    """

    port = port_for.select_random(exclude_ports=bound_ports)
    bound_ports.add(port)

    return port


def release_port(port):
    """
    Free port provided by reserve_port().
    """

    bound_ports.remove(port)


def default_dbname():
    """
    Return default DB name.
    """

    return 'postgres'


def default_username():
    """
    Return default username (current user).
    """

    import getpass
    return getpass.getuser()


def generate_app_name():
    """
    Generate a new application name for node.
    """

    import uuid
    return 'testgres-{}'.format(str(uuid.uuid4()))


def execute_utility(args, logfile):
    """
    Execute utility (pg_ctl, pg_dump etc).

    Args:
        args: utility + arguments (list).
        logfile: path to file to store stdout and stderr.

    Returns:
        stdout of executed utility.
    """

    # run utility
    process = subprocess.Popen(
        args,    # util + params
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    # get result and decode it
    out, _ = process.communicate()
    out = '' if not out else out.decode('utf-8')

    # write new log entry if possible
    try:
        with io.open(logfile, 'a') as file_out:
            # write util's name and args
            file_out.write(u' '.join(args))

            # write output
            if out:
                file_out.write(u'\n')
                file_out.write(out)

            # finally, a separator
            file_out.write(u'\n')
    except IOError:
        pass

    # format exception, if needed
    error_code = process.returncode
    if error_code:
        error_text = (u"{} failed with exit code {}\n"
                      u"log:\n----\n{}\n").format(args[0], error_code, out)

        raise ExecUtilException(error_text, error_code)

    return out


def get_bin_path(filename):
    """
    Return absolute path to an executable using PG_BIN or PG_CONFIG.
    This function does nothing if 'filename' is already absolute.
    """

    # check if it's already absolute
    if os.path.isabs(filename):
        return filename

    # try PG_CONFIG
    pg_config = os.environ.get("PG_CONFIG")
    if pg_config:
        bindir = get_pg_config()["BINDIR"]
        return os.path.join(bindir, filename)

    # try PG_BIN
    pg_bin = os.environ.get("PG_BIN")
    if pg_bin:
        return os.path.join(pg_bin, filename)

    return filename


def get_pg_config():
    """
    Return output of pg_config (provided that it is installed).
    NOTE: this fuction caches the result by default (see TestgresConfig).
    """

    def cache_pg_config_data(cmd):
        # execute pg_config and get the output
        out = subprocess.check_output([cmd]).decode('utf-8')

        data = {}
        for line in out.splitlines():
            if line and '=' in line:
                key, _, value = line.partition('=')
                data[key.strip()] = value.strip()

        # cache data
        global _pg_config_data
        _pg_config_data = data

        return data

    # drop cache if asked to
    if not TestgresConfig.cache_pg_config:
        global _pg_config_data
        _pg_config_data = {}

    # return cached data
    if _pg_config_data:
        return _pg_config_data

    # try PG_CONFIG
    pg_config = os.environ.get("PG_CONFIG")
    if pg_config:
        return cache_pg_config_data(pg_config)

    # try PG_BIN
    pg_bin = os.environ.get("PG_BIN")
    if pg_bin:
        cmd = os.path.join(pg_bin, "pg_config")
        return cache_pg_config_data(cmd)

    # try plain name
    return cache_pg_config_data("pg_config")


def get_pg_version():
    """
    Return PostgreSQL version provided by postmaster.
    """

    # get raw version (e.g. postgres (PostgreSQL) 9.5.7)
    _params = [get_bin_path('postgres'), '--version']
    raw_ver = subprocess.check_output(_params).decode('utf-8')

    # cook version of PostgreSQL
    version = raw_ver.strip().split(' ')[-1] \
                     .partition('devel')[0] \
                     .partition('beta')[0] \
                     .partition('rc')[0]

    return version


def pg_version_ge(version):
    """
    Check if PostgreSQL is 'version' or newer.
    """

    cur_ver = LooseVersion(get_pg_version())
    min_ver = LooseVersion(version)

    return cur_ver >= min_ver


def file_tail(f, num_lines):
    """
    Get last N lines of a file.
    """

    assert num_lines > 0

    bufsize = 8192
    buffers = 1

    f.seek(0, os.SEEK_END)
    end_pos = f.tell()

    while True:
        offset = max(0, end_pos - bufsize * buffers)
        f.seek(offset, os.SEEK_SET)
        pos = f.tell()

        lines = f.readlines()
        cur_lines = len(lines)

        if cur_lines > num_lines or pos == 0:
            return lines[-num_lines:]

        buffers = int(buffers * max(2, num_lines / max(cur_lines, 1)))


def positional_args_hack(*special_cases):
    """
    Convert positional args described by
    'special_cases' into named args.

    Example:
        @positional_args_hack(['abc'], ['def', 'abc'])
        def some_api_func(...)

    This is useful for compatibility.
    """

    cases = dict()

    for case in special_cases:
        k = len(case)
        assert k not in six.iterkeys(cases), 'len must be unique'
        cases[k] = case

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            k = len(args)

            if k in six.iterkeys(cases):
                case = cases[k]

                for i in range(0, k):
                    arg_name = case[i]
                    arg_val = args[i]

                    # transform into named
                    kwargs[arg_name] = arg_val

                # get rid of them
                args = []

            return function(*args, **kwargs)

        return wrapper

    return decorator


def method_decorator(decorator):
    """
    Convert a function decorator into a method decorator.
    """

    def _dec(func):
        def _wrapper(self, *args, **kwargs):
            @decorator
            def bound_func(*args2, **kwargs2):
                return func.__get__(self, type(self))(*args2, **kwargs2)

            # 'bound_func' is a closure and can see 'self'
            return bound_func(*args, **kwargs)

        # preserve docs
        functools.update_wrapper(_wrapper, func)

        return _wrapper

    # preserve docs
    functools.update_wrapper(_dec, decorator)

    # change name for easier debugging
    _dec.__name__ = 'method_decorator({})'.format(decorator.__name__)

    return _dec
