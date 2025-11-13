# coding: utf-8

from __future__ import division
from __future__ import print_function

import os

import sys

from contextlib import contextmanager
from packaging.version import Version, InvalidVersion
import re

from six import iteritems

from .exceptions import ExecUtilException
from .config import testgres_config as tconf
from testgres.operations.os_ops import OsOperations
from testgres.operations.remote_ops import RemoteOperations
from testgres.operations.local_ops import LocalOperations
from testgres.operations.helpers import Helpers as OsHelpers

from .impl.port_manager__generic import PortManager__Generic

# rows returned by PG_CONFIG
_pg_config_data = {}

#
# The old, global "port manager" always worked with LOCAL system
#
_old_port_manager = PortManager__Generic(LocalOperations.get_single_instance())

# ports used by nodes
bound_ports = _old_port_manager._reserved_ports


# re-export version type
class PgVer(Version):
    def __init__(self, version: str) -> None:
        try:
            super().__init__(version)
        except InvalidVersion:
            version = re.sub(r"[a-zA-Z].*", "", version)
            super().__init__(version)


def internal__reserve_port():
    """
    Generate a new port and add it to 'bound_ports'.
    """
    return _old_port_manager.reserve_port()


def internal__release_port(port):
    """
    Free port provided by reserve_port().
    """

    assert type(port) == int  # noqa: E721
    return _old_port_manager.release_port(port)


reserve_port = internal__reserve_port
release_port = internal__release_port


def execute_utility(args, logfile=None, verbose=False):
    """
    Execute utility (pg_ctl, pg_dump etc).

    Args:
        args: utility + arguments (list).
        logfile: path to file to store stdout and stderr.

    Returns:
        stdout of executed utility.
    """
    return execute_utility2(tconf.os_ops, args, logfile, verbose)


def execute_utility2(
        os_ops: OsOperations,
        args,
        logfile=None,
        verbose=False,
        ignore_errors=False,
        exec_env=None,
):
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)
    assert type(verbose) == bool  # noqa: E721
    assert type(ignore_errors) == bool  # noqa: E721
    assert exec_env is None or type(exec_env) == dict  # noqa: E721

    exit_status, out, error = os_ops.exec_command(
        args,
        verbose=True,
        ignore_errors=ignore_errors,
        encoding=OsHelpers.GetDefaultEncoding(),
        exec_env=exec_env)

    out = '' if not out else out

    # write new log entry if possible
    if logfile:
        try:
            os_ops.write(filename=logfile, data=args, truncate=True)
            if out:
                # comment-out lines
                lines = [u'\n'] + ['# ' + line for line in out.splitlines()] + [u'\n']
                os_ops.write(filename=logfile, data=lines)
        except IOError:
            raise ExecUtilException(
                "Problem with writing to logfile `{}` during run command `{}`".format(logfile, args))
    if verbose:
        return exit_status, out, error
    else:
        return out


def get_bin_path(filename):
    """
    Return absolute path to an executable using PG_BIN or PG_CONFIG.
    This function does nothing if 'filename' is already absolute.
    """
    return get_bin_path2(tconf.os_ops, filename)


def get_bin_path2(os_ops: OsOperations, filename):
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)

    # check if it's already absolute
    if os.path.isabs(filename):
        return filename
    if isinstance(os_ops, RemoteOperations):
        pg_config = os.environ.get("PG_CONFIG_REMOTE") or os.environ.get("PG_CONFIG")
    else:
        # try PG_CONFIG - get from local machine
        pg_config = os.environ.get("PG_CONFIG")

    if pg_config:
        bindir = get_pg_config(pg_config, os_ops)["BINDIR"]
        return os_ops.build_path(bindir, filename)

    # try PG_BIN
    pg_bin = os_ops.environ("PG_BIN")
    if pg_bin:
        return os_ops.build_path(pg_bin, filename)

    pg_config_path = os_ops.find_executable('pg_config')
    if pg_config_path:
        bindir = get_pg_config(pg_config_path)["BINDIR"]
        return os_ops.build_path(bindir, filename)

    return filename


def get_pg_config(pg_config_path=None, os_ops=None):
    """
    Return output of pg_config (provided that it is installed).
    NOTE: this function caches the result by default (see GlobalConfig).
    """

    if os_ops is None:
        os_ops = tconf.os_ops

    return get_pg_config2(os_ops, pg_config_path)


def get_pg_config2(os_ops: OsOperations, pg_config_path):
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)

    def cache_pg_config_data(cmd):
        # execute pg_config and get the output
        out = os_ops.exec_command(cmd, encoding='utf-8')

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
    if not tconf.cache_pg_config:
        global _pg_config_data
        _pg_config_data = {}

    # return cached data
    if not pg_config_path and _pg_config_data:
        return _pg_config_data

    # try specified pg_config path or PG_CONFIG
    if pg_config_path:
        return cache_pg_config_data(pg_config_path)

    if isinstance(os_ops, RemoteOperations):
        pg_config = os.environ.get("PG_CONFIG_REMOTE") or os.environ.get("PG_CONFIG")
    else:
        # try PG_CONFIG - get from local machine
        pg_config = os.environ.get("PG_CONFIG")

    if pg_config:
        return cache_pg_config_data(pg_config)

    # try PG_BIN
    pg_bin = os.environ.get("PG_BIN")
    if pg_bin:
        cmd = os_ops.build_path(pg_bin, "pg_config")
        return cache_pg_config_data(cmd)

    # try plain name
    return cache_pg_config_data("pg_config")


def get_pg_version2(os_ops: OsOperations, bin_dir=None):
    """
    Return PostgreSQL version provided by postmaster.
    """
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)

    C_POSTGRES_BINARY = "postgres"

    # Get raw version (e.g., postgres (PostgreSQL) 9.5.7)
    if bin_dir is None:
        postgres_path = get_bin_path2(os_ops, C_POSTGRES_BINARY)
    else:
        # [2025-06-25] OK ?
        assert type(bin_dir) == str  # noqa: E721
        assert bin_dir != ""
        postgres_path = os_ops.build_path(bin_dir, 'postgres')

    cmd = [postgres_path, '--version']
    raw_ver = os_ops.exec_command(cmd, encoding='utf-8')

    return parse_pg_version(raw_ver)


def get_pg_version(bin_dir=None):
    """
    Return PostgreSQL version provided by postmaster.
    """

    return get_pg_version2(tconf.os_ops, bin_dir)


def parse_pg_version(version_out):
    # Generalize removal of system-specific suffixes (anything in parentheses)
    raw_ver = re.sub(r'\([^)]*\)', '', version_out).strip()

    # Cook version of PostgreSQL
    version = raw_ver.split(' ')[-1] \
                     .partition('devel')[0] \
                     .partition('beta')[0] \
                     .partition('rc')[0]
    return version


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


def eprint(*args, **kwargs):
    """
    Print stuff to stderr.
    """
    print(*args, file=sys.stderr, **kwargs)


def options_string(separator=u" ", **kwargs):
    return separator.join(u"{}={}".format(k, v) for k, v in iteritems(kwargs))


@contextmanager
def clean_on_error(node):
    """
    Context manager to wrap PostgresNode and such.
    Calls cleanup() method when underlying code raises an exception.
    """

    try:
        yield node
    except Exception:
        # TODO: should we wrap this in try-block?
        node.cleanup()
        raise
