# coding: utf-8

from __future__ import division
from __future__ import print_function

import os

import sys

from contextlib import contextmanager
from packaging.version import Version, InvalidVersion
import re

from six import iteritems

from .helpers.port_manager import PortManager
from .exceptions import ExecUtilException
from .config import testgres_config as tconf

# rows returned by PG_CONFIG
_pg_config_data = {}

# ports used by nodes
bound_ports = set()


# re-export version type
class PgVer(Version):
    def __init__(self, version: str) -> None:
        try:
            super().__init__(version)
        except InvalidVersion:
            version = re.sub(r"[a-zA-Z].*", "", version)
            super().__init__(version)


def reserve_port():
    """
    Generate a new port and add it to 'bound_ports'.
    """
    port_mng = PortManager()
    port = port_mng.find_free_port(exclude_ports=bound_ports)
    bound_ports.add(port)

    return port


def release_port(port):
    """
    Free port provided by reserve_port().
    """

    bound_ports.discard(port)


def execute_utility(args, logfile=None, verbose=False):
    """
    Execute utility (pg_ctl, pg_dump etc).

    Args:
        args: utility + arguments (list).
        logfile: path to file to store stdout and stderr.

    Returns:
        stdout of executed utility.
    """
    exit_status, out, error = tconf.os_ops.exec_command(args, verbose=True)
    # decode result
    out = '' if not out else out
    if isinstance(out, bytes):
        out = out.decode('utf-8')
    if isinstance(error, bytes):
        error = error.decode('utf-8')

    # write new log entry if possible
    if logfile:
        try:
            tconf.os_ops.write(filename=logfile, data=args, truncate=True)
            if out:
                # comment-out lines
                lines = [u'\n'] + ['# ' + line for line in out.splitlines()] + [u'\n']
                tconf.os_ops.write(filename=logfile, data=lines)
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
    # check if it's already absolute
    if os.path.isabs(filename):
        return filename
    if tconf.os_ops.remote:
        pg_config = os.environ.get("PG_CONFIG_REMOTE") or os.environ.get("PG_CONFIG")
    else:
        # try PG_CONFIG - get from local machine
        pg_config = os.environ.get("PG_CONFIG")

    if pg_config:
        bindir = get_pg_config()["BINDIR"]
        return os.path.join(bindir, filename)

    # try PG_BIN
    pg_bin = tconf.os_ops.environ("PG_BIN")
    if pg_bin:
        return os.path.join(pg_bin, filename)

    pg_config_path = tconf.os_ops.find_executable('pg_config')
    if pg_config_path:
        bindir = get_pg_config(pg_config_path)["BINDIR"]
        return os.path.join(bindir, filename)

    return filename


def get_pg_config(pg_config_path=None, os_ops=None):
    """
    Return output of pg_config (provided that it is installed).
    NOTE: this function caches the result by default (see GlobalConfig).
    """
    if os_ops:
        tconf.os_ops = os_ops

    def cache_pg_config_data(cmd):
        # execute pg_config and get the output
        out = tconf.os_ops.exec_command(cmd, encoding='utf-8')

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
    if tconf.os_ops.remote:
        pg_config = pg_config_path or os.environ.get("PG_CONFIG_REMOTE") or os.environ.get("PG_CONFIG")
    else:
        # try PG_CONFIG - get from local machine
        pg_config = pg_config_path or os.environ.get("PG_CONFIG")
    if pg_config:
        return cache_pg_config_data(pg_config)

    # try PG_BIN
    pg_bin = os.environ.get("PG_BIN")
    if pg_bin:
        cmd = os.path.join(pg_bin, "pg_config")
        return cache_pg_config_data(cmd)

    # try plain name
    return cache_pg_config_data("pg_config")


def get_pg_version(bin_dir=None):
    """
    Return PostgreSQL version provided by postmaster.
    """

    # Get raw version (e.g., postgres (PostgreSQL) 9.5.7)
    postgres_path = os.path.join(bin_dir, 'postgres') if bin_dir else get_bin_path('postgres')
    _params = [postgres_path, '--version']
    raw_ver = tconf.os_ops.exec_command(_params, encoding='utf-8')

    return parse_pg_version(raw_ver)


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
