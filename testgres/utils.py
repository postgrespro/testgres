# coding: utf-8

from __future__ import division
from __future__ import print_function

import io
import os
import port_for
import subprocess
import sys
import tempfile

from contextlib import contextmanager
from distutils.version import LooseVersion
from distutils.spawn import find_executable
from six import iteritems

from .config import testgres_config
from .exceptions import ExecUtilException

# rows returned by PG_CONFIG
_pg_config_data = {}

# ports used by nodes
bound_ports = set()

# re-export version type
PgVer = LooseVersion


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

    bound_ports.discard(port)


def execute_utility(args, logfile=None):
    """
    Execute utility (pg_ctl, pg_dump etc).

    Args:
        args: utility + arguments (list).
        logfile: path to file to store stdout and stderr.

    Returns:
        stdout of executed utility.
    """

    # run utility
    if os.name == 'nt':
        # using output to a temporary file in Windows
        buf = tempfile.NamedTemporaryFile()

        process = subprocess.Popen(
            args,    # util + params
            stdout=buf,
            stderr=subprocess.STDOUT)
        process.communicate()

        # get result
        buf.file.flush()
        buf.file.seek(0)
        out = buf.file.read()
        buf.close()
    else:
        process = subprocess.Popen(
            args,    # util + params
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        # get result
        out, _ = process.communicate()

    # decode result
    out = '' if not out else out.decode('utf-8')

    # format command
    command = u' '.join(args)

    # write new log entry if possible
    if logfile:
        try:
            with io.open(logfile, 'a') as file_out:
                file_out.write(command)

                if out:
                    # comment-out lines
                    lines = ('# ' + line for line in out.splitlines(True))
                    file_out.write(u'\n')
                    file_out.writelines(lines)

                file_out.write(u'\n')
        except IOError:
            pass

    exit_code = process.returncode
    if exit_code:
        message = 'Utility exited with non-zero code'
        raise ExecUtilException(message=message,
                                command=command,
                                exit_code=exit_code,
                                out=out)

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

    pg_config_path = find_executable('pg_config')
    if pg_config_path:
        bindir = get_pg_config(pg_config_path)["BINDIR"]
        return os.path.join(bindir, filename)

    return filename


def get_pg_config(pg_config_path=None):
    """
    Return output of pg_config (provided that it is installed).
    NOTE: this fuction caches the result by default (see GlobalConfig).
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
    if not testgres_config.cache_pg_config:
        global _pg_config_data
        _pg_config_data = {}

    # return cached data
    if _pg_config_data:
        return _pg_config_data

    # try specified pg_config path or PG_CONFIG
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
