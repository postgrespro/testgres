# coding: utf-8

import io
import os
import port_for
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
    Generate a new port and add it to '_bound_ports'.
    """

    global bound_ports
    port = port_for.select_random(exclude_ports=bound_ports)
    bound_ports.add(port)

    return port


def release_port(port):
    """
    Free port provided by reserve_port().
    """

    global bound_ports
    bound_ports.remove(port)


def default_username():
    """
    Return current user.
    """

    import pwd    # used only here
    return pwd.getpwuid(os.getuid())[0]


def explain_exception(e):
    """
    Use this function instead of str(e).
    """

    import traceback    # used only here
    lines = traceback.format_exception_only(type(e), e)
    return ''.join(lines)


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
        args,  # util + params
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

    global _pg_config_data

    def cache_pg_config_data(cmd):
        global _pg_config_data

        # execute pg_config and get the output
        out = subprocess.check_output([cmd]).decode('utf-8')

        data = {}
        for line in out.splitlines():
            if line and '=' in line:
                key, _, value = line.partition('=')
                data[key.strip()] = value.strip()

        _pg_config_data.clear()

        # cache data, if necessary
        if TestgresConfig.cache_pg_config:
            _pg_config_data = data

        return data

    # return cached data, if allowed to
    if TestgresConfig.cache_pg_config and _pg_config_data:
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
