# coding: utf-8

from __future__ import annotations
from __future__ import division
from __future__ import print_function

from .exceptions import InvalidOperationException
from .config import testgres_config as tconf
from .node_state import PostgresNodeState

from testgres.operations.types import T_OS_CMD
from testgres.operations.types import T_OS_EXEC_ENV
from testgres.operations.os_ops import OsOperations
from testgres.operations.os_ops import OsCommandResult
from testgres.operations.remote_ops import RemoteOperations
from testgres.operations.local_ops import LocalOperations

from . import consts

from .impl import internal_utils
from .impl.internal_node_utils import InternalNodeUtils
from .impl.port_manager__generic2 import PortManager__Generic2

import os
import sys
import re
import typing

from contextlib import contextmanager
from packaging.version import Version, InvalidVersion


# rows returned by PG_CONFIG
_pg_config_data = {}

#
# The old, global "port manager" always worked with LOCAL system
#
_old_port_manager = PortManager__Generic2(LocalOperations.get_single_instance())


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
    Generate a new port.
    """
    return _old_port_manager.reserve_port()


def internal__release_port(port):
    """
    Free port provided by reserve_port().
    """

    assert type(port) is int
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
    return execute_utility2(
        tconf.os_ops,
        args,
        logfile,
        verbose,
    )


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
    assert type(verbose) is bool
    assert type(ignore_errors) is bool
    assert exec_env is None or type(exec_env) is dict

    exec_r = execute_utility3(
        os_ops,
        args,
        logfile,
        check=not ignore_errors,
        exec_env=exec_env,
    )

    assert type(exec_r) is OsCommandResult

    assert type(exec_r.returncode) is int
    assert exec_r.stdout is None or type(exec_r.stdout) is str
    assert exec_r.stderr is None or type(exec_r.stderr) is str

    if not verbose:
        return exec_r.stdout

    return exec_r.returncode, exec_r.stdout, exec_r.stderr


def execute_utility3(
    os_ops: OsOperations,
    args: T_OS_CMD,
    logfile: typing.Optional[str] = None,
    check: bool = True,
    exec_env: typing.Optional[T_OS_EXEC_ENV] = None,
) -> OsCommandResult:
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)
    assert type(check) is bool
    assert exec_env is None or type(exec_env) is dict

    return internal_utils.execute_utility3(
        os_ops,
        args,
        logfile,
        check,
        exec_env,
    )


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
    if os_ops.is_abs_path(filename):
        return filename
    if isinstance(os_ops, RemoteOperations):
        pg_config = os.environ.get("PG_CONFIG_REMOTE") or os.environ.get("PG_CONFIG")
    else:
        # try PG_CONFIG - get from local machine
        pg_config = os.environ.get("PG_CONFIG")

    if pg_config:
        bindir = get_pg_config2(os_ops, pg_config)["BINDIR"]
        return os_ops.build_path(bindir, filename)

    # try PG_BIN
    pg_bin = os_ops.environ("PG_BIN")
    if pg_bin:
        return os_ops.build_path(pg_bin, filename)

    pg_config_path = os_ops.find_executable('pg_config')
    if pg_config_path:
        bindir = get_pg_config2(os_ops, pg_config_path)["BINDIR"]
        return os_ops.build_path(bindir, filename)

    return filename


def get_bin_dir(os_ops: OsOperations) -> str:
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)

    if isinstance(os_ops, RemoteOperations):
        pg_config = os.environ.get("PG_CONFIG_REMOTE") or os.environ.get("PG_CONFIG")
    else:
        # try PG_CONFIG - get from local machine
        pg_config = os.environ.get("PG_CONFIG")

    if pg_config:
        return get_pg_config2(os_ops, pg_config)["BINDIR"]

    # try PG_BIN
    pg_bin = os_ops.environ("PG_BIN")
    if pg_bin:
        return pg_bin

    pg_config_path = os_ops.find_executable('pg_config')
    if pg_config_path:
        return get_pg_config2(os_ops, pg_config_path)["BINDIR"]

    postgres = os_ops.find_executable('postgres')
    if postgres:
        return os_ops.get_dirname(postgres)

    raise RuntimeError("BinDir is not detected.")


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
        out = os_ops.run(cmd, encoding='utf-8').stdout
        assert type(out) is str

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
    try:
        pg_config_data = cache_pg_config_data("pg_config")
    except Exception:
        raise InvalidOperationException(
            "Failed to determine how to start pg_config. "
            "Either specify the path to pg_config in PG_CONFIG or "
            "specify the path to the Postgres directory containing "
            "pg_config in PG_BIN, or put pg_config into the system PATH.",
        )
    return pg_config_data


def get_pg_version2(os_ops: OsOperations, bin_dir=None):
    """
    Return PostgreSQL version provided by postmaster.
    """
    assert os_ops is not None
    assert isinstance(os_ops, OsOperations)

    # Get raw version (e.g., postgres (PostgreSQL) 9.5.7)
    if bin_dir is None:
        postgres_path = get_bin_path2(
            os_ops,
            consts.BINARY_NAME__POSTGRES,
        )
    else:
        # [2025-06-25] OK ?
        assert type(bin_dir) is str
        assert bin_dir != ""
        postgres_path = os_ops.build_path(
            bin_dir,
            consts.BINARY_NAME__POSTGRES,
        )

    cmd = [postgres_path, '--version']
    raw_ver = os_ops.run(cmd, encoding='utf-8').stdout
    assert type(raw_ver) is str

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
                     .partition('rc')[0] \
                     .partition('-')[0]
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


def options_string(separator=" ", **kwargs):
    return separator.join("{}={}".format(k, v) for k, v in kwargs.items())


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


def get_pg_node_state(
    os_ops: OsOperations,
    bin_dir: str,
    data_dir: str,
    utils_log_file: typing.Optional[str],
) -> PostgresNodeState:
    assert isinstance(os_ops, OsOperations)
    assert type(bin_dir) is str
    assert type(data_dir) is str
    assert utils_log_file is None or type(utils_log_file) is str

    return InternalNodeUtils.get_pg_node_state(
        os_ops,
        bin_dir,
        data_dir,
        utils_log_file,
    )
