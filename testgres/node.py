# coding: utf-8

import os
import random
import signal
import subprocess
import threading
from queue import Queue

import time

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

# we support both pg8000 and psycopg2
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

from six import raise_from, iteritems, text_type

from .enums import \
    NodeStatus, \
    ProcessType, \
    DumpFormat

from .cache import cached_initdb

from .config import testgres_config

from .connection import NodeConnection

from .consts import \
    DATA_DIR, \
    LOGS_DIR, \
    TMP_NODE, \
    TMP_DUMP, \
    PG_CONF_FILE, \
    PG_AUTO_CONF_FILE, \
    HBA_CONF_FILE, \
    RECOVERY_CONF_FILE, \
    PG_LOG_FILE, \
    UTILS_LOG_FILE, \
    PG_PID_FILE

from .consts import \
    MAX_LOGICAL_REPLICATION_WORKERS, \
    MAX_REPLICATION_SLOTS, \
    MAX_WORKER_PROCESSES, \
    MAX_WAL_SENDERS, \
    WAL_KEEP_SEGMENTS, \
    WAL_KEEP_SIZE

from .decorators import \
    method_decorator, \
    positional_args_hack

from .defaults import \
    default_dbname, \
    default_username, \
    generate_app_name

from .exceptions import \
    CatchUpException,   \
    ExecUtilException,  \
    QueryException,     \
    StartNodeException, \
    TimeoutException,   \
    InitNodeException,  \
    TestgresException,  \
    BackupException

from .logger import TestgresLogger

from .pubsub import Publication, Subscription

from .standby import First

from .utils import \
    PgVer, \
    eprint, \
    get_bin_path, \
    get_pg_version, \
    reserve_port, \
    release_port, \
    execute_utility, \
    options_string, \
    clean_on_error

from .backup import NodeBackup

from .operations.os_ops import ConnectionParams
from .operations.local_ops import LocalOperations
from .operations.remote_ops import RemoteOperations

InternalError = pglib.InternalError
ProgrammingError = pglib.ProgrammingError
OperationalError = pglib.OperationalError


class ProcessProxy(object):
    """
    Wrapper for psutil.Process

    Attributes:
        process: wrapped psutill.Process object
        ptype: instance of ProcessType
    """

    def __init__(self, process, ptype=None):
        self.process = process
        self.ptype = ptype or ProcessType.from_process(process)

    def __getattr__(self, name):
        return getattr(self.process, name)

    def __repr__(self):
        return '{}(ptype={}, process={})'.format(self.__class__.__name__,
                                                 str(self.ptype),
                                                 repr(self.process))


class PostgresNode(object):
    def __init__(self, name=None, port=None, base_dir=None, conn_params: ConnectionParams = ConnectionParams(), bin_dir=None, prefix=None):
        """
        PostgresNode constructor.

        Args:
            name: node's application name.
            port: port to accept connections.
            base_dir: path to node's data directory.
            bin_dir: path to node's binary directory.
        """

        # private
        self._pg_version = PgVer(get_pg_version(bin_dir))
        self._should_free_port = port is None
        self._base_dir = base_dir
        self._bin_dir = bin_dir
        self._prefix = prefix
        self._logger = None
        self._master = None

        # basic
        self.name = name or generate_app_name()
        if testgres_config.os_ops:
            self.os_ops = testgres_config.os_ops
        elif conn_params.ssh_key:
            self.os_ops = RemoteOperations(conn_params)
        else:
            self.os_ops = LocalOperations(conn_params)

        self.port = port or reserve_port()

        self.host = self.os_ops.host
        self.ssh_key = self.os_ops.ssh_key

        # defaults for __exit__()
        self.cleanup_on_good_exit = testgres_config.node_cleanup_on_good_exit
        self.cleanup_on_bad_exit = testgres_config.node_cleanup_on_bad_exit
        self.shutdown_max_attempts = 3

        # NOTE: for compatibility
        self.utils_log_name = self.utils_log_file
        self.pg_log_name = self.pg_log_file

        # Node state
        self.is_started = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.free_port()

        # NOTE: Ctrl+C does not count!
        got_exception = type is not None and type != KeyboardInterrupt

        c1 = self.cleanup_on_good_exit and not got_exception
        c2 = self.cleanup_on_bad_exit and got_exception

        attempts = self.shutdown_max_attempts

        if c1 or c2:
            self.cleanup(attempts)
        else:
            self._try_shutdown(attempts)

    def __repr__(self):
        return "{}(name='{}', port={}, base_dir='{}')".format(
            self.__class__.__name__, self.name, self.port, self.base_dir)

    @property
    def pid(self):
        """
        Return postmaster's PID if node is running, else 0.
        """

        if self.status():
            pid_file = os.path.join(self.data_dir, PG_PID_FILE)
            lines = self.os_ops.readlines(pid_file)
            pid = int(lines[0]) if lines else None
            return pid

        # for clarity
        return 0

    @property
    def auxiliary_pids(self):
        """
        Returns a dict of { ProcessType : PID }.
        """

        result = {}

        for process in self.auxiliary_processes:
            if process.ptype not in result:
                result[process.ptype] = []

            result[process.ptype].append(process.pid)

        return result

    @property
    def auxiliary_processes(self):
        """
        Returns a list of auxiliary processes.
        Each process is represented by :class:`.ProcessProxy` object.
        """
        def is_aux(process):
            return process.ptype != ProcessType.Unknown

        return list(filter(is_aux, self.child_processes))

    @property
    def child_processes(self):
        """
        Returns a list of all child processes.
        Each process is represented by :class:`.ProcessProxy` object.
        """

        # get a list of postmaster's children
        children = self.os_ops.get_process_children(self.pid)

        return [ProcessProxy(p) for p in children]

    @property
    def source_walsender(self):
        """
        Returns master's walsender feeding this replica.
        """

        sql = """
            select pid
            from pg_catalog.pg_stat_replication
            where application_name = %s
        """

        if not self.master:
            raise TestgresException("Node doesn't have a master")

        # master should be on the same host
        assert self.master.host == self.host

        with self.master.connect() as con:
            for row in con.execute(sql, self.name):
                for child in self.master.auxiliary_processes:
                    if child.pid == int(row[0]):
                        return child

        msg = "Master doesn't send WAL to {}".format(self.name)
        raise TestgresException(msg)

    @property
    def master(self):
        return self._master

    @property
    def base_dir(self):
        if not self._base_dir:
            self._base_dir = self.os_ops.mkdtemp(prefix=self._prefix or TMP_NODE)

        # NOTE: it's safe to create a new dir
        if not self.os_ops.path_exists(self._base_dir):
            self.os_ops.makedirs(self._base_dir)

        return self._base_dir

    @property
    def bin_dir(self):
        if not self._bin_dir:
            self._bin_dir = os.path.dirname(get_bin_path("pg_config"))
        return self._bin_dir

    @property
    def logs_dir(self):
        path = os.path.join(self.base_dir, LOGS_DIR)

        # NOTE: it's safe to create a new dir
        if not self.os_ops.path_exists(path):
            self.os_ops.makedirs(path)

        return path

    @property
    def data_dir(self):
        # NOTE: we can't run initdb without user's args
        return os.path.join(self.base_dir, DATA_DIR)

    @property
    def utils_log_file(self):
        return os.path.join(self.logs_dir, UTILS_LOG_FILE)

    @property
    def pg_log_file(self):
        return os.path.join(self.logs_dir, PG_LOG_FILE)

    @property
    def version(self):
        """
        Return PostgreSQL version for this node.

        Returns:
            Instance of :class:`distutils.version.LooseVersion`.
        """
        return self._pg_version

    def _try_shutdown(self, max_attempts):
        attempts = 0

        # try stopping server N times
        while attempts < max_attempts:
            try:
                self.stop()
                break    # OK
            except ExecUtilException:
                pass    # one more time
            except Exception:
                # TODO: probably should kill stray instance
                eprint('cannot stop node {}'.format(self.name))
                break

            attempts += 1

    def _assign_master(self, master):
        """NOTE: this is a private method!"""

        # now this node has a master
        self._master = master

    def _create_recovery_conf(self, username, slot=None):
        """NOTE: this is a private method!"""

        # fetch master of this node
        master = self.master
        assert master is not None

        conninfo = {
            "application_name": self.name,
            "port": master.port,
            "user": username
        }  # yapf: disable

        # host is tricky
        try:
            import ipaddress
            ipaddress.ip_address(master.host)
            conninfo["hostaddr"] = master.host
        except ValueError:
            conninfo["host"] = master.host

        line = (
            "primary_conninfo='{}'\n"
        ).format(options_string(**conninfo))  # yapf: disable
        # Since 12 recovery.conf had disappeared
        if self.version >= PgVer('12'):
            signal_name = os.path.join(self.data_dir, "standby.signal")
            self.os_ops.touch(signal_name)
        else:
            line += "standby_mode=on\n"

        if slot:
            # Connect to master for some additional actions
            with master.connect(username=username) as con:
                # check if slot already exists
                res = con.execute(
                    """
                    select exists (
                        select from pg_catalog.pg_replication_slots
                        where slot_name = %s
                    )
                    """, slot)

                if res[0][0]:
                    raise TestgresException(
                        "Slot '{}' already exists".format(slot))

                # TODO: we should drop this slot after replica's cleanup()
                con.execute(
                    """
                    select pg_catalog.pg_create_physical_replication_slot(%s)
                    """, slot)

            line += "primary_slot_name={}\n".format(slot)

        if self.version >= PgVer('12'):
            self.append_conf(line=line)
        else:
            self.append_conf(filename=RECOVERY_CONF_FILE, line=line)

    def _maybe_start_logger(self):
        if testgres_config.use_python_logging:
            # spawn new logger if it doesn't exist or is stopped
            if not self._logger or not self._logger.is_alive():
                self._logger = TestgresLogger(self.name, self.pg_log_file)
                self._logger.start()

    def _maybe_stop_logger(self):
        if self._logger:
            self._logger.stop()

    def _collect_special_files(self):
        result = []

        # list of important files + last N lines
        files = [
            (os.path.join(self.data_dir, PG_CONF_FILE), 0),
            (os.path.join(self.data_dir, PG_AUTO_CONF_FILE), 0),
            (os.path.join(self.data_dir, RECOVERY_CONF_FILE), 0),
            (os.path.join(self.data_dir, HBA_CONF_FILE), 0),
            (self.pg_log_file, testgres_config.error_log_lines)
        ]  # yapf: disable

        for f, num_lines in files:
            # skip missing files
            if not self.os_ops.path_exists(f):
                continue

            file_lines = self.os_ops.readlines(f, num_lines, binary=True, encoding=None)
            lines = b''.join(file_lines)

            # fill list
            result.append((f, lines))

        return result

    def init(self, initdb_params=None, cached=True, **kwargs):
        """
        Perform initdb for this node.

        Args:
            initdb_params: parameters for initdb (list).
            fsync: should this node use fsync to keep data safe?
            unix_sockets: should we enable UNIX sockets?
            allow_streaming: should this node add a hba entry for replication?

        Returns:
            This instance of :class:`.PostgresNode`
        """

        # initialize this PostgreSQL node
        cached_initdb(
            data_dir=self.data_dir,
            logfile=self.utils_log_file,
            os_ops=self.os_ops,
            params=initdb_params,
            bin_path=self.bin_dir,
            cached=False)

        # initialize default config files
        self.default_conf(**kwargs)

        return self

    def default_conf(self,
                     fsync=False,
                     unix_sockets=True,
                     allow_streaming=True,
                     allow_logical=False,
                     log_statement='all'):
        """
        Apply default settings to this node.

        Args:
            fsync: should this node use fsync to keep data safe?
            unix_sockets: should we enable UNIX sockets?
            allow_streaming: should this node add a hba entry for replication?
            allow_logical: can this node be used as a logical replication publisher?
            log_statement: one of ('all', 'off', 'mod', 'ddl').

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        postgres_conf = os.path.join(self.data_dir, PG_CONF_FILE)
        hba_conf = os.path.join(self.data_dir, HBA_CONF_FILE)

        # filter lines in hba file
        # get rid of comments and blank lines
        hba_conf_file = self.os_ops.readlines(hba_conf)
        lines = [
            s for s in hba_conf_file
            if len(s.strip()) > 0 and not s.startswith('#')
        ]

        # write filtered lines
        self.os_ops.write(hba_conf, lines, truncate=True)

        # replication-related settings
        if allow_streaming:
            # get auth method for host or local users
            def get_auth_method(t):
                return next((s.split()[-1]
                             for s in lines if s.startswith(t)), 'trust')

            # get auth methods
            auth_local = get_auth_method('local')
            auth_host = get_auth_method('host')
            subnet_base = ".".join(self.os_ops.host.split('.')[:-1] + ['0'])

            new_lines = [
                u"local\treplication\tall\t\t\t{}\n".format(auth_local),
                u"host\treplication\tall\t127.0.0.1/32\t{}\n".format(auth_host),
                u"host\treplication\tall\t::1/128\t\t{}\n".format(auth_host),
                u"host\treplication\tall\t{}/24\t\t{}\n".format(subnet_base, auth_host),
                u"host\tall\tall\t{}/24\t\t{}\n".format(subnet_base, auth_host)
            ]  # yapf: disable

            # write missing lines
            self.os_ops.write(hba_conf, new_lines)

        # overwrite config file
        self.os_ops.write(postgres_conf, '', truncate=True)

        self.append_conf(fsync=fsync,
                         max_worker_processes=MAX_WORKER_PROCESSES,
                         log_statement=log_statement,
                         listen_addresses=self.host,
                         port=self.port)  # yapf:disable

        # common replication settings
        if allow_streaming or allow_logical:
            self.append_conf(max_replication_slots=MAX_REPLICATION_SLOTS,
                             max_wal_senders=MAX_WAL_SENDERS)  # yapf: disable

        # binary replication
        if allow_streaming:
            # select a proper wal_level for PostgreSQL
            wal_level = 'replica' if self._pg_version >= PgVer('9.6') else 'hot_standby'

            if self._pg_version < PgVer('13'):
                self.append_conf(hot_standby=True,
                                 wal_keep_segments=WAL_KEEP_SEGMENTS,
                                 wal_level=wal_level)  # yapf: disable
            else:
                self.append_conf(hot_standby=True,
                                 wal_keep_size=WAL_KEEP_SIZE,
                                 wal_level=wal_level)  # yapf: disable

        # logical replication
        if allow_logical:
            if self._pg_version < PgVer('10'):
                raise InitNodeException("Logical replication is only "
                                        "available on PostgreSQL 10 and newer")

            self.append_conf(
                max_logical_replication_workers=MAX_LOGICAL_REPLICATION_WORKERS,
                wal_level='logical')

        # disable UNIX sockets if asked to
        if not unix_sockets:
            self.append_conf(unix_socket_directories='')

        return self

    @method_decorator(positional_args_hack(['filename', 'line']))
    def append_conf(self, line='', filename=PG_CONF_FILE, **kwargs):
        """
        Append line to a config file.

        Args:
            line: string to be appended to config.
            filename: config file (postgresql.conf by default).
            **kwargs: named config options.

        Returns:
            This instance of :class:`.PostgresNode`.

        Examples:
            >>> append_conf(fsync=False)
            >>> append_conf('log_connections = yes')
            >>> append_conf(random_page_cost=1.5, fsync=True, ...)
            >>> append_conf('postgresql.conf', 'synchronous_commit = off')
        """

        lines = [line]

        for option, value in iteritems(kwargs):
            if isinstance(value, bool):
                value = 'on' if value else 'off'
            elif not str(value).replace('.', '', 1).isdigit():
                value = "'{}'".format(value)
            if value == '*':
                lines.append("{} = '*'".format(option))
            else:
                # format a new config line
                lines.append('{} = {}'.format(option, value))

        config_name = os.path.join(self.data_dir, filename)
        conf_text = ''
        for line in lines:
            conf_text += text_type(line) + '\n'
        self.os_ops.write(config_name, conf_text)

        return self

    def status(self):
        """
        Check this node's status.

        Returns:
            An instance of :class:`.NodeStatus`.
        """

        try:
            _params = [
                self._get_bin_path('pg_ctl'),
                "-D", self.data_dir,
                "status"
            ]  # yapf: disable
            status_code, out, error = execute_utility(_params, self.utils_log_file, verbose=True)
            if error and 'does not exist' in error:
                return NodeStatus.Uninitialized
            elif 'no server running' in out:
                return NodeStatus.Stopped
            return NodeStatus.Running

        except ExecUtilException as e:
            # Node is not running
            if e.exit_code == 3:
                return NodeStatus.Stopped

            # Node has no file dir
            elif e.exit_code == 4:
                return NodeStatus.Uninitialized

    def get_control_data(self):
        """
        Return contents of pg_control file.
        """

        # this one is tricky (blame PG 9.4)
        _params = [self._get_bin_path("pg_controldata")]
        _params += ["-D"] if self._pg_version >= PgVer('9.5') else []
        _params += [self.data_dir]

        data = execute_utility(_params, self.utils_log_file)

        out_dict = {}

        for line in data.splitlines():
            key, _, value = line.partition(':')
            out_dict[key.strip()] = value.strip()

        return out_dict

    def slow_start(self, replica=False, dbname='template1', username=None, max_attempts=0):
        """
        Starts the PostgreSQL instance and then polls the instance
        until it reaches the expected state (primary or replica). The state is checked
        using the pg_is_in_recovery() function.

        Args:
               dbname:
               username:
               replica: If True, waits for the instance to be in recovery (i.e., replica mode).
                        If False, waits for the instance to be in primary mode. Default is False.
               max_attempts:
        """
        if not username:
            username = default_username()
        self.start()

        if replica:
            query = 'SELECT pg_is_in_recovery()'
        else:
            query = 'SELECT not pg_is_in_recovery()'
        # Call poll_query_until until the expected value is returned
        self.poll_query_until(query=query,
                              dbname=dbname,
                              username=username,
                              suppress={InternalError,
                                        QueryException,
                                        ProgrammingError,
                                        OperationalError},
                              max_attempts=max_attempts)

    def start(self, params=[], wait=True):
        """
        Starts the PostgreSQL node using pg_ctl if node has not been started.
        By default, it waits for the operation to complete before returning.
        Optionally, it can return immediately without waiting for the start operation
        to complete by setting the `wait` parameter to False.

        Args:
            params: additional arguments for pg_ctl.
            wait: wait until operation completes.

        Returns:
            This instance of :class:`.PostgresNode`.
        """
        if self.is_started:
            return self

        _params = [
            self._get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-l", self.pg_log_file,
            "-w" if wait else '-W',  # --wait or --no-wait
            "start"
        ] + params  # yapf: disable

        startup_retries = 5
        while True:
            try:
                exit_status, out, error = execute_utility(_params, self.utils_log_file, verbose=True)
                if error and 'does not exist' in error:
                    raise Exception
            except Exception as e:
                files = self._collect_special_files()
                if any(len(file) > 1 and 'Is another postmaster already '
                                         'running on port' in file[1].decode() for
                       file in files):
                    print("Detected an issue with connecting to port {0}. "
                          "Trying another port after a 5-second sleep...".format(self.port))
                    self.port = reserve_port()
                    options = {}
                    options['port'] = str(self.port)
                    self.set_auto_conf(options)
                    startup_retries -= 1
                    time.sleep(5)
                    continue

                msg = 'Cannot start node'
                raise_from(StartNodeException(msg, files), e)
            break
        self._maybe_start_logger()
        self.is_started = True
        return self

    def stop(self, params=[], wait=True):
        """
        Stops the PostgreSQL node using pg_ctl if the node has been started.

        Args:
            params: A list of additional arguments for pg_ctl. Defaults to None.
            wait: If True, waits until the operation is complete. Defaults to True.

        Returns:
            This instance of :class:`.PostgresNode`.
        """
        if not self.is_started:
            return self

        _params = [
            self._get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w" if wait else '-W',  # --wait or --no-wait
            "stop"
        ] + params  # yapf: disable

        execute_utility(_params, self.utils_log_file)

        self._maybe_stop_logger()
        self.is_started = False
        return self

    def kill(self, someone=None):
        """
            Kills the PostgreSQL node or a specified auxiliary process if the node is running.

            Args:
                someone: A key to the auxiliary process in the auxiliary_pids dictionary.
                         If None, the main PostgreSQL node process will be killed. Defaults to None.
            """
        if self.is_started:
            sig = signal.SIGKILL if os.name != 'nt' else signal.SIGBREAK
            if someone is None:
                os.kill(self.pid, sig)
            else:
                os.kill(self.auxiliary_pids[someone][0], sig)
            self.is_started = False

    def restart(self, params=[]):
        """
        Restart this node using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        _params = [
            self._get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-l", self.pg_log_file,
            "-w",  # wait
            "restart"
        ] + params  # yapf: disable

        try:
            error_code, out, error = execute_utility(_params, self.utils_log_file, verbose=True)
            if error and 'could not start server' in error:
                raise ExecUtilException
        except ExecUtilException as e:
            msg = 'Cannot restart node'
            files = self._collect_special_files()
            raise_from(StartNodeException(msg, files), e)

        self._maybe_start_logger()

        return self

    def reload(self, params=[]):
        """
        Asynchronously reload config files using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        _params = [
            self._get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "reload"
        ] + params  # yapf: disable

        execute_utility(_params, self.utils_log_file)

        return self

    def promote(self, dbname=None, username=None):
        """
        Promote standby instance to master using pg_ctl. For PostgreSQL versions
        below 10 some additional actions required to ensure that instance
        became writable and hence `dbname` and `username` parameters may be
        needed.

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        _params = [
            self._get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w",  # wait
            "promote"
        ]  # yapf: disable

        execute_utility(_params, self.utils_log_file)

        # for versions below 10 `promote` is asynchronous so we need to wait
        # until it actually becomes writable
        if self._pg_version < PgVer('10'):
            check_query = "SELECT pg_is_in_recovery()"

            self.poll_query_until(query=check_query,
                                  expected=False,
                                  dbname=dbname,
                                  username=username,
                                  max_attempts=0)    # infinite

        # node becomes master itself
        self._master = None

        return self

    def pg_ctl(self, params):
        """
        Invoke pg_ctl with params.

        Args:
            params: arguments for pg_ctl.

        Returns:
            Stdout + stderr of pg_ctl.
        """

        _params = [
            self._get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w"  # wait
        ] + params  # yapf: disable

        return execute_utility(_params, self.utils_log_file)

    def free_port(self):
        """
        Reclaim port owned by this node.
        NOTE: does not free auto selected ports.
        """

        if self._should_free_port:
            self._should_free_port = False
            release_port(self.port)

    def cleanup(self, max_attempts=3):
        """
        Stop node if needed and remove its data/logs directory.
        NOTE: take a look at TestgresConfig.node_cleanup_full.

        Args:
            max_attempts: how many times should we try to stop()?

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        self._try_shutdown(max_attempts)

        # choose directory to be removed
        if testgres_config.node_cleanup_full:
            rm_dir = self.base_dir    # everything
        else:
            rm_dir = self.data_dir    # just data, save logs

        self.os_ops.rmdirs(rm_dir, ignore_errors=True)

        return self

    @method_decorator(positional_args_hack(['dbname', 'query']))
    def psql(self,
             query=None,
             filename=None,
             dbname=None,
             username=None,
             input=None,
             **variables):
        """
        Execute a query using psql.

        Args:
            query: query to be executed.
            filename: file with a query.
            dbname: database name to connect to.
            username: database user name.
            input: raw input to be passed.
            **variables: vars to be set before execution.

        Returns:
            A tuple of (code, stdout, stderr).

        Examples:
            >>> psql('select 1')
            >>> psql('postgres', 'select 2')
            >>> psql(query='select 3', ON_ERROR_STOP=1)
        """

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        psql_params = [
            self._get_bin_path("psql"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
            "-X",  # no .psqlrc
            "-A",  # unaligned output
            "-t",  # print rows only
            "-q"  # run quietly
        ]  # yapf: disable

        # set variables before execution
        for key, value in iteritems(variables):
            psql_params.extend(["--set", '{}={}'.format(key, value)])

        # select query source
        if query:
            if self.os_ops.remote:
                psql_params.extend(("-c", '"{}"'.format(query)))
            else:
                psql_params.extend(("-c", query))
        elif filename:
            psql_params.extend(("-f", filename))
        else:
            raise QueryException('Query or filename must be provided')

        # should be the last one
        psql_params.append(dbname)
        if not self.os_ops.remote:
            # start psql process
            process = subprocess.Popen(psql_params,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)

            # wait until it finishes and get stdout and stderr
            out, err = process.communicate(input=input)
            return process.returncode, out, err
        else:
            status_code, out, err = self.os_ops.exec_command(psql_params, verbose=True, input=input)

            return status_code, out, err

    @method_decorator(positional_args_hack(['dbname', 'query']))
    def safe_psql(self, query=None, expect_error=False, **kwargs):
        """
        Execute a query using psql.

        Args:
            query: query to be executed.
            filename: file with a query.
            dbname: database name to connect to.
            username: database user name.
            input: raw input to be passed.
            expect_error: if True - fail if we didn't get ret
                          if False - fail if we got ret

            **kwargs are passed to psql().

        Returns:
            psql's output as str.
        """

        # force this setting
        kwargs['ON_ERROR_STOP'] = 1
        try:
            ret, out, err = self.psql(query=query, **kwargs)
        except ExecUtilException as e:
            ret = e.exit_code
            out = e.out
            err = e.message
        if ret:
            if expect_error:
                out = (err or b'').decode('utf-8')
            else:
                raise QueryException((err or b'').decode('utf-8'), query)
        elif expect_error:
            assert False, "Exception was expected, but query finished successfully: `{}` ".format(query)

        return out

    def dump(self,
             filename=None,
             dbname=None,
             username=None,
             format=DumpFormat.Plain):
        """
        Dump database into a file using pg_dump.
        NOTE: the file is not removed automatically.

        Args:
            filename: database dump taken by pg_dump.
            dbname: database name to connect to.
            username: database user name.
            format: format argument plain/custom/directory/tar.

        Returns:
            Path to a file containing dump.
        """

        # Check arguments
        if not isinstance(format, DumpFormat):
            try:
                format = DumpFormat(format)
            except ValueError:
                msg = 'Invalid format "{}"'.format(format)
                raise BackupException(msg)

        # Generate tmpfile or tmpdir
        def tmpfile():
            if format == DumpFormat.Directory:
                fname = self.os_ops.mkdtemp(prefix=TMP_DUMP)
            else:
                fname = self.os_ops.mkstemp(prefix=TMP_DUMP)
            return fname

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()
        filename = filename or tmpfile()

        _params = [
            self._get_bin_path("pg_dump"),
            "-p", str(self.port),
            "-h", self.host,
            "-f", filename,
            "-U", username,
            "-d", dbname,
            "-F", format.value
        ]  # yapf: disable

        execute_utility(_params, self.utils_log_file)

        return filename

    def restore(self, filename, dbname=None, username=None):
        """
        Restore database from pg_dump's file.

        Args:
            filename: database dump taken by pg_dump in custom/directory/tar formats.
            dbname: database name to connect to.
            username: database user name.
        """

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        _params = [
            self._get_bin_path("pg_restore"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
            "-d", dbname,
            filename
        ]  # yapf: disable

        # try pg_restore if dump is binary formate, and psql if not
        try:
            execute_utility(_params, self.utils_log_name)
        except ExecUtilException:
            self.psql(filename=filename, dbname=dbname, username=username)

    @method_decorator(positional_args_hack(['dbname', 'query']))
    def poll_query_until(self,
                         query,
                         dbname=None,
                         username=None,
                         max_attempts=0,
                         sleep_time=1,
                         expected=True,
                         commit=True,
                         suppress=None):
        """
        Run a query once per second until it returns 'expected'.
        Query should return a single value (1 row, 1 column).

        Args:
            query: query to be executed.
            dbname: database name to connect to.
            username: database user name.
            max_attempts: how many times should we try? 0 == infinite
            sleep_time: how much should we sleep after a failure?
            expected: what should be returned to break the cycle?
            commit: should (possible) changes be committed?
            suppress: a collection of exceptions to be suppressed.

        Examples:
            >>> poll_query_until('select true')
            >>> poll_query_until('postgres', "select now() > '01.01.2018'")
            >>> poll_query_until('select false', expected=True, max_attempts=4)
            >>> poll_query_until('select 1', suppress={testgres.OperationalError})
        """

        # sanity checks
        assert max_attempts >= 0
        assert sleep_time > 0
        attempts = 0
        while max_attempts == 0 or attempts < max_attempts:
            print(f"Pooling {attempts}")
            try:
                res = self.execute(dbname=dbname,
                                   query=query,
                                   username=username,
                                   commit=commit)

                if expected is None and res is None:
                    return    # done

                if res is None:
                    raise QueryException('Query returned None', query)

                # result set is not empty
                if len(res):
                    if len(res[0]) == 0:
                        raise QueryException('Query returned 0 columns', query)
                    if res[0][0] == expected:
                        return    # done
                # empty result set is considered as None
                elif expected is None:
                    return    # done

            except tuple(suppress or []):
                pass    # we're suppressing them

            time.sleep(sleep_time)
            attempts += 1

        raise TimeoutException('Query timeout')

    @method_decorator(positional_args_hack(['dbname', 'query']))
    def execute(self,
                query,
                dbname=None,
                username=None,
                password=None,
                commit=True):
        """
        Execute a query and return all rows as list.

        Args:
            query: query to be executed.
            dbname: database name to connect to.
            username: database user name.
            password: user's password.
            commit: should we commit this query?

        Returns:
            A list of tuples representing rows.
        """

        with self.connect(dbname=dbname,
                          username=username,
                          password=password,
                          autocommit=commit) as node_con:  # yapf: disable

            res = node_con.execute(query)

            return res

    def backup(self, **kwargs):
        """
        Perform pg_basebackup.

        Args:
            username: database user name.
            xlog_method: a method for collecting the logs ('fetch' | 'stream').
            base_dir: the base directory for data files and logs

        Returns:
            A smart object of type NodeBackup.
        """

        return NodeBackup(node=self, **kwargs)

    def replicate(self, name=None, slot=None, **kwargs):
        """
        Create a binary replica of this node.

        Args:
            name: replica's application name.
            slot: create a replication slot with the specified name.
            username: database user name.
            xlog_method: a method for collecting the logs ('fetch' | 'stream').
            base_dir: the base directory for data files and logs
        """

        # transform backup into a replica
        with clean_on_error(self.backup(**kwargs)) as backup:
            return backup.spawn_replica(name=name, destroy=True, slot=slot)

    def set_synchronous_standbys(self, standbys):
        """
        Set standby synchronization options. This corresponds to
        `synchronous_standby_names <https://www.postgresql.org/docs/current/static/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES>`_
        option. Note that :meth:`~.PostgresNode.reload` or
        :meth:`~.PostgresNode.restart` is needed for changes to take place.

        Args:
            standbys: either :class:`.First` or :class:`.Any` object specifying
                sychronization parameters or just a plain list of
                :class:`.PostgresNode`s replicas which would be equivalent
                to passing ``First(1, <list>)``. For PostgreSQL 9.5 and below
                it is only possible to specify a plain list of standbys as
                `FIRST` and `ANY` keywords aren't supported.

        Example::

            from testgres import get_new_node, First

            master = get_new_node().init().start()
            with master.replicate().start() as standby:
                master.append_conf("synchronous_commit = remote_apply")
                master.set_synchronous_standbys(First(1, [standby]))
                master.restart()

        """
        if self._pg_version >= PgVer('9.6'):
            if isinstance(standbys, Iterable):
                standbys = First(1, standbys)
        else:
            if isinstance(standbys, Iterable):
                standbys = u", ".join(u"\"{}\"".format(r.name)
                                      for r in standbys)
            else:
                raise TestgresException("Feature isn't supported in "
                                        "Postgres 9.5 and below")

        self.append_conf("synchronous_standby_names = '{}'".format(standbys))

    def catchup(self, dbname=None, username=None):
        """
        Wait until async replica catches up with its master.
        """

        if not self.master:
            raise TestgresException("Node doesn't have a master")

        if self._pg_version >= PgVer('10'):
            poll_lsn = "select pg_catalog.pg_current_wal_lsn()::text"
            wait_lsn = "select pg_catalog.pg_last_wal_replay_lsn() >= '{}'::pg_lsn"
        else:
            poll_lsn = "select pg_catalog.pg_current_xlog_location()::text"
            wait_lsn = "select pg_catalog.pg_last_xlog_replay_location() >= '{}'::pg_lsn"

        try:
            # fetch latest LSN
            lsn = self.master.execute(query=poll_lsn,
                                      dbname=dbname,
                                      username=username)[0][0]  # yapf: disable

            # wait until this LSN reaches replica
            self.poll_query_until(query=wait_lsn.format(lsn),
                                  dbname=dbname,
                                  username=username,
                                  max_attempts=0)    # infinite
        except Exception as e:
            raise_from(CatchUpException("Failed to catch up", poll_lsn), e)

    def publish(self, name, **kwargs):
        """
        Create publication for logical replication

        Args:
            pubname: publication name
            tables: tables names list
            dbname: database name where objects or interest are located
            username: replication username
        """
        return Publication(name=name, node=self, **kwargs)

    def subscribe(self,
                  publication,
                  name,
                  dbname=None,
                  username=None,
                  **params):
        """
        Create subscription for logical replication

        Args:
            name: subscription name
            publication: publication object obtained from publish()
            dbname: database name
            username: replication username
            params: subscription parameters (see documentation on `CREATE SUBSCRIPTION
                 <https://www.postgresql.org/docs/current/static/sql-createsubscription.html>`_
                 for details)
        """
        # yapf: disable
        return Subscription(name=name, node=self, publication=publication,
                            dbname=dbname, username=username, **params)
        # yapf: enable

    def pgbench(self,
                dbname=None,
                username=None,
                stdout=None,
                stderr=None,
                options=None):
        """
        Spawn a pgbench process.

        Args:
            dbname: database name to connect to.
            username: database user name.
            stdout: stdout file to be used by Popen.
            stderr: stderr file to be used by Popen.
            options: additional options for pgbench (list).

        Returns:
            Process created by subprocess.Popen.
        """
        if options is None:
            options = []

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        _params = [
            self._get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
        ] + options  # yapf: disable

        # should be the last one
        _params.append(dbname)

        proc = self.os_ops.exec_command(_params, stdout=stdout, stderr=stderr, wait_exit=True, get_process=True)

        return proc

    def pgbench_with_wait(self,
                          dbname=None,
                          username=None,
                          stdout=None,
                          stderr=None,
                          options=None):
        """
        Do pgbench command and wait.

        Args:
            dbname: database name to connect to.
            username: database user name.
            stdout: stdout file to be used by Popen.
            stderr: stderr file to be used by Popen.
            options: additional options for pgbench (list).
        """
        if options is None:
            options = []

        with self.pgbench(dbname, username, stdout, stderr, options) as pgbench:
            pgbench.wait()
        return

    def pgbench_init(self, **kwargs):
        """
        Small wrapper for pgbench_run().
        Sets initialize=True.

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        self.pgbench_run(initialize=True, **kwargs)

        return self

    def pgbench_run(self, dbname=None, username=None, options=[], **kwargs):
        """
        Run pgbench with some options.
        This event is logged (see self.utils_log_file).

        Args:
            dbname: database name to connect to.
            username: database user name.
            options: additional options for pgbench (list).

            **kwargs: named options for pgbench.
                Run pgbench --help to learn more.

        Returns:
            Stdout produced by pgbench.

        Examples:
            >>> pgbench_run(initialize=True, scale=2)
            >>> pgbench_run(time=10)
        """

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        _params = [
            self._get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
        ] + options  # yapf: disable

        for key, value in iteritems(kwargs):
            # rename keys for pgbench
            key = key.replace('_', '-')

            # append option
            if not isinstance(value, bool):
                _params.append('--{}={}'.format(key, value))
            else:
                assert value is True    # just in case
                _params.append('--{}'.format(key))

        # should be the last one
        _params.append(dbname)

        return execute_utility(_params, self.utils_log_file)

    def connect(self,
                dbname=None,
                username=None,
                password=None,
                autocommit=False):
        """
        Connect to a database.

        Args:
            dbname: database name to connect to.
            username: database user name.
            password: user's password.
            autocommit: commit each statement automatically. Also it should be
                set to `True` for statements requiring to be run outside
                a transaction? such as `VACUUM` or `CREATE DATABASE`.

        Returns:
            An instance of :class:`.NodeConnection`.
        """

        return NodeConnection(node=self,
                              dbname=dbname,
                              username=username,
                              password=password,
                              autocommit=autocommit)  # yapf: disable

    def table_checksum(self, table, dbname="postgres"):
        con = self.connect(dbname=dbname)

        curname = "cur_" + str(random.randint(0, 2 ** 48))

        con.execute("""
            DECLARE %s NO SCROLL CURSOR FOR
            SELECT t::text FROM %s as t
        """ % (curname, table))

        que = Queue(maxsize=50)
        sum = 0

        rows = con.execute("FETCH FORWARD 2000 FROM %s" % curname)
        if not rows:
            return 0
        que.put(rows)

        th = None
        if len(rows) == 2000:
            def querier():
                try:
                    while True:
                        rows = con.execute("FETCH FORWARD 2000 FROM %s" % curname)
                        if not rows:
                            break
                        que.put(rows)
                except Exception as e:
                    que.put(e)
                else:
                    que.put(None)

            th = threading.Thread(target=querier)
            th.start()
        else:
            que.put(None)

        while True:
            rows = que.get()
            if rows is None:
                break
            if isinstance(rows, Exception):
                raise rows
            # hash uses SipHash since Python3.4, therefore it is good enough
            for row in rows:
                sum += hash(row[0])

        if th is not None:
            th.join()

        con.execute("CLOSE %s; ROLLBACK;" % curname)

        con.close()
        return sum

    def pgbench_table_checksums(self, dbname="postgres",
                                pgbench_tables=('pgbench_branches',
                                                'pgbench_tellers',
                                                'pgbench_accounts',
                                                'pgbench_history')
                                ):
        return {(table, self.table_checksum(table, dbname))
                for table in pgbench_tables}

    def set_auto_conf(self, options, config='postgresql.auto.conf', rm_options={}):
        """
        Update or remove configuration options in the specified configuration file,
        updates the options specified in the options dictionary, removes any options
        specified in the rm_options set, and writes the updated configuration back to
        the file.

        Args:
            options (dict): A dictionary containing the options to update or add,
                            with the option names as keys and their values as values.
            config (str, optional): The name of the configuration file to update.
                                     Defaults to 'postgresql.auto.conf'.
            rm_options (set, optional): A set containing the names of the options to remove.
                                         Defaults to an empty set.
        """
        # parse postgresql.auto.conf
        path = os.path.join(self.data_dir, config)

        lines = self.os_ops.readlines(path)
        current_options = {}
        current_directives = []
        for line in lines:

            # ignore comments
            if line.startswith('#'):
                continue

            if line.strip() == '':
                continue

            if line.startswith('include'):
                current_directives.append(line)
                continue

            name, var = line.partition('=')[::2]
            name = name.strip()
            var = var.strip()
            var = var.strip('"')
            var = var.strip("'")

            # remove options specified in rm_options list
            if name in rm_options:
                continue

            current_options[name] = var

        for option in options:
            current_options[option] = options[option]

        auto_conf = ''
        for option in current_options:
            auto_conf += "{0} = '{1}'\n".format(
                option, current_options[option])

        for directive in current_directives:
            auto_conf += directive + "\n"

        self.os_ops.write(path, auto_conf, truncate=True)

    def upgrade_from(self, old_node, options=None):
        """
        Upgrade this node from an old node using pg_upgrade.

        Args:
            old_node: An instance of PostgresNode representing the old node.
        """
        if not os.path.exists(old_node.data_dir):
            raise Exception("Old node must be initialized")

        if not os.path.exists(self.data_dir):
            self.init()

        if not options:
            options = []

        pg_upgrade_binary = self._get_bin_path("pg_upgrade")

        if not os.path.exists(pg_upgrade_binary):
            raise Exception("pg_upgrade does not exist in the new node's binary path")

        upgrade_command = [
            pg_upgrade_binary,
            "--old-bindir", old_node.bin_dir,
            "--new-bindir", self.bin_dir,
            "--old-datadir", old_node.data_dir,
            "--new-datadir", self.data_dir,
            "--old-port", str(old_node.port),
            "--new-port", str(self.port),
        ]
        upgrade_command += options

        return self.os_ops.exec_command(upgrade_command)

    def _get_bin_path(self, filename):
        if self.bin_dir:
            bin_path = os.path.join(self.bin_dir, filename)
        else:
            bin_path = get_bin_path(filename)
        return bin_path


class NodeApp:

    def __init__(self, test_path, nodes_to_cleanup, os_ops=LocalOperations()):
        self.test_path = test_path
        self.nodes_to_cleanup = nodes_to_cleanup
        self.os_ops = os_ops

    def make_empty(
            self,
            base_dir=None):
        real_base_dir = os.path.join(self.test_path, base_dir)
        self.os_ops.rmdirs(real_base_dir, ignore_errors=True)
        self.os_ops.makedirs(real_base_dir)

        node = PostgresNode(base_dir=real_base_dir)
        node.should_rm_dirs = True
        self.nodes_to_cleanup.append(node)

        return node

    def make_simple(
            self,
            base_dir=None,
            set_replication=False,
            ptrack_enable=False,
            initdb_params=[],
            pg_options={},
            checksum=True):
        if checksum and '--data-checksums' not in initdb_params:
            initdb_params.append('--data-checksums')
        node = self.make_empty(base_dir)
        node.init(
            initdb_params=initdb_params, allow_streaming=set_replication)

        # set major version
        pg_version_file = self.os_ops.read(os.path.join(node.data_dir, 'PG_VERSION'))
        node.major_version_str = str(pg_version_file.rstrip())
        node.major_version = float(node.major_version_str)

        # Set default parameters
        options = {'max_connections': 100,
                   'shared_buffers': '10MB',
                   'fsync': 'off',
                   'wal_level': 'logical',
                   'hot_standby': 'off',
                   'log_line_prefix': '%t [%p]: [%l-1] ',
                   'log_statement': 'none',
                   'log_duration': 'on',
                   'log_min_duration_statement': 0,
                   'log_connections': 'on',
                   'log_disconnections': 'on',
                   'restart_after_crash': 'off',
                   'autovacuum': 'off'}

        # Allow replication in pg_hba.conf
        if set_replication:
            options['max_wal_senders'] = 10

        if ptrack_enable:
            options['ptrack.map_size'] = '1'
            options['shared_preload_libraries'] = 'ptrack'

        if node.major_version >= 13:
            options['wal_keep_size'] = '200MB'
        else:
            options['wal_keep_segments'] = '12'

        # set default values
        node.set_auto_conf(options)

        # Apply given parameters
        node.set_auto_conf(pg_options)

        # kludge for testgres
        # https://github.com/postgrespro/testgres/issues/54
        # for PG >= 13 remove 'wal_keep_segments' parameter
        if node.major_version >= 13:
            node.set_auto_conf({}, 'postgresql.conf', ['wal_keep_segments'])

        return node
