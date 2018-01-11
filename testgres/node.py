# coding: utf-8

import io
import os
import shutil
import subprocess
import tempfile
import time

from enum import Enum
from six import raise_from

from .cache import cached_initdb as _cached_initdb

from .config import TestgresConfig

from .connection import \
    NodeConnection, \
    InternalError,  \
    ProgrammingError

from .consts import \
    DATA_DIR as _DATA_DIR, \
    LOGS_DIR as _LOGS_DIR, \
    PG_LOG_FILE as _PG_LOG_FILE, \
    UTILS_LOG_FILE as _UTILS_LOG_FILE, \
    DEFAULT_XLOG_METHOD as _DEFAULT_XLOG_METHOD

from .exceptions import \
    CatchUpException,   \
    ExecUtilException,  \
    QueryException,     \
    StartNodeException, \
    TimeoutException

from .logger import TestgresLogger

from .utils import \
    get_bin_path, \
    pg_version_ge as _pg_version_ge, \
    reserve_port as _reserve_port, \
    release_port as _release_port, \
    default_username as _default_username, \
    execute_utility as _execute_utility


class NodeStatus(Enum):
    """
    Status of a PostgresNode
    """

    Running, Stopped, Uninitialized = range(3)

    # for Python 3.x
    def __bool__(self):
        return self.value == NodeStatus.Running.value

    # for Python 2.x
    __nonzero__ = __bool__


class PostgresNode(object):
    def __init__(self,
                 name,
                 port=None,
                 base_dir=None,
                 use_logging=False,
                 master=None):
        global bound_ports

        # public
        self.master = master
        self.name = name
        self.host = '127.0.0.1'
        self.port = port or _reserve_port()
        self.base_dir = base_dir

        # private
        self._should_free_port = port is None
        self._should_rm_dirs = base_dir is None
        self._use_logging = use_logging
        self._logger = None

        # create directories if needed
        self._prepare_dirs()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        global bound_ports

        # stop node if necessary
        self.cleanup()

        # free port if necessary
        self.free_port()

    @property
    def data_dir(self):
        return os.path.join(self.base_dir, _DATA_DIR)

    @property
    def logs_dir(self):
        return os.path.join(self.base_dir, _LOGS_DIR)

    @property
    def utils_log_name(self):
        return os.path.join(self.logs_dir, _UTILS_LOG_FILE)

    @property
    def pg_log_name(self):
        return os.path.join(self.logs_dir, _PG_LOG_FILE)

    def _create_recovery_conf(self, username, master):
        # yapf: disable
        conninfo = (
            u"user={} "
            u"port={} "
            u"host={} "
            u"application_name={}"
        ).format(username, master.port, master.host, master.name)

        # yapf: disable
        line = (
            "primary_conninfo='{}'\n"
            "standby_mode=on\n"
        ).format(conninfo)

        self.append_conf("recovery.conf", line)

    def _prepare_dirs(self):
        if not self.base_dir or not os.path.exists(self.base_dir):
            self.base_dir = tempfile.mkdtemp()

        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

    def _maybe_start_logger(self):
        if self._use_logging:
            # spawn new logger if it doesn't exist or stopped
            if not self._logger or not self._logger.is_alive():
                self._logger = TestgresLogger(self.name, self.pg_log_name)
                self._logger.start()

    def _maybe_stop_logger(self):
        if self._logger:
            self._logger.stop()

    def _format_verbose_error(self):
        # list of important files
        files = [
            os.path.join(self.data_dir, "postgresql.conf"),
            os.path.join(self.data_dir, "recovery.conf"),
            os.path.join(self.data_dir, "pg_hba.conf"),
            self.pg_log_name  # main log file
        ]

        error_text = ""

        for f in files:
            # skip missing files
            if not os.path.exists(f):
                continue

            # append contents
            with io.open(f, "r") as _f:
                lines = _f.read()
                error_text += u"{}:\n----\n{}\n".format(f, lines)

        return error_text

    def init(self,
             fsync=False,
             unix_sockets=True,
             allow_streaming=False,
             initdb_params=[]):
        """
        Perform initdb for this node.

        Args:
            fsync: should this node use fsync to keep data safe?
            unix_sockets: should we enable UNIX sockets?
            allow_streaming: should this node add a hba entry for replication?
            initdb_params: parameters for initdb (list).

        Returns:
            This instance of PostgresNode.
        """

        # create directories if needed
        self._prepare_dirs()

        # initialize this PostgreSQL node
        initdb_log = os.path.join(self.logs_dir, "initdb.log")
        _cached_initdb(self.data_dir, initdb_log, initdb_params)

        # initialize default config files
        self.default_conf(fsync=fsync,
                          unix_sockets=unix_sockets,
                          allow_streaming=allow_streaming)

        return self

    def default_conf(self,
                     fsync=False,
                     unix_sockets=True,
                     allow_streaming=False,
                     log_statement='all'):
        """
        Apply default settings to this node.

        Args:
            fsync: should this node use fsync to keep data safe?
            unix_sockets: should we enable UNIX sockets?
            allow_streaming: should this node add a hba entry for replication?
            log_statement: one of ('all', 'off', 'mod', 'ddl').

        Returns:
            This instance of PostgresNode.
        """

        postgres_conf = os.path.join(self.data_dir, "postgresql.conf")
        hba_conf = os.path.join(self.data_dir, "pg_hba.conf")

        # filter lines in hba file
        with io.open(hba_conf, "r+") as conf:
            # get rid of comments and blank lines
            lines = [
                s for s in conf.readlines()
                if len(s.strip()) > 0 and not s.startswith('#')
            ]

            # write filtered lines
            conf.seek(0)
            conf.truncate()
            conf.writelines(lines)

            # replication-related settings
            if allow_streaming:
                # get auth method for host or local users
                def get_auth_method(t):
                    return next((s.split()[-1] for s in lines
                                 if s.startswith(t)), 'trust')

                # get auth methods
                auth_local = get_auth_method('local')
                auth_host = get_auth_method('host')

                # yapf: disable
                new_lines = [
                    u"local\treplication\tall\t\t\t{}\n".format(auth_local),
                    u"host\treplication\tall\t127.0.0.1/32\t{}\n".format(auth_host),
                    u"host\treplication\tall\t::1/128\t\t{}\n".format(auth_host)
                ]

                # write missing lines
                for line in new_lines:
                    if line not in lines:
                        conf.write(line)

        # overwrite postgresql.conf file
        with io.open(postgres_conf, "w") as conf:
            # remove old lines
            conf.truncate()

            if not fsync:
                conf.write(u"fsync = off\n")

            # yapf: disable
            conf.write(u"log_statement = {}\n"
                       u"listen_addresses = '{}'\n"
                       u"port = {}\n".format(log_statement,
                                             self.host,
                                             self.port))

            # replication-related settings
            if allow_streaming:

                # select a proper wal_level for PostgreSQL
                if _pg_version_ge('9.6'):
                    wal_level = "replica"
                else:
                    wal_level = "hot_standby"

                # yapf: disable
                max_wal_senders = 5
                wal_keep_segments = 20
                conf.write(u"hot_standby = on\n"
                           u"max_wal_senders = {}\n"
                           u"wal_keep_segments = {}\n"
                           u"wal_level = {}\n".format(max_wal_senders,
                                                      wal_keep_segments,
                                                      wal_level))

            # disable UNIX sockets if asked to
            if not unix_sockets:
                conf.write(u"unix_socket_directories = ''\n")

        return self

    def append_conf(self, filename, string):
        """
        Append line to a config file (i.e. postgresql.conf).

        Args:
            filename: name of the config file.
            string: string to be appended to config.

        Returns:
            This instance of PostgresNode.
        """

        config_name = os.path.join(self.data_dir, filename)
        with io.open(config_name, "a") as conf:
            conf.write(u"".join([string, '\n']))

        return self

    def status(self):
        """
        Check this node's status.

        Returns:
            An instance of NodeStatus.
        """

        try:
            # yapf: disable
            _params = [
                get_bin_path("pg_ctl"),
                "-D", self.data_dir,
                "status"
            ]
            _execute_utility(_params, self.utils_log_name)
            return NodeStatus.Running

        except ExecUtilException as e:
            # Node is not running
            if e.exit_code == 3:
                return NodeStatus.Stopped

            # Node has no file dir
            elif e.exit_code == 4:
                return NodeStatus.Uninitialized

    def get_pid(self):
        """
        Return postmaster's pid if node is running, else 0.
        """

        if self.status():
            with io.open(os.path.join(self.data_dir, 'postmaster.pid')) as f:
                return int(f.readline())

        # for clarity
        return 0

    def get_control_data(self):
        """
        Return contents of pg_control file.
        """

        # this one is tricky (blame PG 9.4)
        _params = [get_bin_path("pg_controldata")]
        _params += ["-D"] if _pg_version_ge('9.5') else []
        _params += [self.data_dir]

        data = _execute_utility(_params, self.utils_log_name)

        out_dict = {}

        for line in data.splitlines():
            key, _, value = line.partition(':')
            out_dict[key.strip()] = value.strip()

        return out_dict

    def start(self, params=[]):
        """
        Start this node using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of PostgresNode.
        """

        # yapf: disable
        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-l", self.pg_log_name,
            "-w",  # wait
            "start"
        ] + params

        try:
            _execute_utility(_params, self.utils_log_name)
        except ExecUtilException as e:
            msg = (
                u"Cannot start node\n"
                u"{}\n"    # pg_ctl log
            ).format(self._format_verbose_error())
            raise_from(StartNodeException(msg), e)

        self._maybe_start_logger()

        return self

    def stop(self, params=[]):
        """
        Stop this node using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of PostgresNode.
        """

        # yapf: disable
        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w",  # wait
            "stop"
        ] + params

        _execute_utility(_params, self.utils_log_name)

        self._maybe_stop_logger()

        return self

    def restart(self, params=[]):
        """
        Restart this node using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of PostgresNode.
        """

        # yapf: disable
        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-l", self.pg_log_name,
            "-w",  # wait
            "restart"
        ] + params

        try:
            _execute_utility(_params, self.utils_log_name)
        except ExecUtilException as e:
            msg = (
                u"Cannot restart node\n"
                u"{}\n"    # pg_ctl log
            ).format(self._format_verbose_error())
            raise_from(StartNodeException(msg), e)

        self._maybe_start_logger()

        return self

    def reload(self, params=[]):
        """
        Reload config files using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of PostgresNode.
        """

        # yapf: disable
        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w",  # wait
            "reload"
        ] + params

        _execute_utility(_params, self.utils_log_name)

    def pg_ctl(self, params):
        """
        Invoke pg_ctl with params.

        Args:
            params: arguments for pg_ctl.

        Returns:
            Stdout + stderr of pg_ctl.
        """

        # yapf: disable
        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w"  # wait
        ] + params

        return _execute_utility(_params, self.utils_log_name)

    def free_port(self):
        """
        Reclaim port owned by this node.
        """

        if self._should_free_port:
            _release_port(self.port)

    def cleanup(self, max_attempts=3):
        """
        Stop node if needed and remove its data directory.

        Args:
            max_attempts: how many times should we try to stop()?

        Returns:
            This instance of PostgresNode.
        """

        attempts = 0

        # try stopping server
        while attempts < max_attempts:
            try:
                self.stop()
                break    # OK
            except ExecUtilException:
                pass    # one more time
            except Exception:
                break    # screw this

            attempts += 1

        # remove directory tree if necessary
        if self._should_rm_dirs:

            # choose directory to be removed
            if TestgresConfig.node_cleanup_full:
                rm_dir = self.base_dir    # everything
            else:
                rm_dir = self.data_dir    # just data, save logs

            shutil.rmtree(rm_dir, ignore_errors=True)

        return self

    def psql(self,
             dbname,
             query=None,
             filename=None,
             username=None,
             input=None):
        """
        Execute a query using psql.

        Args:
            dbname: database name to connect to.
            username: database user name.
            filename: file with a query.
            query: query to be executed.
            input: raw input to be passed.

        Returns:
            A tuple of (code, stdout, stderr).
        """

        # Set default username
        username = username or _default_username()

        # yapf: disable
        psql_params = [
            get_bin_path("psql"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
            "-X",  # no .psqlrc
            "-A",  # unaligned output
            "-t",  # print rows only
            "-q",  # run quietly
            dbname
        ]

        # select query source
        if query:
            psql_params.extend(("-c", query))
        elif filename:
            psql_params.extend(("-f", filename))
        elif input:
            pass
        else:
            raise QueryException('Query or filename must be provided')

        # start psql process
        process = subprocess.Popen(
            psql_params,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        # wait until it finishes and get stdout and stderr
        out, err = process.communicate(input=input)
        return process.returncode, out, err

    def safe_psql(self, dbname, query, username=None, input=None):
        """
        Execute a query using psql.

        Args:
            dbname: database name to connect to.
            username: database user name.
            query: query to be executed.
            input: raw input to be passed.

        Returns:
            psql's output as str.
        """

        ret, out, err = self.psql(dbname, query, username=username, input=input)
        if ret:
            err = '' if not err else err.decode('utf-8')
            raise QueryException(err)

        return out

    def dump(self, dbname, username=None, filename=None):
        """
        Dump database into a file using pg_dump.
        NOTE: the file is not removed automatically.

        Args:
            dbname: database name to connect to.
            username: database user name.
            filename: output file.

        Returns:
            Path to a file containing dump.
        """

        # Set default arguments
        username = username or _default_username()
        f, filename = filename or tempfile.mkstemp()
        os.close(f)

        # yapf: disable
        _params = [
            get_bin_path("pg_dump"),
            "-p", str(self.port),
            "-h", self.host,
            "-f", filename,
            "-U", username,
            "-d", dbname
        ]

        _execute_utility(_params, self.utils_log_name)

        return filename

    def restore(self, dbname, filename, username=None):
        """
        Restore database from pg_dump's file.

        Args:
            dbname: database name to connect to.
            filename: database dump taken by pg_dump.
        """

        self.psql(dbname=dbname, filename=filename, username=username)

    def poll_query_until(self,
                         dbname,
                         query,
                         username=None,
                         max_attempts=0,
                         sleep_time=1,
                         expected=True,
                         commit=True,
                         raise_programming_error=True,
                         raise_internal_error=True):
        """
        Run a query once a second until it returs 'expected'.
        Query should return single column.

        Args:
            dbname: database name to connect to.
            query: query to be executed.
            username: database user name.
            max_attempts: how many times should we try? 0 == infinite
            sleep_time: how much should we sleep after a failure?
            expected: what should be returned to break the cycle?
            commit: should (possible) changes be committed?
            raise_programming_error: enable ProgrammingError?
            raise_internal_error: enable InternalError?
        """

        # sanity checks
        assert (max_attempts >= 0)
        assert (sleep_time > 0)

        attempts = 0
        while max_attempts == 0 or attempts < max_attempts:
            try:
                res = self.execute(
                    dbname=dbname,
                    query=query,
                    username=username,
                    commit=commit)

                if expected is None and res is None:
                    return    # done

                if res is None:
                    raise QueryException('Query returned None')

                if len(res) == 0:
                    raise QueryException('Query returned 0 rows')

                if len(res[0]) == 0:
                    raise QueryException('Query returned 0 columns')

                if res[0][0] == expected:
                    return    # done

            except ProgrammingError as e:
                if raise_programming_error:
                    raise e

            except InternalError as e:
                if raise_internal_error:
                    raise e

            time.sleep(sleep_time)
            attempts += 1

        raise TimeoutException('Query timeout')

    def execute(self, dbname, query, username=None, commit=True):
        """
        Execute a query and return all rows as list.

        Args:
            dbname: database name to connect to.
            query: query to be executed.
            username: database user name.
            commit: should we commit this query?

        Returns:
            A list of tuples representing rows.
        """

        with self.connect(dbname, username) as node_con:
            res = node_con.execute(query)
            if commit:
                node_con.commit()
            return res

    def backup(self, username=None, xlog_method=_DEFAULT_XLOG_METHOD):
        """
        Perform pg_basebackup.

        Args:
            username: database user name.
            xlog_method: a method for collecting the logs ('fetch' | 'stream').

        Returns:
            A smart object of type NodeBackup.
        """

        from .backup import NodeBackup
        return NodeBackup(node=self, username=username, xlog_method=xlog_method)

    def replicate(self,
                  name,
                  username=None,
                  xlog_method=_DEFAULT_XLOG_METHOD,
                  use_logging=False):
        """
        Create a binary replica of this node.

        Args:
            name: replica's name.
            username: database user name.
            xlog_method: a method for collecting the logs ('fetch' | 'stream').
            use_logging: enable python logging.
        """

        backup = self.backup(username=username, xlog_method=xlog_method)
        return backup.spawn_replica(name=name, use_logging=use_logging)

    def catchup(self, dbname='postgres', username=None):
        """
        Wait until async replica catches up with its master.
        """

        master = self.master

        if _pg_version_ge('10'):
            poll_lsn = "select pg_current_wal_lsn()::text"
            wait_lsn = "select pg_last_wal_replay_lsn() >= '{}'::pg_lsn"
        else:
            poll_lsn = "select pg_current_xlog_location()::text"
            wait_lsn = "select pg_last_xlog_replay_location() >= '{}'::pg_lsn"

        if not master:
            raise CatchUpException("Master node is not specified")

        try:
            # fetch latest LSN
            lsn = master.execute(
                dbname=dbname, username=username, query=poll_lsn)[0][0]

            # wait until this LSN reaches replica
            self.poll_query_until(
                dbname=dbname,
                username=username,
                query=wait_lsn.format(lsn),
                max_attempts=0)    # infinite
        except Exception as e:
            raise_from(CatchUpException('Failed to catch up'), e)

    def pgbench(self, dbname='postgres', stdout=None, stderr=None, options=[]):
        """
        Spawn a pgbench process.

        Args:
            dbname: database name to connect to.
            stdout: stdout file to be used by Popen.
            stderr: stderr file to be used by Popen.
            options: additional options for pgbench (list).

        Returns:
            Process created by subprocess.Popen.
        """

        # yapf: disable
        _params = [
            get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
        ] + options + [dbname]

        proc = subprocess.Popen(_params, stdout=stdout, stderr=stderr)

        return proc

    def pgbench_run(self, dbname='postgres', options=[]):
        """
        Run pgbench with some options.
        This event is logged (see self.utils_log_name).

        Args:
            dbname: database name to connect to.
            options: additional options for pgbench (list).

        Returns:
            Stdout produced by pgbench.
        """

        # yapf: disable
        _params = [
            get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
        ] + options + [dbname]

        return _execute_utility(_params, self.utils_log_name)

    def connect(self, dbname='postgres', username=None):
        """
        Connect to a database.

        Args:
            dbname: database name to connect to.
            username: database user name.

        Returns:
            An instance of NodeConnection.
        """

        return NodeConnection(parent_node=self,
                              host=self.host,
                              dbname=dbname,
                              username=username)
