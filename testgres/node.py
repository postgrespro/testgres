# coding: utf-8

import io
import os
import shutil
import six
import subprocess
import tempfile
import time

from enum import Enum
from six import raise_from

from .cache import cached_initdb

from .config import TestgresConfig

from .connection import \
    NodeConnection, \
    InternalError,  \
    ProgrammingError

from .consts import \
    DATA_DIR, \
    LOGS_DIR, \
    PG_CONF_FILE, \
    HBA_CONF_FILE, \
    RECOVERY_CONF_FILE, \
    PG_LOG_FILE, \
    UTILS_LOG_FILE, \
    DEFAULT_XLOG_METHOD

from .exceptions import \
    CatchUpException,   \
    ExecUtilException,  \
    QueryException,     \
    StartNodeException, \
    TimeoutException

from .logger import TestgresLogger

from .utils import \
    get_bin_path, \
    file_tail, \
    pg_version_ge, \
    reserve_port, \
    release_port, \
    default_dbname, \
    default_username, \
    generate_app_name, \
    execute_utility, \
    method_decorator, \
    positional_args_hack


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
    def __init__(self, name=None, port=None, base_dir=None, use_logging=False):
        """
        Create a new node manually.

        Args:
            name: node's application name.
            port: port to accept connections.
            base_dir: path to node's data directory.
            use_logging: enable python logging.
        """

        # public
        self.host = '127.0.0.1'
        self.name = name or generate_app_name()
        self.port = port or reserve_port()
        self.base_dir = base_dir

        # private
        self._should_rm_dirs = base_dir is None
        self._should_free_port = port is None
        self._use_logging = use_logging
        self._logger = None
        self._master = None

        # create directories if needed
        self._prepare_dirs()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # stop node if necessary
        self.cleanup()

        # free port if necessary
        self.free_port()

    @property
    def master(self):
        return self._master

    @property
    def data_dir(self):
        return os.path.join(self.base_dir, DATA_DIR)

    @property
    def logs_dir(self):
        return os.path.join(self.base_dir, LOGS_DIR)

    @property
    def utils_log_name(self):
        return os.path.join(self.logs_dir, UTILS_LOG_FILE)

    @property
    def pg_log_name(self):
        return os.path.join(self.logs_dir, PG_LOG_FILE)

    def _assign_master(self, master):
        """NOTE: this is a private method!"""

        # now this node has a master
        self._master = master

    def _create_recovery_conf(self, username):
        """NOTE: this is a private method!"""

        # fetch master of this node
        master = self.master
        assert master is not None

        # yapf: disable
        conninfo = (
            u"application_name={} "
            u"port={} "
            u"user={} "
        ).format(master.name, master.port, username)

        # host is tricky
        try:
            import ipaddress
            ipaddress.ip_address(master.host)
            conninfo += u"hostaddr={}".format(master.host)
        except ValueError:
            conninfo += u"host={}".format(master.host)

        # yapf: disable
        line = (
            "primary_conninfo='{}'\n"
            "standby_mode=on\n"
        ).format(conninfo)

        self.append_conf(RECOVERY_CONF_FILE, line)

    def _prepare_dirs(self):
        """NOTE: this is a private method!"""

        if not self.base_dir:
            self.base_dir = tempfile.mkdtemp()

        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

    def _maybe_start_logger(self):
        """NOTE: this is a private method!"""

        if self._use_logging:
            # spawn new logger if it doesn't exist or stopped
            if not self._logger or not self._logger.is_alive():
                self._logger = TestgresLogger(self.name, self.pg_log_name)
                self._logger.start()

    def _maybe_stop_logger(self):
        """NOTE: this is a private method!"""

        if self._logger:
            self._logger.stop()

    def _format_verbose_error(self, message=None):
        """NOTE: this is a private method!"""

        # list of important files + N of last lines
        files = [
            (os.path.join(self.data_dir, PG_CONF_FILE), 0),
            (os.path.join(self.data_dir, HBA_CONF_FILE), 0),
            (os.path.join(self.data_dir, RECOVERY_CONF_FILE), 0),
            (self.pg_log_name, TestgresConfig.error_log_lines)
        ]

        error_text = ""

        # append message if asked to
        if message:
            error_text += message
            error_text += '\n' * 2

        for f, num_lines in files:
            # skip missing files
            if not os.path.exists(f):
                continue

            with io.open(f, "rb") as _f:
                if num_lines > 0:
                    # take last N lines of file
                    lines = b''.join(file_tail(_f, num_lines)).decode('utf-8')
                else:
                    # read whole file
                    lines = _f.read().decode('utf-8')

                # append contents
                error_text += u"{}:\n----\n{}\n".format(f, lines)

        return error_text

    def init(self,
             fsync=False,
             unix_sockets=True,
             allow_streaming=True,
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
        cached_initdb(self.data_dir, initdb_log, initdb_params)

        # initialize default config files
        self.default_conf(fsync=fsync,
                          unix_sockets=unix_sockets,
                          allow_streaming=allow_streaming)

        return self

    def default_conf(self,
                     fsync=False,
                     unix_sockets=True,
                     allow_streaming=True,
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

        postgres_conf = os.path.join(self.data_dir, PG_CONF_FILE)
        hba_conf = os.path.join(self.data_dir, HBA_CONF_FILE)

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

        # overwrite config file
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
                if pg_version_ge('9.6'):
                    wal_level = "replica"
                else:
                    wal_level = "hot_standby"

                # yapf: disable
                max_wal_senders = 10    # default in PG 10
                wal_keep_segments = 20  # for convenience
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
            execute_utility(_params, self.utils_log_name)
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
        Return postmaster's PID if node is running, else 0.
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
        _params += ["-D"] if pg_version_ge('9.5') else []
        _params += [self.data_dir]

        data = execute_utility(_params, self.utils_log_name)

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
            execute_utility(_params, self.utils_log_name)
        except ExecUtilException as e:
            msg = self._format_verbose_error('Cannot start node')
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

        execute_utility(_params, self.utils_log_name)

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
            execute_utility(_params, self.utils_log_name)
        except ExecUtilException as e:
            msg = self._format_verbose_error('Cannot restart node')
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

        execute_utility(_params, self.utils_log_name)

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

        return execute_utility(_params, self.utils_log_name)

    def free_port(self):
        """
        Reclaim port owned by this node.
        """

        if self._should_free_port:
            release_port(self.port)

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
                pass     # one more time
            except Exception:
                print('cannot stop node {}'.format(self.name))

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

    @method_decorator(positional_args_hack(['dbname', 'query']))
    def psql(self,
             query=None,
             filename=None,
             dbname=None,
             username=None,
             input=None):
        """
        Execute a query using psql.

        Args:
            query: query to be executed.
            filename: file with a query.
            dbname: database name to connect to.
            username: database user name.
            input: raw input to be passed.

        Returns:
            A tuple of (code, stdout, stderr).
        """

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

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

    @method_decorator(positional_args_hack(['dbname', 'query']))
    def safe_psql(self,
                  query,
                  dbname=None,
                  username=None,
                  input=None):
        """
        Execute a query using psql.

        Args:
            query: query to be executed.
            dbname: database name to connect to.
            username: database user name.
            input: raw input to be passed.

        Returns:
            psql's output as str.
        """

        ret, out, err = self.psql(query=query,
                                  dbname=dbname,
                                  username=username,
                                  input=input)
        if ret:
            raise QueryException((err or b'').decode('utf-8'))

        return out

    def dump(self, filename=None, dbname=None, username=None):
        """
        Dump database into a file using pg_dump.
        NOTE: the file is not removed automatically.

        Args:
            filename: database dump taken by pg_dump.
            dbname: database name to connect to.
            username: database user name.

        Returns:
            Path to a file containing dump.
        """

        def tmpfile():
            fd, fname = tempfile.mkstemp()
            os.close(fd)
            return fname

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()
        filename = filename or tmpfile()

        # yapf: disable
        _params = [
            get_bin_path("pg_dump"),
            "-p", str(self.port),
            "-h", self.host,
            "-f", filename,
            "-U", username,
            "-d", dbname
        ]

        execute_utility(_params, self.utils_log_name)

        return filename

    def restore(self, filename, dbname=None, username=None):
        """
        Restore database from pg_dump's file.

        Args:
            filename: database dump taken by pg_dump.
            dbname: database name to connect to.
            username: database user name.
        """

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
                         raise_programming_error=True,
                         raise_internal_error=True):
        """
        Run a query once a second until it returs 'expected'.
        Query should return single column.

        Args:
            query: query to be executed.
            dbname: database name to connect to.
            username: database user name.
            max_attempts: how many times should we try? 0 == infinite
            sleep_time: how much should we sleep after a failure?
            expected: what should be returned to break the cycle?
            commit: should (possible) changes be committed?
            raise_programming_error: enable ProgrammingError?
            raise_internal_error: enable InternalError?
        """

        # sanity checks
        assert max_attempts >= 0
        assert sleep_time > 0

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
                          password=password) as node_con:

            res = node_con.execute(query)

            if commit:
                node_con.commit()

            return res

    def backup(self, username=None, xlog_method=DEFAULT_XLOG_METHOD):
        """
        Perform pg_basebackup.

        Args:
            username: database user name.
            xlog_method: a method for collecting the logs ('fetch' | 'stream').

        Returns:
            A smart object of type NodeBackup.
        """

        from .backup import NodeBackup
        return NodeBackup(node=self,
                          username=username,
                          xlog_method=xlog_method)

    def replicate(self,
                  name=None,
                  username=None,
                  xlog_method=DEFAULT_XLOG_METHOD,
                  use_logging=False):
        """
        Create a binary replica of this node.

        Args:
            name: replica's application name.
            username: database user name.
            xlog_method: a method for collecting the logs ('fetch' | 'stream').
            use_logging: enable python logging.
        """

        backup = self.backup(username=username, xlog_method=xlog_method)

        # transform backup into a replica
        return backup.spawn_replica(name=name,
                                    destroy=True,
                                    use_logging=use_logging)

    def catchup(self, dbname=None, username=None):
        """
        Wait until async replica catches up with its master.
        """

        if not self.master:
            raise CatchUpException("Node doesn't have a master")

        if pg_version_ge('10'):
            poll_lsn = "select pg_current_wal_lsn()::text"
            wait_lsn = "select pg_last_wal_replay_lsn() >= '{}'::pg_lsn"
        else:
            poll_lsn = "select pg_current_xlog_location()::text"
            wait_lsn = "select pg_last_xlog_replay_location() >= '{}'::pg_lsn"

        try:
            # fetch latest LSN
            lsn = self.master.execute(query=poll_lsn,
                                      dbname=dbname,
                                      username=username)[0][0]

            # wait until this LSN reaches replica
            self.poll_query_until(
                query=wait_lsn.format(lsn),
                dbname=dbname,
                username=username,
                max_attempts=0)    # infinite
        except Exception as e:
            raise_from(CatchUpException('Failed to catch up'), e)

    def pgbench(self,
                dbname=None,
                username=None,
                stdout=None,
                stderr=None,
                options=[]):
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

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        # yapf: disable
        _params = [
            get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
        ] + options

        # should be the last one
        _params.append(dbname)

        proc = subprocess.Popen(_params, stdout=stdout, stderr=stderr)

        return proc

    def pgbench_init(self, **kwargs):
        """
        Small wrapper for pgbench_run().
        Sets initialize=True.

        Returns:
            This instance of PostgresNode.
        """

        self.pgbench_run(initialize=True, **kwargs)

        return self

    def pgbench_run(self,
                    dbname=None,
                    username=None,
                    options=[],
                    **kwargs):
        """
        Run pgbench with some options.
        This event is logged (see self.utils_log_name).

        Args:
            dbname: database name to connect to.
            username: database user name.
            options: additional options for pgbench (list).

            **kwargs: named options for pgbench.
                Examples:
                    pgbench_run(initialize=True, scale=2)
                    pgbench_run(time=10)
                Run pgbench --help to learn more.

        Returns:
            Stdout produced by pgbench.
        """

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()

        # yapf: disable
        _params = [
            get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
        ] + options

        for key, value in six.iteritems(kwargs):
            # rename keys for pgbench
            key = key.replace('_', '-')

            # append option
            if not isinstance(value, bool):
                _params.append('--{}={}'.format(key, value))
            else:
                assert value is True  # just in case
                _params.append('--{}'.format(key))

        # should be the last one
        _params.append(dbname)

        return execute_utility(_params, self.utils_log_name)

    def connect(self, dbname=None, username=None, password=None):
        """
        Connect to a database.

        Args:
            dbname: database name to connect to.
            username: database user name.
            password: user's password.

        Returns:
            An instance of NodeConnection.
        """

        return NodeConnection(node=self,
                              dbname=dbname,
                              username=username,
                              password=password)
