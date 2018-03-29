# coding: utf-8

import io
import os
import psutil
import subprocess
import time

from collections import Iterable
from shutil import rmtree
from six import raise_from, iteritems
from tempfile import mkstemp, mkdtemp

from .enums import NodeStatus, ProcessType

from .cache import cached_initdb

from .config import testgres_config

from .connection import \
    NodeConnection, \
    InternalError,  \
    ProgrammingError

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
    MAX_WAL_SENDERS, \
    MAX_REPLICATION_SLOTS, \
    WAL_KEEP_SEGMENTS

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
    TestgresException

from .logger import TestgresLogger

from .standby import First

from .utils import \
    eprint, \
    get_bin_path, \
    file_tail, \
    pg_version_ge, \
    reserve_port, \
    release_port, \
    execute_utility, \
    clean_on_error

from .backup import NodeBackup


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
    def __init__(self, name=None, port=None, base_dir=None):
        """
        PostgresNode constructor.

        Args:
            name: node's application name.
            port: port to accept connections.
            base_dir: path to node's data directory.
        """

        # private
        self._should_free_port = port is None
        self._base_dir = base_dir
        self._logger = None
        self._master = None

        # basic
        self.host = '127.0.0.1'
        self.name = name or generate_app_name()
        self.port = port or reserve_port()

        # defaults for __exit__()
        self.cleanup_on_good_exit = testgres_config.node_cleanup_on_good_exit
        self.cleanup_on_bad_exit = testgres_config.node_cleanup_on_bad_exit
        self.shutdown_max_attempts = 3

        # NOTE: for compatibility
        self.utils_log_name = self.utils_log_file
        self.pg_log_name = self.pg_log_file

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
            with io.open(pid_file) as f:
                return int(f.readline())

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
        children = psutil.Process(self.pid).children()

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
            self._base_dir = mkdtemp(prefix=TMP_NODE)

        # NOTE: it's safe to create a new dir
        if not os.path.exists(self._base_dir):
            os.makedirs(self._base_dir)

        return self._base_dir

    @property
    def logs_dir(self):
        path = os.path.join(self.base_dir, LOGS_DIR)

        # NOTE: it's safe to create a new dir
        if not os.path.exists(path):
            os.makedirs(path)

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

        conninfo = (
            u"application_name={} "
            u"port={} "
            u"user={} "
        ).format(self.name, master.port, username)  # yapf: disable

        # host is tricky
        try:
            import ipaddress
            ipaddress.ip_address(master.host)
            conninfo += u"hostaddr={}".format(master.host)
        except ValueError:
            conninfo += u"host={}".format(master.host)

        line = (
            "primary_conninfo='{}'\n"
            "standby_mode=on\n"
        ).format(conninfo)  # yapf: disable

        if slot:
            # Connect to master for some additional actions
            with master.connect(username=username) as con:
                # check if slot already exists
                res = con.execute("""
                    select exists (
                        select from pg_catalog.pg_replication_slots
                        where slot_name = %s
                    )
                """, slot)

                if res[0][0]:
                    raise TestgresException(
                        "Slot '{}' already exists".format(slot))

                # TODO: we should drop this slot after replica's cleanup()
                con.execute("""
                    select pg_catalog.pg_create_physical_replication_slot(%s)
                """, slot)

            line += "primary_slot_name={}\n".format(slot)

        self.append_conf(RECOVERY_CONF_FILE, line)

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
            if not os.path.exists(f):
                continue

            with io.open(f, "rb") as _f:
                if num_lines > 0:
                    # take last N lines of file
                    lines = b''.join(file_tail(_f, num_lines)).decode('utf-8')
                else:
                    # read whole file
                    lines = _f.read().decode('utf-8')

                # fill list
                result.append((f, lines))

        return result

    def init(self, initdb_params=None, **kwargs):
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
            params=initdb_params)

        # initialize default config files
        self.default_conf(**kwargs)

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
            This instance of :class:`.PostgresNode`.
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

                new_lines = [
                    u"local\treplication\tall\t\t\t{}\n".format(auth_local),
                    u"host\treplication\tall\t127.0.0.1/32\t{}\n".format(auth_host),
                    u"host\treplication\tall\t::1/128\t\t{}\n".format(auth_host)
                ]  # yapf: disable

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

            conf.write(u"log_statement = {}\n"
                       u"listen_addresses = '{}'\n"
                       u"port = {}\n".format(log_statement,
                                             self.host,
                                             self.port))  # yapf: disable

            # replication-related settings
            if allow_streaming:

                # select a proper wal_level for PostgreSQL
                if pg_version_ge('9.6'):
                    wal_level = "replica"
                else:
                    wal_level = "hot_standby"

                conf.write(u"hot_standby = on\n"
                           u"max_wal_senders = {}\n"
                           u"max_replication_slots = {}\n"
                           u"wal_keep_segments = {}\n"
                           u"wal_level = {}\n".format(MAX_WAL_SENDERS,
                                                      MAX_REPLICATION_SLOTS,
                                                      WAL_KEEP_SEGMENTS,
                                                      wal_level))  # yapf: disable

            # disable UNIX sockets if asked to
            if not unix_sockets:
                conf.write(u"unix_socket_directories = ''\n")

        return self

    @method_decorator(positional_args_hack(['filename', 'line']))
    def append_conf(self, line, filename=PG_CONF_FILE):
        """
        Append line to a config file.

        Args:
            line: string to be appended to config.
            filename: config file (postgresql.conf by default).

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        config_name = os.path.join(self.data_dir, filename)
        with io.open(config_name, 'a') as conf:
            conf.write(u''.join([line, '\n']))

        return self

    def status(self):
        """
        Check this node's status.

        Returns:
            An instance of :class:`.NodeStatus`.
        """

        try:
            _params = [
                get_bin_path("pg_ctl"),
                "-D", self.data_dir,
                "status"
            ]  # yapf: disable
            execute_utility(_params, self.utils_log_file)
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
        _params = [get_bin_path("pg_controldata")]
        _params += ["-D"] if pg_version_ge('9.5') else []
        _params += [self.data_dir]

        data = execute_utility(_params, self.utils_log_file)

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
            This instance of :class:`.PostgresNode`.
        """

        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-l", self.pg_log_file,
            "-w",  # wait
            "start"
        ] + params  # yapf: disable

        try:
            execute_utility(_params, self.utils_log_file)
        except ExecUtilException as e:
            msg = 'Cannot start node'
            files = self._collect_special_files()
            raise_from(StartNodeException(msg, files), e)

        self._maybe_start_logger()

        return self

    def stop(self, params=[]):
        """
        Stop this node using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w",  # wait
            "stop"
        ] + params  # yapf: disable

        execute_utility(_params, self.utils_log_file)

        self._maybe_stop_logger()

        return self

    def restart(self, params=[]):
        """
        Restart this node using pg_ctl.

        Args:
            params: additional arguments for pg_ctl.

        Returns:
            This instance of :class:`.PostgresNode`.
        """

        _params = [
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-l", self.pg_log_file,
            "-w",  # wait
            "restart"
        ] + params  # yapf: disable

        try:
            execute_utility(_params, self.utils_log_file)
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
            get_bin_path("pg_ctl"),
            "-D", self.data_dir,
            "-w",  # wait
            "reload"
        ] + params  # yapf: disable

        execute_utility(_params, self.utils_log_file)

    def pg_ctl(self, params):
        """
        Invoke pg_ctl with params.

        Args:
            params: arguments for pg_ctl.

        Returns:
            Stdout + stderr of pg_ctl.
        """

        _params = [
            get_bin_path("pg_ctl"),
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

        rmtree(rm_dir, ignore_errors=True)

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
        ]  # yapf: disable

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
    def safe_psql(self, query=None, **kwargs):
        """
        Execute a query using psql.

        Args:
            query: query to be executed.
            filename: file with a query.
            dbname: database name to connect to.
            username: database user name.
            input: raw input to be passed.

        Returns:
            psql's output as str.
        """

        ret, out, err = self.psql(query=query, **kwargs)
        if ret:
            raise QueryException((err or b'').decode('utf-8'), query)

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
            fd, fname = mkstemp(prefix=TMP_DUMP)
            os.close(fd)
            return fname

        # Set default arguments
        dbname = dbname or default_dbname()
        username = username or default_username()
        filename = filename or tmpfile()

        _params = [
            get_bin_path("pg_dump"),
            "-p", str(self.port),
            "-h", self.host,
            "-f", filename,
            "-U", username,
            "-d", dbname
        ]  # yapf: disable

        execute_utility(_params, self.utils_log_file)

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
                    raise QueryException('Query returned None', query)

                if len(res) == 0:
                    raise QueryException('Query returned 0 rows', query)

                if len(res[0]) == 0:
                    raise QueryException('Query returned 0 columns', query)

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
                          password=password) as node_con:  # yapf: disable

            res = node_con.execute(query)

            if commit:
                node_con.commit()

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
                sychronization parameters. It is also possible to pass simply
                a list of replicas which would be equivalent to passing
                ``First(1, <list>)``

        Example::

            master = get_new_node().init().start()
            with master.replicate.start() as standby:
                master.append_conf("synchronous_commit = remote_apply")
                master.set_synchronous_standbys(First(1, [standby]))
                master.restart()

        """
        if isinstance(standbys, Iterable):
            standbys = First(1, standbys)

        self.append_conf("synchronous_standby_names = '{}'".format(standbys))

    def catchup(self, dbname=None, username=None):
        """
        Wait until async replica catches up with its master.
        """

        if not self.master:
            raise TestgresException("Node doesn't have a master")

        if pg_version_ge('10'):
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
            self.poll_query_until(
                query=wait_lsn.format(lsn),
                dbname=dbname,
                username=username,
                max_attempts=0)    # infinite
        except Exception as e:
            raise_from(CatchUpException("Failed to catch up", poll_lsn), e)

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

        _params = [
            get_bin_path("pgbench"),
            "-p", str(self.port),
            "-h", self.host,
            "-U", username,
        ] + options  # yapf: disable

        # should be the last one
        _params.append(dbname)

        proc = subprocess.Popen(_params, stdout=stdout, stderr=stderr)

        return proc

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

        _params = [
            get_bin_path("pgbench"),
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

    def connect(self, dbname=None, username=None, password=None):
        """
        Connect to a database.

        Args:
            dbname: database name to connect to.
            username: database user name.
            password: user's password.

        Returns:
            An instance of :class:`.NodeConnection`.
        """

        return NodeConnection(node=self,
                              dbname=dbname,
                              username=username,
                              password=password)  # yapf: disable
