# coding: utf-8
"""
testgres.py
        Postgres testing utility

This module was created under influence of Postgres TAP test feature
(PostgresNode.pm module). It can manage Postgres clusters: initialize,
edit configuration files, start/stop cluster, execute queries. The
typical flow may look like:

    with get_new_node('test') as node:
        node.init()
        node.start()
        result = node.psql('postgres', 'SELECT 1')
        print(result)
        node.stop()

Copyright (c) 2016, Postgres Professional
"""

import os
import subprocess
import pwd
import shutil
import time
import six
import port_for

import threading
import logging
import select
import tempfile

from enum import Enum
from distutils.version import LooseVersion

# Try to use psycopg2 by default. If psycopg2 isn't available then use
# pg8000 which is slower but much more portable because uses only
# pure-Python code
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

# ports used by nodes
bound_ports = set()

# threads for loggers
util_threads = []

# chached initdb dir
cached_data_dir = None

# rows returned by PG_CONFIG
pg_config_data = {}


class ExecUtilException(Exception):
    """
    Stores exit code
    """

    def __init__(self, message, exit_code=0):
        super(ExecUtilException, self).__init__(message)
        self.exit_code = exit_code


class ClusterException(Exception):
    pass


class QueryException(Exception):
    pass


class StartNodeException(Exception):
    pass


class InitNodeException(Exception):
    pass


class TestgresLogger(threading.Thread):
    """
    Helper class to implement reading from postgresql.log
    """

    def __init__(self, node_name, fd):
        assert callable(fd.readline)

        threading.Thread.__init__(self)

        self.node_name = node_name
        self.fd = fd
        self.stop_event = threading.Event()
        self.logger = logging.getLogger(node_name)
        self.logger.setLevel(logging.INFO)

    def run(self):
        while self.fd in select.select([self.fd], [], [], 0)[0]:
            line = self.fd.readline()
            if line:
                extra = {'node': self.node_name}
                self.logger.info(line.strip(), extra=extra)
            elif self.stopped():
                break
            else:
                time.sleep(0.1)

    def stop(self):
        self.stop_event.set()

    def stopped(self):
        return self.stop_event.isSet()


def log_watch(node_name, pg_logname):
    """
    Starts thread for node that redirects
    postgresql logs to python logging system
    """

    reader = TestgresLogger(node_name, open(pg_logname, 'r'))
    reader.start()

    global util_threads
    util_threads.append(reader)

    return reader


class NodeConnection(object):
    """
    Transaction wrapper returned by Node
    """

    def __init__(self,
                 parent_node,
                 dbname,
                 host="127.0.0.1",
                 user=None,
                 password=None):

        # Use default user if not specified
        user = user or _default_username()

        self.parent_node = parent_node

        self.connection = pglib.connect(
            database=dbname,
            user=user,
            port=parent_node.port,
            host=host,
            password=password)

        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def begin(self, isolation_level=0):
        # yapf: disable
        levels = [
            'read uncommitted',
            'read committed',
            'repeatable read',
            'serializable'
        ]

        # Check if level is int [0..3]
        if (isinstance(isolation_level, int) and
                isolation_level in range(0, 4)):

            # Replace index with isolation level type
            isolation_level = levels[isolation_level]

        # Or it might be a string
        elif (isinstance(isolation_level, six.text_type) and
              isolation_level.lower() in levels):

            # Nothing to do here
            pass

        # Something is wrong, emit exception
        else:
            raise QueryException(
                'Invalid isolation level "{}"'.format(isolation_level))

        self.cursor.execute(
            'SET TRANSACTION ISOLATION LEVEL {}'.format(isolation_level))

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def execute(self, query, *args):
        self.cursor.execute(query, args)

        try:
            return self.cursor.fetchall()
        except Exception:
            return None

    def close(self):
        self.cursor.close()
        self.connection.close()


class NodeStatus(Enum):
    Running, Stopped, Uninitialized = range(3)

    # for Python 3.x
    def __bool__(self):
        return self.value == NodeStatus.Running.value

    # for Python 2.x
    __nonzero__ = __bool__


class PostgresNode(object):
    def __init__(self, name, port, base_dir=None, use_logging=False):
        global bound_ports

        # check that port is not used
        if port in bound_ports:
            raise InitNodeException('port {} is already in use'.format(port))

        # mark port as used
        bound_ports.add(port)

        self.name = name
        self.host = '127.0.0.1'
        self.port = port
        self.base_dir = base_dir or tempfile.mkdtemp()
        self.use_logging = use_logging
        self.logger = None

        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        global bound_ports

        # stop node if necessary
        self.cleanup()

        # mark port as free
        bound_ports.remove(self.port)

    @property
    def data_dir(self):
        return os.path.join(self.base_dir, "data")

    @property
    def logs_dir(self):
        return os.path.join(self.base_dir, "logs")

    @property
    def utils_logname(self):
        return os.path.join(self.logs_dir, "utils.log")

    @property
    def connstr(self):
        return "port={}".format(self.port)

    def _create_recovery_conf(self, root_node):
        line = (
            "primary_conninfo='{} application_name={}'\n"
            "standby_mode=on\n"
        ).format(root_node.connstr, self.name)

        self.append_conf("recovery.conf", line)

    def init(self, allow_streaming=False, initdb_params=[]):
        """
        Perform initdb for this node.

        Arguments:
        allow_streaming -- should this node add a hba entry for replication?
        initdb_params -- parameters for initdb (list)
        """

        postgres_conf = os.path.join(self.data_dir, "postgresql.conf")
        hba_conf = os.path.join(self.data_dir, "pg_hba.conf")

        # We don't have to reinit it if data directory exists
        if os.path.isfile(postgres_conf):
            raise InitNodeException('Node is already intialized')

        # initialize this PostgreSQL node
        initdb_log = os.path.join(self.logs_dir, "initdb.log")
        _cached_initdb(self.data_dir, initdb_log, initdb_params)

        # add parameters to hba file
        with open(hba_conf, "w") as conf:
            conf.write("# TYPE\tDATABASE\tUSER\tADDRESS\t\tMETHOD\n"
                       "local\tall\t\tall\t\t\ttrust\n"
                       "host\tall\t\tall\t127.0.0.1/32\ttrust\n"
                       "host\tall\t\tall\t::1/128\t\ttrust\n"
                       # replication
                       "local\treplication\tall\t\t\ttrust\n"
                       "host\treplication\tall\t127.0.0.1/32\ttrust\n"
                       "host\treplication\tall\t::1/128\t\ttrust\n")

        # add parameters to config file
        with open(postgres_conf, "w") as conf:
            conf.write("fsync = off\n"
                       "log_statement = all\n"
                       "port = {}\n".format(self.port))

            conf.write("listen_addresses = '{}'\n".format(self.host))

            if allow_streaming:
                # TODO: wal_level = hot_standby (9.5)
                conf.write("max_wal_senders = 5\n"
                           "wal_keep_segments = 20\n"
                           "wal_log_hints = on\n"
                           "hot_standby = on\n"
                           "max_connections = 10\n")

                cur_ver = LooseVersion(get_pg_config().get("VERSION_NUM"))
                min_ver = LooseVersion('9.6.0')

                if cur_ver < min_ver:
                    conf.write("wal_level = hot_standby\n")
                else:
                    conf.write("wal_level = replica\n")

        return self

    def init_from_backup(self, root_node, backup_name, become_replica=False):
        """
        Initialize node from a backup of another node.

        Arguments:
        root_node -- master node that produced a backup (PostgresNode)
        backup_name -- name of the backup (str)
        become replica -- should this node become a binary replica of master?
        """

        # Copy data from backup
        backup_path = os.path.join(root_node.base_dir, backup_name)
        shutil.copytree(backup_path, self.data_dir)
        os.chmod(self.data_dir, 0o0700)

        # Change port in config file
        self.append_conf("postgresql.conf", "port = {}".format(self.port))

        # Should we become replica?
        if become_replica:
            self._create_recovery_conf(root_node)

    def append_conf(self, filename, string):
        """
        Append line to a config file (i.e. postgresql.conf)
        """

        config_name = os.path.join(self.data_dir, filename)
        with open(config_name, "a") as conf:
            conf.write(''.join([string, '\n']))

        return self

    def pg_ctl(self, command, params={}, command_options=[]):
        """
        Runs pg_ctl with specified params. This function is a workhorse
        for start(), stop(), reload() and status() functions.
        """

        _params = [command]

        for key, value in six.iteritems(params):
            _params.append(key)
            if value:
                _params.append(value)

        _execute_utility("pg_ctl", _params, self.utils_logname)

    def status(self):
        """
        Check this node's status (NodeStatus)
        """

        try:
            _params = {"-D": self.data_dir}
            self.pg_ctl("status", _params)
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
        Return postmaster's pid if node is running
        """

        if self.status():
            with open(os.path.join(self.data_dir, 'postmaster.pid')) as f:
                return int(f.readline())

        # for clarity
        return 0

    def get_control_data(self):
        """
        Return contents of pg_control file
        """

        _params = ["-D", self.data_dir]
        lines = _execute_utility("pg_controldata", _params, self.utils_logname)

        out_data = {}

        for line in lines:
            key, value = line.partition(':')[::2]
            out_data[key.strip()] = value.strip()

        return out_data

    def start(self, params={}):
        """
        Start this node
        """

        # choose log_filename
        if self.use_logging:
            tmpfile = tempfile.NamedTemporaryFile('w', dir=self.logs_dir, delete=False)
            log_filename = tmpfile.name

            self.logger = log_watch(self.name, log_filename)
        else:
            log_filename = os.path.join(self.logs_dir, "postgresql.log")

        # choose conf_filename
        conf_filename = os.path.join(self.data_dir, "postgresql.conf")

        # choose hba_filename
        hba_filename = os.path.join(self.data_dir, "pg_hba.conf")

        # choose recovery_filename
        recovery_filename = os.path.join(self.data_dir, "recovery.conf")

        _params = {
            "-D": self.data_dir,
            "-w": None,
            "-l": log_filename,
        }
        _params.update(params)

        try:
            self.pg_ctl("start", _params)
        except ExecUtilException as e:
            def print_node_file(node_file):
                if os.path.exists(node_file):
                    try:
                        with open(node_file, 'r') as f:
                            return f.read()
                    except:
                        pass
                return "### file not found ###\n"

            error_text = (
                "Cannot start node\n"
                "{}\n"  # pg_ctl log
                "{}:\n----\n{}\n"  # postgresql.log
                "{}:\n----\n{}\n"  # postgresql.conf
                "{}:\n----\n{}\n"  # pg_hba.conf
                "{}:\n----\n{}\n"  # recovery.conf
            ).format(str(e),
                     log_filename, print_node_file(log_filename),
                     conf_filename, print_node_file(conf_filename),
                     hba_filename, print_node_file(hba_filename),
                     recovery_filename, print_node_file(recovery_filename))

            raise StartNodeException(error_text)

        return self

    def stop(self, params={}):
        """
        Stop this node
        """

        _params = {"-D": self.data_dir, "-w": None}
        _params.update(params)
        self.pg_ctl("stop", _params)

        if self.logger:
            self.logger.stop()

        return self

    def restart(self, params={}):
        """
        Restart this node
        """

        _params = {"-D": self.data_dir, "-w": None}
        _params.update(params)
        self.pg_ctl("restart", _params)

        return self

    def reload(self, params={}):
        """
        Reload config files
        """

        _params = {"-D": self.data_dir}
        _params.update(params)
        self.pg_ctl("reload", _params)

        return self

    def cleanup(self):
        """
        Stop node if needed and remove its data directory
        """

        # try stopping server
        try:
            self.stop()
        except:
            pass

        # remove data directory
        shutil.rmtree(self.data_dir, ignore_errors=True)

        return self

    def psql(self, dbname, query=None, filename=None, username=None):
        """
        Execute a query using psql and return a tuple (code, stdout, stderr)
        """

        psql = get_bin_path("psql")
        psql_params = [
            psql,
            "-XAtq",
            "-h{}".format(self.host),
            "-p {}".format(self.port),
            dbname
        ]

        if query:
            psql_params.extend(("-c", query))
        elif filename:
            psql_params.extend(("-f", filename))
        else:
            raise QueryException('Query or filename must be provided')

        # Specify user if needed
        if username:
            psql_params.extend(("-U", username))

        # start psql process
        process = subprocess.Popen(psql_params,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        # wait untill it finishes and get stdout and stderr
        out, err = process.communicate()
        return process.returncode, out, err

    def safe_psql(self, dbname, query, username=None):
        """
        Execute a query using psql
        """

        ret, out, err = self.psql(dbname, query, username=username)
        if ret:
            raise QueryException(six.text_type(err))
        return out

    def dump(self, dbname, filename):
        """
        Dump database using pg_dump
        """

        path = os.path.join(self.base_dir, filename)
        _params = [
            "-p {}".format(self.port),
            "-f", path, dbname
        ]

        _execute_utility("pg_dump", _params, self.utils_logname)

    def restore(self, dbname, filename, node=None):
        """
        Restore database from pg_dump's file
        """

        if not node:
            node = self

        path = os.path.join(node.base_dir, filename)
        self.psql(dbname, filename=path)

    def poll_query_until(self, dbname, query):
        """
        Run a query once a second until it returs True
        """

        max_attemps = 60
        attemps = 0

        while attemps < max_attemps:
            ret = self.safe_psql(dbname, query)
            # TODO: fix psql so that it returns result without newline
            if ret == six.b("t\n"):
                return

            time.sleep(1)
            attemps += 1

        raise QueryException("Query timeout")

    def execute(self, dbname, query, username=None, commit=False):
        """
        Execute a query and return all rows
        """

        with self.connect(dbname, username) as node_con:
            res = node_con.execute(query)
            if commit:
                node_con.commit()
            return res

    def backup(self, name):
        """
        Perform pg_basebackup
        """

        backup_path = os.path.join(self.base_dir, name)
        os.makedirs(backup_path)
        _params = [
            "-D", backup_path,
            "-p {}".format(self.port),
            "-X",
            "fetch"
        ]

        _execute_utility("pg_basebackup", _params, self.utils_logname)

        return backup_path

    def pgbench_init(self, dbname='postgres', scale=1, options=[]):
        """
        Prepare database for pgbench
        """

        _params = [
            "-i", "-s",
            "%i" % scale, "-p",
            "%i" % self.port
        ] + options + [dbname]

        _execute_utility("pgbench", _params, self.utils_logname)

        return self

    def pgbench(self, dbname='postgres', stdout=None, stderr=None, options=[]):
        """
        Spawn a pgbench process
        """

        pgbench = get_bin_path("pgbench")
        params = [pgbench, "-p", "%i" % self.port] + options + [dbname]
        proc = subprocess.Popen(params, stdout=stdout, stderr=stderr)

        return proc

    def connect(self, dbname='postgres', username=None):
        return NodeConnection(parent_node=self, dbname=dbname, user=username)


def _default_username():
    """
    Return current user name
    """

    return pwd.getpwuid(os.getuid())[0]


def _cached_initdb(data_dir, initdb_logfile, initdb_params=[]):
    """
    Perform initdb or use cached node files
    """

    def call_initdb(_data_dir):
        _params = [_data_dir, "-N"] + initdb_params
        try:
            _execute_utility("initdb", _params, initdb_logfile)
        except Exception as e:
            raise InitNodeException(e.message)

    # Call initdb if we have custom params
    if initdb_params:
        call_initdb(data_dir)
    # Else we can use cached dir
    else:
        global cached_data_dir

        # Initialize cached initdb
        if cached_data_dir is None:
            cached_data_dir = tempfile.mkdtemp()
            call_initdb(cached_data_dir)

        # Copy cached initdb to current data dir
        shutil.copytree(cached_data_dir, data_dir)


def _execute_utility(util, args, logfile):
    with open(logfile, "a") as file_out:
        # run pg_ctl
        process = subprocess.Popen([get_bin_path(util)] + args,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        # get result of pg_ctl
        out, _ = process.communicate()
        out = out.decode('utf-8')

        # write new log entry
        file_out.write(''.join(map(lambda x: str(x) + ' ', [util] + args)))
        file_out.write('\n')
        file_out.write(out)

        if process.returncode:
            error_text = (
                "{} failed\n"
                "log:\n----\n{}\n"
            ).format(util, out)

            raise ExecUtilException(error_text, process.returncode)

        return out


def get_bin_path(filename):
    """
    Return full path to an executable
    """

    pg_config = get_pg_config()

    if pg_config and "BINDIR" in pg_config:
        return os.path.join(pg_config.get("BINDIR"), filename)

    return filename


def get_pg_config():
    """
    Return output of pg_config
    """

    global pg_config_data

    if not pg_config_data:
        pg_config_cmd = os.environ.get("PG_CONFIG") or "pg_config"

        out = six.StringIO(subprocess.check_output([pg_config_cmd],
                                                   universal_newlines=True))
        for line in out:
            if line and "=" in line:
                key, value = line.split("=", 1)
                pg_config_data[key.strip()] = value.strip()

        # Fetch version of PostgreSQL and save it as VERSION_NUM
        version_parts = pg_config_data.get("VERSION").split(" ")
        pg_config_data["VERSION_NUM"] = version_parts[-1]

    return pg_config_data


def get_new_node(name, base_dir=None, use_logging=False):
    """
    Create new node
    """

    # generate a new unique port
    port = port_for.select_random(exclude_ports=bound_ports)

    return PostgresNode(name, port, base_dir, use_logging=use_logging)
