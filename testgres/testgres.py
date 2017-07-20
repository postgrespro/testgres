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

# Try to use psycopg2 by default. If psycopg2 isn"t available then use
# pg8000 which is slower but much more portable because uses only
# pure-Python code
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

bound_ports = set()
registered_nodes = []
util_threads = []
base_data_dir = None
tmp_dirs = []
pg_config_data = {}


class ClusterException(Exception):
    """
    Predefined exceptions
    """
    pass


class QueryException(Exception):
    """
    Predefined exceptions
    """
    pass


class InitPostgresNodeException(Exception):
    """
    Predefined exceptions
    """
    pass


class PgcontroldataException(Exception):
    def __init__(self, error_text, cmd):
        self.error_text = error_text
        self.cmd = cmd

    def __str__(self):
        self.error_text = repr(self.error_text)
        return '\n ERROR: {0}\n CMD: {1}'.format(self.error_text, self.cmd)


class LogWriter(threading.Thread):
    """
    Helper class to implement reading from postgresql.log
    and redirect it python logging
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
    Starts thread for node that redirects postgresql
    logs to python logging system
    """

    reader = LogWriter(node_name, open(pg_logname, 'r'))
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
        self.parent_node = parent_node
        if user is None:
            user = get_username()
        self.connection = pglib.connect(
            database=dbname,
            user=user,
            port=parent_node.port,
            host=host,
            password=password)

        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.connection.close()

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
        self.connection.close()


class PostgresNode(object):
    def __init__(self, name, port, base_dir=None, use_logging=False):
        global bound_ports

        # check that port is not used
        if port in bound_ports:
            raise InitPostgresNodeException(
                'port {} is already in use'.format(port))

        # mark port as used
        bound_ports.add(port)

        self.name = name
        self.host = '127.0.0.1'
        self.port = port
        if base_dir is None:
            self.base_dir = tempfile.mkdtemp()
            tmp_dirs.append(self.base_dir)
        else:
            self.base_dir = base_dir
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
        self.working = False

        self.use_logging = use_logging
        self.logger = None

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
    def output_filename(self):
        return os.path.join(self.logs_dir, "stdout.log")

    @property
    def error_filename(self):
        return os.path.join(self.logs_dir, "stderr.log")

    @property
    def connstr(self):
        return "port={}".format(self.port)
        # return "port=%s host=%s" % (self.port, self.host)

    def get_bin_path(self, filename):
        """ Returns full path to an executable """
        pg_config = get_config()
        if pg_config is None or "BINDIR" not in pg_config:
            return filename
        else:
            return os.path.join(pg_config.get("BINDIR"), filename)

    def initdb(self, directory, initdb_params=[]):
        initdb = self.get_bin_path("initdb")
        initdb_logfile = os.path.join(self.logs_dir, "initdb.log")

        with open(initdb_logfile, 'a') as file_out:
            ret = subprocess.call(
                [initdb, directory, "-N"] + initdb_params,
                stdout=file_out,
                stderr=subprocess.STDOUT)

            if ret:
                raise ClusterException(
                    "Cluster initialization failed. You"
                    " can find additional information at '%s'" %
                    initdb_logfile)

    def _setup_data_dir(self, data_dir):
        global base_data_dir

        if base_data_dir is None:
            base_data_dir = tempfile.mkdtemp()
            tmp_dirs.append(base_data_dir)
            self.initdb(base_data_dir)

        shutil.copytree(base_data_dir, data_dir)

    def init(self, allows_streaming=False, initdb_params=[]):
        """ Performs initdb """

        postgres_conf = os.path.join(self.data_dir, "postgresql.conf")

        if os.path.isfile(postgres_conf):
            # if data directory exists then we don't need reinit it
            with open(postgres_conf, "a") as conf:
                conf.write("port = {}\n".format(self.port))
            return self

        # initialize cluster
        if initdb_params:
            self.initdb(self.data_dir, initdb_params)
        else:
            self._setup_data_dir(self.data_dir)

        # add parameters to config file
        with open(postgres_conf, "w") as conf:
            conf.write("fsync = off\n"
                       "log_statement = all\n"
                       "port = {}\n".format(self.port))
            conf.write(
                "listen_addresses = '{}'\n".format(self.host))

            if allows_streaming:
                # TODO: wal_level = hot_standby (9.5)
                conf.write("max_wal_senders = 5\n"
                           "wal_keep_segments = 20\n"
                           "wal_log_hints = on\n"
                           "hot_standby = on\n"
                           "max_connections = 10\n")
                if get_config().get("VERSION_NUM") < 906000:
                    conf.write("wal_level = hot_standby\n")
                else:
                    conf.write("wal_level = replica\n")

                self.set_replication_conf()

        return self

    def init_from_backup(self,
                         root_node,
                         backup_name,
                         has_streaming=False,
                         hba_permit_replication=True):
        """ Initializes cluster from backup, made by another node """

        # Copy data from backup
        backup_path = os.path.join(root_node.base_dir, backup_name)
        shutil.copytree(backup_path, self.data_dir)
        os.chmod(self.data_dir, 0o0700)

        # Change port in config file
        self.append_conf("postgresql.conf", "port = {}".format(self.port))
        # Enable streaming
        if hba_permit_replication:
            self.set_replication_conf()
        if has_streaming:
            self.enable_streaming(root_node)

    def set_replication_conf(self):
        hba_conf = os.path.join(self.data_dir, "pg_hba.conf")
        with open(hba_conf, "a") as conf:
            conf.write("local replication all trust\n")
            # conf.write("host replication all 127.0.0.1/32 trust\n")

    def enable_streaming(self, root_node):
        recovery_conf = os.path.join(self.data_dir, "recovery.conf")
        with open(recovery_conf, "a") as conf:
            conf.write("primary_conninfo='{} application_name={}'\n"
                       "standby_mode=on\n".format(root_node.connstr,
                                                  self.name))

    def append_conf(self, filename, string):
        """
        Appends line to a config file like postgresql.conf or pg_hba.conf

        A new line is not automatically appended to the string
        """
        config_name = os.path.join(self.data_dir, filename)
        with open(config_name, "a") as conf:
            conf.write(''.join([string, '\n']))

        return self

    def pg_ctl(self, command, params={}, command_options=[]):
        """
        Runs pg_ctl with specified params

        This function is a workhorse for start(), stop(), reload() and status()
        function
        """

        pg_ctl = self.get_bin_path("pg_ctl")

        arguments = [pg_ctl]
        arguments.append(command)
        for key, value in six.iteritems(params):
            arguments.append(key)
            if value:
                arguments.append(value)

        with open(self.output_filename, "a") as file_out, \
                open(self.error_filename, "a") as file_err:

            res = subprocess.call(
                arguments + command_options, stdout=file_out, stderr=file_err)

            if res > 0:
                with open(self.error_filename, "r") as errfile:
                    text = errfile.readlines()[-1]
                    text += 'Logs at: %s' % self.logs_dir
                    raise ClusterException(text)

    def start(self, params={}):
        """ Starts cluster """

        # choose log_filename
        if self.use_logging:
            tmpfile = tempfile.NamedTemporaryFile('w', dir=self.logs_dir, delete=False)
            log_filename = tmpfile.name

            self.logger = log_watch(self.name, log_filename)
        else:
            log_filename = os.path.join(self.logs_dir, "postgresql.log")

        # choose conf_filename
        conf_filename = os.path.join(self.data_dir, "postgresql.conf")

        _params = {
            "-D": self.data_dir,
            "-w": None,
            "-l": log_filename,
        }
        _params.update(params)

        try:
            self.pg_ctl("start", _params)
        except ClusterException as e:
            def print_node_file(node_file):
                if os.path.exists(node_file):
                    with open(node_file, 'r') as f:
                        print(f.read())
                else:
                    print("File not found: {}".format(node_file))

            # show pg_ctl LOG
            print("\npg_ctl log:\n----")
            print(str(e))

            # show postmaster LOG
            print("\n{}:\n----".format(log_filename))
            print_node_file(log_filename)

            # show CONFIG
            print("\n{}:\n----".format(conf_filename))
            print_node_file(conf_filename)

            raise ClusterException("Cannot start node {}".format(self.name))

        self.working = True
        return self

    def status(self):
        """
        Check cluster status

        If Running - return True
        If not Running - return False
        If there is no data in data_dir or data_dir do not exists - return None
        """
        try:
            # TODO: use result?
            subprocess.check_output([
                self.get_bin_path("pg_ctl"), 'status', '-D',
                '{0}'.format(self.data_dir)
            ])
            return True
        except subprocess.CalledProcessError as e:
            if e.returncode == 3:
                # Not Running
                self.working = False
                return False
            elif e.returncode == 4:
                # No data or directory do not exists
                self.working = False
                return None

    def get_pid(self):
        """
        Check that node is running and return postmaster pid
        Otherwise return None
        """
        if self.status():
            file = open(os.path.join(self.data_dir, 'postmaster.pid'))
            pid = int(file.readline())
            return pid
        else:
            return None

    def get_control_data(self):
        """ Return pg_control content """
        out_data = {}
        pg_controldata = self.get_bin_path("pg_controldata")
        try:
            lines = subprocess.check_output(
                [pg_controldata] + ["-D", self.data_dir],
                stderr=subprocess.STDOUT).decode("utf-8").splitlines()
        except subprocess.CalledProcessError as e:
            raise PgcontroldataException(e.output, e.cmd)

        for line in lines:
            key, value = line.partition(':')[::2]
            out_data[key.strip()] = value.strip()
        return out_data

    def stop(self, params={}):
        """ Stops cluster """
        _params = {"-D": self.data_dir, "-w": None}
        _params.update(params)
        self.pg_ctl("stop", _params)

        if self.logger:
            self.logger.stop()

        self.working = False

        return self

    def restart(self, params={}):
        """ Restarts cluster """
        _params = {"-D": self.data_dir, "-w": None}
        _params.update(params)
        self.pg_ctl("restart", _params)

        return self

    def reload(self, params={}):
        """ Reloads config files """
        _params = {"-D": self.data_dir}
        _params.update(params)
        self.pg_ctl("reload", _params)

        return self

    def cleanup(self):
        """ Stops cluster if needed and removes the data directory """

        # stop server if it still working
        if self.working:
            try:
                self.stop()
            except:
                pass

        # remove data directory
        shutil.rmtree(self.data_dir, ignore_errors=True)
        return self

    def psql(self, dbname, query=None, filename=None, username=None):
        """
        Executes a query by the psql

        Returns a tuple (code, stdout, stderr) in which:
        * code is a return code of psql (0 if alright)
        * stdout and stderr are strings, representing standard output
          and standard error output
        """
        psql = self.get_bin_path("psql")
        psql_params = [
            psql, "-XAtq", "-h{}".format(self.host), "-p {}".format(self.port),
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
        process = subprocess.Popen(
            psql_params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # wait untill it finishes and get stdout and stderr
        out, err = process.communicate()
        return process.returncode, out, err

    def safe_psql(self, dbname, query, username=None):
        """
        Executes a query by the psql

        Returns the stdout if succeed. Otherwise throws
        ClusterException with stderr output
        """
        ret, out, err = self.psql(dbname, query, username=username)
        if ret:
            raise ClusterException("psql failed:\n" + six.text_type(err))
        return out

    def dump(self, dbname, filename):
        """ Invoke pg_dump and exports database to a file as an sql script """
        path = os.path.join(self.base_dir, filename)
        params = [
            self.get_bin_path("pg_dump"), "-p {}".format(self.port), "-f",
            path, dbname
        ]

        with open(self.error_filename, "a") as file_err:
            ret = subprocess.call(params, stderr=file_err)
            if ret:
                raise ClusterException("Dump creation failed")

    def restore(self, dbname, filename, node=None):
        """
        Restore database from dump file

        dbname     name of database we're restoring data to
        filename   path to the dump file
        node       the cluster node where the dump was taken from (if node
                   wasn't specified then assume the same cluster)
        """
        if not node:
            node = self

        path = os.path.join(node.base_dir, filename)
        self.psql(dbname, filename=path)

    def poll_query_until(self, dbname, query):
        """ Runs a query once a second until it returs True """
        max_attemps = 60
        attemps = 0

        while attemps < max_attemps:
            ret = self.safe_psql(dbname, query)
            # TODO: fix psql so that it returns result without newline
            if ret == six.b("t\n"):
                return

            time.sleep(1)
            attemps += 1
        raise QueryException("Timeout while waiting for query to return True")

    def execute(self, dbname, query, username=None, commit=False):
        """ Executes the query and returns all rows """
        with self.connect(dbname, username) as node_con:
            res = node_con.execute(query)
            if commit:
                node_con.commit()
            return res

    def backup(self, name):
        """ Performs pg_basebackup """
        pg_basebackup = self.get_bin_path("pg_basebackup")
        backup_path = os.path.join(self.base_dir, name)
        os.makedirs(backup_path)
        params = [
            pg_basebackup, "-D", backup_path, "-p {}".format(self.port), "-X",
            "fetch"
        ]
        with open(self.output_filename, "a") as file_out, \
                open(self.error_filename, "a") as file_err:
            ret = subprocess.call(params, stdout=file_out, stderr=file_err)
            if ret:
                raise ClusterException("Base backup failed")

            return backup_path

    def pgbench_init(self, dbname='postgres', scale=1, options=[]):
        """ Prepare pgbench database """
        pgbench = self.get_bin_path("pgbench")
        params = [pgbench, "-i", "-s",
                  "%i" % scale, "-p",
                  "%i" % self.port] + options + [dbname]
        with open(self.output_filename, "a") as file_out, \
                open(self.error_filename, "a") as file_err:
            ret = subprocess.call(params, stdout=file_out, stderr=file_err)
            if ret:
                raise ClusterException("pgbench init failed")

            return True

    def pgbench(self, dbname='postgres', stdout=None, stderr=None, options=[]):
        """ Make pgbench process """
        pgbench = self.get_bin_path("pgbench")
        params = [pgbench, "-p", "%i" % self.port] + options + [dbname]
        proc = subprocess.Popen(params, stdout=stdout, stderr=stderr)

        return proc

    def connect(self, dbname='postgres', username=None):
        return NodeConnection(parent_node=self, dbname=dbname, user=username)


def get_username():
    """ Returns current user name """
    return pwd.getpwuid(os.getuid())[0]


def get_config():
    global pg_config_data

    if not pg_config_data:
        pg_config_cmd = os.environ.get("PG_CONFIG") \
            if "PG_CONFIG" in os.environ else "pg_config"

        try:
            out = six.StringIO(
                subprocess.check_output(
                    [pg_config_cmd], universal_newlines=True))
            for line in out:
                if line and "=" in line:
                    key, value = line.split("=", 1)
                    pg_config_data[key.strip()] = value.strip()
        except OSError:
            return None

        # Numeric version format
        version_parts = pg_config_data.get("VERSION").split(" ")
        pg_config_data["VERSION_NUM"] = version_to_num(version_parts[-1])

    return pg_config_data


def version_to_num(version):
    """ Converts PostgreSQL version to number for easier comparison """
    import re

    if not version:
        return 0
    parts = version.split(".")
    while len(parts) < 3:
        parts.append("0")
    num = 0
    for part in parts:
        num = num * 100 + int(re.sub("[^\d]", "", part))
    return num


def get_new_node(name, base_dir=None, use_logging=False):
    global registered_nodes

    # generate a new unique port
    port = port_for.select_random(exclude_ports=bound_ports)

    node = PostgresNode(name, port, base_dir, use_logging=use_logging)
    registered_nodes.append(node)

    return node


def clean_all():
    global registered_nodes
    global base_data_dir
    global tmp_dirs

    for node in registered_nodes:
        node.cleanup()

    for cat in tmp_dirs:
        if os.path.exists():
            shutil.rmtree(cat, ignore_errors=True)

        if cat == base_data_dir:
            base_data_dir = None

    registered_nodes = []
    tmp_dirs = []


def stop_all():
    global registered_nodes
    global util_threads

    for node in registered_nodes:
        # stop server if it's still working
        if node.working:
            node.stop()

    for thread in util_threads:
        thread.stop()
