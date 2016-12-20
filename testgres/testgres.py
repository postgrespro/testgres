# coding: utf-8
"""
testgres.py
        Postgres testing utility

This module was created under influence of Postgres TAP test feature
(PostgresNode.pm module). It can manage Postgres clusters: initialize,
edit configuration files, start/stop cluster, execute queries. The
typical flow may look like:

    try:
        node = get_new_node('test')
        node.init()
        node.start()
        stdout = node.psql('postgres', 'SELECT 1')
        print stdout
        node.stop()
    except ClusterException, e:
        node.cleanup()

Copyright (c) 2016, Postgres Professional
"""

import os
import random
# import socket
import subprocess
import pwd
import tempfile
import shutil
import time
import six

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


registered_nodes = []
last_assigned_port = int(random.random() * 16384) + 49152
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


class NodeConnection(object):

    """
    Transaction wrapper returned by Node
    """

    def __init__(self, parent_node, dbname, host="127.0.0.1", user=None, password=None):
        self.parent_node = parent_node
        if user is None:
            user = get_username()
        self.connection = pglib.connect(
            database=dbname,
            user=user,
            port=parent_node.port,
            host=host,
            password=password
        )

        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.connection.close()

    def begin(self, isolation_level=0):
        levels = ['read uncommitted',
                  'read committed',
                  'repeatable read',
                  'serializable']

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
            raise QueryException('Invalid isolation level "{}"'.format(
                isolation_level))

        self.cursor.execute('SET TRANSACTION ISOLATION LEVEL {}'.format(
            isolation_level))

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

    def __init__(self, name, port, base_dir=None):
        self.name = name
        self.host = '127.0.0.1'
        self.port = port
        if base_dir is None:
            self.base_dir = tempfile.mkdtemp()
        else:
            self.base_dir = base_dir
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
        self.working = False

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
        if "BINDIR" not in pg_config:
            return filename
        else:
            return os.path.join(pg_config.get("BINDIR"), filename)

    def init(self, allows_streaming=False, initdb_params=[]):
        """ Performs initdb """

        postgres_conf = os.path.join(self.data_dir, "postgresql.conf")

        if os.path.isfile(postgres_conf):
            # if data directory exists then we don't need reinit it
            with open(postgres_conf, "a") as conf:
                conf.write("port = {}\n".format(self.port))
            return self

        # initialize cluster
        os.makedirs(self.data_dir)
        initdb = self.get_bin_path("initdb")
        with open(os.path.join(self.logs_dir, "initdb.log"), "a") as file_out:
            ret = subprocess.call(
                [initdb, self.data_dir, "-N"] + initdb_params,
                stdout=file_out,
                stderr=subprocess.STDOUT
            )
            if ret:
                raise ClusterException("Cluster initialization failed")

        # add parameters to config file
        with open(postgres_conf, "a") as conf:
            conf.write(
                "fsync = off\n"
                "log_statement = all\n"
                "port = {}\n".format(self.port))
            conf.write(
                # "unix_socket_directories = '%s'\n"
                # "listen_addresses = ''\n";)
                "listen_addresses = '{}'\n".format(self.host))

            if allows_streaming:
                # TODO: wal_level = hot_standby (9.5)
                conf.write(
                    "max_wal_senders = 5\n"
                    "wal_keep_segments = 20\n"
                    "max_wal_size = 128MB\n"
                    "shared_buffers = 1MB\n"
                    "wal_log_hints = on\n"
                    "hot_standby = on\n"
                    "max_connections = 10\n")
                if get_config().get("VERSION_NUM") < 906000:
                    conf.write("wal_level = hot_standby\n")
                else:
                    conf.write("wal_level = replica\n")

                self.set_replication_conf()

        return self

    def init_from_backup(self, root_node, backup_name, has_streaming=False,
                         hba_permit_replication=True):
        """Initializes cluster from backup, made by another node"""

        # Copy data from backup
        backup_path = os.path.join(root_node.base_dir, backup_name)
        shutil.copytree(backup_path, self.data_dir)
        os.chmod(self.data_dir, 0o0700)

        # Change port in config file
        self.append_conf(
            "postgresql.conf",
            "port = {}".format(self.port)
        )
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
            conf.write(
                "primary_conninfo='{} application_name={}'\n"
                "standby_mode=on\n".format(root_node.connstr, self.name))

    def append_conf(self, filename, string):
        """Appends line to a config file like "postgresql.conf"
        or "pg_hba.conf"

        A new line is not automatically appended to the string
        """
        config_name = os.path.join(self.data_dir, filename)
        with open(config_name, "a") as conf:
            conf.write(''.join([string, '\n']))

        return self

    def pg_ctl(self, command, params, command_options=[]):
        """Runs pg_ctl with specified params

        This function is a workhorse for start(), stop() and reload()
        functions"""
        pg_ctl = self.get_bin_path("pg_ctl")

        arguments = [pg_ctl]
        for key, value in six.iteritems(params):
            arguments.append(key)
            if value:
                arguments.append(value)

        with open(self.output_filename, "a") as file_out, \
                open(self.error_filename, "a") as file_err:

            res = subprocess.call(
                arguments + [command] + command_options,
                stdout=file_out,
                stderr=file_err
            )

            if res > 0:
                with open(self.error_filename, "r") as errfile:
                    raise ClusterException(errfile.readlines()[-1])

    def start(self, params={}):
        """ Starts cluster """
        logfile = os.path.join(self.logs_dir, "postgresql.log")
        _params = {
            "-D": self.data_dir,
            "-w": None,
            "-l": logfile,
        }
        _params.update(params)
        self.pg_ctl("start", _params)

        self.working = True

        return self

    def stop(self, params={}):
        """ Stops cluster """
        _params = {
            "-D": self.data_dir,
            "-w": None
        }
        _params.update(params)
        self.pg_ctl("stop", _params)

        self.working = False

        return self

    def restart(self, params={}):
        """ Restarts cluster """
        _params = {
            "-D": self.data_dir,
            "-w": None
        }
        _params.update(params)
        self.pg_ctl("restart", _params)

        return self

    def reload(self, params={}):
        """Reloads config files"""
        _params = {"-D": self.data_dir}
        _params.update(params)
        self.pg_ctl("reload", _params)

        return self

    def cleanup(self):
        """Stops cluster if needed and removes the data directory"""

        # stop server if it still working
        if self.working:
            try:
                self.stop()
            except:
                pass

        # remove data directory
        shutil.rmtree(self.data_dir)

        return self

    def psql(self, dbname, query=None, filename=None, username=None):
        """Executes a query by the psql

        Returns a tuple (code, stdout, stderr) in which:
        * code is a return code of psql (0 if alright)
        * stdout and stderr are strings, representing standard output
          and standard error output
        """
        psql = self.get_bin_path("psql")
        psql_params = [
            psql, "-XAtq", "-p {}".format(self.port), dbname
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
            psql_params,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # wait untill it finishes and get stdout and stderr
        out, err = process.communicate()
        return process.returncode, out, err

    def safe_psql(self, dbname, query, username=None):
        """Executes a query by the psql

        Returns the stdout if succeed. Otherwise throws the
        ClusterException with stderr output
        """
        ret, out, err = self.psql(dbname, query, username=username)
        if ret:
            raise ClusterException("psql failed:\n" + six.text_type(err))
        return out

    def dump(self, dbname, filename):
        """Invoke pg_dump and exports database to a file as an sql script"""
        path = os.path.join(self.base_dir, filename)
        params = [
            self.get_bin_path("pg_dump"),
            "-p {}".format(self.port),
            "-f", path,
            dbname
        ]

        with open(self.error_filename, "a") as file_err:
            ret = subprocess.call(params, stderr=file_err)
            if ret:
                raise ClusterException("Dump creation failed")

    def restore(self, dbname, filename, node=None):
        """ Restore database from dump file

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
        """Runs a query once a second until it returs True"""
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
        """Executes the query and returns all rows"""
        with self.connect(dbname, username) as node_con:
            res = node_con.execute(query)
            if commit:
                node_con.commit()
            return res

    def backup(self, name):
        """Performs pg_basebackup"""
        pg_basebackup = self.get_bin_path("pg_basebackup")
        backup_path = os.path.join(self.base_dir, name)
        os.makedirs(backup_path)
        params = [pg_basebackup, "-D", backup_path, "-p {}".format(
            self.port), "-x"]
        with open(self.output_filename, "a") as file_out, \
                open(self.error_filename, "a") as file_err:
            ret = subprocess.call(
                params,
                stdout=file_out,
                stderr=file_err
            )
            if ret:
                raise ClusterException("Base backup failed")

            return backup_path

    def pgbench_init(self, dbname='postgres', scale=1, options=[]):
        """Prepare pgbench database"""
        pgbench = self.get_bin_path("pgbench")
        params = [
            pgbench,
            "-i",
            "-s", "%i" % scale,
            "-p", "%i" % self.port
        ] + options + [dbname]
        with open(self.output_filename, "a") as file_out, \
                open(self.error_filename, "a") as file_err:
            ret = subprocess.call(
                params,
                stdout=file_out,
                stderr=file_err
            )
            if ret:
                raise ClusterException("pgbench init failed")

            return True

    def pgbench(self, dbname='postgres', stdout=None, stderr=None, options=[]):
        """Make pgbench process"""
        pgbench = self.get_bin_path("pgbench")
        params = [
            pgbench,
            "-p", "%i" % self.port
        ] + options + [dbname]
        proc = subprocess.Popen(
            params,
            stdout=stdout,
            stderr=stderr
        )

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

        out = six.StringIO(
            subprocess.check_output([pg_config_cmd], universal_newlines=True))
        for line in out:
            if line and "=" in line:
                key, value = line.split("=", 1)
                pg_config_data[key.strip()] = value.strip()

        # Numeric version format
        version_parts = pg_config_data.get("VERSION").split(" ")
        pg_config_data["VERSION_NUM"] = version_to_num(version_parts[-1])

    return pg_config_data


def version_to_num(version):
    """Converts PostgreSQL version to number for easier comparison"""
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


def get_new_node(name, base_dir=None):
    global registered_nodes
    global last_assigned_port

    port = last_assigned_port + 1

    # TODO: check if port is already used

    # found = False
    # while found:
    #   # Check first that candidate port number is not included in
    #   # the list of already-registered nodes.
    #   found = True
    #   for node in registered_nodes:
    #       if node.port == port:
    #           found = False
    #           break

    #   if found:
    #       socket(socket.SOCK,
    #              socket.PF_INET,
    #              socket.SOCK_STREAM,
    #              socket.getprotobyname("tcp"))

    node = PostgresNode(name, port, base_dir)
    registered_nodes.append(node)
    last_assigned_port = port

    return node


def clean_all():
    global registered_nodes
    for node in registered_nodes:
        node.cleanup()
    registered_nodes = []


def stop_all():
    global registered_nodes
    for node in registered_nodes:
        # stop server if it still working
        if node.working:
            node.stop()
