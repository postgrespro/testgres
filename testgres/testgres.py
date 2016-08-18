#coding: utf-8
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
import socket
import subprocess
import pwd
import tempfile
import shutil
import time

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
last_assigned_port = int(random.random() * 16384) + 49152;
pg_config_data = {}


class ClusterException(Exception): pass
class QueryException(Exception): pass


class PostgresNode:
    def __init__(self, name, port):
        self.name = name
        self.host = '127.0.0.1'
        self.port = port
        self.base_dir = tempfile.mkdtemp()
        os.makedirs(self.logs_dir)
        self.working = False

    @property
    def data_dir(self):
        return "%s/data" % self.base_dir

    @property
    def logs_dir(self):
        return "%s/logs" % self.base_dir

    @property
    def output_filename(self):
        return "%s/stdout.log" % self.logs_dir

    @property
    def error_filename(self):
        return "%s/stderr.log" % self.logs_dir

    @property
    def connstr(self):
        return "port=%s" % self.port    
        # return "port=%s host=%s" % (self.port, self.host)

    def get_bin_path(self, filename):
        """ Returns full path to an executable """
        pg_config = get_config()
        if not "BINDIR" in pg_config:
            return filename
        else:
            return "%s/%s" % (pg_config.get("BINDIR"), filename)

    def init(self, allows_streaming=False):
        """ Performs initdb """

        # initialize cluster
        os.makedirs(self.data_dir)
        initdb = self.get_bin_path("initdb")
        with open(self.output_filename, "a") as file_out, \
             open(self.error_filename, "a") as file_err:
            ret = subprocess.call(
                [initdb, self.data_dir, "-N"],
                stdout=file_out,
                stderr=file_err
            )
            if ret:
                raise ClusterException("Cluster initialization failed")

        # add parameters to config file
        config_name = "%s/postgresql.conf" % self.data_dir
        with open(config_name, "a") as conf:
            conf.write(
                "fsync = off\n"
                "log_statement = all\n"
                "port = %s\n" % self.port)
            conf.write(
                # "unix_socket_directories = '%s'\n"
                # "listen_addresses = ''\n";)
                "listen_addresses = '%s'\n" % self.host)

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

    def init_from_backup(self, root_node, backup_name, has_streaming=False, hba_permit_replication=True):
        """Initializes cluster from backup, made by another node"""

        # Copy data from backup
        backup_path = "%s/%s" % (root_node.base_dir, backup_name)
        shutil.copytree(backup_path, self.data_dir)
        os.chmod(self.data_dir, 0o0700)

        # Change port in config file
        self.append_conf(
            "postgresql.conf",
            "port = %s" % self.port
        )
        # Enable streaming
        if hba_permit_replication:
            self.set_replication_conf()
        if has_streaming:
            self.enable_streaming(root_node)

    def set_replication_conf(self):
        hba_conf = "%s/pg_hba.conf" % self.data_dir
        with open(hba_conf, "a") as conf:
            conf.write("local replication all trust\n")
            # conf.write("host replication all 127.0.0.1/32 trust\n")

    def enable_streaming(self, root_node):
        config_name = "%s/recovery.conf" % self.data_dir
        with open(config_name, "a") as conf:
            conf.write(
                "primary_conninfo='%s application_name=%s'\n"
                "standby_mode=on\n"
                % (root_node.connstr, self.name))

    def append_conf(self, filename, string):
        """Appends line to a config file like "postgresql.conf"
        or "pg_hba.conf"

        A new line is not automatically appended to the string
        """
        config_name = "%s/%s" % (self.data_dir, filename)
        with open(config_name, "a") as conf:
            conf.write(string)

    def pg_ctl(self, command, params):
        """Runs pg_ctl with specified params

        This function is a workhorse for start(), stop() and reload()
        functions"""
        pg_ctl = self.get_bin_path("pg_ctl")

        arguments = [pg_ctl]
        for key, value in params.iteritems():
            arguments.append(key)
            if value:
                arguments.append(value)

        with open(self.output_filename, "a") as file_out, \
             open(self.error_filename, "a") as file_err:
            return subprocess.call(
                arguments + [command],
                stdout=file_out,
                stderr=file_err
            )

    def start(self):
        """ Starts cluster """
        logfile = self.logs_dir + "/postgresql.log"
        params = {
            "-D": self.data_dir,
            "-w": None,
            "-l": logfile,
        }
        if self.pg_ctl("start", params):
            raise ClusterException("Cluster startup failed")

        self.working = True

    def stop(self):
        """ Stops cluster """
        params = {
            "-D": self.data_dir,
            "-w": None
        }
        if self.pg_ctl("stop", params):
            raise ClusterException("Cluster stop failed")

        self.working = False

    def reload(self):
        """Reloads config files"""
        params = {"-D": self.data_dir}
        if self.pg_ctl("reload", params):
            raise ClusterException("Cluster reload failed")

    def cleanup(self):
        """Stops cluster if needed and removes the data directory"""

        # stop server if it still working
        if self.working:
            self.stop()

        # remove data directory
        shutil.rmtree(self.data_dir)

    def psql(self, dbname, query):
        """Executes a query by the psql

        Returns a tuple (code, stdout, stderr) in which:
        * code is a return code of psql (0 if alright)
        * stdout and stderr are strings, representing standard output
          and standard error output
        """
        psql = self.get_bin_path("psql")
        psql_params = [
            psql, "-XAtq", "-c", query, "-p %s" % self.port, dbname
        ]

        # start psql process
        process = subprocess.Popen(
            psql_params,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # wait untill it finishes and get stdout and stderr
        out, err = process.communicate()
        return process.returncode, out, err

    def safe_psql(self, dbname, query):
        """Executes a query by the psql

        Returns the stdout if succeed. Otherwise throws the
        ClusterException with stderr output
        """
        ret, out, err = self.psql(dbname, query)
        if ret:
            raise ClusterException("psql failed:\n" + err)
        return out

    def poll_query_until(self, dbname, query):
        """Runs a query once a second until it returs True"""
        max_attemps = 60
        attemps = 0

        while attemps < max_attemps:
            ret = self.safe_psql(dbname, query)

            # TODO: fix psql so that it returns result without newline
            if ret == "t\n":
                return

            time.sleep(1)
            attemps += 1
        raise QueryException("Timeout while waiting for query to return True")

    def execute(self, dbname, query):
        """Executes the query and returns all rows"""
        connection = pglib.connect(
            database=dbname,
            user=get_username(),
            port=self.port,
            host="127.0.0.1"
        )
        cur = connection.cursor()

        cur.execute(query)
        res = cur.fetchall()

        cur.close()
        connection.close()

        return res

    def backup(self, name):
        """Performs pg_basebackup"""
        pg_basebackup = self.get_bin_path("pg_basebackup")
        backup_path = self.base_dir + "/" + name
        os.makedirs(backup_path)
        params = [pg_basebackup, "-D", backup_path, "-p %s" % self.port, "-x"]
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


def get_username():
    """ Returns current user name """
    return pwd.getpwuid(os.getuid())[0]

def get_config():
    global pg_config_data

    if not pg_config_data:
        pg_config_cmd = os.environ.get("PG_CONFIG") \
            if "PG_CONFIG" in os.environ else "pg_config"

        out = subprocess.check_output([pg_config_cmd])
        for line in out.split("\n"):
            if line:
                key, value = unicode(line).split("=", 1)
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
    while len(parts) < 3: parts.append("0")
    num = 0
    for part in parts:
        num = num*100 + int(re.sub("[^\d]", "", part))
    return num

def get_new_node(name):
    global registered_nodes
    global last_assigned_port

    port = last_assigned_port + 1
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

    node = PostgresNode(name, port)
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
