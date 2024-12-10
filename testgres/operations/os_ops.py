import getpass
import locale
import os
import sys

try:
    import psycopg2 as pglib  # noqa: F401
except ImportError:
    try:
        import pg8000 as pglib  # noqa: F401
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")


class ConnectionParams:
    def __init__(self, host='127.0.0.1', port=None, ssh_key=None, username=None, remote=False, skip_ssl=None):
        """
            skip_ssl: if is True, the connection to database is established without SSL.
        """
        self.remote = remote
        self.host = host
        self.port = port
        self.ssh_key = ssh_key
        self.username = username
        if skip_ssl is None:
            skip_ssl = os.getenv("TESTGRES_SKIP_SSL", False)
        self.skip_ssl = skip_ssl


def get_default_encoding():
    if not hasattr(locale, 'getencoding'):
        locale.getencoding = locale.getpreferredencoding
    return locale.getencoding() or 'UTF-8'


class OsOperations:
    def __init__(self, conn_params=ConnectionParams()):
        self.ssh_key = conn_params.ssh_key
        self.username = conn_params.username or getpass.getuser()
        self.host = conn_params.host
        self.port = conn_params.port
        self.conn_params = conn_params

    # Command execution
    def exec_command(self, cmd, **kwargs):
        raise NotImplementedError()

    # Environment setup
    def environ(self, var_name):
        raise NotImplementedError()

    def cwd(self):
        if sys.platform == 'linux':
            cmd = 'pwd'
        elif sys.platform == 'win32':
            cmd = 'cd'
        return self.exec_command(cmd).decode().rstrip()

    def find_executable(self, executable):
        raise NotImplementedError()

    def is_executable(self, file):
        # Check if the file is executable
        raise NotImplementedError()

    def set_env(self, var_name, var_val):
        # Check if the directory is already in PATH
        raise NotImplementedError()

    def get_user(self):
        return self.username

    def get_name(self):
        raise NotImplementedError()

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        raise NotImplementedError()

    def rmdirs(self, path, ignore_errors=True):
        raise NotImplementedError()

    def listdir(self, path):
        raise NotImplementedError()

    def path_exists(self, path):
        raise NotImplementedError()

    @property
    def pathsep(self):
        raise NotImplementedError()

    def mkdtemp(self, prefix=None):
        raise NotImplementedError()

    def copytree(self, src, dst):
        raise NotImplementedError()

    # Work with files
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False):
        raise NotImplementedError()

    def touch(self, filename):
        raise NotImplementedError()

    def read(self, filename, encoding, binary):
        raise NotImplementedError()

    def readlines(self, filename):
        raise NotImplementedError()

    def isfile(self, remote_file):
        raise NotImplementedError()

    # Processes control
    def kill(self, pid, signal):
        # Kill the process
        raise NotImplementedError()

    def get_pid(self):
        # Get current process id
        raise NotImplementedError()

    def get_process_children(self, pid):
        raise NotImplementedError()

    def _get_ssl_options(self):
        """
        Determine the SSL options based on available modules.
        """
        if self.conn_params.skip_ssl:
            if 'psycopg2' in sys.modules:
                return {"sslmode": "disable"}
            elif 'pg8000' in sys.modules:
                return {"ssl_context": None}
        return {}

    # Database control
    def db_connect(self, dbname, user, password=None, host="localhost", port=5432):
        ssl_options = self._get_ssl_options()
        conn = pglib.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
            **ssl_options
        )

        return conn
