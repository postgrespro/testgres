import locale

try:
    import psycopg2 as pglib  # noqa: F401
except ImportError:
    try:
        import pg8000 as pglib  # noqa: F401
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")


class ConnectionParams:
    def __init__(self, host='127.0.0.1', ssh_key=None, username=None):
        self.host = host
        self.ssh_key = ssh_key
        self.username = username


def get_default_encoding():
    return locale.getdefaultlocale()[1] or 'UTF-8'


class OsOperations:
    def __init__(self, username=None):
        self.ssh_key = None
        self.username = username

    # Command execution
    def exec_command(self, cmd, **kwargs):
        raise NotImplementedError()

    # Environment setup
    def environ(self, var_name):
        raise NotImplementedError()

    def find_executable(self, executable):
        raise NotImplementedError()

    def is_executable(self, file):
        # Check if the file is executable
        raise NotImplementedError()

    def set_env(self, var_name, var_val):
        # Check if the directory is already in PATH
        raise NotImplementedError()

    # Get environment variables
    def get_user(self):
        raise NotImplementedError()

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

    # Database control
    def db_connect(self, dbname, user, password=None, host="localhost", port=5432):
        raise NotImplementedError()
