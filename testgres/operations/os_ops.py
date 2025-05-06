import getpass
import locale


class ConnectionParams:
    def __init__(self, host='127.0.0.1', port=None, ssh_key=None, username=None):
        self.host = host
        self.port = port
        self.ssh_key = ssh_key
        self.username = username


def get_default_encoding():
    if not hasattr(locale, 'getencoding'):
        locale.getencoding = locale.getpreferredencoding
    return locale.getencoding() or 'UTF-8'


class OsOperations:
    def __init__(self, username=None):
        self.ssh_key = None
        self.username = username or getpass.getuser()

    # Command execution
    def exec_command(self, cmd, **kwargs):
        raise NotImplementedError()

    # Environment setup
    def environ(self, var_name):
        raise NotImplementedError()

    def cwd(self):
        raise NotImplementedError()

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

    def makedir(self, path: str):
        assert type(path) == str  # noqa: E721
        raise NotImplementedError()

    def rmdirs(self, path, ignore_errors=True):
        raise NotImplementedError()

    def rmdir(self, path: str):
        assert type(path) == str  # noqa: E721
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

    def mkstemp(self, prefix=None):
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

    def read_binary(self, filename, offset):
        assert type(filename) == str  # noqa: E721
        assert type(offset) == int  # noqa: E721
        assert offset >= 0
        raise NotImplementedError()

    def isfile(self, remote_file):
        raise NotImplementedError()

    def isdir(self, dirname):
        raise NotImplementedError()

    def get_file_size(self, filename):
        raise NotImplementedError()

    def remove_file(self, filename):
        assert type(filename) == str  # noqa: E721
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

    def is_port_free(self, number: int):
        assert type(number) == int  # noqa: E721
        raise NotImplementedError()

    def get_tempdir(self) -> str:
        raise NotImplementedError()
