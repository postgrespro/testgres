import getpass
import os
import shutil
import stat
import subprocess
import tempfile

import psutil

from ..exceptions import ExecUtilException
from .os_ops import ConnectionParams, OsOperations
from .os_ops import pglib

try:
    from shutil import which as find_executable
    from shutil import rmtree
except ImportError:
    from distutils.spawn import find_executable
    from distutils import rmtree

CMD_TIMEOUT_SEC = 60
error_markers = [b'error', b'Permission denied', b'fatal']


class LocalOperations(OsOperations):
    def __init__(self, conn_params=None):
        if conn_params is None:
            conn_params = ConnectionParams()
        super(LocalOperations, self).__init__(conn_params.username)
        self.conn_params = conn_params
        self.host = conn_params.host
        self.ssh_key = None
        self.remote = False
        self.username = conn_params.username or self.get_user()

    # Command execution
    def exec_command(self, cmd, wait_exit=False, verbose=False,
                     expect_error=False, encoding=None, shell=False, text=False,
                     input=None, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                     get_process=None, timeout=None):
        """
        Execute a command in a subprocess.

        Args:
        - cmd: The command to execute.
        - wait_exit: Whether to wait for the subprocess to exit before returning.
        - verbose: Whether to return verbose output.
        - expect_error: Whether to raise an error if the subprocess exits with an error status.
        - encoding: The encoding to use for decoding the subprocess output.
        - shell: Whether to use shell when executing the subprocess.
        - text: Whether to return str instead of bytes for the subprocess output.
        - input: The input to pass to the subprocess.
        - stdout: The stdout to use for the subprocess.
        - stderr: The stderr to use for the subprocess.
        - proc: The process to use for subprocess creation.
        :return: The output of the subprocess.
        """
        if os.name == 'nt':
            with tempfile.NamedTemporaryFile() as buf:
                process = subprocess.Popen(cmd, stdout=buf, stderr=subprocess.STDOUT)
                process.communicate()
                buf.seek(0)
                result = buf.read().decode(encoding)
            return result
        else:
            process = subprocess.Popen(
                cmd,
                shell=shell,
                stdout=stdout,
                stderr=stderr,
            )
            if get_process:
                return process

            try:
                result, error = process.communicate(input, timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                raise ExecUtilException("Command timed out after {} seconds.".format(timeout))
            exit_status = process.returncode

            error_found = exit_status != 0 or any(marker in error for marker in error_markers)

            if encoding:
                result = result.decode(encoding)
                error = error.decode(encoding)

            if expect_error:
                raise Exception(result, error)

            if exit_status != 0 or error_found:
                if exit_status == 0:
                    exit_status = 1
                raise ExecUtilException(message='Utility exited with non-zero code. Error `{}`'.format(error),
                                        command=cmd,
                                        exit_code=exit_status,
                                        out=result)
            if verbose:
                return exit_status, result, error
            else:
                return result

    # Environment setup
    def environ(self, var_name):
        return os.environ.get(var_name)

    def find_executable(self, executable):
        return find_executable(executable)

    def is_executable(self, file):
        # Check if the file is executable
        return os.stat(file).st_mode & stat.S_IXUSR

    def set_env(self, var_name, var_val):
        # Check if the directory is already in PATH
        os.environ[var_name] = var_val

    # Get environment variables
    def get_user(self):
        return getpass.getuser()

    def get_name(self):
        return os.name

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        if remove_existing:
            shutil.rmtree(path, ignore_errors=True)
        try:
            os.makedirs(path)
        except FileExistsError:
            pass

    def rmdirs(self, path, ignore_errors=True):
        return rmtree(path, ignore_errors=ignore_errors)

    def listdir(self, path):
        return os.listdir(path)

    def path_exists(self, path):
        return os.path.exists(path)

    @property
    def pathsep(self):
        os_name = self.get_name()
        if os_name == "posix":
            pathsep = ":"
        elif os_name == "nt":
            pathsep = ";"
        else:
            raise Exception("Unsupported operating system: {}".format(os_name))
        return pathsep

    def mkdtemp(self, prefix=None):
        return tempfile.mkdtemp(prefix='{}'.format(prefix))

    def mkstemp(self, prefix=None):
        fd, filename = tempfile.mkstemp(prefix=prefix)
        os.close(fd)  # Close the file descriptor immediately after creating the file
        return filename

    def copytree(self, src, dst):
        return shutil.copytree(src, dst)

    # Work with files
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False):
        """
        Write data to a file locally
        Args:
            filename: The file path where the data will be written.
            data: The data to be written to the file.
            truncate: If True, the file will be truncated before writing ('w' or 'wb' option);
                      if False (default), data will be appended ('a' or 'ab' option).
            binary: If True, the data will be written in binary mode ('wb' or 'ab' option);
                    if False (default), the data will be written in text mode ('w' or 'a' option).
            read_and_write: If True, the file will be opened with read and write permissions ('r+' option);
                            if False (default), only write permission will be used ('w', 'a', 'wb', or 'ab' option)
        """
        # If it is a bytes str or list
        if isinstance(data, bytes) or isinstance(data, list) and all(isinstance(item, bytes) for item in data):
            binary = True
        mode = "wb" if binary else "w"
        if not truncate:
            mode = "ab" if binary else "a"
        if read_and_write:
            mode = "r+b" if binary else "r+"

        with open(filename, mode) as file:
            if isinstance(data, list):
                file.writelines(data)
            else:
                file.write(data)

    def touch(self, filename):
        """
        Create a new file or update the access and modification times of an existing file.
        Args:
            filename (str): The name of the file to touch.

        This method behaves as the 'touch' command in Unix. It's equivalent to calling 'touch filename' in the shell.
        """
        # cross-python touch(). It is vulnerable to races, but who cares?
        with open(filename, "a"):
            os.utime(filename, None)

    def read(self, filename, encoding=None, binary=False):
        mode = "rb" if binary else "r"
        with open(filename, mode) as file:
            content = file.read()
            if binary:
                return content
            if isinstance(content, bytes):
                return content.decode(encoding or 'utf-8')
            return content

    def readlines(self, filename, num_lines=0, binary=False, encoding=None):
        """
        Read lines from a local file.
        If num_lines is greater than 0, only the last num_lines lines will be read.
        """
        assert num_lines >= 0
        mode = 'rb' if binary else 'r'
        if num_lines == 0:
            with open(filename, mode, encoding=encoding) as file:  # open in binary mode
                return file.readlines()

        else:
            bufsize = 8192
            buffers = 1

            with open(filename, mode, encoding=encoding) as file:  # open in binary mode
                file.seek(0, os.SEEK_END)
                end_pos = file.tell()

                while True:
                    offset = max(0, end_pos - bufsize * buffers)
                    file.seek(offset, os.SEEK_SET)
                    pos = file.tell()
                    lines = file.readlines()
                    cur_lines = len(lines)

                    if cur_lines >= num_lines or pos == 0:
                        return lines[-num_lines:]  # get last num_lines from lines

                    buffers = int(
                        buffers * max(2, int(num_lines / max(cur_lines, 1)))
                    )  # Adjust buffer size

    def isfile(self, remote_file):
        return os.path.isfile(remote_file)

    def isdir(self, dirname):
        return os.path.isdir(dirname)

    def remove_file(self, filename):
        return os.remove(filename)

    # Processes control
    def kill(self, pid, signal):
        # Kill the process
        cmd = "kill -{} {}".format(signal, pid)
        return self.exec_command(cmd)

    def get_pid(self):
        # Get current process id
        return os.getpid()

    def get_process_children(self, pid):
        return psutil.Process(pid).children()

    # Database control
    def db_connect(self, dbname, user, password=None, host="localhost", port=5432):
        conn = pglib.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
        )
        return conn
