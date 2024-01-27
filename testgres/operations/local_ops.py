import getpass
import os
import shutil
import stat
import subprocess
import tempfile

import psutil

from ..exceptions import ExecUtilException
from .os_ops import ConnectionParams, OsOperations, pglib, get_default_encoding

try:
    from shutil import which as find_executable
    from shutil import rmtree
except ImportError:
    from distutils.spawn import find_executable
    from distutils import rmtree

CMD_TIMEOUT_SEC = 60
error_markers = [b'error', b'Permission denied', b'fatal']


def has_errors(output):
    if output:
        if isinstance(output, str):
            output = output.encode(get_default_encoding())
        return any(marker in output for marker in error_markers)
    return False


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

    @staticmethod
    def _raise_exec_exception(message, command, exit_code, output):
        """Raise an ExecUtilException."""
        raise ExecUtilException(message=message.format(output),
                                command=' '.join(command) if isinstance(command, list) else command,
                                exit_code=exit_code,
                                out=output)

    @staticmethod
    def _process_output(encoding, temp_file_path):
        """Process the output of a command from a temporary file."""
        with open(temp_file_path, 'rb') as temp_file:
            output = temp_file.read()
            if encoding:
                output = output.decode(encoding)
            return output, None  # In Windows stderr writing in stdout

    def _run_command(self, cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding):
        """Execute a command and return the process and its output."""
        if os.name == 'nt' and stdout is None:  # Windows
            with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
                stdout = temp_file
                stderr = subprocess.STDOUT
                process = subprocess.Popen(
                    cmd,
                    shell=shell,
                    stdin=stdin or subprocess.PIPE if input is not None else None,
                    stdout=stdout,
                    stderr=stderr,
                )
                if get_process:
                    return process, None, None
                temp_file_path = temp_file.name

            # Wait process finished
            process.wait()

            output, error = self._process_output(encoding, temp_file_path)
            return process, output, error
        else:  # Other OS
            process = subprocess.Popen(
                cmd,
                shell=shell,
                stdin=stdin or subprocess.PIPE if input is not None else None,
                stdout=stdout or subprocess.PIPE,
                stderr=stderr or subprocess.PIPE,
            )
            if get_process:
                return process, None, None
            try:
                output, error = process.communicate(input=input.encode(encoding) if input else None, timeout=timeout)
                if encoding:
                    output = output.decode(encoding)
                    error = error.decode(encoding)
                return process, output, error
            except subprocess.TimeoutExpired:
                process.kill()
                raise ExecUtilException("Command timed out after {} seconds.".format(timeout))

    def exec_command(self, cmd, wait_exit=False, verbose=False, expect_error=False, encoding=None, shell=False,
                     text=False, input=None, stdin=None, stdout=None, stderr=None, get_process=False, timeout=None):
        """
        Execute a command in a subprocess and handle the output based on the provided parameters.
        """
        process, output, error = self._run_command(cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding)
        if get_process:
            return process
        if process.returncode != 0 or (has_errors(error) and not expect_error):
            self._raise_exec_exception('Utility exited with non-zero code. Error `{}`', cmd, process.returncode, error)

        if verbose:
            return process.returncode, output, error
        else:
            return output

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
        return self.username or getpass.getuser()

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
                return content.decode(encoding or get_default_encoding())
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
