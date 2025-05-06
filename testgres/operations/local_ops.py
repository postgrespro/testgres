import getpass
import logging
import os
import shutil
import stat
import subprocess
import tempfile
import time
import socket

import psutil
import typing
import threading

from ..exceptions import ExecUtilException
from ..exceptions import InvalidOperationException
from .os_ops import ConnectionParams, OsOperations, get_default_encoding
from .raise_error import RaiseError
from .helpers import Helpers

try:
    from shutil import which as find_executable
    from shutil import rmtree
except ImportError:
    from distutils.spawn import find_executable
    from distutils import rmtree

CMD_TIMEOUT_SEC = 60


class LocalOperations(OsOperations):
    sm_single_instance: OsOperations = None
    sm_single_instance_guard = threading.Lock()

    def __init__(self, conn_params=None):
        if conn_params is None:
            conn_params = ConnectionParams()
        super(LocalOperations, self).__init__(conn_params.username)
        self.conn_params = conn_params
        self.host = conn_params.host
        self.ssh_key = None
        self.remote = False
        self.username = conn_params.username or getpass.getuser()

    @staticmethod
    def get_single_instance() -> OsOperations:
        assert __class__ == LocalOperations
        assert __class__.sm_single_instance_guard is not None

        if __class__.sm_single_instance is not None:
            assert type(__class__.sm_single_instance) == __class__  # noqa: E721
            return __class__.sm_single_instance

        with __class__.sm_single_instance_guard:
            if __class__.sm_single_instance is None:
                __class__.sm_single_instance = __class__()
        assert __class__.sm_single_instance is not None
        assert type(__class__.sm_single_instance) == __class__  # noqa: E721
        return __class__.sm_single_instance

    @staticmethod
    def _process_output(encoding, temp_file_path):
        """Process the output of a command from a temporary file."""
        with open(temp_file_path, 'rb') as temp_file:
            output = temp_file.read()
            if encoding:
                output = output.decode(encoding)
            return output, None  # In Windows stderr writing in stdout

    def _run_command__nt(self, cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding, exec_env=None):
        assert exec_env is None or type(exec_env) == dict  # noqa: E721

        # TODO: why don't we use the data from input?

        extParams: typing.Dict[str, str] = dict()

        if exec_env is None:
            pass
        elif len(exec_env) == 0:
            pass
        else:
            env = os.environ.copy()
            assert type(env) == dict  # noqa: E721
            for v in exec_env.items():
                assert type(v) == tuple  # noqa: E721
                assert len(v) == 2
                assert type(v[0]) == str  # noqa: E721
                assert v[0] != ""

                if v[1] is None:
                    env.pop(v[0], None)
                else:
                    assert type(v[1]) == str  # noqa: E721
                    env[v[0]] = v[1]

            extParams["env"] = env

        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
            stdout = temp_file
            stderr = subprocess.STDOUT
            process = subprocess.Popen(
                cmd,
                shell=shell,
                stdin=stdin or subprocess.PIPE if input is not None else None,
                stdout=stdout,
                stderr=stderr,
                **extParams,
            )
            if get_process:
                return process, None, None
            temp_file_path = temp_file.name

        # Wait process finished
        process.wait()

        output, error = self._process_output(encoding, temp_file_path)
        return process, output, error

    def _run_command__generic(self, cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding, exec_env=None):
        assert exec_env is None or type(exec_env) == dict  # noqa: E721

        input_prepared = None
        if not get_process:
            input_prepared = Helpers.PrepareProcessInput(input, encoding)  # throw

        assert input_prepared is None or (type(input_prepared) == bytes)  # noqa: E721

        extParams: typing.Dict[str, str] = dict()

        if exec_env is None:
            pass
        elif len(exec_env) == 0:
            pass
        else:
            env = os.environ.copy()
            assert type(env) == dict  # noqa: E721
            for v in exec_env.items():
                assert type(v) == tuple  # noqa: E721
                assert len(v) == 2
                assert type(v[0]) == str  # noqa: E721
                assert v[0] != ""

                if v[1] is None:
                    env.pop(v[0], None)
                else:
                    assert type(v[1]) == str  # noqa: E721
                    env[v[0]] = v[1]

            extParams["env"] = env

        process = subprocess.Popen(
            cmd,
            shell=shell,
            stdin=stdin or subprocess.PIPE if input is not None else None,
            stdout=stdout or subprocess.PIPE,
            stderr=stderr or subprocess.PIPE,
            **extParams
        )
        assert not (process is None)
        if get_process:
            return process, None, None
        try:
            output, error = process.communicate(input=input_prepared, timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            raise ExecUtilException("Command timed out after {} seconds.".format(timeout))

        assert type(output) == bytes  # noqa: E721
        assert type(error) == bytes  # noqa: E721

        if encoding:
            output = output.decode(encoding)
            error = error.decode(encoding)
        return process, output, error

    def _run_command(self, cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding, exec_env=None):
        """Execute a command and return the process and its output."""
        if os.name == 'nt' and stdout is None:  # Windows
            method = __class__._run_command__nt
        else:  # Other OS
            method = __class__._run_command__generic

        return method(self, cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding, exec_env=exec_env)

    def exec_command(self, cmd, wait_exit=False, verbose=False, expect_error=False, encoding=None, shell=False,
                     text=False, input=None, stdin=None, stdout=None, stderr=None, get_process=False, timeout=None,
                     ignore_errors=False, exec_env=None):
        """
        Execute a command in a subprocess and handle the output based on the provided parameters.
        """
        assert type(expect_error) == bool  # noqa: E721
        assert type(ignore_errors) == bool  # noqa: E721
        assert exec_env is None or type(exec_env) == dict  # noqa: E721

        process, output, error = self._run_command(cmd, shell, input, stdin, stdout, stderr, get_process, timeout, encoding, exec_env=exec_env)
        if get_process:
            return process

        if expect_error:
            if process.returncode == 0:
                raise InvalidOperationException("We expected an execution error.")
        elif ignore_errors:
            pass
        elif process.returncode == 0:
            pass
        else:
            assert not expect_error
            assert not ignore_errors
            assert process.returncode != 0
            RaiseError.UtilityExitedWithNonZeroCode(
                cmd=cmd,
                exit_code=process.returncode,
                msg_arg=error or output,
                error=error,
                out=output)

        if verbose:
            return process.returncode, output, error

        return output

    # Environment setup
    def environ(self, var_name):
        return os.environ.get(var_name)

    def cwd(self):
        return os.getcwd()

    def find_executable(self, executable):
        return find_executable(executable)

    def is_executable(self, file):
        # Check if the file is executable
        assert stat.S_IXUSR != 0
        return (os.stat(file).st_mode & stat.S_IXUSR) == stat.S_IXUSR

    def set_env(self, var_name, var_val):
        # Check if the directory is already in PATH
        os.environ[var_name] = var_val

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

    def makedir(self, path: str):
        assert type(path) == str  # noqa: E721
        os.mkdir(path)

    # [2025-02-03] Old name of parameter attempts is "retries".
    def rmdirs(self, path, ignore_errors=True, attempts=3, delay=1):
        """
        Removes a directory and its contents, retrying on failure.

        :param path: Path to the directory.
        :param ignore_errors: If True, ignore errors.
        :param retries: Number of attempts to remove the directory.
        :param delay: Delay between attempts in seconds.
        """
        assert type(path) == str  # noqa: E721
        assert type(ignore_errors) == bool  # noqa: E721
        assert type(attempts) == int  # noqa: E721
        assert type(delay) == int or type(delay) == float  # noqa: E721
        assert attempts > 0
        assert delay >= 0

        attempt = 0
        while True:
            assert attempt < attempts
            attempt += 1
            try:
                rmtree(path)
            except FileNotFoundError:
                pass
            except Exception as e:
                if attempt < attempt:
                    errMsg = "Failed to remove directory {0} on attempt {1} ({2}): {3}".format(
                        path, attempt, type(e).__name__, e
                    )
                    logging.warning(errMsg)
                    time.sleep(delay)
                    continue

                assert attempt == attempts
                if not ignore_errors:
                    raise

                return False

            # OK!
            return True

    def rmdir(self, path: str):
        assert type(path) == str  # noqa: E721
        os.rmdir(path)

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
            truncate: If True, the file will be truncated before writing ('w' option);
                      if False (default), data will be appended ('a' option).
            binary: If True, the data will be written in binary mode ('b' option);
                    if False (default), the data will be written in text mode.
            read_and_write: If True, the file will be opened with read and write permissions ('+' option);
                            if False (default), only write permission will be used.
        """
        if isinstance(data, bytes) or isinstance(data, list) and all(isinstance(item, bytes) for item in data):
            binary = True

        mode = "w" if truncate else "a"

        if read_and_write:
            mode += "+"

        # If it is a bytes str or list
        if binary:
            mode += "b"

        assert type(mode) == str  # noqa: E721
        assert mode != ""

        with open(filename, mode) as file:
            if isinstance(data, list):
                data2 = [__class__._prepare_line_to_write(s, binary) for s in data]
                file.writelines(data2)
            else:
                data2 = __class__._prepare_data_to_write(data, binary)
                file.write(data2)

    @staticmethod
    def _prepare_line_to_write(data, binary):
        data = __class__._prepare_data_to_write(data, binary)

        if binary:
            assert type(data) == bytes  # noqa: E721
            return data.rstrip(b'\n') + b'\n'

        assert type(data) == str  # noqa: E721
        return data.rstrip('\n') + '\n'

    @staticmethod
    def _prepare_data_to_write(data, binary):
        if isinstance(data, bytes):
            return data if binary else data.decode()

        if isinstance(data, str):
            return data if not binary else data.encode()

        raise InvalidOperationException("Unknown type of data type [{0}].".format(type(data).__name__))

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
        assert type(filename) == str  # noqa: E721
        assert encoding is None or type(encoding) == str  # noqa: E721
        assert type(binary) == bool  # noqa: E721

        if binary:
            if encoding is not None:
                raise InvalidOperationException("Enconding is not allowed for read binary operation")

            return self._read__binary(filename)

        # python behavior
        assert (None or "abc") == "abc"
        assert ("" or "abc") == "abc"

        return self._read__text_with_encoding(filename, encoding or get_default_encoding())

    def _read__text_with_encoding(self, filename, encoding):
        assert type(filename) == str  # noqa: E721
        assert type(encoding) == str  # noqa: E721
        with open(filename, mode='r', encoding=encoding) as file:  # open in a text mode
            content = file.read()
            assert type(content) == str  # noqa: E721
            return content

    def _read__binary(self, filename):
        assert type(filename) == str  # noqa: E721
        with open(filename, 'rb') as file:  # open in a binary mode
            content = file.read()
            assert type(content) == bytes  # noqa: E721
            return content

    def readlines(self, filename, num_lines=0, binary=False, encoding=None):
        """
        Read lines from a local file.
        If num_lines is greater than 0, only the last num_lines lines will be read.
        """
        assert type(num_lines) == int  # noqa: E721
        assert type(filename) == str  # noqa: E721
        assert type(binary) == bool  # noqa: E721
        assert encoding is None or type(encoding) == str  # noqa: E721
        assert num_lines >= 0

        if binary:
            assert encoding is None
            pass
        elif encoding is None:
            encoding = get_default_encoding()
            assert type(encoding) == str  # noqa: E721
        else:
            assert type(encoding) == str  # noqa: E721
            pass

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

    def read_binary(self, filename, offset):
        assert type(filename) == str  # noqa: E721
        assert type(offset) == int  # noqa: E721

        if offset < 0:
            raise ValueError("Negative 'offset' is not supported.")

        with open(filename, 'rb') as file:  # open in a binary mode
            file.seek(offset, os.SEEK_SET)
            r = file.read()
            assert type(r) == bytes  # noqa: E721
            return r

    def isfile(self, remote_file):
        return os.path.isfile(remote_file)

    def isdir(self, dirname):
        return os.path.isdir(dirname)

    def get_file_size(self, filename):
        assert filename is not None
        assert type(filename) == str  # noqa: E721
        return os.path.getsize(filename)

    def remove_file(self, filename):
        return os.remove(filename)

    # Processes control
    def kill(self, pid, signal, expect_error=False):
        # Kill the process
        cmd = "kill -{} {}".format(signal, pid)
        return self.exec_command(cmd, expect_error=expect_error)

    def get_pid(self):
        # Get current process id
        return os.getpid()

    def get_process_children(self, pid):
        assert type(pid) == int  # noqa: E721
        return psutil.Process(pid).children()

    def is_port_free(self, number: int) -> bool:
        assert type(number) == int  # noqa: E721

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("", number))
                return True
            except OSError:
                return False

    def get_tempdir(self) -> str:
        r = tempfile.gettempdir()
        assert r is not None
        assert type(r) == str  # noqa: E721
        assert os.path.exists(r)
        return r
