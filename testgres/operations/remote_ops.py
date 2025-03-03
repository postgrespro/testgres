import getpass
import os
import platform
import subprocess
import tempfile
import io

# we support both pg8000 and psycopg2
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

from ..exceptions import ExecUtilException
from ..exceptions import InvalidOperationException
from .os_ops import OsOperations, ConnectionParams, get_default_encoding
from .raise_error import RaiseError
from .helpers import Helpers

error_markers = [b'error', b'Permission denied', b'fatal', b'No such file or directory']


class PsUtilProcessProxy:
    def __init__(self, ssh, pid):
        self.ssh = ssh
        self.pid = pid

    def kill(self):
        command = "kill {}".format(self.pid)
        self.ssh.exec_command(command)

    def cmdline(self):
        command = "ps -p {} -o cmd --no-headers".format(self.pid)
        stdin, stdout, stderr = self.ssh.exec_command(command, verbose=True, encoding=get_default_encoding())
        cmdline = stdout.strip()
        return cmdline.split()


class RemoteOperations(OsOperations):
    def __init__(self, conn_params: ConnectionParams):
        if not platform.system().lower() == "linux":
            raise EnvironmentError("Remote operations are supported only on Linux!")

        super().__init__(conn_params.username)
        self.conn_params = conn_params
        self.host = conn_params.host
        self.port = conn_params.port
        self.ssh_key = conn_params.ssh_key
        self.ssh_args = []
        if self.ssh_key:
            self.ssh_args += ["-i", self.ssh_key]
        if self.port:
            self.ssh_args += ["-p", self.port]
        self.remote = True
        self.username = conn_params.username or getpass.getuser()
        self.ssh_dest = f"{self.username}@{self.host}" if conn_params.username else self.host

    def __enter__(self):
        return self

    def exec_command(self, cmd, wait_exit=False, verbose=False, expect_error=False,
                     encoding=None, shell=True, text=False, input=None, stdin=None, stdout=None,
                     stderr=None, get_process=None, timeout=None, ignore_errors=False):
        """
        Execute a command in the SSH session.
        Args:
        - cmd (str): The command to be executed.
        """
        assert type(expect_error) == bool  # noqa: E721
        assert type(ignore_errors) == bool  # noqa: E721

        input_prepared = None
        if not get_process:
            input_prepared = Helpers.PrepareProcessInput(input, encoding)  # throw

        assert input_prepared is None or (type(input_prepared) == bytes)  # noqa: E721

        if type(cmd) == str:  # noqa: E721
            cmd_s = cmd
        elif type(cmd) == list:  # noqa: E721
            cmd_s = subprocess.list2cmdline(cmd)
        else:
            raise ValueError("Invalid 'cmd' argument type - {0}".format(type(cmd).__name__))

        assert type(cmd_s) == str  # noqa: E721

        cmd_items = __class__._make_exec_env_list()
        cmd_items.append(cmd_s)

        env_cmd_s = ';'.join(cmd_items)

        ssh_cmd = ['ssh', self.ssh_dest] + self.ssh_args + [env_cmd_s]

        process = subprocess.Popen(ssh_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert not (process is None)
        if get_process:
            return process

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
                msg_arg=error,
                error=error,
                out=output)

        if verbose:
            return process.returncode, output, error

        return output

    # Environment setup
    def environ(self, var_name: str) -> str:
        """
        Get the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        """
        cmd = "echo ${}".format(var_name)
        return self.exec_command(cmd, encoding=get_default_encoding()).strip()

    def cwd(self):
        cmd = 'pwd'
        return self.exec_command(cmd, encoding=get_default_encoding()).rstrip()

    def find_executable(self, executable):
        search_paths = self.environ("PATH")
        if not search_paths:
            return None

        search_paths = search_paths.split(self.pathsep)
        for path in search_paths:
            remote_file = os.path.join(path, executable)
            if self.isfile(remote_file):
                return remote_file

        return None

    def is_executable(self, file):
        # Check if the file is executable
        command = ["test", "-x", file]

        exit_status, output, error = self.exec_command(cmd=command, encoding=get_default_encoding(), ignore_errors=True, verbose=True)

        assert type(output) == str  # noqa: E721
        assert type(error) == str  # noqa: E721

        if exit_status == 0:
            return True

        if exit_status == 1:
            return False

        errMsg = "Test operation returns an unknown result code: {0}. File name is [{1}].".format(
            exit_status,
            file)

        RaiseError.CommandExecutionError(
            cmd=command,
            exit_code=exit_status,
            msg_arg=errMsg,
            error=error,
            out=output
        )

    def set_env(self, var_name: str, var_val: str):
        """
        Set the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        - var_val (str): The value to be set for the environment variable.
        """
        return self.exec_command("export {}={}".format(var_name, var_val))

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd, encoding=get_default_encoding()).strip()

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        """
        Create a directory in the remote server.
        Args:
        - path (str): The path to the directory to be created.
        - remove_existing (bool): If True, the existing directory at the path will be removed.
        """
        if remove_existing:
            cmd = "rm -rf {} && mkdir -p {}".format(path, path)
        else:
            cmd = "mkdir -p {}".format(path)
        try:
            exit_status, result, error = self.exec_command(cmd, verbose=True)
        except ExecUtilException as e:
            raise Exception("Couldn't create dir {} because of error {}".format(path, e.message))
        if exit_status != 0:
            raise Exception("Couldn't create dir {} because of error {}".format(path, error))
        return result

    def rmdirs(self, path, verbose=False, ignore_errors=True):
        """
        Remove a directory in the remote server.
        Args:
        - path (str): The path to the directory to be removed.
        - verbose (bool): If True, return exit status, result, and error.
        - ignore_errors (bool): If True, do not raise error if directory does not exist.
        """
        cmd = "rm -rf {}".format(path)
        exit_status, result, error = self.exec_command(cmd, verbose=True)
        if verbose:
            return exit_status, result, error
        else:
            return result

    def listdir(self, path):
        """
        List all files and directories in a directory.
        Args:
        path (str): The path to the directory.
        """
        result = self.exec_command("ls {}".format(path))
        return result.splitlines()

    def path_exists(self, path):
        command = ["test", "-e", path]

        exit_status, output, error = self.exec_command(cmd=command, encoding=get_default_encoding(), ignore_errors=True, verbose=True)

        assert type(output) == str  # noqa: E721
        assert type(error) == str  # noqa: E721

        if exit_status == 0:
            return True

        if exit_status == 1:
            return False

        errMsg = "Test operation returns an unknown result code: {0}. Path is [{1}].".format(
            exit_status,
            path)

        RaiseError.CommandExecutionError(
            cmd=command,
            exit_code=exit_status,
            msg_arg=errMsg,
            error=error,
            out=output
        )

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
        """
        Creates a temporary directory in the remote server.
        Args:
        - prefix (str): The prefix of the temporary directory name.
        """
        if prefix:
            command = ["mktemp", "-d", "-t", prefix + "XXXXX"]
        else:
            command = ["mktemp", "-d"]

        exec_exitcode, exec_output, exec_error = self.exec_command(command, verbose=True, encoding=get_default_encoding(), ignore_errors=True)

        assert type(exec_exitcode) == int  # noqa: E721
        assert type(exec_output) == str  # noqa: E721
        assert type(exec_error) == str  # noqa: E721

        if exec_exitcode != 0:
            RaiseError.CommandExecutionError(
                cmd=command,
                exit_code=exec_exitcode,
                message="Could not create temporary directory.",
                error=exec_error,
                out=exec_output)

        temp_dir = exec_output.strip()
        return temp_dir

    def mkstemp(self, prefix=None):
        """
        Creates a temporary file in the remote server.
        Args:
        - prefix (str): The prefix of the temporary directory name.
        """
        if prefix:
            command = ["mktemp", "-t", prefix + "XXXXX"]
        else:
            command = ["mktemp"]

        exec_exitcode, exec_output, exec_error = self.exec_command(command, verbose=True, encoding=get_default_encoding(), ignore_errors=True)

        assert type(exec_exitcode) == int  # noqa: E721
        assert type(exec_output) == str  # noqa: E721
        assert type(exec_error) == str  # noqa: E721

        if exec_exitcode != 0:
            RaiseError.CommandExecutionError(
                cmd=command,
                exit_code=exec_exitcode,
                message="Could not create temporary file.",
                error=exec_error,
                out=exec_output)

        temp_file = exec_output.strip()
        return temp_file

    def copytree(self, src, dst):
        if not os.path.isabs(dst):
            dst = os.path.join('~', dst)
        if self.isdir(dst):
            raise FileExistsError("Directory {} already exists.".format(dst))
        return self.exec_command("cp -r {} {}".format(src, dst))

    # Work with files
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False, encoding=None):
        if not encoding:
            encoding = get_default_encoding()
        mode = "wb" if binary else "w"

        with tempfile.NamedTemporaryFile(mode=mode, delete=False) as tmp_file:
            # For scp the port is specified by a "-P" option
            scp_args = ['-P' if x == '-p' else x for x in self.ssh_args]

            if not truncate:
                scp_cmd = ['scp'] + scp_args + [f"{self.ssh_dest}:{filename}", tmp_file.name]
                subprocess.run(scp_cmd, check=False)  # The file might not exist yet
                tmp_file.seek(0, os.SEEK_END)

            if isinstance(data, list):
                data2 = [__class__._prepare_line_to_write(s, binary, encoding) for s in data]
                tmp_file.writelines(data2)
            else:
                data2 = __class__._prepare_data_to_write(data, binary, encoding)
                tmp_file.write(data2)

            tmp_file.flush()
            scp_cmd = ['scp'] + scp_args + [tmp_file.name, f"{self.ssh_dest}:{filename}"]
            subprocess.run(scp_cmd, check=True)

            remote_directory = os.path.dirname(filename)
            mkdir_cmd = ['ssh'] + self.ssh_args + [self.ssh_dest, f"mkdir -p {remote_directory}"]
            subprocess.run(mkdir_cmd, check=True)

            os.remove(tmp_file.name)

    @staticmethod
    def _prepare_line_to_write(data, binary, encoding):
        data = __class__._prepare_data_to_write(data, binary, encoding)

        if binary:
            assert type(data) == bytes  # noqa: E721
            return data.rstrip(b'\n') + b'\n'

        assert type(data) == str  # noqa: E721
        return data.rstrip('\n') + '\n'

    @staticmethod
    def _prepare_data_to_write(data, binary, encoding):
        if isinstance(data, bytes):
            return data if binary else data.decode(encoding)

        if isinstance(data, str):
            return data if not binary else data.encode(encoding)

        raise InvalidOperationException("Unknown type of data type [{0}].".format(type(data).__name__))

    def touch(self, filename):
        """
        Create a new file or update the access and modification times of an existing file on the remote server.

        Args:
            filename (str): The name of the file to touch.

        This method behaves as the 'touch' command in Unix. It's equivalent to calling 'touch filename' in the shell.
        """
        self.exec_command("touch {}".format(filename))

    def read(self, filename, binary=False, encoding=None):
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
        content = self._read__binary(filename)
        assert type(content) == bytes  # noqa: E721
        buf0 = io.BytesIO(content)
        buf1 = io.TextIOWrapper(buf0, encoding=encoding)
        content_s = buf1.read()
        assert type(content_s) == str  # noqa: E721
        return content_s

    def _read__binary(self, filename):
        assert type(filename) == str  # noqa: E721
        cmd = ["cat", filename]
        content = self.exec_command(cmd)
        assert type(content) == bytes  # noqa: E721
        return content

    def readlines(self, filename, num_lines=0, binary=False, encoding=None):
        if num_lines > 0:
            cmd = "tail -n {} {}".format(num_lines, filename)
        else:
            cmd = "cat {}".format(filename)

        result = self.exec_command(cmd, encoding=encoding)

        if not binary and result:
            lines = result.decode(encoding or get_default_encoding()).splitlines()
        else:
            lines = result.splitlines()

        return lines

    def read_binary(self, filename, offset):
        assert type(filename) == str  # noqa: E721
        assert type(offset) == int  # noqa: E721

        if offset < 0:
            raise ValueError("Negative 'offset' is not supported.")

        cmd = ["tail", "-c", "+{}".format(offset + 1), filename]
        r = self.exec_command(cmd)
        assert type(r) == bytes  # noqa: E721
        return r

    def isfile(self, remote_file):
        stdout = self.exec_command("test -f {}; echo $?".format(remote_file))
        result = int(stdout.strip())
        return result == 0

    def isdir(self, dirname):
        cmd = "if [ -d {} ]; then echo True; else echo False; fi".format(dirname)
        response = self.exec_command(cmd)
        return response.strip() == b"True"

    def get_file_size(self, filename):
        C_ERR_SRC = "RemoteOpertions::get_file_size"

        assert filename is not None
        assert type(filename) == str  # noqa: E721
        cmd = ["du", "-b", filename]

        s = self.exec_command(cmd, encoding=get_default_encoding())
        assert type(s) == str  # noqa: E721

        if len(s) == 0:
            raise Exception(
                "[BUG CHECK] Can't get size of file [{2}]. Remote operation returned an empty string. Check point [{0}][{1}].".format(
                    C_ERR_SRC,
                    "#001",
                    filename
                )
            )

        i = 0

        while i < len(s) and s[i].isdigit():
            assert s[i] >= '0'
            assert s[i] <= '9'
            i += 1

        if i == 0:
            raise Exception(
                "[BUG CHECK] Can't get size of file [{2}]. Remote operation returned a bad formatted string. Check point [{0}][{1}].".format(
                    C_ERR_SRC,
                    "#002",
                    filename
                )
            )

        if i == len(s):
            raise Exception(
                "[BUG CHECK] Can't get size of file [{2}]. Remote operation returned a bad formatted string. Check point [{0}][{1}].".format(
                    C_ERR_SRC,
                    "#003",
                    filename
                )
            )

        if not s[i].isspace():
            raise Exception(
                "[BUG CHECK] Can't get size of file [{2}]. Remote operation returned a bad formatted string. Check point [{0}][{1}].".format(
                    C_ERR_SRC,
                    "#004",
                    filename
                )
            )

        r = 0

        for i2 in range(0, i):
            ch = s[i2]
            assert ch >= '0'
            assert ch <= '9'
            # Here is needed to check overflow or that it is a human-valid result?
            r = (r * 10) + ord(ch) - ord('0')

        return r

    def remove_file(self, filename):
        cmd = "rm {}".format(filename)
        return self.exec_command(cmd)

    # Processes control
    def kill(self, pid, signal):
        # Kill the process
        cmd = "kill -{} {}".format(signal, pid)
        return self.exec_command(cmd)

    def get_pid(self):
        # Get current process id
        return int(self.exec_command("echo $$", encoding=get_default_encoding()))

    def get_process_children(self, pid):
        command = ["ssh"] + self.ssh_args + [self.ssh_dest, f"pgrep -P {pid}"]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            children = result.stdout.strip().splitlines()
            return [PsUtilProcessProxy(self, int(child_pid.strip())) for child_pid in children]
        else:
            raise ExecUtilException(f"Error in getting process children. Error: {result.stderr}")

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

    @staticmethod
    def _make_exec_env_list() -> list[str]:
        result = list[str]()
        for envvar in os.environ.items():
            if not __class__._does_put_envvar_into_exec_cmd(envvar[0]):
                continue
            qvalue = __class__._quote_envvar(envvar[1])
            assert type(qvalue) == str   # noqa: E721
            result.append(envvar[0] + "=" + qvalue)
            continue

        return result

    sm_envs_for_exec_cmd = ["LANG", "LANGUAGE"]

    @staticmethod
    def _does_put_envvar_into_exec_cmd(name: str) -> bool:
        assert type(name) == str  # noqa: E721
        name = name.upper()
        if name.startswith("LC_"):
            return True
        if name in __class__.sm_envs_for_exec_cmd:
            return True
        return False

    @staticmethod
    def _quote_envvar(value: str) -> str:
        assert type(value) == str  # noqa: E721
        result = "\""
        for ch in value:
            if ch == "\"":
                result += "\\\""
            elif ch == "\\":
                result += "\\\\"
            else:
                result += ch
        result += "\""
        return result


def normalize_error(error):
    if isinstance(error, bytes):
        return error.decode()
    return error
