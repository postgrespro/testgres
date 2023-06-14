import os
import tempfile
from typing import Optional

import paramiko
from paramiko import SSHClient

from logger import log
from .os_ops import OsOperations
from .os_ops import pglib

error_markers = [b'error', b'Permission denied']


class RemoteOperations(OsOperations):
    def __init__(self, hostname="localhost", host="127.0.0.1", ssh_key=None, username=None):
        super().__init__(username)
        self.host = host
        self.ssh_key = ssh_key
        self.remote = True
        self.ssh = self.ssh_connect()
        self.username = username or self.get_user()

    def __del__(self):
        if self.ssh:
            self.ssh.close()

    def ssh_connect(self) -> Optional[SSHClient]:
        if not self.remote:
            return None
        else:
            key = self._read_ssh_key()
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host, username=self.username, pkey=key)
            return ssh

    def _read_ssh_key(self):
        try:
            with open(self.ssh_key, "r") as f:
                key_data = f.read()
                if "BEGIN OPENSSH PRIVATE KEY" in key_data:
                    key = paramiko.Ed25519Key.from_private_key_file(self.ssh_key)
                else:
                    key = paramiko.RSAKey.from_private_key_file(self.ssh_key)
                return key
        except FileNotFoundError:
            log.error(f"No such file or directory: '{self.ssh_key}'")
        except Exception as e:
            log.error(f"An error occurred while reading the ssh key: {e}")

    def exec_command(self, cmd: str, wait_exit=False, verbose=False, expect_error=False,
                     encoding=None, shell=True, text=False, input=None, stdout=None,
                     stderr=None, proc=None):
        """
        Execute a command in the SSH session.
        Args:
        - cmd (str): The command to be executed.
        """
        if isinstance(cmd, list):
            cmd = " ".join(cmd)
        try:
            if input:
                stdin, stdout, stderr = self.ssh.exec_command(cmd)
                stdin.write(input.encode("utf-8"))
                stdin.flush()
            else:
                stdin, stdout, stderr = self.ssh.exec_command(cmd)
            exit_status = 0
            if wait_exit:
                exit_status = stdout.channel.recv_exit_status()

            if encoding:
                result = stdout.read().decode(encoding)
                error = stderr.read().decode(encoding)
            else:
                result = stdout.read()
                error = stderr.read()

            if expect_error:
                raise Exception(result, error)

            if encoding:
                error_found = exit_status != 0 or any(
                    marker.decode(encoding) in error for marker in error_markers)
            else:
                error_found = exit_status != 0 or any(
                    marker in error for marker in error_markers)

            if error_found:
                log.error(
                    f"Problem in executing command: `{cmd}`\nerror: {error}\nexit_code: {exit_status}"
                )
                if exit_status == 0:
                    exit_status = 1

            if verbose:
                return exit_status, result, error
            else:
                return result

        except Exception as e:
            log.error(f"Unexpected error while executing command `{cmd}`: {e}")
            return None

    # Environment setup
    def environ(self, var_name: str) -> str:
        """
        Get the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        """
        cmd = f"echo ${var_name}"
        return self.exec_command(cmd).strip()

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
        is_exec = self.exec_command(f"test -x {file} && echo OK")
        return is_exec == b"OK\n"

    def add_to_path(self, new_path):
        pathsep = self.pathsep
        # Check if the directory is already in PATH
        path = self.environ("PATH")
        if new_path not in path.split(pathsep):
            if self.remote:
                self.exec_command(f"export PATH={new_path}{pathsep}{path}")
            else:
                os.environ["PATH"] = f"{new_path}{pathsep}{path}"
        return pathsep

    def set_env(self, var_name: str, var_val: str) -> None:
        """
        Set the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        - var_val (str): The value to be set for the environment variable.
        """
        return self.exec_command(f"export {var_name}={var_val}")

    # Get environment variables
    def get_user(self):
        return self.exec_command("echo $USER")

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd).strip()

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        """
        Create a directory in the remote server.
        Args:
        - path (str): The path to the directory to be created.
        - remove_existing (bool): If True, the existing directory at the path will be removed.
        """
        if remove_existing:
            cmd = f"rm -rf {path} && mkdir -p {path}"
        else:
            cmd = f"mkdir -p {path}"
        exit_status, result, error = self.exec_command(cmd, verbose=True)
        if exit_status != 0:
            raise Exception(f"Couldn't create dir {path} because of error {error}")
        return result

    def rmdirs(self, path, verbose=False, ignore_errors=True):
        """
        Remove a directory in the remote server.
        Args:
        - path (str): The path to the directory to be removed.
        - verbose (bool): If True, return exit status, result, and error.
        - ignore_errors (bool): If True, do not raise error if directory does not exist.
        """
        cmd = f"rm -rf {path}"
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
        result = self.exec_command(f"ls {path}")
        return result.splitlines()

    def path_exists(self, path):
        result = self.exec_command(f"test -e {path}; echo $?", encoding='utf-8')
        return int(result.strip()) == 0

    @property
    def pathsep(self):
        os_name = self.get_name()
        if os_name == "posix":
            pathsep = ":"
        elif os_name == "nt":
            pathsep = ";"
        else:
            raise Exception(f"Unsupported operating system: {os_name}")
        return pathsep

    def mkdtemp(self, prefix=None):
        """
        Creates a temporary directory in the remote server.
        Args:
        prefix (str): The prefix of the temporary directory name.
        """
        temp_dir = self.exec_command(f"mkdtemp -d {prefix}", encoding='utf-8')
        return temp_dir.strip()

    def mkstemp(self, prefix=None):
        cmd = f"mktemp {prefix}XXXXXX"
        filename = self.exec_command(cmd).strip()
        return filename

    def copytree(self, src, dst):
        return self.exec_command(f"cp -r {src} {dst}")

    # Work with files
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False, encoding='utf-8'):
        """
        Write data to a file on a remote host

        Args:
        - filename (str): The file path where the data will be written.
        - data (bytes or str): The data to be written to the file.
        - truncate (bool): If True, the file will be truncated before writing ('w' or 'wb' option);
                         if False (default), data will be appended ('a' or 'ab' option).
        - binary (bool): If True, the data will be written in binary mode ('wb' or 'ab' option);
                       if False (default), the data will be written in text mode ('w' or 'a' option).
        - read_and_write (bool): If True, the file will be opened with read and write permissions ('r+' option);
                               if False (default), only write permission will be used ('w', 'a', 'wb', or 'ab' option).
        """
        mode = "wb" if binary else "w"
        if not truncate:
            mode = "ab" if binary else "a"
        if read_and_write:
            mode = "r+b" if binary else "r+"

        with tempfile.NamedTemporaryFile(mode=mode) as tmp_file:
            if isinstance(data, bytes) and not binary:
                data = data.decode(encoding)
            elif isinstance(data, str) and binary:
                data = data.encode(encoding)

            tmp_file.write(data)
            tmp_file.flush()

            with self.ssh_connect() as ssh:
                sftp = ssh.open_sftp()
                sftp.put(tmp_file.name, filename)
                sftp.close()

    def touch(self, filename):
        """
        Create a new file or update the access and modification times of an existing file on the remote server.

        Args:
            filename (str): The name of the file to touch.

        This method behaves as the 'touch' command in Unix. It's equivalent to calling 'touch filename' in the shell.
        """
        self.exec_command(f"touch {filename}")

    def read(self, filename, binary=False, encoding=None):
        cmd = f"cat {filename}"
        result = self.exec_command(cmd, encoding=encoding)

        if not binary and result:
            result = result.decode(encoding or 'utf-8')

        return result

    def readlines(self, filename, num_lines=0, binary=False, encoding=None):
        if num_lines > 0:
            cmd = f"tail -n {num_lines} {filename}"
        else:
            cmd = f"cat {filename}"

        result = self.exec_command(cmd, encoding=encoding)

        if not binary and result:
            lines = result.decode(encoding or 'utf-8').splitlines()
        else:
            lines = result.splitlines()

        return lines

    def isfile(self, remote_file):
        stdout = self.exec_command(f"test -f {remote_file}; echo $?")
        result = int(stdout.strip())
        return result == 0

    # Processes control
    def kill(self, pid, signal):
        # Kill the process
        cmd = f"kill -{signal} {pid}"
        return self.exec_command(cmd)

    def get_pid(self):
        # Get current process id
        return self.exec_command("echo $$")

    # Database control
    def db_connect(self, dbname, user, password=None, host="127.0.0.1", hostname="localhost", port=5432):
        """
        Connects to a PostgreSQL database on the remote system.
        Args:
        - dbname (str): The name of the database to connect to.
        - user (str): The username for the database connection.
        - password (str, optional): The password for the database connection. Defaults to None.
        - host (str, optional): The IP address of the remote system. Defaults to "127.0.0.1".
        - hostname (str, optional): The hostname of the remote system. Defaults to "localhost".
        - port (int, optional): The port number of the PostgreSQL service. Defaults to 5432.

        This function establishes a connection to a PostgreSQL database on the remote system using the specified
        parameters. It returns a connection object that can be used to interact with the database.
        """
        transport = self.ssh.get_transport()
        local_port = 9090  # or any other available port

        transport.open_channel(
            'direct-tcpip',
            (hostname, port),
            (host, local_port)
        )

        conn = pglib.connect(
            host=host,
            port=local_port,
            database=dbname,
            user=user,
            password=password,
        )
        return conn

