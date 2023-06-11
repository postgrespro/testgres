import os
import tempfile
from contextlib import contextmanager

from testgres.logger import log

from .os_ops import OsOperations
from .os_ops import pglib

import paramiko


class RemoteOperations(OsOperations):
    """
    This class specifically supports work with Linux systems. It utilizes the SSH
    for making connections and performing various file and directory operations, command executions,
    environment setup and management, process control, and database connections.
    It uses the Paramiko library for SSH connections and operations.

    Some methods are designed to work with specific Linux shell commands, and thus may not work as expected
    on other non-Linux systems.

    Attributes:
    - hostname (str): The remote system's hostname. Default 'localhost'.
    - host (str): The remote system's IP address. Default '127.0.0.1'.
    - ssh_key (str): Path to the SSH private key for authentication.
    - username (str): Username for the remote system.
    - ssh (paramiko.SSHClient): SSH connection to the remote system.
    """

    def __init__(
        self, hostname="localhost", host="127.0.0.1", ssh_key=None, username=None
    ):
        super().__init__(username)
        self.hostname = hostname
        self.host = host
        self.ssh_key = ssh_key
        self.remote = True
        self.ssh = self.connect()
        self.username = username or self.get_user()

    def __del__(self):
        if self.ssh:
            self.ssh.close()

    @contextmanager
    def ssh_connect(self):
        if not self.remote:
            yield None
        else:
            with open(self.ssh_key, "r") as f:
                key_data = f.read()
                if "BEGIN OPENSSH PRIVATE KEY" in key_data:
                    key = paramiko.Ed25519Key.from_private_key_file(self.ssh_key)
                else:
                    key = paramiko.RSAKey.from_private_key_file(self.ssh_key)

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(self.host, username=self.username, pkey=key)
                yield ssh

    def connect(self):
        with self.ssh_connect() as ssh:
            return ssh

    # Command execution
    def exec_command(
        self, cmd, wait_exit=False, verbose=False, expect_error=False, encoding="utf-8"
    ):
        if isinstance(cmd, list):
            cmd = " ".join(cmd)
        log.debug(f"os_ops.exec_command: `{cmd}`; remote={self.remote}")
        # Source global profile file + execute command
        try:
            cmd = f"source /etc/profile.d/custom.sh; {cmd}"
            with self.ssh_connect() as ssh:
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = 0
                if wait_exit:
                    exit_status = stdout.channel.recv_exit_status()
                result = stdout.read().decode(encoding)
                error = stderr.read().decode(encoding)

            if expect_error:
                raise Exception(result, error)
            if exit_status != 0 or "error" in error.lower():
                log.error(
                    f"Problem in executing command: `{cmd}`\nerror: {error}\nexit_code: {exit_status}"
                )
                exit(1)

            if verbose:
                return exit_status, result, error
            else:
                return result

        except Exception as e:
            log.error(f"Unexpected error while executing command `{cmd}`: {e}")
            return None

    # Environment setup
    def environ(self, var_name):
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
        return self.exec_command(f"test -x {file} && echo OK") == "OK\n"

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

    def set_env(self, var_name, var_val):
        # Check if the directory is already in PATH
        return self.exec_command(f"export {var_name}={var_val}")

    # Get environment variables
    def get_user(self):
        return self.exec_command("echo $USER")

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd).strip()

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        if remove_existing:
            cmd = f"rm -rf {path} && mkdir -p {path}"
        else:
            cmd = f"mkdir -p {path}"
        return self.exec_command(cmd)

    def rmdirs(self, path, ignore_errors=True):
        cmd = f"rm -rf {path}"
        return self.exec_command(cmd)

    def listdir(self, path):
        result = self.exec_command(f"ls {path}")
        return result.splitlines()

    def path_exists(self, path):
        result = self.exec_command(f"test -e {path}; echo $?")
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
        temp_dir = self.exec_command(f"mkdtemp -d {prefix}")
        return temp_dir.strip()

    def mkstemp(self, prefix=None):
        cmd = f"mktemp {prefix}XXXXXX"
        filename = self.exec_command(cmd).strip()
        return filename

    def copytree(self, src, dst):
        return self.exec_command(f"cp -r {src} {dst}")

    # Work with files
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False):
        """
        Write data to a file on a remote host
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
        mode = "wb" if binary else "w"
        if not truncate:
            mode = "a" + mode
        if read_and_write:
            mode = "r+" + mode

        with tempfile.NamedTemporaryFile(mode=mode) as tmp_file:
            if isinstance(data, list):
                tmp_file.writelines(data)
            else:
                tmp_file.write(data)
            tmp_file.flush()

            sftp = self.ssh.open_sftp()
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

    def read(self, filename, encoding="utf-8"):
        cmd = f"cat {filename}"
        return self.exec_command(cmd, encoding=encoding)

    def readlines(self, filename, num_lines=0, encoding=None):
        encoding = encoding or "utf-8"
        if num_lines > 0:
            cmd = f"tail -n {num_lines} {filename}"
            lines = self.exec_command(cmd, encoding)
        else:
            lines = self.read(filename, encoding=encoding).splitlines()
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
    def db_connect(self, dbname, user, password=None, host="localhost", port=5432):
        local_port = self.ssh.forward_remote_port(host, port)
        conn = pglib.connect(
            host=host,
            port=local_port,
            database=dbname,
            user=user,
            password=password,
        )
        return conn
