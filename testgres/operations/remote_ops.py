import os
import tempfile
import time
from typing import Optional

import sshtunnel

import paramiko
from paramiko import SSHClient

from ..exceptions import ExecUtilException

from .os_ops import OsOperations, ConnectionParams
from .os_ops import pglib

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0


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
        stdin, stdout, stderr = self.ssh.exec_command(command)
        cmdline = stdout.read().decode('utf-8').strip()
        return cmdline.split()


class RemoteOperations(OsOperations):
    def __init__(self, conn_params: ConnectionParams):
        super().__init__(conn_params.username)
        self.conn_params = conn_params
        self.host = conn_params.host
        self.ssh_key = conn_params.ssh_key
        self.ssh = self.ssh_connect()
        self.remote = True
        self.username = conn_params.username or self.get_user()
        self.tunnel = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_tunnel()
        if getattr(self, 'ssh', None):
            self.ssh.close()

    def __del__(self):
        if getattr(self, 'ssh', None):
            self.ssh.close()

    def close_tunnel(self):
        if getattr(self, 'tunnel', None):
            self.tunnel.stop(force=True)
            start_time = time.time()
            while self.tunnel.is_active:
                if time.time() - start_time > sshtunnel.TUNNEL_TIMEOUT:
                    break
                time.sleep(0.5)

    def ssh_connect(self) -> Optional[SSHClient]:
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
            raise ExecUtilException(message="No such file or directory: '{}'".format(self.ssh_key))
        except Exception as e:
            ExecUtilException(message="An error occurred while reading the ssh key: {}".format(e))

    def exec_command(self, cmd: str, wait_exit=False, verbose=False, expect_error=False,
                     encoding=None, shell=True, text=False, input=None, stdin=None, stdout=None,
                     stderr=None, proc=None):
        """
        Execute a command in the SSH session.
        Args:
        - cmd (str): The command to be executed.
        """
        if self.ssh is None or not self.ssh.get_transport() or not self.ssh.get_transport().is_active():
            self.ssh = self.ssh_connect()

        if isinstance(cmd, list):
            cmd = ' '.join(item.decode('utf-8') if isinstance(item, bytes) else item for item in cmd)
        if input:
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            stdin.write(input)
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
            if exit_status == 0:
                exit_status = 1
            if encoding:
                message = "Utility exited with non-zero code. Error: {}".format(error.decode(encoding))
            else:
                message = b"Utility exited with non-zero code. Error: " + error
            raise ExecUtilException(message=message,
                                    command=cmd,
                                    exit_code=exit_status,
                                    out=result)

        if verbose:
            return exit_status, result, error
        else:
            return result

    # Environment setup
    def environ(self, var_name: str) -> str:
        """
        Get the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        """
        cmd = "echo ${}".format(var_name)
        return self.exec_command(cmd, encoding='utf-8').strip()

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
        is_exec = self.exec_command("test -x {} && echo OK".format(file))
        return is_exec == b"OK\n"

    def set_env(self, var_name: str, var_val: str):
        """
        Set the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        - var_val (str): The value to be set for the environment variable.
        """
        return self.exec_command("export {}={}".format(var_name, var_val))

    # Get environment variables
    def get_user(self):
        return self.exec_command("echo $USER", encoding='utf-8').strip()

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd, encoding='utf-8').strip()

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
        result = self.exec_command("test -e {}; echo $?".format(path), encoding='utf-8')
        return int(result.strip()) == 0

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
            temp_dir = self.exec_command("mktemp -d {}XXXXX".format(prefix), encoding='utf-8')
        else:
            temp_dir = self.exec_command("mktemp -d", encoding='utf-8')

        if temp_dir:
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.join('/home', self.username, temp_dir.strip())
            return temp_dir
        else:
            raise ExecUtilException("Could not create temporary directory.")

    def mkstemp(self, prefix=None):
        if prefix:
            temp_dir = self.exec_command("mktemp {}XXXXX".format(prefix), encoding='utf-8')
        else:
            temp_dir = self.exec_command("mktemp", encoding='utf-8')

        if temp_dir:
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.join('/home', self.username, temp_dir.strip())
            return temp_dir
        else:
            raise ExecUtilException("Could not create temporary directory.")

    def copytree(self, src, dst):
        if not os.path.isabs(dst):
            dst = os.path.join('~', dst)
        if self.isdir(dst):
            raise FileExistsError("Directory {} already exists.".format(dst))
        return self.exec_command("cp -r {} {}".format(src, dst))

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

        with tempfile.NamedTemporaryFile(mode=mode, delete=False) as tmp_file:
            if not truncate:
                with self.ssh_connect() as ssh:
                    sftp = ssh.open_sftp()
                    try:
                        sftp.get(filename, tmp_file.name)
                        tmp_file.seek(0, os.SEEK_END)
                    except FileNotFoundError:
                        pass  # File does not exist yet, we'll create it
                    sftp.close()
            if isinstance(data, bytes) and not binary:
                data = data.decode(encoding)
            elif isinstance(data, str) and binary:
                data = data.encode(encoding)
            if isinstance(data, list):
                # ensure each line ends with a newline
                data = [(s if isinstance(s, str) else s.decode('utf-8')).rstrip('\n') + '\n' for s in data]
                tmp_file.writelines(data)
            else:
                tmp_file.write(data)
            tmp_file.flush()

            with self.ssh_connect() as ssh:
                sftp = ssh.open_sftp()
                remote_directory = os.path.dirname(filename)
                try:
                    sftp.stat(remote_directory)
                except IOError:
                    sftp.mkdir(remote_directory)
                sftp.put(tmp_file.name, filename)
                sftp.close()

            os.remove(tmp_file.name)

    def touch(self, filename):
        """
        Create a new file or update the access and modification times of an existing file on the remote server.

        Args:
            filename (str): The name of the file to touch.

        This method behaves as the 'touch' command in Unix. It's equivalent to calling 'touch filename' in the shell.
        """
        self.exec_command("touch {}".format(filename))

    def read(self, filename, binary=False, encoding=None):
        cmd = "cat {}".format(filename)
        result = self.exec_command(cmd, encoding=encoding)

        if not binary and result:
            result = result.decode(encoding or 'utf-8')

        return result

    def readlines(self, filename, num_lines=0, binary=False, encoding=None):
        if num_lines > 0:
            cmd = "tail -n {} {}".format(num_lines, filename)
        else:
            cmd = "cat {}".format(filename)

        result = self.exec_command(cmd, encoding=encoding)

        if not binary and result:
            lines = result.decode(encoding or 'utf-8').splitlines()
        else:
            lines = result.splitlines()

        return lines

    def isfile(self, remote_file):
        stdout = self.exec_command("test -f {}; echo $?".format(remote_file))
        result = int(stdout.strip())
        return result == 0

    def isdir(self, dirname):
        cmd = "if [ -d {} ]; then echo True; else echo False; fi".format(dirname)
        response = self.exec_command(cmd)
        return response.strip() == b"True"

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
        return int(self.exec_command("echo $$", encoding='utf-8'))

    def get_process_children(self, pid):
        command = "pgrep -P {}".format(pid)
        stdin, stdout, stderr = self.ssh.exec_command(command)
        children = stdout.readlines()
        return [PsUtilProcessProxy(self.ssh, int(child_pid.strip())) for child_pid in children]

    # Database control
    def db_connect(self, dbname, user, password=None, host="127.0.0.1", port=5432, ssh_key=None):
        """
        Connects to a PostgreSQL database on the remote system.
        Args:
        - dbname (str): The name of the database to connect to.
        - user (str): The username for the database connection.
        - password (str, optional): The password for the database connection. Defaults to None.
        - host (str, optional): The IP address of the remote system. Defaults to "localhost".
        - port (int, optional): The port number of the PostgreSQL service. Defaults to 5432.

        This function establishes a connection to a PostgreSQL database on the remote system using the specified
        parameters. It returns a connection object that can be used to interact with the database.
        """
        self.close_tunnel()
        self.tunnel = sshtunnel.open_tunnel(
            (host, 22),  # Remote server IP and SSH port
            ssh_username=user or self.username,
            ssh_pkey=ssh_key or self.ssh_key,
            remote_bind_address=(host, port),  # PostgreSQL server IP and PostgreSQL port
            local_bind_address=('localhost', port)  # Local machine IP and available port
        )

        self.tunnel.start()

        try:
            conn = pglib.connect(
                host=host,  # change to 'localhost' because we're connecting through a local ssh tunnel
                port=self.tunnel.local_bind_port,  # use the local bind port set up by the tunnel
                database=dbname,
                user=user or self.username,
                password=password
            )

            return conn
        except Exception as e:
            self.tunnel.stop()
            raise ExecUtilException("Could not create db tunnel. {}".format(e))
