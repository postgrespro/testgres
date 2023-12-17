import logging
import os
import subprocess
import tempfile
import platform

# we support both pg8000 and psycopg2
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

from ..exceptions import ExecUtilException
from .os_ops import OsOperations, ConnectionParams, get_default_encoding

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
        self.ssh_key = conn_params.ssh_key
        if self.ssh_key:
            self.ssh_cmd = ["-i", self.ssh_key]
        else:
            self.ssh_cmd = []
        self.remote = True
        self.username = conn_params.username or self.get_user()
        self.add_known_host(self.host)
        self.tunnel_process = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_ssh_tunnel()

    def establish_ssh_tunnel(self, local_port, remote_port):
        """
        Establish an SSH tunnel from a local port to a remote PostgreSQL port.
        """
        ssh_cmd = ['-N', '-L', f"{local_port}:localhost:{remote_port}"]
        self.tunnel_process = self.exec_command(ssh_cmd, get_process=True, timeout=300)

    def close_ssh_tunnel(self):
        if hasattr(self, 'tunnel_process'):
            self.tunnel_process.terminate()
            self.tunnel_process.wait()
            del self.tunnel_process
        else:
            print("No active tunnel to close.")

    def add_known_host(self, host):
        known_hosts_path = os.path.expanduser("~/.ssh/known_hosts")
        cmd = 'ssh-keyscan -H %s >> %s' % (host, known_hosts_path)

        try:
            subprocess.check_call(cmd, shell=True)
            logging.info("Successfully added %s to known_hosts." % host)
        except subprocess.CalledProcessError as e:
            raise Exception("Failed to add %s to known_hosts. Error: %s" % (host, str(e)))

    def exec_command(self, cmd, wait_exit=False, verbose=False, expect_error=False,
                     encoding=None, shell=True, text=False, input=None, stdin=None, stdout=None,
                     stderr=None, get_process=None, timeout=None):
        """
        Execute a command in the SSH session.
        Args:
        - cmd (str): The command to be executed.
        """
        ssh_cmd = []
        if isinstance(cmd, str):
            ssh_cmd = ['ssh', f"{self.username}@{self.host}"] + self.ssh_cmd + [cmd]
        elif isinstance(cmd, list):
            ssh_cmd = ['ssh', f"{self.username}@{self.host}"] + self.ssh_cmd + cmd
        process = subprocess.Popen(ssh_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if get_process:
            return process

        try:
            result, error = process.communicate(input, timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            raise ExecUtilException("Command timed out after {} seconds.".format(timeout))

        exit_status = process.returncode

        if encoding:
            result = result.decode(encoding)
            error = error.decode(encoding)

        if expect_error:
            raise Exception(result, error)

        if not error:
            error_found = 0
        else:
            error_found = exit_status != 0 or any(
                marker in error for marker in [b'error', b'Permission denied', b'fatal', b'No such file or directory'])

        if error_found:
            if isinstance(error, bytes):
                message = b"Utility exited with non-zero code. Error: " + error
            else:
                message = f"Utility exited with non-zero code. Error: {error}"
            raise ExecUtilException(message=message, command=cmd, exit_code=exit_status, out=result)

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
        return self.exec_command(cmd, encoding=get_default_encoding()).strip()

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
        return self.exec_command("echo $USER", encoding=get_default_encoding()).strip()

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
        result = self.exec_command("test -e {}; echo $?".format(path), encoding=get_default_encoding())
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
            command = ["ssh"] + self.ssh_cmd + [f"{self.username}@{self.host}", f"mktemp -d {prefix}XXXXX"]
        else:
            command = ["ssh"] + self.ssh_cmd + [f"{self.username}@{self.host}", "mktemp -d"]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            temp_dir = result.stdout.strip()
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.join('/home', self.username, temp_dir)
            return temp_dir
        else:
            raise ExecUtilException(f"Could not create temporary directory. Error: {result.stderr}")

    def mkstemp(self, prefix=None):
        if prefix:
            temp_dir = self.exec_command("mktemp {}XXXXX".format(prefix), encoding=get_default_encoding())
        else:
            temp_dir = self.exec_command("mktemp", encoding=get_default_encoding())

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
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False, encoding=None):
        if not encoding:
            encoding = get_default_encoding()
        mode = "wb" if binary else "w"
        if not truncate:
            mode = "ab" if binary else "a"
        if read_and_write:
            mode = "r+b" if binary else "r+"

        with tempfile.NamedTemporaryFile(mode=mode, delete=False) as tmp_file:
            if not truncate:
                scp_cmd = ['scp'] + self.ssh_cmd + [f"{self.username}@{self.host}:{filename}", tmp_file.name]
                subprocess.run(scp_cmd, check=False)  # The file might not exist yet
                tmp_file.seek(0, os.SEEK_END)

            if isinstance(data, bytes) and not binary:
                data = data.decode(encoding)
            elif isinstance(data, str) and binary:
                data = data.encode(encoding)

            if isinstance(data, list):
                data = [(s if isinstance(s, str) else s.decode(get_default_encoding())).rstrip('\n') + '\n' for s in data]
                tmp_file.writelines(data)
            else:
                tmp_file.write(data)

            tmp_file.flush()
            scp_cmd = ['scp'] + self.ssh_cmd + [tmp_file.name, f"{self.username}@{self.host}:{filename}"]
            subprocess.run(scp_cmd, check=True)

            remote_directory = os.path.dirname(filename)
            mkdir_cmd = ['ssh'] + self.ssh_cmd + [f"{self.username}@{self.host}", f"mkdir -p {remote_directory}"]
            subprocess.run(mkdir_cmd, check=True)

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
            result = result.decode(encoding or get_default_encoding())

        return result

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
        return int(self.exec_command("echo $$", encoding=get_default_encoding()))

    def get_process_children(self, pid):
        command = ["ssh"] + self.ssh_cmd + [f"{self.username}@{self.host}", f"pgrep -P {pid}"]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            children = result.stdout.strip().splitlines()
            return [PsUtilProcessProxy(self, int(child_pid.strip())) for child_pid in children]
        else:
            raise ExecUtilException(f"Error in getting process children. Error: {result.stderr}")

    # Database control
    def db_connect(self, dbname, user, password=None, host="localhost", port=5432):
        """
         Established SSH tunnel and Connects to a PostgreSQL
        """
        self.establish_ssh_tunnel(local_port=port, remote_port=5432)
        try:
            conn = pglib.connect(
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
            )
            return conn
        except Exception as e:
            raise Exception(f"Could not connect to the database. Error: {e}")
