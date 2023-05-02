import base64
import getpass
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from shutil import rmtree

try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError("You must have psycopg2 or pg8000 modules installed")

from defaults import default_username
from testgres.logger import log

import paramiko


class OsOperations:

    def __init__(self, host='127.0.0.1', hostname='localhost', ssh_key=None, username=default_username()):
        self.host = host
        self.ssh_key = ssh_key
        self.username = username
        self.remote = not (self.host == '127.0.0.1' and hostname == 'localhost')
        self.ssh = None

        if self.remote:
            self.ssh = self.connect()

    def __del__(self):
        if self.ssh:
            self.ssh.close()

    @contextmanager
    def ssh_connect(self):
        if not self.remote:
            yield None
        else:
            with open(self.ssh_key, 'r') as f:
                key_data = f.read()
                if 'BEGIN OPENSSH PRIVATE KEY' in key_data:
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

    def exec_command(self, cmd, wait_exit=False, verbose=False, expect_error=False):
        if isinstance(cmd, list):
            cmd = ' '.join(cmd)
        log.debug(f"os_ops.exec_command: `{cmd}`; remote={self.remote}")
        # Source global profile file + execute command
        try:
            if self.remote:
                cmd = f"source /etc/profile.d/custom.sh; {cmd}"
                with self.ssh_connect() as ssh:
                    stdin, stdout, stderr = ssh.exec_command(cmd)
                    exit_status = 0
                    if wait_exit:
                        exit_status = stdout.channel.recv_exit_status()
                    result = stdout.read().decode('utf-8')
                    error = stderr.read().decode('utf-8')
            else:
                process = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                         timeout=60)
                exit_status = process.returncode
                result = process.stdout
                error = process.stderr

            if expect_error:
                raise Exception(result, error)
            if exit_status != 0 or 'error' in error.lower():
                log.error(f"Problem in executing command: `{cmd}`\nerror: {error}\nexit_code: {exit_status}")
                exit(1)

            if verbose:
                return exit_status, result, error
            else:
                return result

        except Exception as e:
            log.error(f"Unexpected error while executing command `{cmd}`: {e}")
            return None

    def makedirs(self, path, remove_existing=False):
        if remove_existing:
            cmd = f'rm -rf {path} && mkdir -p {path}'
        else:
            cmd = f'mkdir -p {path}'
        self.exec_command(cmd)
        
    def rmdirs(self, path, ignore_errors=True):
        if self.remote:
            cmd = f'rm -rf {path}'
            self.exec_command(cmd)
        else:
            rmtree(path, ignore_errors=ignore_errors)

    def mkdtemp(self, prefix=None):
        if self.remote:
            temp_dir = self.exec_command(f'mkdtemp -d {prefix}')
            return temp_dir.strip()
        else:
            return tempfile.mkdtemp(prefix=prefix)

    def path_exists(self, path):
        if self.remote:
            result = self.exec_command(f'test -e {path}; echo $?')
            return int(result.strip()) == 0
        else:
            return os.path.exists(path)

    def copytree(self, src, dst):
        if self.remote:
            self.exec_command(f'cp -r {src} {dst}')
        else:
            shutil.copytree(src, dst)

    def listdir(self, path):
        if self.remote:
            result = self.exec_command(f'ls {path}')
            return result.splitlines()
        else:
            return os.listdir(path)

    def write(self, filename, data, truncate=False, binary=False, read_and_write=False):
        """
        Write data to a file, both locally and on a remote host.

        :param filename: The file path where the data will be written.
        :param data: The data to be written to the file.
        :param truncate: If True, the file will be truncated before writing ('w' or 'wb' option);
                         if False (default), data will be appended ('a' or 'ab' option).
        :param binary: If True, the data will be written in binary mode ('wb' or 'ab' option);
                       if False (default), the data will be written in text mode ('w' or 'a' option).
        :param read_and_write: If True, the file will be opened with read and write permissions ('r+' option);
                               if False (default), only write permission will be used ('w', 'a', 'wb', or 'ab' option).
        """
        mode = 'wb' if binary else 'w'
        if not truncate:
            mode = 'a' + mode
        if read_and_write:
            mode = 'r+' + mode

        if self.remote:
            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(data)
                tmp_file.flush()

                sftp = self.ssh.open_sftp()
                sftp.put(tmp_file.name, filename)
                sftp.close()
        else:
            with open(filename, mode) as file:
                file.write(data)

    def read(self, filename):
        cmd = f'cat {filename}'
        return self.exec_command(cmd)

    def readlines(self, filename):
        return self.read(filename).splitlines()

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd).strip()

    def kill(self, pid, signal):
        cmd = f'kill -{signal} {pid}'
        self.exec_command(cmd)

    def environ(self, var_name):
        cmd = f"echo ${var_name}"
        return self.exec_command(cmd).strip()

    @property
    def pathsep(self):
        return ':' if self.get_name() == 'posix' else ';'

    def isfile(self, remote_file):
        if self.remote:
            stdout = self.exec_command(f'test -f {remote_file}; echo $?')
            result = int(stdout.strip())
            return result == 0
        else:
            return os.path.isfile(remote_file)

    def find_executable(self, executable):
        search_paths = self.environ('PATH')
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
        if self.remote:
            if not self.exec_command(f"test -x {file} && echo OK") == 'OK\n':
                return False
        else:
            if not os.access(file, os.X_OK):
                return False
        return True

    def add_to_path(self, new_path):
        os_name = self.get_name()
        if os_name == 'posix':
            dir_del = ':'
        elif os_name == 'nt':
            dir_del = ';'
        else:
            raise Exception(f"Unsupported operating system: {os_name}")

        # Check if the directory is already in PATH
        path = self.environ('PATH')
        if new_path not in path.split(dir_del):
            if self.remote:
                self.exec_command(f"export PATH={new_path}{dir_del}{path}")
            else:
                os.environ['PATH'] = f"{new_path}{dir_del}{path}"
        return dir_del

    def set_env(self, var_name, var_val):
        # Check if the directory is already in PATH
        if self.remote:
            self.exec_command(f"export {var_name}={var_val}")
        else:
            os.environ[var_name] = var_val

    def get_pid(self):
        # Get current process id
        if self.remote:
            process_id = self.exec_command(f"echo $$")
        else:
            process_id = os.getpid()
        return process_id

    def get_user(self):
        # Get current user
        if self.remote:
            user = self.exec_command(f"echo $USER")
        else:
            user = getpass.getuser()
        return user

    @contextmanager
    def db_connect(self, dbname, user, password=None, host='localhost', port=5432):
        if self.remote:
            with self.ssh_connect() as ssh:
                # Set up a local port forwarding on a random port
                local_port = ssh.forward_remote_port(host, port)
                conn = pglib.connect(
                    host=host,
                    port=local_port,
                    dbname=dbname,
                    user=user,
                    password=password,
                )
                try:
                    yield conn
                finally:
                    conn.close()
                    ssh.close_forwarded_tcp(local_port)
        else:
            with pglib.connect(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
            ) as conn:
                yield conn


