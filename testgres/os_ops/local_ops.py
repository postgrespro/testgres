import getpass
import os
import shutil
import subprocess
import tempfile
from shutil import rmtree

from testgres.logger import log

from .os_ops import OsOperations
from .os_ops import pglib

CMD_TIMEOUT_SEC = 60


class LocalOperations(OsOperations):
    def __init__(self, username=None):
        super().__init__()
        self.username = username or self.get_user()

    # Command execution
    def exec_command(self, cmd, wait_exit=False, verbose=False, expect_error=False):
        if isinstance(cmd, list):
            cmd = " ".join(cmd)
        log.debug(f"os_ops.exec_command: `{cmd}`; remote={self.remote}")
        # Source global profile file + execute command
        try:
            process = subprocess.run(
                cmd,
                shell=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=CMD_TIMEOUT_SEC,
            )
            exit_status = process.returncode
            result = process.stdout
            error = process.stderr

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
        return os.access(file, os.X_OK)

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
        os.environ[var_name] = var_val

    # Get environment variables
    def get_user(self):
        return getpass.getuser()

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd).strip()

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        if remove_existing and os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

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
            raise Exception(f"Unsupported operating system: {os_name}")
        return pathsep

    def mkdtemp(self, prefix=None):
        return tempfile.mkdtemp(prefix=prefix)

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
        mode = "wb" if binary else "w"
        if not truncate:
            mode = "a" + mode
        if read_and_write:
            mode = "r+" + mode

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

    def read(self, filename):
        with open(filename, "r") as file:
            return file.read()

    def readlines(self, filename, num_lines=0, encoding=None):
        """
        Read lines from a local file.
        If num_lines is greater than 0, only the last num_lines lines will be read.
        """
        assert num_lines >= 0

        if num_lines == 0:
            with open(filename, "r", encoding=encoding) as file:
                return file.readlines()

        else:
            bufsize = 8192
            buffers = 1

            with open(filename, "r", encoding=encoding) as file:
                file.seek(0, os.SEEK_END)
                end_pos = file.tell()

                while True:
                    offset = max(0, end_pos - bufsize * buffers)
                    file.seek(offset, os.SEEK_SET)
                    pos = file.tell()
                    lines = file.readlines()
                    cur_lines = len(lines)

                    if cur_lines >= num_lines or pos == 0:
                        return lines[-num_lines:]

                    buffers = int(
                        buffers * max(2, int(num_lines / max(cur_lines, 1)))
                    )  # Adjust buffer size

    def isfile(self, remote_file):
        return os.path.isfile(remote_file)

    # Processes control
    def kill(self, pid, signal):
        # Kill the process
        cmd = f"kill -{signal} {pid}"
        return self.exec_command(cmd)

    def get_pid(self):
        # Get current process id
        return os.getpid()

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
