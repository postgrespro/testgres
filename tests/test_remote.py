import os

import pytest

from testgres import ExecUtilException
from testgres import RemoteOperations
from testgres import ConnectionParams


class TestRemoteOperations:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        conn_params = ConnectionParams(host=os.getenv('RDBMS_TESTPOOL1_HOST') or '127.0.0.1',
                                       username=os.getenv('USER'),
                                       ssh_key=os.getenv('RDBMS_TESTPOOL_SSHKEY'))
        self.operations = RemoteOperations(conn_params)

    def test_exec_command_success(self):
        """
        Test exec_command for successful command execution.
        """
        cmd = "python3 --version"
        response = self.operations.exec_command(cmd, wait_exit=True)

        assert b'Python 3.' in response

    def test_exec_command_failure(self):
        """
        Test exec_command for command execution failure.
        """
        cmd = "nonexistent_command"
        try:
            exit_status, result, error = self.operations.exec_command(cmd, verbose=True, wait_exit=True)
        except ExecUtilException as e:
            error = e.message
        assert error == b'Utility exited with non-zero code. Error: bash: line 1: nonexistent_command: command not found\n'

    def test_is_executable_true(self):
        """
        Test is_executable for an existing executable.
        """
        cmd = os.getenv('PG_CONFIG')
        response = self.operations.is_executable(cmd)

        assert response is True

    def test_is_executable_false(self):
        """
        Test is_executable for a non-executable.
        """
        cmd = "python"
        response = self.operations.is_executable(cmd)

        assert response is False

    def test_makedirs_and_rmdirs_success(self):
        """
        Test makedirs and rmdirs for successful directory creation and removal.
        """
        cmd = "pwd"
        pwd = self.operations.exec_command(cmd, wait_exit=True, encoding='utf-8').strip()

        path = "{}/test_dir".format(pwd)

        # Test makedirs
        self.operations.makedirs(path)
        assert self.operations.path_exists(path)

        # Test rmdirs
        self.operations.rmdirs(path)
        assert not self.operations.path_exists(path)

    def test_makedirs_and_rmdirs_failure(self):
        """
        Test makedirs and rmdirs for directory creation and removal failure.
        """
        # Try to create a directory in a read-only location
        path = "/root/test_dir"

        # Test makedirs
        with pytest.raises(Exception):
            self.operations.makedirs(path)

        # Test rmdirs
        try:
            exit_status, result, error = self.operations.rmdirs(path, verbose=True)
        except ExecUtilException as e:
            error = e.message
        assert error == b"Utility exited with non-zero code. Error: rm: cannot remove '/root/test_dir': Permission denied\n"

    def test_listdir(self):
        """
        Test listdir for listing directory contents.
        """
        path = "/etc"
        files = self.operations.listdir(path)

        assert isinstance(files, list)

    def test_path_exists_true(self):
        """
        Test path_exists for an existing path.
        """
        path = "/etc"
        response = self.operations.path_exists(path)

        assert response is True

    def test_path_exists_false(self):
        """
        Test path_exists for a non-existing path.
        """
        path = "/nonexistent_path"
        response = self.operations.path_exists(path)

        assert response is False

    def test_write_text_file(self):
        """
        Test write for writing data to a text file.
        """
        filename = "/tmp/test_file.txt"
        data = "Hello, world!"

        self.operations.write(filename, data, truncate=True)
        self.operations.write(filename, data)

        response = self.operations.read(filename)

        assert response == data + data

    def test_write_binary_file(self):
        """
        Test write for writing data to a binary file.
        """
        filename = "/tmp/test_file.bin"
        data = b"\x00\x01\x02\x03"

        self.operations.write(filename, data, binary=True, truncate=True)

        response = self.operations.read(filename, binary=True)

        assert response == data

    def test_read_text_file(self):
        """
        Test read for reading data from a text file.
        """
        filename = "/etc/hosts"

        response = self.operations.read(filename)

        assert isinstance(response, str)

    def test_read_binary_file(self):
        """
        Test read for reading data from a binary file.
        """
        filename = "/usr/bin/python3"

        response = self.operations.read(filename, binary=True)

        assert isinstance(response, bytes)

    def test_touch(self):
        """
        Test touch for creating a new file or updating access and modification times of an existing file.
        """
        filename = "/tmp/test_file.txt"

        self.operations.touch(filename)

        assert self.operations.isfile(filename)

    def test_isfile_true(self):
        """
        Test isfile for an existing file.
        """
        filename = "/etc/hosts"

        response = self.operations.isfile(filename)

        assert response is True

    def test_isfile_false(self):
        """
        Test isfile for a non-existing file.
        """
        filename = "/nonexistent_file.txt"

        response = self.operations.isfile(filename)

        assert response is False
