# coding: utf-8
import os

import pytest
import re
import logging

from ..testgres import ExecUtilException
from ..testgres import RemoteOperations
from ..testgres import LocalOperations
from ..testgres import ConnectionParams
from ..testgres import utils as testgres_utils


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
        while True:
            try:
                self.operations.exec_command(cmd, verbose=True, wait_exit=True)
            except ExecUtilException as e:
                assert type(e.exit_code) == int  # noqa: E721
                assert e.exit_code == 127

                assert type(e.message) == str  # noqa: E721
                assert type(e.error) == bytes  # noqa: E721

                assert e.message.startswith("Utility exited with non-zero code (127). Error:")
                assert "nonexistent_command" in e.message
                assert "not found" in e.message
                assert b"nonexistent_command" in e.error
                assert b"not found" in e.error
                break
            raise Exception("We wait an exception!")

    def test_exec_command_failure__expect_error(self):
        """
        Test exec_command for command execution failure.
        """
        cmd = "nonexistent_command"

        exit_status, result, error = self.operations.exec_command(cmd, verbose=True, wait_exit=True, shell=True, expect_error=True)

        assert exit_status == 127
        assert result == b''
        assert type(error) == bytes  # noqa: E721
        assert b"nonexistent_command" in error
        assert b"not found" in error

    def test_is_executable_true(self):
        """
        Test is_executable for an existing executable.
        """
        local_ops = LocalOperations()
        cmd = testgres_utils.get_bin_path2(local_ops, "pg_config")
        cmd = local_ops.exec_command([cmd, "--bindir"], encoding="utf-8")
        cmd = cmd.rstrip()
        cmd = os.path.join(cmd, "pg_config")
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
        assert os.path.exists(path)
        assert self.operations.path_exists(path)

        # Test rmdirs
        self.operations.rmdirs(path)
        assert not os.path.exists(path)
        assert not self.operations.path_exists(path)

    def test_makedirs_failure(self):
        """
        Test makedirs for failure.
        """
        # Try to create a directory in a read-only location
        path = "/root/test_dir"

        # Test makedirs
        with pytest.raises(Exception):
            self.operations.makedirs(path)

    def test_mkdtemp__default(self):
        path = self.operations.mkdtemp()
        logging.info("Path is [{0}].".format(path))
        assert os.path.exists(path)
        os.rmdir(path)
        assert not os.path.exists(path)

    def test_mkdtemp__custom(self):
        C_TEMPLATE = "abcdef"
        path = self.operations.mkdtemp(C_TEMPLATE)
        logging.info("Path is [{0}].".format(path))
        assert os.path.exists(path)
        assert C_TEMPLATE in os.path.basename(path)
        os.rmdir(path)
        assert not os.path.exists(path)

    def test_rmdirs(self):
        path = self.operations.mkdtemp()
        assert os.path.exists(path)

        assert self.operations.rmdirs(path, ignore_errors=False) is True
        assert not os.path.exists(path)

    def test_rmdirs__01_with_subfolder(self):
        # folder with subfolder
        path = self.operations.mkdtemp()
        assert os.path.exists(path)

        dir1 = os.path.join(path, "dir1")
        assert not os.path.exists(dir1)

        self.operations.makedirs(dir1)
        assert os.path.exists(dir1)

        assert self.operations.rmdirs(path, ignore_errors=False) is True
        assert not os.path.exists(path)
        assert not os.path.exists(dir1)

    def test_rmdirs__02_with_file(self):
        # folder with file
        path = self.operations.mkdtemp()
        assert os.path.exists(path)

        file1 = os.path.join(path, "file1.txt")
        assert not os.path.exists(file1)

        self.operations.touch(file1)
        assert os.path.exists(file1)

        assert self.operations.rmdirs(path, ignore_errors=False) is True
        assert not os.path.exists(path)
        assert not os.path.exists(file1)

    def test_rmdirs__03_with_subfolder_and_file(self):
        # folder with subfolder and file
        path = self.operations.mkdtemp()
        assert os.path.exists(path)

        dir1 = os.path.join(path, "dir1")
        assert not os.path.exists(dir1)

        self.operations.makedirs(dir1)
        assert os.path.exists(dir1)

        file1 = os.path.join(dir1, "file1.txt")
        assert not os.path.exists(file1)

        self.operations.touch(file1)
        assert os.path.exists(file1)

        assert self.operations.rmdirs(path, ignore_errors=False) is True
        assert not os.path.exists(path)
        assert not os.path.exists(dir1)
        assert not os.path.exists(file1)

    def test_rmdirs__try_to_delete_nonexist_path(self):
        path = "/root/test_dir"

        assert self.operations.rmdirs(path, ignore_errors=False) is True

    def test_rmdirs__try_to_delete_file(self):
        path = self.operations.mkstemp()
        assert os.path.exists(path)

        with pytest.raises(ExecUtilException) as x:
            self.operations.rmdirs(path, ignore_errors=False)

        assert os.path.exists(path)
        assert type(x.value) == ExecUtilException   # noqa: E721
        assert x.value.message == "Utility exited with non-zero code (20). Error: `cannot remove '" + path + "': it is not a directory`"
        assert type(x.value.error) == str  # noqa: E721
        assert x.value.error.strip() == "cannot remove '" + path + "': it is not a directory"
        assert type(x.value.exit_code) == int  # noqa: E721
        assert x.value.exit_code == 20

    def test_listdir(self):
        """
        Test listdir for listing directory contents.
        """
        path = "/etc"
        files = self.operations.listdir(path)
        assert isinstance(files, list)
        for f in files:
            assert f is not None
            assert type(f) == str  # noqa: E721

    def test_path_exists_true__directory(self):
        """
        Test path_exists for an existing directory.
        """
        assert self.operations.path_exists("/etc") is True

    def test_path_exists_true__file(self):
        """
        Test path_exists for an existing file.
        """
        assert self.operations.path_exists(__file__) is True

    def test_path_exists_false__directory(self):
        """
        Test path_exists for a non-existing directory.
        """
        assert self.operations.path_exists("/nonexistent_path") is False

    def test_path_exists_false__file(self):
        """
        Test path_exists for a non-existing file.
        """
        assert self.operations.path_exists("/etc/nonexistent_path.txt") is False

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

    def test_read__unknown_file(self):
        """
        Test RemoteOperations::read with unknown file.
        """

        with pytest.raises(ExecUtilException) as x:
            self.operations.read("/dummy")

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)

    def test_read_binary__spec__unk_file(self):
        """
        Test RemoteOperations::read_binary with unknown file.
        """

        with pytest.raises(ExecUtilException) as x:
            self.operations.read_binary("/dummy", 0)

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)

    def test_get_file_size__unk_file(self):
        """
        Test RemoteOperations::get_file_size.
        """

        with pytest.raises(ExecUtilException) as x:
            self.operations.get_file_size("/dummy")

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)

    def test_touch(self):
        """
        Test touch for creating a new file or updating access and modification times of an existing file.
        """
        filename = "/tmp/test_file.txt"

        self.operations.touch(filename)

        assert self.operations.isfile(filename)
