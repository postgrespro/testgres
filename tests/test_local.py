# coding: utf-8
import os

import pytest
import re
import logging

from ..testgres import ExecUtilException
from ..testgres import LocalOperations

from .helpers.run_conditions import RunConditions


class TestLocalOperations:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.operations = LocalOperations()

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

    def test_exec_command_success(self):
        """
        Test exec_command for successful command execution.
        """
        RunConditions.skip_if_windows()

        cmd = "python3 --version"
        response = self.operations.exec_command(cmd, wait_exit=True, shell=True)

        assert b'Python 3.' in response

    def test_exec_command_failure(self):
        """
        Test exec_command for command execution failure.
        """
        RunConditions.skip_if_windows()

        cmd = "nonexistent_command"
        while True:
            try:
                self.operations.exec_command(cmd, wait_exit=True, shell=True)
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
        RunConditions.skip_if_windows()

        cmd = "nonexistent_command"

        exit_status, result, error = self.operations.exec_command(cmd, verbose=True, wait_exit=True, shell=True, expect_error=True)

        assert exit_status == 127
        assert result == b''
        assert type(error) == bytes  # noqa: E721
        assert b"nonexistent_command" in error
        assert b"not found" in error

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

    def test_read__unknown_file(self):
        """
        Test LocalOperations::read with unknown file.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.read("/dummy")

    def test_read_binary__spec__unk_file(self):
        """
        Test LocalOperations::read_binary with unknown file.
        """

        with pytest.raises(
                FileNotFoundError,
                match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.read_binary("/dummy", 0)

    def test_read_binary__spec__negative_offset(self):
        """
        Test LocalOperations::read_binary with negative offset.
        """

        with pytest.raises(
                ValueError,
                match=re.escape("Negative 'offset' is not supported.")):
            self.operations.read_binary(__file__, -1)

    def test_get_file_size(self):
        """
        Test LocalOperations::get_file_size.
        """
        filename = __file__  # current file

        sz0 = os.path.getsize(filename)
        assert type(sz0) == int  # noqa: E721

        sz1 = self.operations.get_file_size(filename)
        assert type(sz1) == int  # noqa: E721
        assert sz1 == sz0

    def test_get_file_size__unk_file(self):
        """
        Test LocalOperations::get_file_size.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.get_file_size("/dummy")

    def test_cwd(self):
        """
        Test cwd.
        """
        v = self.operations.cwd()

        assert v is not None
        assert type(v) == str  # noqa: E721

        expectedValue = os.getcwd()
        assert expectedValue is not None
        assert type(expectedValue) == str  # noqa: E721
        assert expectedValue != ""  # research

        # Comp result
        assert v == expectedValue
