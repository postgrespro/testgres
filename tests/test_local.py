import os

import pytest
import re

from testgres import ExecUtilException
from testgres import LocalOperations

from .helpers.run_conditions import RunConditions


class TestLocalOperations:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.operations = LocalOperations()

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
                error = e.message
                break
            raise Exception("We wait an exception!")
        assert error == "Utility exited with non-zero code. Error: `/bin/sh: 1: nonexistent_command: not found`"

    def test_exec_command_failure__expect_error(self):
        """
        Test exec_command for command execution failure.
        """
        RunConditions.skip_if_windows()

        cmd = "nonexistent_command"

        exit_status, result, error = self.operations.exec_command(cmd, verbose=True, wait_exit=True, shell=True, expect_error=True)

        assert error == b'/bin/sh: 1: nonexistent_command: not found\n'
        assert exit_status == 127
        assert result == b''

    def test_read_binary__spec(self):
        """
        Test LocalOperations::read_binary.
        """
        filename = __file__  # current file

        with open(filename, 'rb') as file:  # open in a binary mode
            response0 = file.read()

        assert type(response0) == bytes  # noqa: E721

        response1 = self.operations.read_binary(filename, 0)
        assert type(response1) == bytes  # noqa: E721
        assert response1 == response0

        response2 = self.operations.read_binary(filename, 1)
        assert type(response2) == bytes  # noqa: E721
        assert len(response2) < len(response1)
        assert len(response2) + 1 == len(response1)
        assert response2 == response1[1:]

        response3 = self.operations.read_binary(filename, len(response1))
        assert type(response3) == bytes  # noqa: E721
        assert len(response3) == 0

        response4 = self.operations.read_binary(filename, len(response2))
        assert type(response4) == bytes  # noqa: E721
        assert len(response4) == 1
        assert response4[0] == response1[len(response1) - 1]

        response5 = self.operations.read_binary(filename, len(response1) + 1)
        assert type(response5) == bytes  # noqa: E721
        assert len(response5) == 0

    def test_read_binary__spec__unk_file(self):
        """
        Test LocalOperations::read_binary with unknown file.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.read_binary("/dummy", 0)
