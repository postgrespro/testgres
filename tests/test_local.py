import os

import pytest
import re

from testgres import ExecUtilException
from testgres import InvalidOperationException
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

    def test_read__text(self):
        """
        Test LocalOperations::read for text data.
        """
        filename = __file__  # current file

        with open(filename, 'r') as file:  # open in a text mode
            response0 = file.read()

        assert type(response0) == str  # noqa: E721

        response1 = self.operations.read(filename)
        assert type(response1) == str  # noqa: E721
        assert response1 == response0

        response2 = self.operations.read(filename, encoding=None, binary=False)
        assert type(response2) == str  # noqa: E721
        assert response2 == response0

        response3 = self.operations.read(filename, encoding="")
        assert type(response3) == str  # noqa: E721
        assert response3 == response0

        response4 = self.operations.read(filename, encoding="UTF-8")
        assert type(response4) == str  # noqa: E721
        assert response4 == response0

    def test_read__binary(self):
        """
        Test LocalOperations::read for binary data.
        """
        filename = __file__  # current file

        with open(filename, 'rb') as file:  # open in a binary mode
            response0 = file.read()

        assert type(response0) == bytes  # noqa: E721

        response1 = self.operations.read(filename, binary=True)
        assert type(response1) == bytes  # noqa: E721
        assert response1 == response0

    def test_read__binary_and_encoding(self):
        """
        Test LocalOperations::read for binary data and encoding.
        """
        filename = __file__  # current file

        with pytest.raises(
                InvalidOperationException,
                match=re.escape("Enconding is not allowed for read binary operation")):
            self.operations.read(filename, encoding="", binary=True)

    def test_read__unknown_file(self):
        """
        Test LocalOperations::read with unknown file.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.read("/dummy")

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

    def test_isfile_true(self):
        """
        Test isfile for an existing file.
        """
        filename = __file__

        response = self.operations.isfile(filename)

        assert response is True

    def test_isfile_false__not_exist(self):
        """
        Test isfile for a non-existing file.
        """
        filename = os.path.join(os.path.dirname(__file__), "nonexistent_file.txt")

        response = self.operations.isfile(filename)

        assert response is False

    def test_isfile_false__directory(self):
        """
        Test isfile for a firectory.
        """
        name = os.path.dirname(__file__)

        assert self.operations.isdir(name)

        response = self.operations.isfile(name)

        assert response is False

    def test_isdir_true(self):
        """
        Test isdir for an existing directory.
        """
        name = os.path.dirname(__file__)

        response = self.operations.isdir(name)

        assert response is True

    def test_isdir_false__not_exist(self):
        """
        Test isdir for a non-existing directory.
        """
        name = os.path.join(os.path.dirname(__file__), "it_is_nonexistent_directory")

        response = self.operations.isdir(name)

        assert response is False

    def test_isdir_false__file(self):
        """
        Test isdir for a file.
        """
        name = __file__

        assert self.operations.isfile(name)

        response = self.operations.isdir(name)

        assert response is False
