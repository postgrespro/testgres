# coding: utf-8
import os

import pytest
import re
import tempfile

from ..testgres import ExecUtilException
from ..testgres import InvalidOperationException
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
                assert e.message == "Utility exited with non-zero code (127). Error: `bash: line 1: nonexistent_command: command not found`"
                assert type(e.error) == bytes  # noqa: E721
                assert e.error.strip() == b"bash: line 1: nonexistent_command: command not found"
                break
            raise Exception("We wait an exception!")

    def test_exec_command_failure__expect_error(self):
        """
        Test exec_command for command execution failure.
        """
        cmd = "nonexistent_command"

        exit_status, result, error = self.operations.exec_command(cmd, verbose=True, wait_exit=True, shell=True, expect_error=True)

        assert error == b'bash: line 1: nonexistent_command: command not found\n'
        assert exit_status == 127
        assert result == b''

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
        while True:
            try:
                self.operations.rmdirs(path, verbose=True)
            except ExecUtilException as e:
                assert e.message == "Utility exited with non-zero code (1). Error: `rm: cannot remove '/root/test_dir': Permission denied`"
                assert type(e.error) == bytes  # noqa: E721
                assert e.error.strip() == b"rm: cannot remove '/root/test_dir': Permission denied"
                break
            raise Exception("We wait an exception!")

    def test_listdir(self):
        """
        Test listdir for listing directory contents.
        """
        path = "/etc"
        files = self.operations.listdir(path)

        assert isinstance(files, list)

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

    def test_read__text(self):
        """
        Test RemoteOperations::read for text data.
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
        Test RemoteOperations::read for binary data.
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
        Test RemoteOperations::read for binary data and encoding.
        """
        filename = __file__  # current file

        with pytest.raises(
                InvalidOperationException,
                match=re.escape("Enconding is not allowed for read binary operation")):
            self.operations.read(filename, encoding="", binary=True)

    def test_read__unknown_file(self):
        """
        Test RemoteOperations::read with unknown file.
        """

        with pytest.raises(
                ExecUtilException,
                match=re.escape("cat: /dummy: No such file or directory")):
            self.operations.read("/dummy")

    def test_read_binary__spec(self):
        """
        Test RemoteOperations::read_binary.
        """
        filename = __file__  # currnt file

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
        Test RemoteOperations::read_binary with unknown file.
        """

        with pytest.raises(ExecUtilException, match=re.escape("tail: cannot open '/dummy' for reading: No such file or directory")):
            self.operations.read_binary("/dummy", 0)

    def test_read_binary__spec__negative_offset(self):
        """
        Test RemoteOperations::read_binary with negative offset.
        """

        with pytest.raises(
                ValueError,
                match=re.escape("Negative 'offset' is not supported.")):
            self.operations.read_binary(__file__, -1)

    def test_get_file_size(self):
        """
        Test RemoteOperations::get_file_size.
        """
        filename = __file__  # current file

        sz0 = os.path.getsize(filename)
        assert type(sz0) == int  # noqa: E721

        sz1 = self.operations.get_file_size(filename)
        assert type(sz1) == int  # noqa: E721
        assert sz1 == sz0

    def test_get_file_size__unk_file(self):
        """
        Test RemoteOperations::get_file_size.
        """

        with pytest.raises(ExecUtilException, match=re.escape("du: cannot access '/dummy': No such file or directory")):
            self.operations.get_file_size("/dummy")

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

    def test_cwd(self):
        """
        Test cwd.
        """
        v = self.operations.cwd()

        assert v is not None
        assert type(v) == str  # noqa: E721
        assert v != ""

    class tagWriteData001:
        def __init__(self, sign, source, cp_rw, cp_truncate, cp_binary, cp_data, result):
            self.sign = sign
            self.source = source
            self.call_param__rw = cp_rw
            self.call_param__truncate = cp_truncate
            self.call_param__binary = cp_binary
            self.call_param__data = cp_data
            self.result = result

    sm_write_data001 = [
        tagWriteData001("A001", "1234567890", False, False, False, "ABC", "1234567890ABC"),
        tagWriteData001("A002", b"1234567890", False, False, True, b"ABC", b"1234567890ABC"),

        tagWriteData001("B001", "1234567890", False, True, False, "ABC", "ABC"),
        tagWriteData001("B002", "1234567890", False, True, False, "ABC1234567890", "ABC1234567890"),
        tagWriteData001("B003", b"1234567890", False, True, True, b"ABC", b"ABC"),
        tagWriteData001("B004", b"1234567890", False, True, True, b"ABC1234567890", b"ABC1234567890"),

        tagWriteData001("C001", "1234567890", True, False, False, "ABC", "1234567890ABC"),
        tagWriteData001("C002", b"1234567890", True, False, True, b"ABC", b"1234567890ABC"),

        tagWriteData001("D001", "1234567890", True, True, False, "ABC", "ABC"),
        tagWriteData001("D002", "1234567890", True, True, False, "ABC1234567890", "ABC1234567890"),
        tagWriteData001("D003", b"1234567890", True, True, True, b"ABC", b"ABC"),
        tagWriteData001("D004", b"1234567890", True, True, True, b"ABC1234567890", b"ABC1234567890"),

        tagWriteData001("E001", "\0001234567890\000", False, False, False, "\000ABC\000", "\0001234567890\000\000ABC\000"),
        tagWriteData001("E002", b"\0001234567890\000", False, False, True, b"\000ABC\000", b"\0001234567890\000\000ABC\000"),

        tagWriteData001("F001", "a\nb\n", False, False, False, ["c", "d"], "a\nb\nc\nd\n"),
        tagWriteData001("F002", b"a\nb\n", False, False, True, [b"c", b"d"], b"a\nb\nc\nd\n"),

        tagWriteData001("G001", "a\nb\n", False, False, False, ["c\n\n", "d\n"], "a\nb\nc\nd\n"),
        tagWriteData001("G002", b"a\nb\n", False, False, True, [b"c\n\n", b"d\n"], b"a\nb\nc\nd\n"),
    ]

    @pytest.fixture(
        params=sm_write_data001,
        ids=[x.sign for x in sm_write_data001],
    )
    def write_data001(self, request):
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param) == __class__.tagWriteData001  # noqa: E721
        return request.param

    def test_write(self, write_data001):
        assert type(write_data001) == __class__.tagWriteData001  # noqa: E721

        mode = "w+b" if write_data001.call_param__binary else "w+"

        with tempfile.NamedTemporaryFile(mode=mode, delete=True) as tmp_file:
            tmp_file.write(write_data001.source)
            tmp_file.flush()

            self.operations.write(
                tmp_file.name,
                write_data001.call_param__data,
                read_and_write=write_data001.call_param__rw,
                truncate=write_data001.call_param__truncate,
                binary=write_data001.call_param__binary)

            tmp_file.seek(0)

            s = tmp_file.read()

            assert s == write_data001.result
