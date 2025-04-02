# coding: utf-8
from .helpers.os_ops_descrs import OsOpsDescr
from .helpers.os_ops_descrs import OsOpsDescrs
from .helpers.os_ops_descrs import OsOperations
from .helpers.run_conditions import RunConditions

import os

import pytest
import re
import tempfile
import logging

from ..testgres import InvalidOperationException
from ..testgres import ExecUtilException


class TestOsOpsCommon:
    sm_os_ops_descrs: list[OsOpsDescr] = [
        OsOpsDescrs.sm_local_os_ops_descr,
        OsOpsDescrs.sm_remote_os_ops_descr
    ]

    @pytest.fixture(
        params=[descr.os_ops for descr in sm_os_ops_descrs],
        ids=[descr.sign for descr in sm_os_ops_descrs]
    )
    def os_ops(self, request: pytest.FixtureRequest) -> OsOperations:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, OsOperations)
        return request.param

    def test_exec_command_success(self, os_ops: OsOperations):
        """
        Test exec_command for successful command execution.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        cmd = ["sh", "-c", "python3 --version"]

        response = os_ops.exec_command(cmd)

        assert b'Python 3.' in response

    def test_exec_command_failure(self, os_ops: OsOperations):
        """
        Test exec_command for command execution failure.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        cmd = ["sh", "-c", "nonexistent_command"]

        while True:
            try:
                os_ops.exec_command(cmd)
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

    def test_exec_command_failure__expect_error(self, os_ops: OsOperations):
        """
        Test exec_command for command execution failure.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        cmd = ["sh", "-c", "nonexistent_command"]

        exit_status, result, error = os_ops.exec_command(cmd, verbose=True, expect_error=True)

        assert exit_status == 127
        assert result == b''
        assert type(error) == bytes  # noqa: E721
        assert b"nonexistent_command" in error
        assert b"not found" in error

    def test_is_executable_true(self, os_ops: OsOperations):
        """
        Test is_executable for an existing executable.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        response = os_ops.is_executable("/bin/sh")

        assert response is True

    def test_is_executable_false(self, os_ops: OsOperations):
        """
        Test is_executable for a non-executable.
        """
        assert isinstance(os_ops, OsOperations)

        response = os_ops.is_executable(__file__)

        assert response is False

    def test_makedirs_and_rmdirs_success(self, os_ops: OsOperations):
        """
        Test makedirs and rmdirs for successful directory creation and removal.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        cmd = "pwd"
        pwd = os_ops.exec_command(cmd, wait_exit=True, encoding='utf-8').strip()

        path = "{}/test_dir".format(pwd)

        # Test makedirs
        os_ops.makedirs(path)
        assert os.path.exists(path)
        assert os_ops.path_exists(path)

        # Test rmdirs
        os_ops.rmdirs(path)
        assert not os.path.exists(path)
        assert not os_ops.path_exists(path)

    def test_makedirs_failure(self, os_ops: OsOperations):
        """
        Test makedirs for failure.
        """
        # Try to create a directory in a read-only location
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        path = "/root/test_dir"

        # Test makedirs
        with pytest.raises(Exception):
            os_ops.makedirs(path)

    def test_listdir(self, os_ops: OsOperations):
        """
        Test listdir for listing directory contents.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        path = "/etc"
        files = os_ops.listdir(path)
        assert isinstance(files, list)
        for f in files:
            assert f is not None
            assert type(f) == str  # noqa: E721

    def test_path_exists_true__directory(self, os_ops: OsOperations):
        """
        Test path_exists for an existing directory.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.path_exists("/etc") is True

    def test_path_exists_true__file(self, os_ops: OsOperations):
        """
        Test path_exists for an existing file.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.path_exists(__file__) is True

    def test_path_exists_false__directory(self, os_ops: OsOperations):
        """
        Test path_exists for a non-existing directory.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.path_exists("/nonexistent_path") is False

    def test_path_exists_false__file(self, os_ops: OsOperations):
        """
        Test path_exists for a non-existing file.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.path_exists("/etc/nonexistent_path.txt") is False

    def test_mkdtemp__default(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        path = os_ops.mkdtemp()
        logging.info("Path is [{0}].".format(path))
        assert os.path.exists(path)
        os.rmdir(path)
        assert not os.path.exists(path)

    def test_mkdtemp__custom(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        C_TEMPLATE = "abcdef"
        path = os_ops.mkdtemp(C_TEMPLATE)
        logging.info("Path is [{0}].".format(path))
        assert os.path.exists(path)
        assert C_TEMPLATE in os.path.basename(path)
        os.rmdir(path)
        assert not os.path.exists(path)

    def test_write_text_file(self, os_ops: OsOperations):
        """
        Test write for writing data to a text file.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = os_ops.mkstemp()
        data = "Hello, world!"

        os_ops.write(filename, data, truncate=True)
        os_ops.write(filename, data)

        response = os_ops.read(filename)

        assert response == data + data

        os_ops.remove_file(filename)

    def test_write_binary_file(self, os_ops: OsOperations):
        """
        Test write for writing data to a binary file.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = "/tmp/test_file.bin"
        data = b"\x00\x01\x02\x03"

        os_ops.write(filename, data, binary=True, truncate=True)

        response = os_ops.read(filename, binary=True)

        assert response == data

    def test_read_text_file(self, os_ops: OsOperations):
        """
        Test read for reading data from a text file.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = "/etc/hosts"

        response = os_ops.read(filename)

        assert isinstance(response, str)

    def test_read_binary_file(self, os_ops: OsOperations):
        """
        Test read for reading data from a binary file.
        """
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = "/usr/bin/python3"

        response = os_ops.read(filename, binary=True)

        assert isinstance(response, bytes)

    def test_read__text(self, os_ops: OsOperations):
        """
        Test OsOperations::read for text data.
        """
        assert isinstance(os_ops, OsOperations)

        filename = __file__  # current file

        with open(filename, 'r') as file:  # open in a text mode
            response0 = file.read()

        assert type(response0) == str  # noqa: E721

        response1 = os_ops.read(filename)
        assert type(response1) == str  # noqa: E721
        assert response1 == response0

        response2 = os_ops.read(filename, encoding=None, binary=False)
        assert type(response2) == str  # noqa: E721
        assert response2 == response0

        response3 = os_ops.read(filename, encoding="")
        assert type(response3) == str  # noqa: E721
        assert response3 == response0

        response4 = os_ops.read(filename, encoding="UTF-8")
        assert type(response4) == str  # noqa: E721
        assert response4 == response0

    def test_read__binary(self, os_ops: OsOperations):
        """
        Test OsOperations::read for binary data.
        """
        filename = __file__  # current file

        with open(filename, 'rb') as file:  # open in a binary mode
            response0 = file.read()

        assert type(response0) == bytes  # noqa: E721

        response1 = os_ops.read(filename, binary=True)
        assert type(response1) == bytes  # noqa: E721
        assert response1 == response0

    def test_read__binary_and_encoding(self, os_ops: OsOperations):
        """
        Test RemoteOperations::read for binary data and encoding.
        """
        assert isinstance(os_ops, OsOperations)

        filename = __file__  # current file

        with pytest.raises(
                InvalidOperationException,
                match=re.escape("Enconding is not allowed for read binary operation")):
            os_ops.read(filename, encoding="", binary=True)

    def test_read_binary__spec(self, os_ops: OsOperations):
        """
        Test RemoteOperations::read_binary.
        """
        assert isinstance(os_ops, OsOperations)

        filename = __file__  # currnt file

        with open(filename, 'rb') as file:  # open in a binary mode
            response0 = file.read()

        assert type(response0) == bytes  # noqa: E721

        response1 = os_ops.read_binary(filename, 0)
        assert type(response1) == bytes  # noqa: E721
        assert response1 == response0

        response2 = os_ops.read_binary(filename, 1)
        assert type(response2) == bytes  # noqa: E721
        assert len(response2) < len(response1)
        assert len(response2) + 1 == len(response1)
        assert response2 == response1[1:]

        response3 = os_ops.read_binary(filename, len(response1))
        assert type(response3) == bytes  # noqa: E721
        assert len(response3) == 0

        response4 = os_ops.read_binary(filename, len(response2))
        assert type(response4) == bytes  # noqa: E721
        assert len(response4) == 1
        assert response4[0] == response1[len(response1) - 1]

        response5 = os_ops.read_binary(filename, len(response1) + 1)
        assert type(response5) == bytes  # noqa: E721
        assert len(response5) == 0

    def test_read_binary__spec__negative_offset(self, os_ops: OsOperations):
        """
        Test RemoteOperations::read_binary with negative offset.
        """
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(
                ValueError,
                match=re.escape("Negative 'offset' is not supported.")):
            os_ops.read_binary(__file__, -1)

    def test_get_file_size(self, os_ops: OsOperations):
        """
        Test RemoteOperations::get_file_size.
        """
        assert isinstance(os_ops, OsOperations)

        filename = __file__  # current file

        sz0 = os.path.getsize(filename)
        assert type(sz0) == int  # noqa: E721

        sz1 = os_ops.get_file_size(filename)
        assert type(sz1) == int  # noqa: E721
        assert sz1 == sz0

    def test_isfile_true(self, os_ops: OsOperations):
        """
        Test isfile for an existing file.
        """
        assert isinstance(os_ops, OsOperations)

        filename = __file__

        response = os_ops.isfile(filename)

        assert response is True

    def test_isfile_false__not_exist(self, os_ops: OsOperations):
        """
        Test isfile for a non-existing file.
        """
        assert isinstance(os_ops, OsOperations)

        filename = os.path.join(os.path.dirname(__file__), "nonexistent_file.txt")

        response = os_ops.isfile(filename)

        assert response is False

    def test_isfile_false__directory(self, os_ops: OsOperations):
        """
        Test isfile for a firectory.
        """
        assert isinstance(os_ops, OsOperations)

        name = os.path.dirname(__file__)

        assert os_ops.isdir(name)

        response = os_ops.isfile(name)

        assert response is False

    def test_isdir_true(self, os_ops: OsOperations):
        """
        Test isdir for an existing directory.
        """
        assert isinstance(os_ops, OsOperations)

        name = os.path.dirname(__file__)

        response = os_ops.isdir(name)

        assert response is True

    def test_isdir_false__not_exist(self, os_ops: OsOperations):
        """
        Test isdir for a non-existing directory.
        """
        assert isinstance(os_ops, OsOperations)

        name = os.path.join(os.path.dirname(__file__), "it_is_nonexistent_directory")

        response = os_ops.isdir(name)

        assert response is False

    def test_isdir_false__file(self, os_ops: OsOperations):
        """
        Test isdir for a file.
        """
        assert isinstance(os_ops, OsOperations)

        name = __file__

        assert os_ops.isfile(name)

        response = os_ops.isdir(name)

        assert response is False

    def test_cwd(self, os_ops: OsOperations):
        """
        Test cwd.
        """
        assert isinstance(os_ops, OsOperations)

        v = os_ops.cwd()

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

    def test_write(self, write_data001: tagWriteData001, os_ops: OsOperations):
        assert type(write_data001) == __class__.tagWriteData001  # noqa: E721
        assert isinstance(os_ops, OsOperations)

        mode = "w+b" if write_data001.call_param__binary else "w+"

        with tempfile.NamedTemporaryFile(mode=mode, delete=True) as tmp_file:
            tmp_file.write(write_data001.source)
            tmp_file.flush()

            os_ops.write(
                tmp_file.name,
                write_data001.call_param__data,
                read_and_write=write_data001.call_param__rw,
                truncate=write_data001.call_param__truncate,
                binary=write_data001.call_param__binary)

            tmp_file.seek(0)

            s = tmp_file.read()

            assert s == write_data001.result
