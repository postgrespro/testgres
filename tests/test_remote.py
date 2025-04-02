# coding: utf-8
import os

import pytest
import logging

from ..testgres import ExecUtilException
from ..testgres import RemoteOperations
from ..testgres import ConnectionParams


class TestRemoteOperations:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        conn_params = ConnectionParams(host=os.getenv('RDBMS_TESTPOOL1_HOST') or '127.0.0.1',
                                       username=os.getenv('USER'),
                                       ssh_key=os.getenv('RDBMS_TESTPOOL_SSHKEY'))
        self.operations = RemoteOperations(conn_params)

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
