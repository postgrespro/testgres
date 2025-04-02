# coding: utf-8
import os

import pytest

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
