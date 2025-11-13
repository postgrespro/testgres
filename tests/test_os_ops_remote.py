# coding: utf-8

from .helpers.global_data import OsOpsDescrs
from .helpers.global_data import OsOperations

from src import ExecUtilException

import os
import pytest


class TestOsOpsRemote:
    @pytest.fixture
    def os_ops(self):
        return OsOpsDescrs.sm_remote_os_ops

    def test_rmdirs__try_to_delete_nonexist_path(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        path = "/root/test_dir"

        assert os_ops.rmdirs(path, ignore_errors=False) is True

    def test_rmdirs__try_to_delete_file(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        path = os_ops.mkstemp()
        assert type(path) == str  # noqa: E721
        assert os.path.exists(path)

        with pytest.raises(ExecUtilException) as x:
            os_ops.rmdirs(path, ignore_errors=False)

        assert os.path.exists(path)
        assert type(x.value) == ExecUtilException   # noqa: E721
        assert x.value.message == "Utility exited with non-zero code (20). Error: `cannot remove '" + path + "': it is not a directory`"
        assert type(x.value.error) == str  # noqa: E721
        assert x.value.error.strip() == "cannot remove '" + path + "': it is not a directory"
        assert type(x.value.exit_code) == int  # noqa: E721
        assert x.value.exit_code == 20

    def test_read__unknown_file(self, os_ops: OsOperations):
        """
        Test RemoteOperations::read with unknown file.
        """
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(ExecUtilException) as x:
            os_ops.read("/dummy")

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)

    def test_read_binary__spec__unk_file(self, os_ops: OsOperations):
        """
        Test RemoteOperations::read_binary with unknown file.
        """
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(ExecUtilException) as x:
            os_ops.read_binary("/dummy", 0)

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)

    def test_get_file_size__unk_file(self, os_ops: OsOperations):
        """
        Test RemoteOperations::get_file_size.
        """
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(ExecUtilException) as x:
            os_ops.get_file_size("/dummy")

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)
