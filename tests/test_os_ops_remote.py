# coding: utf-8

from .helpers.global_data import OsOpsDescr
from .helpers.global_data import OsOpsDescrs
from .helpers.global_data import OsOperations
from .helpers.local_check import LocalCheck

from src import ExecUtilException

import pytest


class TestOsOpsRemote:
    @pytest.fixture
    def os_ops_descr(self) -> OsOpsDescr:
        assert type(OsOpsDescrs.sm_remote_os_ops_descr) is OsOpsDescr
        return OsOpsDescrs.sm_remote_os_ops_descr

    def test_rmdirs__try_to_delete_nonexist_path(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        path = "/root/test_dir"

        assert os_ops.rmdirs(path, ignore_errors=False) is True
        return

    def test_rmdirs__try_to_delete_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        path = os_ops.mkstemp()
        assert type(path) is str
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)

        with pytest.raises(ExecUtilException) as x:
            os_ops.rmdirs(path, ignore_errors=False)

        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)
        assert type(x.value) is ExecUtilException
        assert type(x.value.description) is str
        assert x.value.description == "Utility exited with non-zero code (20). Error: `cannot remove '" + path + "': it is not a directory`"
        assert x.value.message.startswith(x.value.description)
        assert type(x.value.error) is str
        assert x.value.error.strip() == "cannot remove '" + path + "': it is not a directory"
        assert type(x.value.exit_code) is int
        assert x.value.exit_code == 20
        return

    def test_read__unknown_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test RemoteOperations::read with unknown file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(ExecUtilException) as x:
            os_ops.read("/dummy")

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)
        return

    def test_read_binary__spec__unk_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test RemoteOperations::read_binary with unknown file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(ExecUtilException) as x:
            os_ops.read_binary("/dummy", 0)

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)
        return

    def test_get_file_size__unk_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test RemoteOperations::get_file_size.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(ExecUtilException) as x:
            os_ops.get_file_size("/dummy")

        assert "Utility exited with non-zero code (1)." in str(x.value)
        assert "No such file or directory" in str(x.value)
        assert "/dummy" in str(x.value)
        return
