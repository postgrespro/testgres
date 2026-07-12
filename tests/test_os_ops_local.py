# coding: utf-8
from .helpers.global_data import OsOpsDescr
from .helpers.global_data import OsOpsDescrs
from .helpers.global_data import OsOperations

import os

import pytest
import re


class TestOsOpsLocal:
    @pytest.fixture
    def os_ops_descr(self) -> OsOpsDescr:
        assert type(OsOpsDescrs.sm_local_os_ops_descr) is OsOpsDescr
        return OsOpsDescrs.sm_local_os_ops_descr

    def test_read__unknown_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test LocalOperations::read with unknown file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            os_ops.read("/dummy")
        return

    def test_read_binary__spec__unk_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test LocalOperations::read_binary with unknown file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(
                FileNotFoundError,
                match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            os_ops.read_binary("/dummy", 0)
        return

    def test_get_file_size__unk_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test LocalOperations::get_file_size.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            os_ops.get_file_size("/dummy")
        return

    def test_cwd(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test cwd.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.cwd()

        assert v is not None
        assert type(v) is str

        expectedValue = os.getcwd()
        assert expectedValue is not None
        assert type(expectedValue) is str
        assert expectedValue != ""  # research

        # Comp result
        assert v == expectedValue
        return
