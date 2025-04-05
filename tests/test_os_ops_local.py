# coding: utf-8
from .helpers.global_data import OsOpsDescrs
from .helpers.global_data import OsOperations

import os

import pytest
import re


class TestOsOpsLocal:
    @pytest.fixture
    def os_ops(self):
        return OsOpsDescrs.sm_local_os_ops

    def test_read__unknown_file(self, os_ops: OsOperations):
        """
        Test LocalOperations::read with unknown file.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            os_ops.read("/dummy")

    def test_read_binary__spec__unk_file(self, os_ops: OsOperations):
        """
        Test LocalOperations::read_binary with unknown file.
        """

        with pytest.raises(
                FileNotFoundError,
                match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            os_ops.read_binary("/dummy", 0)

    def test_get_file_size__unk_file(self, os_ops: OsOperations):
        """
        Test LocalOperations::get_file_size.
        """
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            os_ops.get_file_size("/dummy")

    def test_cwd(self, os_ops: OsOperations):
        """
        Test cwd.
        """
        assert isinstance(os_ops, OsOperations)

        v = os_ops.cwd()

        assert v is not None
        assert type(v) == str  # noqa: E721

        expectedValue = os.getcwd()
        assert expectedValue is not None
        assert type(expectedValue) == str  # noqa: E721
        assert expectedValue != ""  # research

        # Comp result
        assert v == expectedValue
