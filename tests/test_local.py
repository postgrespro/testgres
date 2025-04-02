# coding: utf-8
import os

import pytest
import re
import logging

from ..testgres import LocalOperations


class TestLocalOperations:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.operations = LocalOperations()

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

    def test_read__unknown_file(self):
        """
        Test LocalOperations::read with unknown file.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.read("/dummy")

    def test_read_binary__spec__unk_file(self):
        """
        Test LocalOperations::read_binary with unknown file.
        """

        with pytest.raises(
                FileNotFoundError,
                match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.read_binary("/dummy", 0)

    def test_get_file_size__unk_file(self):
        """
        Test LocalOperations::get_file_size.
        """

        with pytest.raises(FileNotFoundError, match=re.escape("[Errno 2] No such file or directory: '/dummy'")):
            self.operations.get_file_size("/dummy")

    def test_cwd(self):
        """
        Test cwd.
        """
        v = self.operations.cwd()

        assert v is not None
        assert type(v) == str  # noqa: E721

        expectedValue = os.getcwd()
        assert expectedValue is not None
        assert type(expectedValue) == str  # noqa: E721
        assert expectedValue != ""  # research

        # Comp result
        assert v == expectedValue
