# coding: utf-8
import pytest
import platform


class RunConditions:
    # It is not a test kit!
    __test__ = False

    def skip_if_windows():
        if platform.system().lower() == "windows":
            pytest.skip("This test does not support Windows.")
