# coding: utf-8
import pytest
import platform


class RunConditions:
    # It is not a test kit!
    __test__ = False

    @staticmethod
    def skip_if_windows():
        if platform.system().lower() == "windows":
            pytest.skip("This test does not support Windows.")

    @staticmethod
    def skip_if_darwin():
        if platform.system().lower() == "darwin":
            pytest.skip("This test does not support Darwin.")

    @staticmethod
    def skip_if_linux():
        if platform.system().lower() == "linux":
            pytest.skip("This test does not support Linux.")
