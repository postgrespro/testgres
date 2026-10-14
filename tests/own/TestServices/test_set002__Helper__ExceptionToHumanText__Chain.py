from ...conftest_helpers import TestServices

import pytest
import logging


class TestSet002__Helper__ExceptionToHumanText__Chain:
    def test_001(self):
        with pytest.raises(expected_exception=AssertionError) as x:
            v = 1
            assert v == 2

        assert isinstance(x.value, AssertionError)

        msg = TestServices.Helper__ExceptionToHumanText__Chain(x.value)

        logging.info("msg: {}".format(msg))

        assert "---- 1. Exception (AssertionError):" in msg
        assert "v == 2" in msg
        assert "AssertionError: assert 1 == 2" in msg
        return

    def test_002(self):
        with pytest.raises(expected_exception=AssertionError) as x:
            v = 1
            try:
                raise RuntimeError("aaa")
            except Exception:
                assert v == 2

        assert isinstance(x.value, AssertionError)

        msg = TestServices.Helper__ExceptionToHumanText__Chain(x.value)

        logging.info("msg: {}".format(msg))

        assert "---- 1. Exception (RuntimeError):" in msg
        assert "aaa" in msg
        assert "---- 2. Exception (AssertionError):" in msg
        assert "v == 2" in msg
        assert "AssertionError: assert 1 == 2" in msg
        return
