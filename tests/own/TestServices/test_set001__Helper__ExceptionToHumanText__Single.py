from ...conftest_helpers import TestServices

import pytest
import logging


class TestSet001__Helper__ExceptionToHumanText__Single:
    def test_001(self):
        with pytest.raises(expected_exception=AssertionError) as x:
            v = 1
            assert v == 2

        assert isinstance(x.value, AssertionError)

        msg = TestServices.Helper__ExceptionToHumanText__Single(x.value)

        logging.info("msg: {}".format(msg))

        assert "Exception (AssertionError)." in msg
        assert "v == 2" in msg
        assert "AssertionError: assert 1 == 2" in msg
        return
