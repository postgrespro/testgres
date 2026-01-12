from src.exceptions import QueryTimeoutException
from src.exceptions import TimeoutException
from src.exceptions import QueryException


class TestSet001:
    def test_001__default(self):
        # It is an alias
        assert TimeoutException == QueryTimeoutException
        assert issubclass(TimeoutException, QueryException)
        return
