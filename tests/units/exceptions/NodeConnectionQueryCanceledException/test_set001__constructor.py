from src.exceptions import NodeConnectionQueryCanceledException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionQueryCanceledException()
        assert type(e) is NodeConnectionQueryCanceledException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionQueryCanceledException()"
        return

    def test_002__message(self):
        e = NodeConnectionQueryCanceledException(message="abc\n123")
        assert type(e) is NodeConnectionQueryCanceledException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionQueryCanceledException(message='abc\\n123')"
        return
