from src.exceptions import NodeConnectionException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionException()
        assert type(e) is NodeConnectionException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionException()"
        return

    def test_002__message(self):
        e = NodeConnectionException(message="abc\n123")
        assert type(e) is NodeConnectionException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionException(message='abc\\n123')"
        return
