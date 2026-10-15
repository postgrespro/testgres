from src.exceptions import NodeConnectionArrayContentNotSupportedException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionArrayContentNotSupportedException()
        assert type(e) is NodeConnectionArrayContentNotSupportedException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionArrayContentNotSupportedException()"
        return

    def test_002__message(self):
        e = NodeConnectionArrayContentNotSupportedException(message="abc\n123")
        assert type(e) is NodeConnectionArrayContentNotSupportedException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionArrayContentNotSupportedException(message='abc\\n123')"
        return
