from src.exceptions import NodeConnectionNotSupportedException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionNotSupportedException()
        assert type(e) is NodeConnectionNotSupportedException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionNotSupportedException()"
        return

    def test_002__message(self):
        e = NodeConnectionNotSupportedException(message="abc\n123")
        assert type(e) is NodeConnectionNotSupportedException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionNotSupportedException(message='abc\\n123')"
        return
