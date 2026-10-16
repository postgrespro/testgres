from src.exceptions import NodeConnectionDataException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionDataException()
        assert type(e) is NodeConnectionDataException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionDataException()"
        return

    def test_002__message(self):
        e = NodeConnectionDataException(message="abc\n123")
        assert type(e) is NodeConnectionDataException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionDataException(message='abc\\n123')"
        return
