from src.exceptions import NodeConnectionInternalException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionInternalException()
        assert type(e) is NodeConnectionInternalException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionInternalException()"
        return

    def test_002__message(self):
        e = NodeConnectionInternalException(message="abc\n123")
        assert type(e) is NodeConnectionInternalException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionInternalException(message='abc\\n123')"
        return
