from src.exceptions import NodeConnectionIntegrityException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionIntegrityException()
        assert type(e) is NodeConnectionIntegrityException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionIntegrityException()"
        return

    def test_002__message(self):
        e = NodeConnectionIntegrityException(message="abc\n123")
        assert type(e) is NodeConnectionIntegrityException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionIntegrityException(message='abc\\n123')"
        return
