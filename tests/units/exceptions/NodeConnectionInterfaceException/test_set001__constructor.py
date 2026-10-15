from src.exceptions import NodeConnectionDatabaseException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionDatabaseException()
        assert type(e) is NodeConnectionDatabaseException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionDatabaseException()"
        return

    def test_002__message(self):
        e = NodeConnectionDatabaseException(message="abc\n123")
        assert type(e) is NodeConnectionDatabaseException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionDatabaseException(message='abc\\n123')"
        return
