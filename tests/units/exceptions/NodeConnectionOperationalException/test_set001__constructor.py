from src.exceptions import NodeConnectionOperationalException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionOperationalException()
        assert type(e) is NodeConnectionOperationalException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionOperationalException()"
        return

    def test_002__message(self):
        e = NodeConnectionOperationalException(message="abc\n123")
        assert type(e) is NodeConnectionOperationalException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionOperationalException(message='abc\\n123')"
        return
