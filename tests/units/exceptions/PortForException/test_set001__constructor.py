from src.exceptions import PortForException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = PortForException()
        assert type(e) is PortForException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "PortForException()"
        return

    def test_002__message(self):
        e = PortForException(message="abc\n123")
        assert type(e) is PortForException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "PortForException(message='abc\\n123')"
        return
