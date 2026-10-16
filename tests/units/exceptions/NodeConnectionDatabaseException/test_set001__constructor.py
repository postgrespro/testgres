from src.exceptions import NodeConnectionInterfaceException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionInterfaceException()
        assert type(e) is NodeConnectionInterfaceException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionInterfaceException()"
        return

    def test_002__message(self):
        e = NodeConnectionInterfaceException(message="abc\n123")
        assert type(e) is NodeConnectionInterfaceException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionInterfaceException(message='abc\\n123')"
        return
