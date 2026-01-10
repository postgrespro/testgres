from src.exceptions import InitNodeException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = InitNodeException()
        assert type(e) == InitNodeException  # noqa: E721
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "InitNodeException()"
        return

    def test_002__message(self):
        e = InitNodeException(message="abc\n123")
        assert type(e) == InitNodeException  # noqa: E721
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "InitNodeException(message='abc\\n123')"
        return
