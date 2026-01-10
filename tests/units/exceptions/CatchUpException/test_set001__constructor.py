from src.exceptions import CatchUpException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = CatchUpException()
        assert type(e) == CatchUpException  # noqa: E721
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "CatchUpException()"
        return

    def test_002__message(self):
        e = CatchUpException(message="abc\n123")
        assert type(e) == CatchUpException  # noqa: E721
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "CatchUpException(message='abc\\n123')"
        return
