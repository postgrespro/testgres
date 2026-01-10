from src.exceptions import QueryTimeoutException
from src.exceptions import QueryException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = QueryTimeoutException()
        assert type(e) == QueryTimeoutException  # noqa: E721
        assert isinstance(e, QueryException)
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert e.description is None
        assert e.query is None
        assert str(e) == ""
        assert repr(e) == "QueryTimeoutException()"
        return

    def test_002__message(self):
        e = QueryTimeoutException(message="abc\n123")
        assert type(e) == QueryTimeoutException  # noqa: E721
        assert isinstance(e, QueryException)
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert e.description == "abc\n123"
        assert e.query is None
        assert str(e) == "abc\n123"
        assert repr(e) == "QueryTimeoutException(message='abc\\n123')"
        return

    def test_003__query(self):
        e = QueryTimeoutException(query="cba\n321")
        assert type(e) == QueryTimeoutException  # noqa: E721
        assert isinstance(e, QueryException)
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "Query: cba\n321"
        assert e.description is None
        assert e.query == "cba\n321"
        assert str(e) == "Query: cba\n321"
        assert repr(e) == "QueryTimeoutException(query='cba\\n321')"
        return

    def test_004__all(self):
        e = QueryTimeoutException(message="mmm", query="cba\n321")
        assert type(e) == QueryTimeoutException  # noqa: E721
        assert isinstance(e, QueryException)
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "mmm\nQuery: cba\n321"
        assert e.description == "mmm"
        assert e.query == "cba\n321"
        assert str(e) == "mmm\nQuery: cba\n321"
        assert repr(e) == "QueryTimeoutException(message='mmm', query='cba\\n321')"
        return
