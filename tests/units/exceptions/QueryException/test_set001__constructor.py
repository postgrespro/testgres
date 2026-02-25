from src.exceptions import QueryException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = QueryException()
        assert type(e) is QueryException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert e.description is None
        assert e.query is None
        assert str(e) == ""
        assert repr(e) == "QueryException()"
        return

    def test_002__message(self):
        e = QueryException(message="abc\n123")
        assert type(e) is QueryException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert e.description == "abc\n123"
        assert e.query is None
        assert str(e) == "abc\n123"
        assert repr(e) == "QueryException(message='abc\\n123')"
        return

    def test_003__query(self):
        e = QueryException(query="cba\n321")
        assert type(e) is QueryException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "Query: cba\n321"
        assert e.description is None
        assert e.query == "cba\n321"
        assert str(e) == "Query: cba\n321"
        assert repr(e) == "QueryException(query='cba\\n321')"
        return

    def test_004__all(self):
        e = QueryException(message="mmm", query="cba\n321")
        assert type(e) is QueryException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "mmm\nQuery: cba\n321"
        assert e.description == "mmm"
        assert e.query == "cba\n321"
        assert str(e) == "mmm\nQuery: cba\n321"
        assert repr(e) == "QueryException(message='mmm', query='cba\\n321')"
        return
