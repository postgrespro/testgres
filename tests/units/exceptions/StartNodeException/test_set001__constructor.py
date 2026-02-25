from src.exceptions import StartNodeException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = StartNodeException()
        assert type(e) is StartNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert e.description is None
        assert e.files is None
        assert str(e) == ""
        assert repr(e) == "StartNodeException()"
        return

    def test_002__message(self):
        e = StartNodeException(message="abc\n123")
        assert type(e) is StartNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert e.description == "abc\n123"
        assert e.files is None
        assert str(e) == "abc\n123"
        assert repr(e) == "StartNodeException(message='abc\\n123')"
        return

    def test_003__files(self):
        e = StartNodeException(files=[("f\n1", b'line1\nline2')])
        assert type(e) is StartNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "f\n1\n----\nb'line1\\nline2'\n"
        assert e.description is None
        assert e.files == [("f\n1", b'line1\nline2')]
        assert str(e) == "f\n1\n----\nb'line1\\nline2'\n"
        assert repr(e) == "StartNodeException(files=[('f\\n1', b'line1\\nline2')])"
        return

    def test_004__all(self):
        e = StartNodeException(message="mmm", files=[("f\n1", b'line1\nline2')])
        assert type(e) is StartNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "mmm\nf\n1\n----\nb'line1\\nline2'\n"
        assert e.description == "mmm"
        assert e.files == [("f\n1", b'line1\nline2')]
        assert str(e) == "mmm\nf\n1\n----\nb'line1\\nline2'\n"
        assert repr(e) == "StartNodeException(message='mmm', files=[('f\\n1', b'line1\\nline2')])"
        return
