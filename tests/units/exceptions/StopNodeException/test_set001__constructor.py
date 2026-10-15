from src.exceptions import StopNodeException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = StopNodeException()
        assert type(e) is StopNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert e.description is None
        assert e.files is None
        assert str(e) == ""
        assert repr(e) == "StopNodeException()"
        return

    def test_002__message(self):
        e = StopNodeException(message="abc\n123")
        assert type(e) is StopNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert e.description == "abc\n123"
        assert e.files is None
        assert str(e) == "abc\n123"
        assert repr(e) == "StopNodeException(message='abc\\n123')"
        return

    def test_003__files(self):
        e = StopNodeException(files=[("f\n1", b'line1\nline2')])
        assert type(e) is StopNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "f\n1\n----\nb'line1\\nline2'\n"
        assert e.description is None
        assert e.files == [("f\n1", b'line1\nline2')]
        assert str(e) == "f\n1\n----\nb'line1\\nline2'\n"
        assert repr(e) == "StopNodeException(files=[('f\\n1', b'line1\\nline2')])"
        return

    def test_004__all(self):
        e = StopNodeException(message="mmm", files=[("f\n1", b'line1\nline2')])
        assert type(e) is StopNodeException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "mmm\nf\n1\n----\nb'line1\\nline2'\n"
        assert e.description == "mmm"
        assert e.files == [("f\n1", b'line1\nline2')]
        assert str(e) == "mmm\nf\n1\n----\nb'line1\\nline2'\n"
        assert repr(e) == "StopNodeException(message='mmm', files=[('f\\n1', b'line1\\nline2')])"
        return
