from src.exceptions import NodeConnectionTransactionRollbackException
from src.exceptions import TestgresException as testgres__TestgresException


class TestSet001_Constructor:
    def test_001__default(self):
        e = NodeConnectionTransactionRollbackException()
        assert type(e) is NodeConnectionTransactionRollbackException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == ""
        assert str(e) == ""
        assert repr(e) == "NodeConnectionTransactionRollbackException()"
        return

    def test_002__message(self):
        e = NodeConnectionTransactionRollbackException(message="abc\n123")
        assert type(e) is NodeConnectionTransactionRollbackException
        assert isinstance(e, testgres__TestgresException)
        assert e.source is None
        assert e.message == "abc\n123"
        assert str(e) == "abc\n123"
        assert repr(e) == "NodeConnectionTransactionRollbackException(message='abc\\n123')"
        return
