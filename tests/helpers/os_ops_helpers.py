# coding: utf-8
from testgres.operations.local_ops import OsOperations


class OsOpsHelpers:
    @staticmethod
    def is_localhost(os_ops: OsOperations) -> bool:
        assert isinstance(os_ops, OsOperations)

        host = os_ops.host

        if host == "127.0.0.1":
            return True

        if host == "localhost":
            return True

        return False
