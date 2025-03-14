from ...testgres.operations.os_ops import OsOperations
from ...testgres.operations.os_ops import ConnectionParams
from ...testgres.operations.local_ops import LocalOperations
from ...testgres.operations.remote_ops import RemoteOperations

import os


class OsOpsDescr:
    os_ops: OsOperations
    sign: str

    def __init__(self, os_ops: OsOperations, sign: str):
        assert isinstance(os_ops, OsOperations)
        assert type(sign) == str  # noqa: E721
        self.os_ops = os_ops
        self.sign = sign


class OsOpsDescrs:
    sm_remote_conn_params = ConnectionParams(
        host=os.getenv('RDBMS_TESTPOOL1_HOST') or '127.0.0.1',
        username=os.getenv('USER'),
        ssh_key=os.getenv('RDBMS_TESTPOOL_SSHKEY'))

    sm_remote_os_ops = RemoteOperations(sm_remote_conn_params)

    sm_remote_os_ops_descr = OsOpsDescr(sm_remote_os_ops, "remote_ops")

    sm_local_os_ops = LocalOperations()

    sm_local_os_ops_descr = OsOpsDescr(sm_local_os_ops, "local_ops")
