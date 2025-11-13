from testgres.operations.os_ops import OsOperations
from testgres.operations.os_ops import ConnectionParams
from testgres.operations.local_ops import LocalOperations
from testgres.operations.remote_ops import RemoteOperations

from src.node import PortManager
from src.node import PortManager__ThisHost
from src.node import PortManager__Generic

import os


class OsOpsDescr:
    sign: str
    os_ops: OsOperations

    def __init__(self, sign: str, os_ops: OsOperations):
        assert type(sign) == str  # noqa: E721
        assert isinstance(os_ops, OsOperations)
        self.sign = sign
        self.os_ops = os_ops


class OsOpsDescrs:
    sm_remote_conn_params = ConnectionParams(
        host=os.getenv('RDBMS_TESTPOOL1_HOST') or '127.0.0.1',
        username=os.getenv('USER'),
        ssh_key=os.getenv('RDBMS_TESTPOOL_SSHKEY'))

    sm_remote_os_ops = RemoteOperations(sm_remote_conn_params)

    sm_remote_os_ops_descr = OsOpsDescr("remote_ops", sm_remote_os_ops)

    sm_local_os_ops = LocalOperations.get_single_instance()

    sm_local_os_ops_descr = OsOpsDescr("local_ops", sm_local_os_ops)


class PortManagers:
    sm_remote_port_manager = PortManager__Generic(OsOpsDescrs.sm_remote_os_ops)

    sm_local_port_manager = PortManager__ThisHost.get_single_instance()

    sm_local2_port_manager = PortManager__Generic(OsOpsDescrs.sm_local_os_ops)


class PostgresNodeService:
    sign: str
    os_ops: OsOperations
    port_manager: PortManager

    def __init__(self, sign: str, os_ops: OsOperations, port_manager: PortManager):
        assert type(sign) == str  # noqa: E721
        assert isinstance(os_ops, OsOperations)
        assert isinstance(port_manager, PortManager)
        self.sign = sign
        self.os_ops = os_ops
        self.port_manager = port_manager


class PostgresNodeServices:
    sm_remote = PostgresNodeService(
        "remote",
        OsOpsDescrs.sm_remote_os_ops,
        PortManagers.sm_remote_port_manager
    )

    sm_local = PostgresNodeService(
        "local",
        OsOpsDescrs.sm_local_os_ops,
        PortManagers.sm_local_port_manager
    )

    sm_local2 = PostgresNodeService(
        "local2",
        OsOpsDescrs.sm_local_os_ops,
        PortManagers.sm_local2_port_manager
    )
