from .helpers.os_ops_descrs import OsOpsDescr
from .helpers.os_ops_descrs import OsOpsDescrs
from .helpers.os_ops_descrs import OsOperations

from ..testgres.node import PgVer
from ..testgres.node import PostgresNode
from ..testgres.utils import get_pg_version2
from ..testgres import ProcessType
from ..testgres import TestgresException
from ..testgres import StartNodeException
from ..testgres import QueryException
from ..testgres import ExecUtilException

import pytest
import six
import logging
import time


class TestTestgresCommon:
    sm_os_ops_descrs: list[OsOpsDescr] = [
        OsOpsDescrs.sm_local_os_ops_descr,
        OsOpsDescrs.sm_remote_os_ops_descr
    ]

    @pytest.fixture(
        params=[descr.os_ops for descr in sm_os_ops_descrs],
        ids=[descr.sign for descr in sm_os_ops_descrs]
    )
    def os_ops(self, request: pytest.FixtureRequest) -> OsOperations:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, OsOperations)
        return request.param

    def test_version_management(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        a = PgVer('10.0')
        b = PgVer('10')
        c = PgVer('9.6.5')
        d = PgVer('15.0')
        e = PgVer('15rc1')
        f = PgVer('15beta4')
        h = PgVer('15.3biha')
        i = PgVer('15.3')
        g = PgVer('15.3.1bihabeta1')
        k = PgVer('15.3.1')

        assert (a == b)
        assert (b > c)
        assert (a > c)
        assert (d > e)
        assert (e > f)
        assert (d > f)
        assert (h > f)
        assert (h == i)
        assert (g == k)
        assert (g > h)

        version = get_pg_version2(os_ops)

        with __class__.helper__get_node(os_ops) as node:
            assert (isinstance(version, six.string_types))
            assert (isinstance(node.version, PgVer))
            assert (node.version == PgVer(version))

    def test_child_pids(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        master_processes = [
            ProcessType.AutovacuumLauncher,
            ProcessType.BackgroundWriter,
            ProcessType.Checkpointer,
            ProcessType.StatsCollector,
            ProcessType.WalSender,
            ProcessType.WalWriter,
        ]

        postgresVersion = get_pg_version2(os_ops)

        if __class__.helper__pg_version_ge(postgresVersion, '10'):
            master_processes.append(ProcessType.LogicalReplicationLauncher)

        if __class__.helper__pg_version_ge(postgresVersion, '14'):
            master_processes.remove(ProcessType.StatsCollector)

        repl_processes = [
            ProcessType.Startup,
            ProcessType.WalReceiver,
        ]

        def LOCAL__test_auxiliary_pids(
            node: PostgresNode,
            expectedTypes: list[ProcessType]
        ) -> list[ProcessType]:
            # returns list of the absence processes
            assert node is not None
            assert type(node) == PostgresNode  # noqa: E721
            assert expectedTypes is not None
            assert type(expectedTypes) == list  # noqa: E721

            pids = node.auxiliary_pids
            assert pids is not None  # noqa: E721
            assert type(pids) == dict  # noqa: E721

            result = list[ProcessType]()
            for ptype in expectedTypes:
                if not (ptype in pids):
                    result.append(ptype)
            return result

        def LOCAL__check_auxiliary_pids__multiple_attempts(
                node: PostgresNode,
                expectedTypes: list[ProcessType]):
            assert node is not None
            assert type(node) == PostgresNode  # noqa: E721
            assert expectedTypes is not None
            assert type(expectedTypes) == list  # noqa: E721

            nAttempt = 0

            while nAttempt < 5:
                nAttempt += 1

                logging.info("Test pids of [{0}] node. Attempt #{1}.".format(
                    node.name,
                    nAttempt
                ))

                if nAttempt > 1:
                    time.sleep(1)

                absenceList = LOCAL__test_auxiliary_pids(node, expectedTypes)
                assert absenceList is not None
                assert type(absenceList) == list  # noqa: E721
                if len(absenceList) == 0:
                    logging.info("Bingo!")
                    return

                logging.info("These processes are not found: {0}.".format(absenceList))
                continue

            raise Exception("Node {0} does not have the following processes: {1}.".format(
                node.name,
                absenceList
            ))

        with __class__.helper__get_node(os_ops).init().start() as master:

            # master node doesn't have a source walsender!
            with pytest.raises(expected_exception=TestgresException):
                master.source_walsender

            with master.connect() as con:
                assert (con.pid > 0)

            with master.replicate().start() as replica:

                # test __str__ method
                str(master.child_processes[0])

                LOCAL__check_auxiliary_pids__multiple_attempts(
                    master,
                    master_processes)

                LOCAL__check_auxiliary_pids__multiple_attempts(
                    replica,
                    repl_processes)

                master_pids = master.auxiliary_pids

                # there should be exactly 1 source walsender for replica
                assert (len(master_pids[ProcessType.WalSender]) == 1)
                pid1 = master_pids[ProcessType.WalSender][0]
                pid2 = replica.source_walsender.pid
                assert (pid1 == pid2)

                replica.stop()

                # there should be no walsender after we've stopped replica
                with pytest.raises(expected_exception=TestgresException):
                    replica.source_walsender

    def test_exceptions(self):
        str(StartNodeException('msg', [('file', 'lines')]))
        str(ExecUtilException('msg', 'cmd', 1, 'out'))
        str(QueryException('msg', 'query'))

    @staticmethod
    def helper__get_node(os_ops: OsOperations, name=None):
        assert isinstance(os_ops, OsOperations)
        return PostgresNode(name, os_ops=os_ops)

    @staticmethod
    def helper__pg_version_ge(ver1: str, ver2: str) -> bool:
        assert type(ver1) == str  # noqa: E721
        assert type(ver2) == str  # noqa: E721
        v1 = PgVer(ver1)
        v2 = PgVer(ver2)
        return v1 >= v2
