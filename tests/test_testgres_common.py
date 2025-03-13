from .helpers.os_ops_descrs import OsOpsDescr
from .helpers.os_ops_descrs import OsOpsDescrs
from .helpers.os_ops_descrs import OsOperations

from ..testgres.node import PgVer
from ..testgres.node import PostgresNode
from ..testgres.utils import get_pg_version2
from ..testgres.utils import file_tail
from ..testgres.utils import get_bin_path2
from ..testgres import ProcessType
from ..testgres import NodeStatus
from ..testgres import IsolationLevel
from ..testgres import TestgresException
from ..testgres import InitNodeException
from ..testgres import StartNodeException
from ..testgres import QueryException
from ..testgres import ExecUtilException
from ..testgres import TimeoutException
from ..testgres import ProgrammingError
from ..testgres import scoped_config

import pytest
import six
import logging
import time
import tempfile
import uuid
import os


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

    def test_double_init(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops).init() as node:
            # can't initialize node more than once
            with pytest.raises(expected_exception=InitNodeException):
                node.init()

    def test_init_after_cleanup(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops) as node:
            node.init().start().execute('select 1')
            node.cleanup()
            node.init().start().execute('select 1')

    def test_init_unique_system_id(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        # this function exists in PostgreSQL 9.6+
        current_version = get_pg_version2(os_ops)

        __class__.helper__skip_test_if_util_not_exist(os_ops, "pg_resetwal")
        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, '9.6')

        query = 'select system_identifier from pg_control_system()'

        with scoped_config(cache_initdb=False):
            with __class__.helper__get_node(os_ops).init().start() as node0:
                id0 = node0.execute(query)[0]

        with scoped_config(cache_initdb=True,
                           cached_initdb_unique=True) as config:
            assert (config.cache_initdb)
            assert (config.cached_initdb_unique)

            # spawn two nodes; ids must be different
            with __class__.helper__get_node(os_ops).init().start() as node1, \
                    __class__.helper__get_node(os_ops).init().start() as node2:
                id1 = node1.execute(query)[0]
                id2 = node2.execute(query)[0]

                # ids must increase
                assert (id1 > id0)
                assert (id2 > id1)

    def test_node_exit(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with pytest.raises(expected_exception=QueryException):
            with __class__.helper__get_node(os_ops).init() as node:
                base_dir = node.base_dir
                node.safe_psql('select 1')

        # we should save the DB for "debugging"
        assert (os_ops.path_exists(base_dir))
        os_ops.rmdirs(base_dir, ignore_errors=True)

        with __class__.helper__get_node(os_ops).init() as node:
            base_dir = node.base_dir

        # should have been removed by default
        assert not (os_ops.path_exists(base_dir))

    def test_double_start(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops).init().start() as node:
            # can't start node more than once
            node.start()
            assert (node.is_started)

    def test_uninitialized_start(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops) as node:
            # node is not initialized yet
            with pytest.raises(expected_exception=StartNodeException):
                node.start()

    def test_restart(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops) as node:
            node.init().start()

            # restart, ok
            res = node.execute('select 1')
            assert (res == [(1,)])
            node.restart()
            res = node.execute('select 2')
            assert (res == [(2,)])

            # restart, fail
            with pytest.raises(expected_exception=StartNodeException):
                node.append_conf('pg_hba.conf', 'DUMMY')
                node.restart()

    def test_reload(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops) as node:
            node.init().start()

            # change client_min_messages and save old value
            cmm_old = node.execute('show client_min_messages')
            node.append_conf(client_min_messages='DEBUG1')

            # reload config
            node.reload()

            # check new value
            cmm_new = node.execute('show client_min_messages')
            assert ('debug1' == cmm_new[0][0].lower())
            assert (cmm_old != cmm_new)

    def test_pg_ctl(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops) as node:
            node.init().start()

            status = node.pg_ctl(['status'])
            assert ('PID' in status)

    def test_status(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        assert (NodeStatus.Running)
        assert not (NodeStatus.Stopped)
        assert not (NodeStatus.Uninitialized)

        # check statuses after each operation
        with __class__.helper__get_node(os_ops) as node:
            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Uninitialized)

            node.init()

            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Stopped)

            node.start()

            assert (node.pid != 0)
            assert (node.status() == NodeStatus.Running)

            node.stop()

            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Stopped)

            node.cleanup()

            assert (node.pid == 0)
            assert (node.status() == NodeStatus.Uninitialized)

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

    def test_auto_name(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        with __class__.helper__get_node(os_ops).init(allow_streaming=True).start() as m:
            with m.replicate().start() as r:
                # check that nodes are running
                assert (m.status())
                assert (r.status())

                # check their names
                assert (m.name != r.name)
                assert ('testgres' in m.name)
                assert ('testgres' in r.name)

    def test_file_tail(self):
        s1 = "the quick brown fox jumped over that lazy dog\n"
        s2 = "abc\n"
        s3 = "def\n"

        with tempfile.NamedTemporaryFile(mode='r+', delete=True) as f:
            sz = 0
            while sz < 3 * 8192:
                sz += len(s1)
                f.write(s1)
            f.write(s2)
            f.write(s3)

            f.seek(0)
            lines = file_tail(f, 3)
            assert (lines[0] == s1)
            assert (lines[1] == s2)
            assert (lines[2] == s3)

            f.seek(0)
            lines = file_tail(f, 1)
            assert (lines[0] == s3)

    def test_isolation_levels(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops).init().start() as node:
            with node.connect() as con:
                # string levels
                con.begin('Read Uncommitted').commit()
                con.begin('Read Committed').commit()
                con.begin('Repeatable Read').commit()
                con.begin('Serializable').commit()

                # enum levels
                con.begin(IsolationLevel.ReadUncommitted).commit()
                con.begin(IsolationLevel.ReadCommitted).commit()
                con.begin(IsolationLevel.RepeatableRead).commit()
                con.begin(IsolationLevel.Serializable).commit()

                # check wrong level
                with pytest.raises(expected_exception=QueryException):
                    con.begin('Garbage').commit()

    def test_users(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops).init().start() as node:
            node.psql('create role test_user login')
            value = node.safe_psql('select 1', username='test_user')
            value = __class__.helper__rm_carriage_returns(value)
            assert (value == b'1\n')

    def test_poll_query_until(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init().start()

            get_time = 'select extract(epoch from now())'
            check_time = 'select extract(epoch from now()) - {} >= 5'

            start_time = node.execute(get_time)[0][0]
            node.poll_query_until(query=check_time.format(start_time))
            end_time = node.execute(get_time)[0][0]

            assert (end_time - start_time >= 5)

            # check 0 columns
            with pytest.raises(expected_exception=QueryException):
                node.poll_query_until(
                    query='select from pg_catalog.pg_class limit 1')

            # check None, fail
            with pytest.raises(expected_exception=QueryException):
                node.poll_query_until(query='create table abc (val int)')

            # check None, ok
            node.poll_query_until(query='create table def()',
                                  expected=None)    # returns nothing

            # check 0 rows equivalent to expected=None
            node.poll_query_until(
                query='select * from pg_catalog.pg_class where true = false',
                expected=None)

            # check arbitrary expected value, fail
            with pytest.raises(expected_exception=TimeoutException):
                node.poll_query_until(query='select 3',
                                      expected=1,
                                      max_attempts=3,
                                      sleep_time=0.01)

            # check arbitrary expected value, ok
            node.poll_query_until(query='select 2', expected=2)

            # check timeout
            with pytest.raises(expected_exception=TimeoutException):
                node.poll_query_until(query='select 1 > 2',
                                      max_attempts=3,
                                      sleep_time=0.01)

            # check ProgrammingError, fail
            with pytest.raises(expected_exception=ProgrammingError):
                node.poll_query_until(query='dummy1')

            # check ProgrammingError, ok
            with pytest.raises(expected_exception=(TimeoutException)):
                node.poll_query_until(query='dummy2',
                                      max_attempts=3,
                                      sleep_time=0.01,
                                      suppress={ProgrammingError})

            # check 1 arg, ok
            node.poll_query_until('select true')

    def test_logging(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        C_MAX_ATTEMPTS = 50
        # This name is used for testgres logging, too.
        C_NODE_NAME = "testgres_tests." + __class__.__name__ + "test_logging-master-" + uuid.uuid4().hex

        logging.info("Node name is [{0}]".format(C_NODE_NAME))

        with tempfile.NamedTemporaryFile('w', delete=True) as logfile:
            formatter = logging.Formatter(fmt="%(node)-5s: %(message)s")
            handler = logging.FileHandler(filename=logfile.name)
            handler.formatter = formatter
            logger = logging.getLogger(C_NODE_NAME)
            assert logger is not None
            assert len(logger.handlers) == 0

            try:
                # It disables to log on the root level
                logger.propagate = False
                logger.addHandler(handler)

                with scoped_config(use_python_logging=True):
                    with __class__.helper__get_node(os_ops, name=C_NODE_NAME) as master:
                        logging.info("Master node is initilizing")
                        master.init()

                        logging.info("Master node is starting")
                        master.start()

                        logging.info("Dummy query is executed a few times")
                        for _ in range(20):
                            master.execute('select 1')
                            time.sleep(0.01)

                        # let logging worker do the job
                        time.sleep(0.1)

                        logging.info("Master node log file is checking")
                        nAttempt = 0

                        while True:
                            assert nAttempt <= C_MAX_ATTEMPTS
                            if nAttempt == C_MAX_ATTEMPTS:
                                raise Exception("Test failed!")

                            # let logging worker do the job
                            time.sleep(0.1)

                            nAttempt += 1

                            logging.info("Attempt {0}".format(nAttempt))

                            # check that master's port is found
                            with open(logfile.name, 'r') as log:
                                lines = log.readlines()

                            assert lines is not None
                            assert type(lines) == list  # noqa: E721

                            def LOCAL__test_lines():
                                for s in lines:
                                    if any(C_NODE_NAME in s for s in lines):
                                        logging.info("OK. We found the node_name in a line \"{0}\"".format(s))
                                        return True
                                    return False

                            if LOCAL__test_lines():
                                break

                            logging.info("Master node log file does not have an expected information.")
                            continue

                        # test logger after stop/start/restart
                        logging.info("Master node is stopping...")
                        master.stop()
                        logging.info("Master node is staring again...")
                        master.start()
                        logging.info("Master node is restaring...")
                        master.restart()
                        assert (master._logger.is_alive())
            finally:
                # It is a hack code to logging cleanup
                logging._acquireLock()
                assert logging.Logger.manager is not None
                assert C_NODE_NAME in logging.Logger.manager.loggerDict.keys()
                logging.Logger.manager.loggerDict.pop(C_NODE_NAME, None)
                assert not (C_NODE_NAME in logging.Logger.manager.loggerDict.keys())
                assert not (handler in logging._handlers.values())
                logging._releaseLock()
        # GO HOME!
        return

    @staticmethod
    def helper__get_node(os_ops: OsOperations, name=None):
        assert isinstance(os_ops, OsOperations)
        return PostgresNode(name, os_ops=os_ops)

    @staticmethod
    def helper__skip_test_if_pg_version_is_not_ge(ver1: str, ver2):
        assert type(ver1) == str  # noqa: E721
        assert type(ver2) == str  # noqa: E721
        if not __class__.helper__pg_version_ge(ver1, ver2):
            pytest.skip('requires {0}+'.format(ver2))

    @staticmethod
    def helper__pg_version_ge(ver1: str, ver2: str) -> bool:
        assert type(ver1) == str  # noqa: E721
        assert type(ver2) == str  # noqa: E721
        v1 = PgVer(ver1)
        v2 = PgVer(ver2)
        return v1 >= v2

    @staticmethod
    def helper__rm_carriage_returns(out):
        """
        In Windows we have additional '\r' symbols in output.
        Let's get rid of them.
        """
        if isinstance(out, (int, float, complex)):
            return out

        if isinstance(out, tuple):
            return tuple(__class__.helper__rm_carriage_returns(item) for item in out)

        if isinstance(out, bytes):
            return out.replace(b'\r', b'')

        assert type(out) == str  # noqa: E721
        return out.replace('\r', '')

    @staticmethod
    def helper__skip_test_if_util_not_exist(os_ops: OsOperations, name: str):
        assert isinstance(os_ops, OsOperations)
        assert type(name) == str  # noqa: E721
        if not __class__.helper__util_exists(os_ops, name):
            pytest.skip('might be missing')

    @staticmethod
    def helper__util_exists(os_ops: OsOperations, util):
        assert isinstance(os_ops, OsOperations)

        def good_properties(f):
            return (os_ops.path_exists(f) and  # noqa: W504
                    os_ops.isfile(f) and  # noqa: W504
                    os_ops.is_executable(f))  # yapf: disable

        # try to resolve it
        if good_properties(get_bin_path2(os_ops, util)):
            return True

        # check if util is in PATH
        for path in os_ops.environ("PATH").split(os.pathsep):
            if good_properties(os.path.join(path, util)):
                return True
