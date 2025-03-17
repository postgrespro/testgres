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
from ..testgres import InvalidOperationException
from ..testgres import BackupException
from ..testgres import ProgrammingError
from ..testgres import scoped_config
from ..testgres import First, Any

from contextlib import contextmanager

import pytest
import six
import logging
import time
import tempfile
import uuid
import os
import re


@contextmanager
def removing(os_ops: OsOperations, f):
    assert isinstance(os_ops, OsOperations)

    try:
        yield f
    finally:
        if os_ops.isfile(f):
            os_ops.remove_file(f)

        elif os_ops.isdir(f):
            os_ops.rmdirs(f, ignore_errors=True)


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

    def test_psql(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops).init().start() as node:

            # check returned values (1 arg)
            res = node.psql('select 1')
            assert (__class__.helper__rm_carriage_returns(res) == (0, b'1\n', b''))

            # check returned values (2 args)
            res = node.psql('postgres', 'select 2')
            assert (__class__.helper__rm_carriage_returns(res) == (0, b'2\n', b''))

            # check returned values (named)
            res = node.psql(query='select 3', dbname='postgres')
            assert (__class__.helper__rm_carriage_returns(res) == (0, b'3\n', b''))

            # check returned values (1 arg)
            res = node.safe_psql('select 4')
            assert (__class__.helper__rm_carriage_returns(res) == b'4\n')

            # check returned values (2 args)
            res = node.safe_psql('postgres', 'select 5')
            assert (__class__.helper__rm_carriage_returns(res) == b'5\n')

            # check returned values (named)
            res = node.safe_psql(query='select 6', dbname='postgres')
            assert (__class__.helper__rm_carriage_returns(res) == b'6\n')

            # check feeding input
            node.safe_psql('create table horns (w int)')
            node.safe_psql('copy horns from stdin (format csv)',
                           input=b"1\n2\n3\n\\.\n")
            _sum = node.safe_psql('select sum(w) from horns')
            assert (__class__.helper__rm_carriage_returns(_sum) == b'6\n')

            # check psql's default args, fails
            with pytest.raises(expected_exception=QueryException):
                node.psql()

            node.stop()

            # check psql on stopped node, fails
            with pytest.raises(expected_exception=QueryException):
                node.safe_psql('select 1')

    def test_safe_psql__expect_error(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops).init().start() as node:
            err = node.safe_psql('select_or_not_select 1', expect_error=True)
            assert (type(err) == str)  # noqa: E721
            assert ('select_or_not_select' in err)
            assert ('ERROR:  syntax error at or near "select_or_not_select"' in err)

            # ---------
            with pytest.raises(
                expected_exception=InvalidOperationException,
                match="^" + re.escape("Exception was expected, but query finished successfully: `select 1;`.") + "$"
            ):
                node.safe_psql("select 1;", expect_error=True)

            # ---------
            res = node.safe_psql("select 1;", expect_error=False)
            assert (__class__.helper__rm_carriage_returns(res) == b'1\n')

    def test_transactions(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops).init().start() as node:

            with node.connect() as con:
                con.begin()
                con.execute('create table test(val int)')
                con.execute('insert into test values (1)')
                con.commit()

                con.begin()
                con.execute('insert into test values (2)')
                res = con.execute('select * from test order by val asc')
                assert (res == [(1, ), (2, )])
                con.rollback()

                con.begin()
                res = con.execute('select * from test')
                assert (res == [(1, )])
                con.rollback()

                con.begin()
                con.execute('drop table test')
                con.commit()

    def test_control_data(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:

            # node is not initialized yet
            with pytest.raises(expected_exception=ExecUtilException):
                node.get_control_data()

            node.init()
            data = node.get_control_data()

            # check returned dict
            assert data is not None
            assert (any('pg_control' in s for s in data.keys()))

    def test_backup_simple(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as master:

            # enable streaming for backups
            master.init(allow_streaming=True)

            # node must be running
            with pytest.raises(expected_exception=BackupException):
                master.backup()

            # it's time to start node
            master.start()

            # fill node with some data
            master.psql('create table test as select generate_series(1, 4) i')

            with master.backup(xlog_method='stream') as backup:
                with backup.spawn_primary().start() as slave:
                    res = slave.execute('select * from test order by i asc')
                    assert (res == [(1, ), (2, ), (3, ), (4, )])

    def test_backup_multiple(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup1, \
                    node.backup(xlog_method='fetch') as backup2:
                assert (backup1.base_dir != backup2.base_dir)

            with node.backup(xlog_method='fetch') as backup:
                with backup.spawn_primary('node1', destroy=False) as node1, \
                        backup.spawn_primary('node2', destroy=False) as node2:
                    assert (node1.base_dir != node2.base_dir)

    def test_backup_exhaust(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup:
                # exhaust backup by creating new node
                with backup.spawn_primary():
                    pass

                # now let's try to create one more node
                with pytest.raises(expected_exception=BackupException):
                    backup.spawn_primary()

    def test_backup_wrong_xlog_method(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init(allow_streaming=True).start()

            with pytest.raises(
                expected_exception=BackupException,
                match="^" + re.escape('Invalid xlog_method "wrong"') + "$"
            ):
                node.backup(xlog_method='wrong')

    def test_pg_ctl_wait_option(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        C_MAX_ATTEMPTS = 50

        node = __class__.helper__get_node(os_ops)
        assert node.status() == NodeStatus.Uninitialized
        node.init()
        assert node.status() == NodeStatus.Stopped
        node.start(wait=False)
        nAttempt = 0
        while True:
            if nAttempt == C_MAX_ATTEMPTS:
                #
                # [2025-03-11]
                #  We have an unexpected problem with this test in CI
                #  Let's get an additional information about this test failure.
                #
                logging.error("Node was not stopped.")
                if not node.os_ops.path_exists(node.pg_log_file):
                    logging.warning("Node log does not exist.")
                else:
                    logging.info("Let's read node log file [{0}]".format(node.pg_log_file))
                    logFileData = node.os_ops.read(node.pg_log_file, binary=False)
                    logging.info("Node log file content:\n{0}".format(logFileData))

                raise Exception("Could not stop node.")

            nAttempt += 1

            if nAttempt > 1:
                logging.info("Wait 1 second.")
                time.sleep(1)
                logging.info("")

            logging.info("Try to stop node. Attempt #{0}.".format(nAttempt))

            try:
                node.stop(wait=False)
                break
            except ExecUtilException as e:
                # it's ok to get this exception here since node
                # could be not started yet
                logging.info("Node is not stopped. Exception ({0}): {1}".format(type(e).__name__, e))
            continue

        logging.info("OK. Stop command was executed. Let's wait while our node will stop really.")
        nAttempt = 0
        while True:
            if nAttempt == C_MAX_ATTEMPTS:
                raise Exception("Could not stop node.")

            nAttempt += 1
            if nAttempt > 1:
                logging.info("Wait 1 second.")
                time.sleep(1)
                logging.info("")

            logging.info("Attempt #{0}.".format(nAttempt))
            s1 = node.status()

            if s1 == NodeStatus.Running:
                continue

            if s1 == NodeStatus.Stopped:
                break

            raise Exception("Unexpected node status: {0}.".format(s1))

        logging.info("OK. Node is stopped.")
        node.cleanup()

    def test_replicate(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init(allow_streaming=True).start()

            with node.replicate().start() as replica:
                res = replica.execute('select 1')
                assert (res == [(1, )])

                node.execute('create table test (val int)', commit=True)

                replica.catchup()

                res = node.execute('select * from test')
                assert (res == [])

    def test_synchronous_replication(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        current_version = get_pg_version2(os_ops)

        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, "9.6")

        with __class__.helper__get_node(os_ops) as master:
            old_version = not __class__.helper__pg_version_ge(current_version, '9.6')

            master.init(allow_streaming=True).start()

            if not old_version:
                master.append_conf('synchronous_commit = remote_apply')

            # create standby
            with master.replicate() as standby1, master.replicate() as standby2:
                standby1.start()
                standby2.start()

                # check formatting
                assert (
                    '1 ("{}", "{}")'.format(standby1.name, standby2.name) == str(First(1, (standby1, standby2)))
                )  # yapf: disable
                assert (
                    'ANY 1 ("{}", "{}")'.format(standby1.name, standby2.name) == str(Any(1, (standby1, standby2)))
                )  # yapf: disable

                # set synchronous_standby_names
                master.set_synchronous_standbys(First(2, [standby1, standby2]))
                master.restart()

                # the following part of the test is only applicable to newer
                # versions of PostgresQL
                if not old_version:
                    master.safe_psql('create table abc(a int)')

                    # Create a large transaction that will take some time to apply
                    # on standby to check that it applies synchronously
                    # (If set synchronous_commit to 'on' or other lower level then
                    # standby most likely won't catchup so fast and test will fail)
                    master.safe_psql(
                        'insert into abc select generate_series(1, 1000000)')
                    res = standby1.safe_psql('select count(*) from abc')
                    assert (__class__.helper__rm_carriage_returns(res) == b'1000000\n')

    def test_logical_replication(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        current_version = get_pg_version2(os_ops)

        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, "10")

        with __class__.helper__get_node(os_ops) as node1, __class__.helper__get_node(os_ops) as node2:
            node1.init(allow_logical=True)
            node1.start()
            node2.init().start()

            create_table = 'create table test (a int, b int)'
            node1.safe_psql(create_table)
            node2.safe_psql(create_table)

            # create publication / create subscription
            pub = node1.publish('mypub')
            sub = node2.subscribe(pub, 'mysub')

            node1.safe_psql('insert into test values (1, 1), (2, 2)')

            # wait until changes apply on subscriber and check them
            sub.catchup()
            res = node2.execute('select * from test')
            assert (res == [(1, 1), (2, 2)])

            # disable and put some new data
            sub.disable()
            node1.safe_psql('insert into test values (3, 3)')

            # enable and ensure that data successfully transferred
            sub.enable()
            sub.catchup()
            res = node2.execute('select * from test')
            assert (res == [(1, 1), (2, 2), (3, 3)])

            # Add new tables. Since we added "all tables" to publication
            # (default behaviour of publish() method) we don't need
            # to explicitly perform pub.add_tables()
            create_table = 'create table test2 (c char)'
            node1.safe_psql(create_table)
            node2.safe_psql(create_table)
            sub.refresh()

            # put new data
            node1.safe_psql('insert into test2 values (\'a\'), (\'b\')')
            sub.catchup()
            res = node2.execute('select * from test2')
            assert (res == [('a', ), ('b', )])

            # drop subscription
            sub.drop()
            pub.drop()

            # create new publication and subscription for specific table
            # (omitting copying data as it's already done)
            pub = node1.publish('newpub', tables=['test'])
            sub = node2.subscribe(pub, 'newsub', copy_data=False)

            node1.safe_psql('insert into test values (4, 4)')
            sub.catchup()
            res = node2.execute('select * from test')
            assert (res == [(1, 1), (2, 2), (3, 3), (4, 4)])

            # explicitly add table
            with pytest.raises(expected_exception=ValueError):
                pub.add_tables([])    # fail
            pub.add_tables(['test2'])
            node1.safe_psql('insert into test2 values (\'c\')')
            sub.catchup()
            res = node2.execute('select * from test2')
            assert (res == [('a', ), ('b', )])

    def test_logical_catchup(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        """ Runs catchup for 100 times to be sure that it is consistent """

        current_version = get_pg_version2(os_ops)

        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, "10")

        with __class__.helper__get_node(os_ops) as node1, __class__.helper__get_node(os_ops) as node2:
            node1.init(allow_logical=True)
            node1.start()
            node2.init().start()

            create_table = 'create table test (key int primary key, val int); '
            node1.safe_psql(create_table)
            node1.safe_psql('alter table test replica identity default')
            node2.safe_psql(create_table)

            # create publication / create subscription
            sub = node2.subscribe(node1.publish('mypub'), 'mysub')

            for i in range(0, 100):
                node1.execute('insert into test values ({0}, {0})'.format(i))
                sub.catchup()
                res = node2.execute('select * from test')
                assert (res == [(i, i, )])
                node1.execute('delete from test')

    def test_logical_replication_fail(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        current_version = get_pg_version2(os_ops)

        __class__.helper__skip_test_if_pg_version_is_ge(current_version, "10")

        with __class__.helper__get_node(os_ops) as node:
            with pytest.raises(expected_exception=InitNodeException):
                node.init(allow_logical=True)

    def test_replication_slots(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init(allow_streaming=True).start()

            with node.replicate(slot='slot1').start() as replica:
                replica.execute('select 1')

                # cannot create new slot with the same name
                with pytest.raises(expected_exception=TestgresException):
                    node.replicate(slot='slot1')

    def test_incorrect_catchup(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as node:
            node.init(allow_streaming=True).start()

            # node has no master, can't catch up
            with pytest.raises(expected_exception=TestgresException):
                node.catchup()

    def test_promotion(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        with __class__.helper__get_node(os_ops) as master:
            master.init().start()
            master.safe_psql('create table abc(id serial)')

            with master.replicate().start() as replica:
                master.stop()
                replica.promote()

                # make standby becomes writable master
                replica.safe_psql('insert into abc values (1)')
                res = replica.safe_psql('select * from abc')
                assert (__class__.helper__rm_carriage_returns(res) == b'1\n')

    def test_dump(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)
        query_create = 'create table test as select generate_series(1, 2) as val'
        query_select = 'select * from test order by val asc'

        with __class__.helper__get_node(os_ops).init().start() as node1:

            node1.execute(query_create)
            for format in ['plain', 'custom', 'directory', 'tar']:
                with removing(os_ops, node1.dump(format=format)) as dump:
                    with __class__.helper__get_node(os_ops).init().start() as node3:
                        if format == 'directory':
                            assert (os.path.isdir(dump))
                        else:
                            assert (os.path.isfile(dump))
                        # restore dump
                        node3.restore(filename=dump)
                        res = node3.execute(query_select)
                        assert (res == [(1, ), (2, )])

    @staticmethod
    def helper__get_node(os_ops: OsOperations, name=None):
        assert isinstance(os_ops, OsOperations)
        return PostgresNode(name, conn_params=None, os_ops=os_ops)

    @staticmethod
    def helper__skip_test_if_pg_version_is_not_ge(ver1: str, ver2: str):
        assert type(ver1) == str  # noqa: E721
        assert type(ver2) == str  # noqa: E721
        if not __class__.helper__pg_version_ge(ver1, ver2):
            pytest.skip('requires {0}+'.format(ver2))

    @staticmethod
    def helper__skip_test_if_pg_version_is_ge(ver1: str, ver2: str):
        assert type(ver1) == str  # noqa: E721
        assert type(ver2) == str  # noqa: E721
        if __class__.helper__pg_version_ge(ver1, ver2):
            pytest.skip('requires <{0}'.format(ver2))

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
