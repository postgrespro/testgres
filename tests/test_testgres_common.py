from __future__ import annotations

from .helpers.global_data import PostgresNodeService
from .helpers.global_data import PostgresNodeServices
from .helpers.global_data import OsOperations
from .helpers.global_data import PortManager

from src.node import PgVer
from src.node import PostgresNode
from src.node import PostgresNodeLogReader
from src.node import PostgresNodeUtils
from src.utils import get_pg_version2
from src.utils import file_tail
from src.utils import get_bin_path2
from src import ProcessType
from src import NodeStatus
from src import IsolationLevel
from src import NodeApp

# New name prevents to collect test-functions in TestgresException and fixes
# the problem with pytest warning.
from src import TestgresException as testgres_TestgresException

from src import InitNodeException
from src import StartNodeException
from src import QueryException
from src import ExecUtilException
from src import TimeoutException
from src import InvalidOperationException
from src import BackupException
from src import ProgrammingError
from src import scoped_config
from src import First, Any

from contextlib import contextmanager

import pytest
import six
import logging
import time
import tempfile
import uuid
import os
import re
import subprocess
import typing


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
    sm_node_svcs: typing.List[PostgresNodeService] = [
        PostgresNodeServices.sm_local,
        PostgresNodeServices.sm_local2,
        PostgresNodeServices.sm_remote,
    ]

    @pytest.fixture(
        params=sm_node_svcs,
        ids=[descr.sign for descr in sm_node_svcs]
    )
    def node_svc(self, request: pytest.FixtureRequest) -> PostgresNodeService:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, PostgresNodeService)
        assert isinstance(request.param.os_ops, OsOperations)
        assert isinstance(request.param.port_manager, PortManager)
        return request.param

    def test_version_management(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

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

        version = get_pg_version2(node_svc.os_ops)

        with __class__.helper__get_node(node_svc) as node:
            assert (isinstance(version, six.string_types))
            assert (isinstance(node.version, PgVer))
            assert (node.version == PgVer(version))

    def test_node_repr(self, node_svc: PostgresNodeService):
        with __class__.helper__get_node(node_svc).init() as node:
            pattern = r"PostgresNode\(name='.+', port=.+, base_dir='.+'\)"
            assert re.match(pattern, str(node)) is not None

    def test_custom_init(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
            # enable page checksums
            node.init(initdb_params=['-k']).start()

        with __class__.helper__get_node(node_svc) as node:
            node.init(
                allow_streaming=True,
                initdb_params=['--auth-local=reject', '--auth-host=reject'])

            hba_file = os.path.join(node.data_dir, 'pg_hba.conf')
            lines = node.os_ops.readlines(hba_file)

            # check number of lines
            assert (len(lines) >= 6)

            # there should be no trust entries at all
            assert not (any('trust' in s for s in lines))

    def test_double_init(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc).init() as node:
            # can't initialize node more than once
            with pytest.raises(expected_exception=InitNodeException):
                node.init()

    def test_init_after_cleanup(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
            node.init().start().execute('select 1')
            node.cleanup()
            node.init().start().execute('select 1')

    def test_init_unique_system_id(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        # this function exists in PostgreSQL 9.6+
        current_version = get_pg_version2(node_svc.os_ops)

        __class__.helper__skip_test_if_util_not_exist(node_svc.os_ops, "pg_resetwal")
        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, '9.6')

        query = 'select system_identifier from pg_control_system()'

        with scoped_config(cache_initdb=False):
            with __class__.helper__get_node(node_svc).init().start() as node0:
                id0 = node0.execute(query)[0]

        with scoped_config(cache_initdb=True,
                           cached_initdb_unique=True) as config:
            assert (config.cache_initdb)
            assert (config.cached_initdb_unique)

            # spawn two nodes; ids must be different
            with __class__.helper__get_node(node_svc).init().start() as node1, \
                    __class__.helper__get_node(node_svc).init().start() as node2:
                id1 = node1.execute(query)[0]
                id2 = node2.execute(query)[0]

                # ids must increase
                assert (id1 > id0)
                assert (id2 > id1)

    def test_node_exit(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with pytest.raises(expected_exception=QueryException):
            with __class__.helper__get_node(node_svc).init() as node:
                base_dir = node.base_dir
                node.safe_psql('select 1')

        # we should save the DB for "debugging"
        assert (node_svc.os_ops.path_exists(base_dir))
        node_svc.os_ops.rmdirs(base_dir, ignore_errors=True)

        with __class__.helper__get_node(node_svc).init() as node:
            base_dir = node.base_dir

        # should have been removed by default
        assert not (node_svc.os_ops.path_exists(base_dir))

    def test_double_start(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc).init().start() as node:
            # can't start node more than once
            node.start()
            assert (node.is_started)

    def test_uninitialized_start(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
            # node is not initialized yet
            with pytest.raises(expected_exception=StartNodeException):
                node.start()

    def test_restart(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
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

    def test_reload(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
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

    def test_pg_ctl(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
            node.init().start()

            status = node.pg_ctl(['status'])
            assert ('PID' in status)

    def test_status(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        assert (NodeStatus.Running)
        assert not (NodeStatus.Stopped)
        assert not (NodeStatus.Uninitialized)

        # check statuses after each operation
        with __class__.helper__get_node(node_svc) as node:
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

    def test_child_pids(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        master_processes = [
            ProcessType.AutovacuumLauncher,
            ProcessType.BackgroundWriter,
            ProcessType.Checkpointer,
            ProcessType.StatsCollector,
            ProcessType.WalSender,
            ProcessType.WalWriter,
        ]

        postgresVersion = get_pg_version2(node_svc.os_ops)

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
            expectedTypes: typing.List[ProcessType]
        ) -> typing.List[ProcessType]:
            # returns list of the absence processes
            assert node is not None
            assert type(node) == PostgresNode  # noqa: E721
            assert expectedTypes is not None
            assert type(expectedTypes) == list  # noqa: E721

            pids = node.auxiliary_pids
            assert pids is not None  # noqa: E721
            assert type(pids) == dict  # noqa: E721

            result: typing.List[ProcessType] = list()
            for ptype in expectedTypes:
                if not (ptype in pids):
                    result.append(ptype)
            return result

        def LOCAL__check_auxiliary_pids__multiple_attempts(
                node: PostgresNode,
                expectedTypes: typing.List[ProcessType]):
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

        with __class__.helper__get_node(node_svc).init().start() as master:

            # master node doesn't have a source walsender!
            with pytest.raises(expected_exception=testgres_TestgresException):
                master.source_walsender

            with master.connect() as con:
                assert (con.pid > 0)

            with master.replicate().start() as replica:
                assert type(replica) == PostgresNode  # noqa: E721

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
                with pytest.raises(expected_exception=testgres_TestgresException):
                    replica.source_walsender

    def test_exceptions(self):
        str(StartNodeException('msg', [('file', 'lines')]))
        str(ExecUtilException('msg', 'cmd', 1, 'out'))
        str(QueryException('msg', 'query'))

    def test_auto_name(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc).init(allow_streaming=True).start() as m:
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

    def test_isolation_levels(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init().start() as node:
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

    def test_users(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init().start() as node:
            node.psql('create role test_user login')
            value = node.safe_psql('select 1', username='test_user')
            value = __class__.helper__rm_carriage_returns(value)
            assert (value == b'1\n')

    def test_poll_query_until(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
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

    def test_logging(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
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
                    with __class__.helper__get_node(node_svc, name=C_NODE_NAME) as master:
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
                with logging._lock:
                    assert logging.Logger.manager is not None
                    assert C_NODE_NAME in logging.Logger.manager.loggerDict.keys()
                    logging.Logger.manager.loggerDict.pop(C_NODE_NAME, None)
                    assert not (C_NODE_NAME in logging.Logger.manager.loggerDict.keys())
                    assert not (handler in logging._handlers.values())
        # GO HOME!
        return

    def test_psql(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init().start() as node:

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
                r = node.psql()  # raises!
                logging.error("node.psql returns [{}]".format(r))

            node.stop()

            # check psql on stopped node, fails
            with pytest.raises(expected_exception=QueryException):
                # [2025-04-03] This call does not raise exception! I do not know why.
                r = node.safe_psql('select 1')  # raises!
                logging.error("node.safe_psql returns [{}]".format(r))

    def test_psql__another_port(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init() as node1:
            with __class__.helper__get_node(node_svc).init() as node2:
                node1.start()
                node2.start()
                assert node1.port != node2.port
                assert node1.host == node2.host

                node1.stop()

                logging.info("test table in node2 is creating ...")
                node2.safe_psql(
                    dbname="postgres",
                    query="create table test (id integer);"
                )

                logging.info("try to find test table through node1.psql ...")
                res = node1.psql(
                    dbname="postgres",
                    query="select count(*) from pg_class where relname='test'",
                    host=node2.host,
                    port=node2.port,
                )
                assert (__class__.helper__rm_carriage_returns(res) == (0, b'1\n', b''))

    def test_psql__another_bad_host(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init() as node:
            logging.info("try to execute node1.psql ...")
            res = node.psql(
                dbname="postgres",
                query="select count(*) from pg_class where relname='test'",
                host="DUMMY_HOST_NAME",
                port=node.port,
            )

            res2 = __class__.helper__rm_carriage_returns(res)

            assert res2[0] != 0
            assert b"DUMMY_HOST_NAME" in res[2]

    def test_safe_psql__another_port(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init() as node1:
            with __class__.helper__get_node(node_svc).init() as node2:
                node1.start()
                node2.start()
                assert node1.port != node2.port
                assert node1.host == node2.host

                node1.stop()

                logging.info("test table in node2 is creating ...")
                node2.safe_psql(
                    dbname="postgres",
                    query="create table test (id integer);"
                )

                logging.info("try to find test table through node1.psql ...")
                res = node1.safe_psql(
                    dbname="postgres",
                    query="select count(*) from pg_class where relname='test'",
                    host=node2.host,
                    port=node2.port,
                )
                assert (__class__.helper__rm_carriage_returns(res) == b'1\n')

    def test_safe_psql__another_bad_host(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init() as node:
            logging.info("try to execute node1.psql ...")

            with pytest.raises(expected_exception=Exception) as x:
                node.safe_psql(
                    dbname="postgres",
                    query="select count(*) from pg_class where relname='test'",
                    host="DUMMY_HOST_NAME",
                    port=node.port,
                )

            assert "DUMMY_HOST_NAME" in str(x.value)

    def test_safe_psql__expect_error(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init().start() as node:
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

    def test_transactions(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc).init().start() as node:

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

    def test_control_data(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:

            # node is not initialized yet
            with pytest.raises(expected_exception=ExecUtilException):
                node.get_control_data()

            node.init()
            data = node.get_control_data()

            # check returned dict
            assert data is not None
            assert (any('pg_control' in s for s in data.keys()))

    def test_backup_simple(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as master:

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

    def test_backup_multiple(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup1, \
                    node.backup(xlog_method='fetch') as backup2:
                assert (backup1.base_dir != backup2.base_dir)

            with node.backup(xlog_method='fetch') as backup:
                with backup.spawn_primary('node1', destroy=False) as node1, \
                        backup.spawn_primary('node2', destroy=False) as node2:
                    assert (node1.base_dir != node2.base_dir)

    def test_backup_exhaust(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup:
                # exhaust backup by creating new node
                with backup.spawn_primary():
                    pass

                # now let's try to create one more node
                with pytest.raises(expected_exception=BackupException):
                    backup.spawn_primary()

    def test_backup_wrong_xlog_method(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
            node.init(allow_streaming=True).start()

            with pytest.raises(
                expected_exception=BackupException,
                match="^" + re.escape('Invalid xlog_method "wrong"') + "$"
            ):
                node.backup(xlog_method='wrong')

    def test_pg_ctl_wait_option(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        C_MAX_ATTEMPT = 5

        nAttempt = 0

        while True:
            if nAttempt == C_MAX_ATTEMPT:
                raise Exception("PostgresSQL did not start.")

            nAttempt += 1
            logging.info("------------------------ NODE #{}".format(
                nAttempt
            ))

            with __class__.helper__get_node(node_svc, port=12345) as node:
                if self.impl__test_pg_ctl_wait_option(node_svc, node):
                    break
            continue

        logging.info("OK. Test is passed. Number of attempts is {}".format(
            nAttempt
        ))
        return

    def impl__test_pg_ctl_wait_option(
        self,
        node_svc: PostgresNodeService,
        node: PostgresNode
    ) -> None:
        assert isinstance(node_svc, PostgresNodeService)
        assert isinstance(node, PostgresNode)
        assert node.status() == NodeStatus.Uninitialized

        C_MAX_ATTEMPTS = 50

        node.init()
        assert node.status() == NodeStatus.Stopped

        node_log_reader = PostgresNodeLogReader(node, from_beginnig=True)

        node.start(wait=False)
        nAttempt = 0
        while True:
            if PostgresNodeUtils.delect_port_conflict(node_log_reader):
                logging.info("Node port {} conflicted with another PostgreSQL instance.".format(
                    node.port
                ))
                return False

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
        return True

    def test_replicate(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
            node.init(allow_streaming=True).start()

            with node.replicate().start() as replica:
                res = replica.execute('select 1')
                assert (res == [(1, )])

                node.execute('create table test (val int)', commit=True)

                replica.catchup()

                res = node.execute('select * from test')
                assert (res == [])

    def test_synchronous_replication(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        current_version = get_pg_version2(node_svc.os_ops)

        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, "9.6")

        with __class__.helper__get_node(node_svc) as master:
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

    def test_logical_replication(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        current_version = get_pg_version2(node_svc.os_ops)

        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, "10")

        with __class__.helper__get_node(node_svc) as node1, __class__.helper__get_node(node_svc) as node2:
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

    def test_logical_catchup(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        """ Runs catchup for 100 times to be sure that it is consistent """

        current_version = get_pg_version2(node_svc.os_ops)

        __class__.helper__skip_test_if_pg_version_is_not_ge(current_version, "10")

        with __class__.helper__get_node(node_svc) as node1, __class__.helper__get_node(node_svc) as node2:
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

    def test_logical_replication_fail(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        current_version = get_pg_version2(node_svc.os_ops)

        __class__.helper__skip_test_if_pg_version_is_ge(current_version, "10")

        with __class__.helper__get_node(node_svc) as node:
            with pytest.raises(expected_exception=InitNodeException):
                node.init(allow_logical=True)

    def test_replication_slots(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
            node.init(allow_streaming=True).start()

            with node.replicate(slot='slot1').start() as replica:
                replica.execute('select 1')

                # cannot create new slot with the same name
                with pytest.raises(expected_exception=testgres_TestgresException):
                    node.replicate(slot='slot1')

    def test_incorrect_catchup(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as node:
            node.init(allow_streaming=True).start()

            # node has no master, can't catch up
            with pytest.raises(expected_exception=testgres_TestgresException):
                node.catchup()

    def test_promotion(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        with __class__.helper__get_node(node_svc) as master:
            master.init().start()
            master.safe_psql('create table abc(id serial)')

            with master.replicate().start() as replica:
                master.stop()
                replica.promote()

                # make standby becomes writable master
                replica.safe_psql('insert into abc values (1)')
                res = replica.safe_psql('select * from abc')
                assert (__class__.helper__rm_carriage_returns(res) == b'1\n')

    def test_dump(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)
        query_create = 'create table test as select generate_series(1, 2) as val'
        query_select = 'select * from test order by val asc'

        with __class__.helper__get_node(node_svc).init().start() as node1:

            node1.execute(query_create)
            for format in ['plain', 'custom', 'directory', 'tar']:
                with removing(node_svc.os_ops, node1.dump(format=format)) as dump:
                    with __class__.helper__get_node(node_svc).init().start() as node3:
                        if format == 'directory':
                            assert (os.path.isdir(dump))
                        else:
                            assert (os.path.isfile(dump))
                        # restore dump
                        node3.restore(filename=dump)
                        res = node3.execute(query_select)
                        assert (res == [(1, ), (2, )])

    def test_pgbench(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        __class__.helper__skip_test_if_util_not_exist(node_svc.os_ops, "pgbench")

        with __class__.helper__get_node(node_svc).init().start() as node:
            # initialize pgbench DB and run benchmarks
            node.pgbench_init(
                scale=2,
                foreign_keys=True,
                options=['-q']
            ).pgbench_run(time=2)

            # run TPC-B benchmark
            proc = node.pgbench(stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                options=['-T3'])
            out = proc.communicate()[0]
            assert (b'tps = ' in out)

    def test_unix_sockets(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
            node.init(unix_sockets=False, allow_streaming=True)
            node.start()

            res_exec = node.execute('select 1')
            assert (res_exec == [(1,)])
            res_psql = node.safe_psql('select 1')
            assert (res_psql == b'1\n')

            with node.replicate() as r:
                assert type(r) == PostgresNode  # noqa: E721
                r.start()
                res_exec = r.execute('select 1')
                assert (res_exec == [(1,)])
                res_psql = r.safe_psql('select 1')
                assert (res_psql == b'1\n')

    def test_the_same_port(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        with __class__.helper__get_node(node_svc) as node:
            node.init().start()
            assert (node._should_free_port)
            assert (type(node.port) == int)  # noqa: E721
            node_port_copy = node.port
            r = node.safe_psql("SELECT 1;")
            assert (__class__.helper__rm_carriage_returns(r) == b'1\n')

            with __class__.helper__get_node(node_svc, port=node.port) as node2:
                assert (type(node2.port) == int)  # noqa: E721
                assert (node2.port == node.port)
                assert not (node2._should_free_port)

                with pytest.raises(
                    expected_exception=StartNodeException,
                    match=re.escape("Cannot start node")
                ):
                    node2.init().start()

            # node is still working
            assert (node.port == node_port_copy)
            assert (node._should_free_port)
            r = node.safe_psql("SELECT 3;")
            assert (__class__.helper__rm_carriage_returns(r) == b'3\n')

    class tagPortManagerProxy(PortManager):
        m_PrevPortManager: PortManager

        m_DummyPortNumber: int
        m_DummyPortMaxUsage: int

        m_DummyPortCurrentUsage: int
        m_DummyPortTotalUsage: int

        def __init__(self, prevPortManager: PortManager, dummyPortNumber: int, dummyPortMaxUsage: int):
            assert isinstance(prevPortManager, PortManager)
            assert type(dummyPortNumber) == int  # noqa: E721
            assert type(dummyPortMaxUsage) == int  # noqa: E721
            assert dummyPortNumber >= 0
            assert dummyPortMaxUsage >= 0

            super().__init__()

            self.m_PrevPortManager = prevPortManager

            self.m_DummyPortNumber = dummyPortNumber
            self.m_DummyPortMaxUsage = dummyPortMaxUsage

            self.m_DummyPortCurrentUsage = 0
            self.m_DummyPortTotalUsage = 0

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            assert self.m_DummyPortCurrentUsage == 0

            assert self.m_PrevPortManager is not None

        def reserve_port(self) -> int:
            assert type(self.m_DummyPortMaxUsage) == int  # noqa: E721
            assert type(self.m_DummyPortTotalUsage) == int  # noqa: E721
            assert type(self.m_DummyPortCurrentUsage) == int  # noqa: E721
            assert self.m_DummyPortTotalUsage >= 0
            assert self.m_DummyPortCurrentUsage >= 0

            assert self.m_DummyPortTotalUsage <= self.m_DummyPortMaxUsage
            assert self.m_DummyPortCurrentUsage <= self.m_DummyPortTotalUsage

            assert self.m_PrevPortManager is not None
            assert isinstance(self.m_PrevPortManager, PortManager)

            if self.m_DummyPortTotalUsage == self.m_DummyPortMaxUsage:
                return self.m_PrevPortManager.reserve_port()

            self.m_DummyPortTotalUsage += 1
            self.m_DummyPortCurrentUsage += 1
            return self.m_DummyPortNumber

        def release_port(self, dummyPortNumber: int):
            assert type(dummyPortNumber) == int  # noqa: E721

            assert type(self.m_DummyPortMaxUsage) == int  # noqa: E721
            assert type(self.m_DummyPortTotalUsage) == int  # noqa: E721
            assert type(self.m_DummyPortCurrentUsage) == int  # noqa: E721
            assert self.m_DummyPortTotalUsage >= 0
            assert self.m_DummyPortCurrentUsage >= 0

            assert self.m_DummyPortTotalUsage <= self.m_DummyPortMaxUsage
            assert self.m_DummyPortCurrentUsage <= self.m_DummyPortTotalUsage

            assert self.m_PrevPortManager is not None
            assert isinstance(self.m_PrevPortManager, PortManager)

            if self.m_DummyPortCurrentUsage > 0 and dummyPortNumber == self.m_DummyPortNumber:
                assert self.m_DummyPortTotalUsage > 0
                self.m_DummyPortCurrentUsage -= 1
                return

            return self.m_PrevPortManager.release_port(dummyPortNumber)

    def test_port_rereserve_during_node_start(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721
        assert PostgresNode._C_MAX_START_ATEMPTS == 5

        C_COUNT_OF_BAD_PORT_USAGE = 3

        with __class__.helper__get_node(node_svc) as node1:
            node1.init().start()
            assert node1._should_free_port
            assert type(node1.port) == int  # noqa: E721
            node1_port_copy = node1.port
            assert __class__.helper__rm_carriage_returns(node1.safe_psql("SELECT 1;")) == b'1\n'

            with __class__.tagPortManagerProxy(node_svc.port_manager, node1.port, C_COUNT_OF_BAD_PORT_USAGE) as proxy:
                assert proxy.m_DummyPortNumber == node1.port
                with __class__.helper__get_node(node_svc, port_manager=proxy) as node2:
                    assert node2._should_free_port
                    assert node2.port == node1.port

                    node2.init().start()

                    assert node2.port != node1.port
                    assert node2._should_free_port
                    assert proxy.m_DummyPortCurrentUsage == 0
                    assert proxy.m_DummyPortTotalUsage == C_COUNT_OF_BAD_PORT_USAGE
                    assert node2.is_started
                    r = node2.safe_psql("SELECT 2;")
                    assert __class__.helper__rm_carriage_returns(r) == b'2\n'

            # node1 is still working
            assert node1.port == node1_port_copy
            assert node1._should_free_port
            r = node1.safe_psql("SELECT 3;")
            assert __class__.helper__rm_carriage_returns(r) == b'3\n'

    def test_port_conflict(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721
        assert PostgresNode._C_MAX_START_ATEMPTS > 1

        C_COUNT_OF_BAD_PORT_USAGE = PostgresNode._C_MAX_START_ATEMPTS

        with __class__.helper__get_node(node_svc) as node1:
            node1.init().start()
            assert node1._should_free_port
            assert type(node1.port) == int  # noqa: E721
            node1_port_copy = node1.port
            assert __class__.helper__rm_carriage_returns(node1.safe_psql("SELECT 1;")) == b'1\n'

            with __class__.tagPortManagerProxy(node_svc.port_manager, node1.port, C_COUNT_OF_BAD_PORT_USAGE) as proxy:
                assert proxy.m_DummyPortNumber == node1.port
                with __class__.helper__get_node(node_svc, port_manager=proxy) as node2:
                    assert node2._should_free_port
                    assert node2.port == node1.port

                    with pytest.raises(
                        expected_exception=StartNodeException,
                        match=re.escape("Cannot start node after multiple attempts.")
                    ):
                        node2.init().start()

                    assert node2.port == node1.port
                    assert node2._should_free_port
                    assert proxy.m_DummyPortCurrentUsage == 1
                    assert proxy.m_DummyPortTotalUsage == C_COUNT_OF_BAD_PORT_USAGE
                    assert not node2.is_started

                # node2 must release our dummyPort (node1.port)
                assert (proxy.m_DummyPortCurrentUsage == 0)

            # node1 is still working
            assert node1.port == node1_port_copy
            assert node1._should_free_port
            r = node1.safe_psql("SELECT 3;")
            assert __class__.helper__rm_carriage_returns(r) == b'3\n'

    def test_try_to_get_port_after_free_manual_port(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        with __class__.helper__get_node(node_svc) as node1:
            assert node1 is not None
            assert type(node1) == PostgresNode  # noqa: E721
            assert node1.port is not None
            assert type(node1.port) == int  # noqa: E721
            with __class__.helper__get_node(node_svc, port=node1.port, port_manager=None) as node2:
                assert node2 is not None
                assert type(node1) == PostgresNode  # noqa: E721
                assert node2 is not node1
                assert node2.port is not None
                assert type(node2.port) == int  # noqa: E721
                assert node2.port == node1.port

                logging.info("Release node2 port")
                node2.free_port()

                logging.info("try to get node2.port...")
                with pytest.raises(
                    InvalidOperationException,
                    match="^" + re.escape("PostgresNode port is not defined.") + "$"
                ):
                    p = node2.port
                    assert p is None

    def test_try_to_start_node_after_free_manual_port(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        with __class__.helper__get_node(node_svc) as node1:
            assert node1 is not None
            assert type(node1) == PostgresNode  # noqa: E721
            assert node1.port is not None
            assert type(node1.port) == int  # noqa: E721
            with __class__.helper__get_node(node_svc, port=node1.port, port_manager=None) as node2:
                assert node2 is not None
                assert type(node1) == PostgresNode  # noqa: E721
                assert node2 is not node1
                assert node2.port is not None
                assert type(node2.port) == int  # noqa: E721
                assert node2.port == node1.port

                logging.info("Release node2 port")
                node2.free_port()

                logging.info("node2 is trying to start...")
                with pytest.raises(
                    InvalidOperationException,
                    match="^" + re.escape("Can't start PostgresNode. Port is not defined.") + "$"
                ):
                    node2.start()

    def test_node__os_ops(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert node_svc.os_ops is not None
        assert isinstance(node_svc.os_ops, OsOperations)

        with PostgresNode(name="node", os_ops=node_svc.os_ops, port_manager=node_svc.port_manager) as node:
            # retest
            assert node_svc.os_ops is not None
            assert isinstance(node_svc.os_ops, OsOperations)

            assert node.os_ops is node_svc.os_ops
            # one more time
            assert node.os_ops is node_svc.os_ops

    def test_node__port_manager(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        with PostgresNode(name="node", os_ops=node_svc.os_ops, port_manager=node_svc.port_manager) as node:
            # retest
            assert node_svc.port_manager is not None
            assert isinstance(node_svc.port_manager, PortManager)

            assert node.port_manager is node_svc.port_manager
            # one more time
            assert node.port_manager is node_svc.port_manager

    def test_node__port_manager_and_explicit_port(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        port = node_svc.port_manager.reserve_port()
        assert type(port) == int  # noqa: E721

        try:
            with PostgresNode(name="node", port=port, os_ops=node_svc.os_ops) as node:
                # retest
                assert isinstance(node_svc.os_ops, OsOperations)
                assert node_svc.port_manager is not None
                assert isinstance(node_svc.port_manager, PortManager)

                assert node.port_manager is None
                assert node.os_ops is node_svc.os_ops

                # one more time
                assert node.port_manager is None
                assert node.os_ops is node_svc.os_ops
        finally:
            node_svc.port_manager.release_port(port)

    def test_node__no_port_manager(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        port = node_svc.port_manager.reserve_port()
        assert type(port) == int  # noqa: E721

        try:
            with PostgresNode(name="node", port=port, os_ops=node_svc.os_ops, port_manager=None) as node:
                # retest
                assert isinstance(node_svc.os_ops, OsOperations)
                assert node_svc.port_manager is not None
                assert isinstance(node_svc.port_manager, PortManager)

                assert node.port_manager is None
                assert node.os_ops is node_svc.os_ops

                # one more time
                assert node.port_manager is None
                assert node.os_ops is node_svc.os_ops
        finally:
            node_svc.port_manager.release_port(port)

    class tag_rmdirs_protector:
        _os_ops: OsOperations
        _cwd: str
        _old_rmdirs: any
        _cwd: str

        def __init__(self, os_ops: OsOperations):
            self._os_ops = os_ops
            self._cwd = os.path.abspath(os_ops.cwd())
            self._old_rmdirs = os_ops.rmdirs

        def __enter__(self):
            assert self._os_ops.rmdirs == self._old_rmdirs
            self._os_ops.rmdirs = self.proxy__rmdirs
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            assert self._os_ops.rmdirs == self.proxy__rmdirs
            self._os_ops.rmdirs = self._old_rmdirs
            return False

        def proxy__rmdirs(self, path, ignore_errors=True):
            raise Exception("Call of rmdirs is not expected!")

    def test_node_app__make_empty__base_dir_is_None(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        tmp_dir = node_svc.os_ops.mkdtemp()
        assert tmp_dir is not None
        assert type(tmp_dir) == str  # noqa: E721
        logging.info("temp directory is [{}]".format(tmp_dir))

        # -----------
        os_ops = node_svc.os_ops.create_clone()
        assert os_ops is not node_svc.os_ops

        # -----------
        with __class__.tag_rmdirs_protector(os_ops):
            node_app = NodeApp(test_path=tmp_dir, os_ops=os_ops)
            assert node_app.os_ops is os_ops

            with pytest.raises(expected_exception=BaseException) as x:
                node_app.make_empty(base_dir=None)

            if type(x.value) == AssertionError:  # noqa: E721
                pass
            else:
                assert type(x.value) == ValueError  # noqa: E721
                assert str(x.value) == "Argument 'base_dir' is not defined."

        # -----------
        logging.info("temp directory [{}] is deleting".format(tmp_dir))
        node_svc.os_ops.rmdir(tmp_dir)

    def test_node_app__make_empty__base_dir_is_Empty(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        tmp_dir = node_svc.os_ops.mkdtemp()
        assert tmp_dir is not None
        assert type(tmp_dir) == str  # noqa: E721
        logging.info("temp directory is [{}]".format(tmp_dir))

        # -----------
        os_ops = node_svc.os_ops.create_clone()
        assert os_ops is not node_svc.os_ops

        # -----------
        with __class__.tag_rmdirs_protector(os_ops):
            node_app = NodeApp(test_path=tmp_dir, os_ops=os_ops)
            assert node_app.os_ops is os_ops

            with pytest.raises(expected_exception=ValueError) as x:
                node_app.make_empty(base_dir="")

            assert str(x.value) == "Argument 'base_dir' is empty."

        # -----------
        logging.info("temp directory [{}] is deleting".format(tmp_dir))
        node_svc.os_ops.rmdir(tmp_dir)

    def test_node_app__make_empty(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        tmp_dir = node_svc.os_ops.mkdtemp()
        assert tmp_dir is not None
        assert type(tmp_dir) == str  # noqa: E721
        logging.info("temp directory is [{}]".format(tmp_dir))

        # -----------
        node_app = NodeApp(
            test_path=tmp_dir,
            os_ops=node_svc.os_ops,
            port_manager=node_svc.port_manager
        )

        assert node_app.os_ops is node_svc.os_ops
        assert node_app.port_manager is node_svc.port_manager
        assert type(node_app.nodes_to_cleanup) == list  # noqa: E721
        assert len(node_app.nodes_to_cleanup) == 0

        node: PostgresNode = None
        try:
            node = node_app.make_simple("node")
            assert node is not None
            assert isinstance(node, PostgresNode)
            assert node.os_ops is node_svc.os_ops
            assert node.port_manager is node_svc.port_manager

            assert type(node_app.nodes_to_cleanup) == list  # noqa: E721
            assert len(node_app.nodes_to_cleanup) == 1
            assert node_app.nodes_to_cleanup[0] is node

            node.slow_start()
        finally:
            if node is not None:
                node.stop()
                node.release_resources()

        node.cleanup(release_resources=True)

        # -----------
        logging.info("temp directory [{}] is deleting".format(tmp_dir))
        node_svc.os_ops.rmdir(tmp_dir)

    def test_node_app__make_simple__checksum(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        tmp_dir = node_svc.os_ops.mkdtemp()
        assert tmp_dir is not None
        assert type(tmp_dir) == str  # noqa: E721

        logging.info("temp directory is [{}]".format(tmp_dir))
        node_app = NodeApp(test_path=tmp_dir, os_ops=node_svc.os_ops)

        C_NODE = "node"

        # -----------
        def LOCAL__test(checksum: bool, initdb_params: typing.Optional[list]):
            initdb_params0 = initdb_params
            initdb_params0_copy = initdb_params0.copy() if initdb_params0 is not None else None

            with node_app.make_simple(C_NODE, checksum=checksum, initdb_params=initdb_params):
                assert initdb_params is initdb_params0
                if initdb_params0 is not None:
                    assert initdb_params0 == initdb_params0_copy

            assert initdb_params is initdb_params0
            if initdb_params0 is not None:
                assert initdb_params0 == initdb_params0_copy

        # -----------
        LOCAL__test(checksum=False, initdb_params=None)
        LOCAL__test(checksum=True, initdb_params=None)

        # -----------
        params = []
        LOCAL__test(checksum=False, initdb_params=params)
        LOCAL__test(checksum=True, initdb_params=params)

        # -----------
        params = ["--no-sync"]
        LOCAL__test(checksum=False, initdb_params=params)
        LOCAL__test(checksum=True, initdb_params=params)

        # -----------
        params = ["--data-checksums"]
        LOCAL__test(checksum=False, initdb_params=params)
        LOCAL__test(checksum=True, initdb_params=params)

        # -----------
        logging.info("temp directory [{}] is deleting".format(tmp_dir))
        node_svc.os_ops.rmdir(tmp_dir)

    def test_node_app__make_empty_with_explicit_port(self, node_svc: PostgresNodeService):
        assert type(node_svc) == PostgresNodeService  # noqa: E721

        assert isinstance(node_svc.os_ops, OsOperations)
        assert node_svc.port_manager is not None
        assert isinstance(node_svc.port_manager, PortManager)

        tmp_dir = node_svc.os_ops.mkdtemp()
        assert tmp_dir is not None
        assert type(tmp_dir) == str  # noqa: E721
        logging.info("temp directory is [{}]".format(tmp_dir))

        # -----------
        node_app = NodeApp(
            test_path=tmp_dir,
            os_ops=node_svc.os_ops,
            port_manager=node_svc.port_manager
        )

        assert node_app.os_ops is node_svc.os_ops
        assert node_app.port_manager is node_svc.port_manager
        assert type(node_app.nodes_to_cleanup) == list  # noqa: E721
        assert len(node_app.nodes_to_cleanup) == 0

        port = node_app.port_manager.reserve_port()
        assert type(port) == int  # noqa: E721

        node: PostgresNode = None
        try:
            node = node_app.make_simple("node", port=port)
            assert node is not None
            assert isinstance(node, PostgresNode)
            assert node.os_ops is node_svc.os_ops
            assert node.port_manager is None  # <---------
            assert node.port == port
            assert node._should_free_port == False  # noqa: E712

            assert type(node_app.nodes_to_cleanup) == list  # noqa: E721
            assert len(node_app.nodes_to_cleanup) == 1
            assert node_app.nodes_to_cleanup[0] is node

            node.slow_start()
        finally:
            if node is not None:
                node.stop()
                node.free_port()

        assert node._port is None
        assert not node._should_free_port

        node.cleanup(release_resources=True)

        # -----------
        logging.info("temp directory [{}] is deleting".format(tmp_dir))
        node_svc.os_ops.rmdir(tmp_dir)

    @staticmethod
    def helper__get_node(
        node_svc: PostgresNodeService,
        name: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        port_manager: typing.Optional[PortManager] = None
    ) -> PostgresNode:
        assert isinstance(node_svc, PostgresNodeService)
        assert isinstance(node_svc.os_ops, OsOperations)
        assert isinstance(node_svc.port_manager, PortManager)

        if port_manager is None:
            port_manager = node_svc.port_manager

        return PostgresNode(
            name,
            port=port,
            os_ops=node_svc.os_ops,
            port_manager=port_manager if port is None else None
        )

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
