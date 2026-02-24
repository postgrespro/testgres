from __future__ import annotations

from tests.helpers.global_data import PostgresNodeService
from tests.helpers.global_data import PostgresNodeServices
from tests.helpers.global_data import OsOperations
from tests.helpers.global_data import PortManager
from tests.helpers.utils import Utils as HelperUtils
from tests.helpers.pg_node_utils import PostgresNodeUtils as PostgresNodeTestUtils

from src import PostgresNode
from src import NodeStatus
from src import NodeConnection

from src.node import PostgresNodeLogReader

import pytest
import typing
import logging


class TestSet001__start:
    @pytest.fixture(
        params=PostgresNodeServices.sm_locals_and_remotes,
        ids=[descr.sign for descr in PostgresNodeServices.sm_locals_and_remotes]
    )
    def node_svc(self, request: pytest.FixtureRequest) -> PostgresNodeService:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, PostgresNodeService)
        assert isinstance(request.param.os_ops, OsOperations)
        assert isinstance(request.param.port_manager, PortManager)
        return request.param

    class tagData001:
        wait: typing.Optional[bool]

        def __init__(self, wait: typing.Optional[bool]):
            assert wait is None or type(wait) is bool
            self.wait = wait
            return

    sm_Data001: typing.List[tagData001] = [
        tagData001(None),
        tagData001(True)
    ]

    @pytest.fixture(
            params=sm_Data001,
            ids=["wait={}".format(x.wait) for x in sm_Data001]
    )
    def data001(self, request: pytest.FixtureRequest) -> tagData001:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "tagData001"
        return request.param

    def test_001__wait_true(
        self,
        node_svc: PostgresNodeService,
        data001: tagData001
    ):
        assert isinstance(node_svc, PostgresNodeService)
        assert type(data001) == __class__.tagData001  # noqa: E721
        assert data001.wait is None or type(data001.wait) is bool

        with PostgresNodeTestUtils.get_node(node_svc) as node:
            assert type(node) is PostgresNode
            node.init()
            assert not node.is_started
            assert node.status() == NodeStatus.Stopped

            kwargs = {}

            if data001.wait is not None:
                assert data001.wait == True  # noqa: E712
                kwargs["wait"] = data001.wait

            node.start(**kwargs)
            assert node.is_started
            assert node.status() == NodeStatus.Running

            # Internals
            assert type(node._manually_started_pm_pid) is int
            assert node._manually_started_pm_pid != 0
            assert node._manually_started_pm_pid != node._C_PM_PID__IS_NOT_DETECTED
            assert node._manually_started_pm_pid == node.pid
        return

    def test_002__wait_false(self, node_svc: PostgresNodeService):
        assert isinstance(node_svc, PostgresNodeService)

        C_MAX_ATTEMPTS = 3

        attempt = 0

        while True:
            assert type(attempt) is int
            assert attempt >= 0
            assert attempt <= C_MAX_ATTEMPTS

            if attempt == C_MAX_ATTEMPTS:
                raise RuntimeError("Node is not started")

            attempt += 1

            logging.info("------------- attempt #{}".format(attempt))

            if attempt > 1:
                HelperUtils.PrintAndSleep(5)

            with PostgresNodeTestUtils.get_node(node_svc) as node:
                assert type(node) is PostgresNode
                node.init()
                assert not node.is_started
                assert node.status() == NodeStatus.Stopped

                node_log_reader = PostgresNodeLogReader(node, from_beginnig=False)
                node.start(wait=False)
                assert node.is_started
                assert node.status() in [NodeStatus.Stopped,  NodeStatus.Running]

                # Internals
                assert type(node._manually_started_pm_pid) is int
                assert node._manually_started_pm_pid == node._C_PM_PID__IS_NOT_DETECTED

                logging.info("Wait for running state ...")

                try:
                    PostgresNodeTestUtils.wait_for_running_state(
                        node=node,
                        node_log_reader=node_log_reader,
                        timeout=60,
                    )
                except PostgresNodeTestUtils.PortConflictNodeException as e:
                    logging.warning("Exception {}: {}".format(
                        type(e).__name__,
                        e,
                    ))
                    continue

                logging.info("Node is running.")
                assert node.status() == NodeStatus.Running
                return

    def test_003__exec_env(
        self,
        node_svc: PostgresNodeService,
    ):
        assert isinstance(node_svc, PostgresNodeService)

        with PostgresNodeTestUtils.get_node(node_svc) as node:
            assert type(node) is PostgresNode
            node.init()
            assert not node.is_started
            assert node.status() == NodeStatus.Stopped

            C_ENV_NAME = "MYTESTVAR"
            C_ENV_VALUE = "abcdefg"

            envs = {
                C_ENV_NAME: C_ENV_VALUE
            }

            node.start(exec_env=envs)
            assert node.is_started
            assert node.status() == NodeStatus.Running

            with node.connect(dbname="postgres") as cn:
                assert type(cn) is NodeConnection

                cn.execute("CREATE TEMP TABLE cmd_out(content text);")
                cn.commit()
                cn.execute("COPY cmd_out FROM PROGRAM 'bash -c \'\'echo ${}\'\'';".format(
                    C_ENV_NAME,
                ))
                cn.commit()
                recs = cn.execute("select content from cmd_out;")
                assert type(recs) is list
                assert len(recs) == 1
                assert type(recs[0]) == tuple  # noqa: E721
                rec = recs[0]
                assert len(rec) == 1
                assert rec[0] == C_ENV_VALUE
                logging.info("Env has value [{}]. It is OK!".find(rec[0]))
        return

    def test_004__params_is_None(
        self,
        node_svc: PostgresNodeService,
    ):
        assert isinstance(node_svc, PostgresNodeService)

        with PostgresNodeTestUtils.get_node(node_svc) as node:
            assert type(node) is PostgresNode
            node.init()
            assert not node.is_started
            assert node.status() == NodeStatus.Stopped

            node.start(params=None)
            assert node.is_started
            assert node.status() == NodeStatus.Running
        return

    def test_005__params_is_empty(
        self,
        node_svc: PostgresNodeService,
    ):
        assert isinstance(node_svc, PostgresNodeService)

        with PostgresNodeTestUtils.get_node(node_svc) as node:
            assert type(node) is PostgresNode
            node.init()
            assert not node.is_started
            assert node.status() == NodeStatus.Stopped

            node.start(params=[])
            assert node.is_started
            assert node.status() == NodeStatus.Running
        return
