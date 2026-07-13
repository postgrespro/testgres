from __future__ import annotations

from tests.helpers.global_data import PostgresNodeService
from tests.helpers.global_data import PostgresNodeServices
from tests.helpers.global_data import OsOperations
from tests.helpers.global_data import PortManager
from tests.helpers.pg_node_utils import PostgresNodeUtils as PostgresNodeTestUtils

from src import PostgresNode
from src import NodeStatus
from src.impl.platforms.internal_platform_utils_factory import create_internal_platform_utils
from src.impl.platforms.internal_platform_utils_factory import InternalPlatformUtils

import pytest
import typing


class TestSet010__FindPostmaster:
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

    def test_001__ok(
        self,
        node_svc: PostgresNodeService,
    ):
        assert isinstance(node_svc, PostgresNodeService)

        with PostgresNodeTestUtils.get_node(node_svc) as node:
            assert type(node) is PostgresNode
            node.init()
            assert not node.is_started
            assert node.status() == NodeStatus.Stopped

            node.start()
            assert node.is_started
            assert node.status() == NodeStatus.Running

            # Internals
            assert type(node._manually_started_pm_pid) is int
            assert node._manually_started_pm_pid != 0
            assert node._manually_started_pm_pid != node._C_PM_PID__IS_NOT_DETECTED
            assert node._manually_started_pm_pid == node.pid

            platform_utils = create_internal_platform_utils(node.os_ops)
            assert platform_utils is not None
            assert isinstance(platform_utils, InternalPlatformUtils)

            r = platform_utils.FindPostmaster(
                node.os_ops,
                node.bin_dir,
                node.data_dir
            )

            assert r is not None
            assert type(r) is InternalPlatformUtils.FindPostmasterResult
            assert r.code == InternalPlatformUtils.FindPostmasterResultCode.ok
            assert r.pid == node.pid

        return

    def test_002__not_found(
        self,
        node_svc: PostgresNodeService,
    ):
        assert isinstance(node_svc, PostgresNodeService)

        with PostgresNodeTestUtils.get_node(node_svc) as node:
            assert type(node) is PostgresNode
            node.init()
            assert not node.is_started
            assert node.status() == NodeStatus.Stopped

            platform_utils = create_internal_platform_utils(node.os_ops)
            assert platform_utils is not None
            assert isinstance(platform_utils, InternalPlatformUtils)

            r = platform_utils.FindPostmaster(
                node.os_ops,
                node.bin_dir,
                node.data_dir
            )

            assert r is not None
            assert type(r) is InternalPlatformUtils.FindPostmasterResult
            assert r.code == InternalPlatformUtils.FindPostmasterResultCode.not_found
            assert r.pid is None

        return
