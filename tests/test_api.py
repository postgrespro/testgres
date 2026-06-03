from src import api as testgres_api
from src.node import PostgresNode

from tests.helpers.global_data import OsOpsDescrs


class TestAPI:
    def test_001__get_new_node(self):
        C_NODE_NAME = "abc"

        with testgres_api.get_new_node(name=C_NODE_NAME) as node:
            assert type(node) is PostgresNode
            assert node.name == C_NODE_NAME
            node.init()
            node.slow_start()
            node.stop()
        return

    def test_001__get_remote_node(self):
        C_NODE_NAME = "abc"

        conn_params = OsOpsDescrs.sm_remote_conn_params

        with testgres_api.get_remote_node(name=C_NODE_NAME, conn_params=conn_params) as node:
            assert type(node) is PostgresNode
            assert node.name == C_NODE_NAME
            node.init()
            node.slow_start()
            node.stop()
        return
