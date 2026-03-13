from __future__ import annotations

import os
import shutil
import pytest

import testgres
from ...pg_probackup2.app import ProbackupApp
from ...pg_probackup2.init_helpers import Init, init_params
from ..storage.fs_backup import FSTestBackupDir


class ProbackupTest:
    pg_node: testgres.NodeApp

    @staticmethod
    def probackup_is_available() -> bool:
        p = os.environ.get("PGPROBACKUPBIN")

        if p is None:
            return False

        if not os.path.exists(p):
            return False

        return True

    @pytest.fixture(autouse=True, scope="function")
    def implicit_fixture(self, request: pytest.FixtureRequest):
        assert isinstance(request, pytest.FixtureRequest)
        self.helper__setUp(request)
        yield
        self.helper__tearDown()

    def helper__setUp(self, request: pytest.FixtureRequest):
        assert isinstance(request, pytest.FixtureRequest)

        self.helper__setup_test_environment(request)
        self.helper__setup_test_paths()
        self.helper__setup_backup_dir()
        self.helper__setup_probackup()

    def helper__setup_test_environment(self, request: pytest.FixtureRequest):
        assert isinstance(request, pytest.FixtureRequest)

        self.output = None
        self.cmd = None
        self.nodes_to_cleanup = []
        self.module_name, self.fname = request.node.cls.__name__, request.node.name
        self.test_env = Init().test_env()

    def helper__setup_test_paths(self):
        self.rel_path = os.path.join(self.module_name, self.fname)
        self.test_path = os.path.join(init_params.tmp_path, self.rel_path)
        os.makedirs(self.test_path, exist_ok=True)
        self.pb_log_path = os.path.join(self.test_path, "pb_log")

    def helper__setup_backup_dir(self):
        self.backup_dir = self.helper__build_backup_dir('backup')
        self.backup_dir.cleanup()

    def helper__setup_probackup(self):
        self.pg_node = testgres.NodeApp(self.test_path, self.nodes_to_cleanup)
        self.pb = ProbackupApp(self, self.pg_node, self.pb_log_path, self.test_env,
                               auto_compress_alg='zlib', backup_dir=self.backup_dir)

    def helper__tearDown(self):
        if os.path.exists(self.test_path):
            shutil.rmtree(self.test_path)

    def helper__build_backup_dir(self, backup='backup'):
        return FSTestBackupDir(rel_path=self.rel_path, backup=backup)


@pytest.mark.skipif(not ProbackupTest.probackup_is_available(), reason="Check that PGPROBACKUPBIN is defined and is valid.")
class TestBasic(ProbackupTest):
    def test_full_backup(self):
        assert self.pg_node is not None
        assert type(self.pg_node) == testgres.NodeApp  # noqa: E721
        assert self.pb is not None
        assert type(self.pb) == ProbackupApp  # noqa: E721

        # Setting up a simple test node
        node = self.pg_node.make_simple('node', pg_options={"fsync": "off", "synchronous_commit": "off"})

        assert node is not None
        assert type(node) == testgres.PostgresNode  # noqa: E721

        with node:
            # Initialize and configure Probackup
            self.pb.init()
            self.pb.add_instance('node', node)
            self.pb.set_archiving('node', node)

            # Start the node and initialize pgbench
            node.slow_start()
            node.pgbench_init(scale=100, no_vacuum=True)

            # Perform backup and validation
            backup_id = self.pb.backup_node('node', node)
            out = self.pb.validate('node', backup_id)

            # Check if the backup is valid
            assert f"INFO: Backup {backup_id} is valid" in out
