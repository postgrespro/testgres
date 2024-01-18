import os
import shutil
import unittest
import testgres
from pg_probackup2.app import ProbackupApp
from pg_probackup2.init_helpers import Init, init_params
from pg_probackup2.app import build_backup_dir


class TestUtils:
    @staticmethod
    def get_module_and_function_name(test_id):
        try:
            module_name = test_id.split('.')[-2]
            fname = test_id.split('.')[-1]
        except IndexError:
            print(f"Couldn't get module name and function name from test_id: `{test_id}`")
            module_name, fname = test_id.split('(')[1].split('.')[1], test_id.split('(')[0]
        return module_name, fname


class ProbackupTest(unittest.TestCase):
    def setUp(self):
        self.setup_test_environment()
        self.setup_test_paths()
        self.setup_backup_dir()
        self.setup_probackup()

    def setup_test_environment(self):
        self.output = None
        self.cmd = None
        self.nodes_to_cleanup = []
        self.module_name, self.fname = TestUtils.get_module_and_function_name(self.id())
        self.test_env = Init().test_env()

    def setup_test_paths(self):
        self.rel_path = os.path.join(self.module_name, self.fname)
        self.test_path = os.path.join(init_params.tmp_path, self.rel_path)
        os.makedirs(self.test_path)
        self.pb_log_path = os.path.join(self.test_path, "pb_log")

    def setup_backup_dir(self):
        self.backup_dir = build_backup_dir(self, 'backup')
        self.backup_dir.cleanup()

    def setup_probackup(self):
        self.pg_node = testgres.NodeApp(self.test_path, self.nodes_to_cleanup)
        self.pb = ProbackupApp(self, self.pg_node, self.pb_log_path, self.test_env,
                               auto_compress_alg='zlib', backup_dir=self.backup_dir)

    def tearDown(self):
        if os.path.exists(self.test_path):
            shutil.rmtree(self.test_path)


class BasicTest(ProbackupTest):
    def test_full_backup(self):
        # Setting up a simple test node
        node = self.pg_node.make_simple('node', pg_options={"fsync": "off", "synchronous_commit": "off"})

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
        self.assertIn(f"INFO: Backup {backup_id} is valid", out)


if __name__ == "__main__":
    unittest.main()
