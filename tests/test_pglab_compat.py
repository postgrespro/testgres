import inspect

from src.pglab import NamespaceOperations
from src.backup import NodeBackup
from src.node import PostgresNode


def test_testgres_target_execution_context_api():
    replicate = inspect.signature(PostgresNode.replicate).parameters
    assert "target_os_ops" in replicate
    assert "target_host" in replicate
    assert "target_port" in replicate

    backup_init = inspect.signature(NodeBackup.__init__).parameters
    assert "os_ops" in backup_init

    spawn_replica = inspect.signature(NodeBackup.spawn_replica).parameters
    assert "target_host" in spawn_replica
    assert "target_port" in spawn_replica


def test_namespace_operations_exposes_explicit_pg_bin():
    ops = NamespaceOperations(
        namespace="pglab-test-unused",
        os_user="postgres",
        db_user="postgres",
        pg_bin="/opt/postgresql/bin",
    )
    assert ops.environ("PG_BIN") == "/opt/postgresql/bin"
