import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

from src.pglab import PgLab, IOLimits
from .helpers.pglab_test_config import (
    DB_USER,
    OS_USER,
    PG_BIN,
    PGLAB_INTEGRATION_MARKS,
    TEST_ROOT,
)


pytestmark = PGLAB_INTEGRATION_MARKS


def run(*args, check=True):
    return subprocess.run(
        [str(a) for a in args],
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def netns_exists(name):
    r = run("ip", "netns", "list")
    return any(line.split()[0] == name for line in r.stdout.splitlines() if line.strip())


def firewall_source_zone(subnet):
    fw = shutil.which("firewall-cmd")
    if fw is None:
        return None
    if run(fw, "--state", check=False).returncode != 0:
        return None
    r = run(fw, "--get-zone-of-source={}".format(subnet), check=False)
    if r.returncode != 0:
        return ""
    return r.stdout.strip()


@pytest.fixture
def lab():
    # Keep names short: Linux IFNAMSIZ is 16 bytes including NUL.
    token = uuid.uuid4().hex[:5]
    bridge = "plt" + token
    base = str(TEST_ROOT / ("pglab-pytest-" + token))

    instance = PgLab(
        pg_bin=PG_BIN,
        base_dir=base,
        subnet="10.231.{}.0/24".format(int(token[:2], 16) % 200 + 20),
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
    )
    try:
        yield instance
    finally:
        instance.close()
        shutil.rmtree(base, ignore_errors=True)


def test_primary_starts_in_netns_and_uses_tcp(lab):
    primary = lab.create_primary("p1", "10.231.{}.10".format(lab.subnet.network_address.packed[2]))
    primary.init().start()

    assert primary.execute("select 1") == [(1,)]
    expected = primary.ip
    assert primary.execute(
        "select host(inet_server_addr()) = '{}'".format(expected)
    ) == [(True,)]

    # We deliberately disable the shared host Unix socket, because netns does
    # not isolate the filesystem and every node may use TCP port 5432.
    assert primary.execute("show unix_socket_directories") == [("",)]


def test_replication_and_catchup(lab):
    third = lab.subnet.network_address.packed[2]
    primary = lab.create_primary("p1", "10.231.{}.10".format(third))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", "10.231.{}.11".format(third))
    replica.start()
    replica.catchup()

    assert replica.execute("select pg_is_in_recovery()") == [(True,)]

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1), (2), (3)")
    replica.catchup()

    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2, 3],)]

    rows = primary.execute("""
        select application_name, state, host(client_addr)
          from pg_stat_replication
         where application_name = 'r1'
    """)
    assert rows == [("r1", "streaming", replica.ip)]


def test_replica_target_port_and_storage_root(lab):
    third = lab.subnet.network_address.packed[2]
    primary = lab.create_primary("p1", "10.231.{}.10".format(third), port=5432)
    primary.init().start()

    replica = lab.create_replica(
        "p1",
        "r1",
        "10.231.{}.11".format(third),
        port=5433,
    )

    assert replica.port == 5433
    assert Path(replica.pg.base_dir) == lab.base_dir / "r1"

    replica.start()
    replica.catchup()

    assert replica.execute("show port") == [("5433",)]
    assert replica.execute("select pg_is_in_recovery()") == [(True,)]


def test_partition_and_heal(lab):
    third = lab.subnet.network_address.packed[2]
    primary = lab.create_primary("p1", "10.231.{}.10".format(third))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", "10.231.{}.11".format(third))
    replica.start()
    replica.catchup()

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()
    assert replica.execute("select count(*) from t") == [(1,)]

    lab.partition("p1", "r1")
    primary.execute("insert into t values (2)")

    # Controller -> replica connectivity remains available; only node-to-node
    # traffic is blackholed.
    assert replica.execute("select count(*) from t") == [(1,)]

    # The sender should stop being streaming after the partition has had a
    # chance to break the connection. Don't make timing the essence of this
    # test; the important assertion is that replay cannot advance.
    lab.heal_partition("p1", "r1")
    replica.catchup()

    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2],)]


def test_postgres_backend_is_in_node_cgroup(lab):
    third = lab.subnet.network_address.packed[2]
    primary = lab.create_primary(
        "p1",
        "10.231.{}.10".format(third),
        io=IOLimits(read_bps=50 * 1024 * 1024),
    )
    primary.init().start()

    backend_pid = primary.execute("select pg_backend_pid()")[0][0]
    membership = Path(primary.os_ops.cgroup_membership_file)

    assert membership.exists()

    pids = {
        int(line)
        for line in membership.read_text().splitlines()
        if line.strip().isdigit()
    }
    assert backend_pid in pids


def test_io_limit_is_programmed(lab):
    third = lab.subnet.network_address.packed[2]
    limit = 32 * 1024 * 1024

    primary = lab.create_primary(
        "p1",
        "10.231.{}.10".format(third),
        io=IOLimits(read_bps=limit),
    )
    primary.init().start()

    cg = Path(primary.cgroup)
    version = lab.cgroup_backend.version

    if version == 2:
        text = (cg / "io.max").read_text()
        assert "rbps={}".format(limit) in text
    elif version == 1:
        text = (cg / "blkio.throttle.read_bps_device").read_text()
        assert any(
            line.split()[-1] == str(limit)
            for line in text.splitlines()
            if line.strip()
        )
    else:
        pytest.fail("unsupported cgroup backend version: {!r}".format(version))


def test_cleanup_removes_namespace_bridge_and_runtime_firewall():
    token = uuid.uuid4().hex[:5]
    bridge = "plc" + token
    base = str(TEST_ROOT / ("pglab-cleanup-" + token))
    subnet = "10.232.{}.0/24".format(int(token[:2], 16) % 200 + 20)

    zone_before = firewall_source_zone(subnet)

    lab = PgLab(
        pg_bin=PG_BIN,
        base_dir=base,
        subnet=subnet,
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
    )

    node = lab.create_primary("cleanup", str(list(lab.subnet.hosts())[9]))
    node.init().start()
    namespace = node.namespace

    assert netns_exists(namespace)
    assert run("ip", "link", "show", "dev", bridge, check=False).returncode == 0

    lab.close()

    assert not netns_exists(namespace)
    assert run("ip", "link", "show", "dev", bridge, check=False).returncode != 0
    assert not Path(base).exists()

    # PgLab must not remove a pre-existing user firewalld assignment. If there
    # was no assignment before, there must not be one left afterwards.
    if zone_before == "":
        assert firewall_source_zone(subnet) == ""
