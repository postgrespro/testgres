import os
import shutil
import subprocess
import time
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


def wait_until(fn, timeout=15.0, interval=0.2, desc="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except Exception as exc:
            last = exc
        time.sleep(interval)
    if last is not None:
        raise AssertionError("{} not reached; last error: {!r}".format(desc, last))
    raise AssertionError("{} not reached".format(desc))


def make_lab(prefix="flt", subnet_octet=None):
    token = uuid.uuid4().hex[:5]
    bridge = (prefix + token)[:15]
    base = str(TEST_ROOT / ("pglab-faults-" + token))
    if subnet_octet is None:
        subnet_octet = int(token[:2], 16) % 180 + 40
    subnet = "10.233.{}.0/24".format(subnet_octet)

    return PgLab(
        pg_bin=PG_BIN,
        base_dir=base,
        subnet=subnet,
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
    )


@pytest.fixture
def lab():
    instance = make_lab()
    try:
        yield instance
    finally:
        instance.close()


def node_ip(lab, hostno):
    return str(list(lab.subnet.hosts())[hostno - 1])


def pg_stat_replication(primary):
    return primary.execute("""
        select application_name, state, host(client_addr)
          from pg_stat_replication
         order by application_name
    """)


def test_two_replicas_same_port_and_independent_catchup(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10), port=5432)
    primary.init().start()

    r1 = lab.create_replica("p1", "r1", node_ip(lab, 11), port=5432)
    r2 = lab.create_replica("p1", "r2", node_ip(lab, 12), port=5432)

    r1.start()
    r2.start()
    r1.catchup()
    r2.catchup()

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1), (2), (3)")
    r1.catchup()
    r2.catchup()

    assert r1.execute("select array_agg(i order by i) from t") == [([1, 2, 3],)]
    assert r2.execute("select array_agg(i order by i) from t") == [([1, 2, 3],)]

    rows = pg_stat_replication(primary)
    assert rows == [
        ("r1", "streaming", r1.ip),
        ("r2", "streaming", r2.ip),
    ]

    assert primary.port == r1.port == r2.port == 5432


def test_replica_restart_rejoins_streaming(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", node_ip(lab, 11))
    replica.start()
    replica.catchup()

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    replica.stop()
    primary.execute("insert into t values (2), (3)")

    replica.start()
    replica.catchup()

    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2, 3],)]

    wait_until(
        lambda: pg_stat_replication(primary) == [
            ("r1", "streaming", replica.ip)
        ],
        desc="replica streaming after restart",
    )


def test_primary_restart_replica_recovers(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", node_ip(lab, 11))
    replica.start()
    replica.catchup()

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    primary.stop()

    # Replica remains queryable in recovery while upstream is unavailable.
    assert replica.execute("select pg_is_in_recovery()") == [(True,)]
    assert replica.execute("select array_agg(i order by i) from t") == [([1],)]

    primary.start()
    primary.execute("insert into t values (2)")
    replica.catchup()

    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2],)]


def test_network_down_up_recovers_streaming(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", node_ip(lab, 11))
    replica.start()
    replica.catchup()

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    replica.network_down()
    primary.execute("insert into t values (2)")

    # Controller may lose direct reachability to the replica while eth0 is down,
    # so do not query it here. Restore networking first.
    replica.network_up()

    wait_until(
        lambda: run(
            "ip", "netns", "exec", replica.namespace,
            "ping", "-c", "1", "-W", "1", primary.ip,
            check=False,
        ).returncode == 0,
        timeout=10,
        desc="replica network restored",
    )

    replica.catchup()
    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2],)]


def test_live_io_limit_change(lab):
    first = 64 * 1024 * 1024
    second = 8 * 1024 * 1024

    primary = lab.create_primary(
        "p1",
        node_ip(lab, 10),
        io=IOLimits(read_bps=first),
    )
    primary.init().start()

    lab.set_io_limits("p1", IOLimits(read_bps=second))

    cg = Path(primary.cgroup)
    if lab.cgroup_backend.version == 2:
        text = (cg / "io.max").read_text()
        assert "rbps={}".format(second) in text
        assert "rbps={}".format(first) not in text
    elif lab.cgroup_backend.version == 1:
        text = (cg / "blkio.throttle.read_bps_device").read_text()
        values = [
            line.split()[-1]
            for line in text.splitlines()
            if line.strip()
        ]
        assert str(second) in values
    else:
        pytest.fail("unsupported cgroup backend")


def test_exception_inside_context_cleans_everything():
    token = uuid.uuid4().hex[:5]
    bridge = ("boom" + token)[:15]
    base = str(TEST_ROOT / ("pglab-boom-" + token))
    subnet = "10.234.{}.0/24".format(int(token[:2], 16) % 180 + 40)

    namespace = None
    with pytest.raises(RuntimeError, match="boom"):
        with PgLab(
            pg_bin=PG_BIN,
            base_dir=base,
            subnet=subnet,
            bridge_name=bridge,
            os_user=OS_USER,
            db_superuser=DB_USER,
        ) as lab:
            p = lab.create_primary("p1", str(list(lab.subnet.hosts())[9]))
            namespace = p.namespace
            p.init().start()
            raise RuntimeError("boom")

    assert namespace is not None
    assert namespace not in run("ip", "netns", "list").stdout
    assert run("ip", "link", "show", "dev", bridge, check=False).returncode != 0
    assert not Path(base).exists()


def test_keep_data_preserves_pgdata():
    token = uuid.uuid4().hex[:5]
    bridge = ("keep" + token)[:15]
    base = TEST_ROOT / ("pglab-keep-" + token)
    subnet = "10.236.{}.0/24".format(int(token[:2], 16) % 180 + 40)

    lab = PgLab(
        pg_bin=PG_BIN,
        base_dir=str(base),
        subnet=subnet,
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
        keep_data=True,
    )

    primary = lab.create_primary("p1", str(list(lab.subnet.hosts())[9]))
    primary.init().start()
    primary.execute("create table keep_me(i int)")

    pgdata = Path(primary.pg.data_dir)
    lab.close()

    try:
        assert pgdata.is_dir()
        assert (pgdata / "PG_VERSION").is_file()
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_cleanup_when_postgres_was_sigkilled(capsys):
    lab = make_lab(prefix="kill")
    bridge = lab.bridge_name
    base = lab.base_dir

    p = lab.create_primary("p1", node_ip(lab, 10))
    p.init().start()


    # Find actual postmaster pid from postmaster.pid, then kill -9 it.
    pgdata = Path(p.pg.data_dir)
    lines = (pgdata / "postmaster.pid").read_text().splitlines()
    actual_postmaster = int(lines[0])

    os.kill(actual_postmaster, 9)

    wait_until(
        lambda: not Path("/proc/{}".format(actual_postmaster)).exists(),
        timeout=10,
        desc="postmaster SIGKILL",
    )

    # close() must tolerate the already-dead server and still clean isolation.
    lab.close()

    captured = capsys.readouterr()
    assert "cannot stop node" not in captured.err
    assert run("ip", "link", "show", "dev", bridge, check=False).returncode != 0
    assert not Path(base).exists()


def test_recreate_same_lab_after_close():
    token = uuid.uuid4().hex[:5]
    bridge = ("same" + token)[:15]
    base = str(TEST_ROOT / ("pglab-same-" + token))
    subnet = "10.235.{}.0/24".format(int(token[:2], 16) % 180 + 40)

    for cycle in (1, 2):
        with PgLab(
            pg_bin=PG_BIN,
            base_dir=base,
            subnet=subnet,
            bridge_name=bridge,
            os_user=OS_USER,
            db_superuser=DB_USER,
        ) as lab:
            p = lab.create_primary("p1", str(list(lab.subnet.hosts())[9]))
            p.init().start()
            assert p.execute("select {}".format(cycle)) == [(cycle,)]

        assert run("ip", "link", "show", "dev", bridge, check=False).returncode != 0
        assert not Path(base).exists()


def test_partition_only_one_replica(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init().start()

    r1 = lab.create_replica("p1", "r1", node_ip(lab, 11))
    r2 = lab.create_replica("p1", "r2", node_ip(lab, 12))
    r1.start()
    r2.start()
    r1.catchup()
    r2.catchup()

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    r1.catchup()
    r2.catchup()

    lab.partition("p1", "r1")
    primary.execute("insert into t values (2)")

    # r2 should continue normally while r1 is isolated.
    r2.catchup()
    assert r2.execute("select array_agg(i order by i) from t") == [([1, 2],)]

    # r1 is still queryable from the controller and should not have replayed 2.
    assert r1.execute("select array_agg(i order by i) from t") == [([1],)]

    lab.heal_partition("p1", "r1")
    r1.catchup()
    assert r1.execute("select array_agg(i order by i) from t") == [([1, 2],)]


def test_replication_survives_repeated_partition_cycles(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", node_ip(lab, 11))
    replica.start()
    replica.catchup()

    primary.execute("create table t(i int primary key)")

    for i in range(1, 6):
        lab.partition("p1", "r1")
        primary.execute("insert into t values ({})".format(i))
        lab.heal_partition("p1", "r1")
        replica.catchup()

        assert replica.execute(
            "select coalesce(max(i), 0), count(*) from t"
        ) == [(i, i)]
