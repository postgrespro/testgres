import subprocess
import time
import uuid
from pathlib import Path

import pytest

from src.pglab import PgLab, PgLabError
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


@pytest.fixture
def lab():
    token = uuid.uuid4().hex[:5]
    bridge = ("net" + token)[:15]
    base = str(TEST_ROOT / ("pglab-netem-" + token))
    subnet = "10.236.{}.0/24".format(int(token[:2], 16) % 180 + 40)

    instance = PgLab(
        pg_bin=PG_BIN,
        base_dir=base,
        subnet=subnet,
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
    )
    try:
        yield instance
    finally:
        instance.close()


def node_ip(lab, hostno):
    return str(list(lab.subnet.hosts())[hostno - 1])


def make_pair(lab):
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init().start()

    replica = lab.create_replica("p1", "r1", node_ip(lab, 11))
    replica.start()
    replica.catchup()
    return primary, replica


def qdisc(node):
    return run(
        "ip", "netns", "exec", node.namespace,
        "tc", "qdisc", "show", "dev", node.veth_ns,
    ).stdout


def postmaster_pid(node):
    return int((Path(node.pg.data_dir) / "postmaster.pid").read_text().splitlines()[0])


def proc_state(pid):
    for line in Path("/proc/{}/status".format(pid)).read_text().splitlines():
        if line.startswith("State:"):
            return line.split()[1]
    raise AssertionError("no State in /proc/{}/status".format(pid))


def test_netem_delay_is_installed_and_cleared(lab):
    _, replica = make_pair(lab)

    replica.netem(delay_ms=150, jitter_ms=20)

    text = qdisc(replica)
    assert "netem" in text
    assert "delay" in text
    assert "150" in text

    replica.clear_netem()
    assert "netem" not in qdisc(replica)


def test_replication_survives_latency_and_loss(lab):
    primary, replica = make_pair(lab)

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    # Large enough to exercise retransmits without making the test excessively
    # slow.  The fault applies to replica egress, including TCP ACKs.
    replica.netem(delay_ms=80, jitter_ms=20, loss_pct=2)

    primary.execute(
        "insert into t select i from generate_series(2, 1001) as g(i)"
    )

    # Avoid an aggressive Testgres catchup timeout while netem is active.
    time.sleep(1.0)
    replica.clear_netem()
    replica.catchup()

    assert replica.execute("select count(*), max(i) from t") == [(1001, 1001)]


def test_netem_total_loss_then_recovery(lab):
    primary, replica = make_pair(lab)

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    replica.netem(loss_pct=100)
    primary.execute("insert into t values (2), (3)")

    assert "loss 100%" in qdisc(replica)

    replica.clear_netem()
    replica.catchup()

    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2, 3],)]


def test_netem_can_be_replaced_live(lab):
    _, replica = make_pair(lab)

    replica.netem(delay_ms=50)
    first = qdisc(replica)
    assert "50" in first

    replica.netem(delay_ms=250, loss_pct=1)
    second = qdisc(replica)
    assert "250" in second
    assert "loss 1%" in second

    replica.clear_netem()


def test_netem_validation(lab, capsys):
    node = lab.create_primary("p1", node_ip(lab, 10))

    with pytest.raises(PgLabError):
        node.netem()

    with pytest.raises(PgLabError):
        node.netem(loss_pct=101)

    with pytest.raises(PgLabError):
        node.netem(jitter_ms=10)

    with pytest.raises(PgLabError):
        node.netem(reorder_pct=10)

    # Closing an intentionally uninitialized node should not leak Testgres'
    # internal stop diagnostic to the test output.
    lab.close()
    captured = capsys.readouterr()
    assert "cannot stop node" not in captured.err


def test_freeze_replica_then_resume_and_catchup(lab):
    primary, replica = make_pair(lab)

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    pid = postmaster_pid(replica)
    replica.freeze()

    wait_until(
        lambda: proc_state(pid) in ("T", "t"),
        timeout=5,
        desc="replica postmaster stopped",
    )

    primary.execute("insert into t values (2), (3), (4)")

    replica.unfreeze()

    wait_until(
        lambda: proc_state(pid) not in ("T", "t"),
        timeout=5,
        desc="replica postmaster resumed",
    )

    replica.catchup()
    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2, 3, 4],)]


def test_freeze_primary_then_resume(lab):
    primary, replica = make_pair(lab)

    primary.execute("create table t(i int primary key)")
    primary.execute("insert into t values (1)")
    replica.catchup()

    pid = postmaster_pid(primary)
    primary.freeze()

    wait_until(
        lambda: proc_state(pid) in ("T", "t"),
        timeout=5,
        desc="primary postmaster stopped",
    )

    # Replica remains readable while upstream is frozen.
    assert replica.execute("select array_agg(i order by i) from t") == [([1],)]

    primary.unfreeze()
    wait_until(
        lambda: proc_state(pid) not in ("T", "t"),
        timeout=5,
        desc="primary postmaster resumed",
    )

    primary.execute("insert into t values (2)")
    replica.catchup()
    assert replica.execute("select array_agg(i order by i) from t") == [([1, 2],)]


def test_close_unfreezes_before_cleanup():
    token = uuid.uuid4().hex[:5]
    bridge = ("frz" + token)[:15]
    base = str(TEST_ROOT / ("pglab-frozen-close-" + token))
    subnet = "10.237.{}.0/24".format(int(token[:2], 16) % 180 + 40)

    lab = PgLab(
        pg_bin=PG_BIN,
        base_dir=base,
        subnet=subnet,
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
    )

    node = lab.create_primary("p1", str(list(lab.subnet.hosts())[9]))
    node.init().start()
    node.freeze()

    # Regression test: close() must not hang trying to pg_ctl-stop SIGSTOPed PG.
    started = time.monotonic()
    lab.close()
    elapsed = time.monotonic() - started

    assert elapsed < 15
    assert run("ip", "link", "show", "dev", bridge, check=False).returncode != 0
    assert not Path(base).exists()


def test_close_cleans_netem():
    token = uuid.uuid4().hex[:5]
    bridge = ("tc" + token)[:15]
    base = str(TEST_ROOT / ("pglab-netem-close-" + token))
    subnet = "10.238.{}.0/24".format(int(token[:2], 16) % 180 + 40)

    lab = PgLab(
        pg_bin=PG_BIN,
        base_dir=base,
        subnet=subnet,
        bridge_name=bridge,
        os_user=OS_USER,
        db_superuser=DB_USER,
    )

    node = lab.create_primary("p1", str(list(lab.subnet.hosts())[9]))
    node.init().start()
    node.netem(delay_ms=100, loss_pct=1)
    assert "netem" in qdisc(node)

    lab.close()

    # Namespace deletion itself removes the qdisc; the important thing is that
    # cleanup succeeds while a netem profile is active.
    assert node.namespace not in run("ip", "netns", "list").stdout
    assert not Path(base).exists()
