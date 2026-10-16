import os
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from src.pglab import PgLab
from .helpers.pglab_test_config import (
    DB_USER,
    OS_USER,
    PG_BIN,
    PGLAB_INTEGRATION_MARKS,
    TEST_ROOT,
)


PSQL = str(Path(PG_BIN) / "psql") if PG_BIN else None

pytestmark = PGLAB_INTEGRATION_MARKS + [
    pytest.mark.skipif(shutil.which("tc") is None, reason="tc/iproute2 is required"),
]


def run(*args, check=True, timeout=None, env=None, stdout=subprocess.PIPE):
    full_env = os.environ.copy()
    full_env["LC_ALL"] = "C"
    if env:
        full_env.update(env)

    return subprocess.run(
        [str(a) for a in args],
        check=check,
        text=True,
        stdout=stdout,
        stderr=subprocess.PIPE,
        timeout=timeout,
        env=full_env,
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
        raise AssertionError(
            "{} not reached; last error: {!r}".format(desc, last)
        )
    raise AssertionError("{} not reached".format(desc))


@pytest.fixture
def lab():
    token = uuid.uuid4().hex[:5]
    bridge = ("eff" + token)[:15]
    base = str(TEST_ROOT / ("pglab-netem-effects-" + token))
    subnet = "10.239.{}.0/24".format(int(token[:2], 16) % 180 + 40)

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


def ping_times(node, target, count=4, timeout=10):
    proc = run(
        "ip", "netns", "exec", node.namespace,
        "ping", "-n", "-c", str(count), "-W", "2", target,
        check=False,
        timeout=timeout,
    )
    times = [
        float(x)
        for x in re.findall(r"time[=<]([0-9.]+)\s*ms", proc.stdout)
    ]
    return proc, times


def qdisc_stats(node):
    return run(
        "ip", "netns", "exec", node.namespace,
        "tc", "-s", "qdisc", "show", "dev", node.veth_ns,
    ).stdout


def psql_query(node, sql, connect_timeout=2, timeout=6):
    return run(
        PSQL,
        "-X",
        "-h", node.ip,
        "-p", str(node.port),
        "-U", DB_USER,
        "-d", "postgres",
        "-Atq",
        "-c", sql,
        check=False,
        timeout=timeout,
        env={
            "PGCONNECT_TIMEOUT": str(connect_timeout),
            "PGSSLMODE": "disable",
        },
    )


def test_netem_delay_changes_real_rtt(lab):
    """Prove that netem changes packet RTT, not only qdisc configuration."""
    primary, replica = make_pair(lab)

    base_proc, base_times = ping_times(replica, primary.ip)
    assert base_proc.returncode == 0
    assert len(base_times) >= 3
    baseline = sum(base_times) / len(base_times)

    replica.netem(delay_ms=200)

    delayed_proc, delayed_times = ping_times(replica, primary.ip)
    assert delayed_proc.returncode == 0
    assert len(delayed_times) >= 3
    delayed = sum(delayed_times) / len(delayed_times)

    assert delayed >= baseline + 150, (
        "netem did not visibly increase RTT: baseline={:.3f} ms, "
        "delayed={:.3f} ms".format(baseline, delayed)
    )

    replica.clear_netem()

    recovered_proc, recovered_times = ping_times(replica, primary.ip)
    assert recovered_proc.returncode == 0
    recovered = sum(recovered_times) / len(recovered_times)
    assert recovered < delayed - 100


def test_netem_100_percent_loss_really_drops_packets(lab):
    """100% loss must break connectivity and tc must account dropped packets."""
    primary, replica = make_pair(lab)

    ok, _ = ping_times(replica, primary.ip, count=2)
    assert ok.returncode == 0

    replica.netem(loss_pct=100)

    broken, times = ping_times(replica, primary.ip, count=3)
    assert broken.returncode != 0
    assert times == []

    stats = qdisc_stats(replica)
    match = re.search(r"dropped\s+(\d+)", stats)
    assert match is not None, stats
    assert int(match.group(1)) > 0, stats

    replica.clear_netem()

    healed, times = ping_times(replica, primary.ip, count=2)
    assert healed.returncode == 0
    assert len(times) == 2


def test_netem_loss_breaks_real_postgresql_connection(lab):
    """Show an application-visible failure: psql cannot connect through 100% loss."""
    _, replica = make_pair(lab)

    before = psql_query(replica, "select 42")
    assert before.returncode == 0
    assert before.stdout.strip() == "42"

    replica.netem(loss_pct=100)

    started = time.monotonic()
    broken = psql_query(replica, "select 42", connect_timeout=2, timeout=5)
    elapsed = time.monotonic() - started

    assert broken.returncode != 0
    assert elapsed >= 1.0
    assert (
        "timeout" in broken.stderr.lower()
        or "could not connect" in broken.stderr.lower()
        or "connection" in broken.stderr.lower()
    ), broken.stderr

    replica.clear_netem()

    after = psql_query(replica, "select 42")
    assert after.returncode == 0
    assert after.stdout.strip() == "42"


def test_netem_rate_limit_is_observable_by_postgresql_client(lab):
    """Rate limiting should turn a fast COPY into a visibly slow transfer."""
    _, replica = make_pair(lab)

    sql = (
        "copy ("
        "select repeat('x', 1024) "
        "from generate_series(1, 128)"
        ") to stdout"
    )

    started = time.monotonic()
    normal = run(
        PSQL,
        "-X", "-h", replica.ip, "-p", str(replica.port),
        "-U", DB_USER, "-d", "postgres", "-q", "-c", sql,
        check=False,
        timeout=10,
        env={"PGCONNECT_TIMEOUT": "2", "PGSSLMODE": "disable"},
        stdout=subprocess.DEVNULL,
    )
    baseline = time.monotonic() - started
    assert normal.returncode == 0, normal.stderr

    replica.netem(rate_kbit=256)

    started = time.monotonic()
    limited = run(
        PSQL,
        "-X", "-h", replica.ip, "-p", str(replica.port),
        "-U", DB_USER, "-d", "postgres", "-q", "-c", sql,
        check=False,
        timeout=15,
        env={"PGCONNECT_TIMEOUT": "2", "PGSSLMODE": "disable"},
        stdout=subprocess.DEVNULL,
    )
    throttled = time.monotonic() - started
    assert limited.returncode == 0, limited.stderr

    assert throttled > 2.0, (
        "rate limit had no visible effect: baseline={:.3f}s throttled={:.3f}s"
        .format(baseline, throttled)
    )
    assert throttled > baseline * 3, (
        "rate limit effect too small: baseline={:.3f}s throttled={:.3f}s"
        .format(baseline, throttled)
    )


def test_netem_loss_creates_real_replication_lag_then_recovers(lab):
    """Prove that packet loss stalls streaming replication, then catchup heals it."""
    primary, replica = make_pair(lab)

    primary.execute("create table walhog(id int primary key, payload text)")
    primary.execute("insert into walhog values (1, repeat('a', 1024))")
    replica.catchup()

    baseline_lag = primary.execute("""
        select coalesce(
            pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),
            0
        )::bigint
        from pg_stat_replication
        where application_name = 'r1'
    """)[0][0]
    assert baseline_lag < 1024 * 1024

    replica.netem(loss_pct=100)

    primary.execute("""
        insert into walhog
        select g, repeat(md5(g::text), 32)
        from generate_series(2, 20002) as g
    """)
    primary.execute("select pg_switch_wal()")
    time.sleep(2.0)

    lag = primary.execute("""
        select coalesce(
            pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),
            0
        )::bigint
        from pg_stat_replication
        where application_name = 'r1'
    """)[0][0]

    assert lag > 1024 * 1024, "expected replication lag, got {} bytes".format(lag)

    replica.clear_netem()
    replica.catchup()

    assert replica.execute(
        "select count(*), max(id) from walhog"
    ) == [(20002, 20002)]

    recovered_lag = primary.execute("""
        select coalesce(
            pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),
            0
        )::bigint
        from pg_stat_replication
        where application_name = 'r1'
    """)[0][0]

    assert recovered_lag < lag


def test_replica_cannot_recover_after_required_wal_is_recycled(lab):
    """A long enough outage must become unrecoverable without WAL retention.

    This deliberately runs without a physical replication slot and with
    wal_keep_size=0.  We first break replica egress with netem, wait until the
    primary drops the walsender, then advance/checkpoint WAL until the segment
    needed by the replica has actually disappeared from pg_wal.  Restoring the
    network must not magically heal the replica: a new base backup (or an
    archive containing the missing WAL) is required.
    """
    primary = lab.create_primary("p1", node_ip(lab, 10))
    primary.init()

    # Keep the test compact and deterministic: once the replica is disconnected,
    # old WAL can be recycled aggressively.
    primary.append_conf(
        wal_keep_size=0,
        min_wal_size="32MB",
        max_wal_size="64MB",
        wal_sender_timeout="2s",
        checkpoint_timeout="1h",
    )
    primary.start()

    replica = lab.create_replica("p1", "r1", node_ip(lab, 11))
    replica.start()
    replica.catchup()

    primary.execute("create table wal_gap(id int primary key)")
    primary.execute("insert into wal_gap values (1)")
    replica.catchup()
    assert replica.execute("select array_agg(id order by id) from wal_gap") == [([1],)]

    # Capture the point at which both sides are known to be caught up.
    disconnect_lsn = primary.execute(
        "select pg_current_wal_lsn()::text"
    )[0][0]
    required_wal = primary.execute(
        "select pg_walfile_name('{}'::pg_lsn)".format(disconnect_lsn)
    )[0][0]

    assert primary.execute(
        "select exists (select 1 from pg_ls_waldir() where name = '{}')".format(
            required_wal
        )
    ) == [(True,)]

    # 100% loss on replica egress drops ACKs/status replies.  With the short
    # wal_sender_timeout the primary closes the old streaming connection; all
    # subsequent reconnect attempts from the replica are also dropped.
    replica.netem(loss_pct=100)

    wait_until(
        lambda: primary.execute("""
            select count(*)
              from pg_stat_replication
             where application_name = 'r1'
        """)[0][0] == 0,
        timeout=10,
        desc="walsender to disappear after netem loss",
    )

    # No WAL is generated between the last catchup and the walsender going
    # away.  Now move sufficiently far ahead and checkpoint until the segment
    # containing the replica's required position is really gone.  We test the
    # filesystem-visible WAL set rather than assuming that N switches are enough
    # on every PostgreSQL version/configuration.
    removed = False
    switches = 0

    for _ in range(8):
        primary.execute(
            "select pg_switch_wal() from generate_series(1, 4)"
        )
        switches += 4
        primary.execute("checkpoint")

        exists = primary.execute(
            "select exists (select 1 from pg_ls_waldir() where name = '{}')".format(
                required_wal
            )
        )[0][0]

        if not exists:
            removed = True
            break

    assert removed, (
        "required WAL {} was still present after {} forced WAL switches"
        .format(required_wal, switches)
    )

    # Put an unmistakable change after the WAL gap.
    primary.execute("insert into wal_gap values (2)")
    target_lsn = primary.execute(
        "select pg_current_wal_lsn()::text"
    )[0][0]

    print(
        "WAL gap created: disconnect_lsn={}, removed_wal={}, "
        "switches={}, target_lsn={}".format(
            disconnect_lsn, required_wal, switches, target_lsn
        )
    )

    # The network is healthy again, but the missing WAL is no longer available
    # from the primary.  Do NOT call Testgres catchup() here: its current
    # implementation waits indefinitely (max_attempts=0) by design.
    replica.clear_netem()

    # Give walreceiver several reconnect attempts.  The replica itself is
    # reachable from the controller again, so we can observe that replay does
    # not cross the gap.
    deadline = time.monotonic() + 8.0
    observed_counts = []

    while time.monotonic() < deadline:
        observed_counts.append(
            replica.execute("select count(*) from wal_gap")[0][0]
        )

        caught_up = replica.execute(
            "select pg_last_wal_replay_lsn() >= '{}'::pg_lsn".format(
                target_lsn
            )
        )[0][0]

        assert not caught_up, (
            "replica unexpectedly crossed a WAL segment that was already "
            "recycled on the primary"
        )
        time.sleep(0.5)

    assert set(observed_counts) == {1}, observed_counts
    assert primary.execute("select array_agg(id order by id) from wal_gap") == [([1, 2],)]
    assert replica.execute("select array_agg(id order by id) from wal_gap") == [([1],)]
    assert replica.execute("select pg_is_in_recovery()") == [(True,)]

    print(
        "Replica stayed behind after network recovery as expected; "
        "a new base backup or WAL archive is required."
    )
