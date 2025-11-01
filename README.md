[![Build Status](https://api.travis-ci.com/postgrespro/testgres.svg?branch=master)](https://travis-ci.com/github/postgrespro/testgres)
[![codecov](https://codecov.io/gh/postgrespro/testgres/branch/master/graph/badge.svg)](https://codecov.io/gh/postgrespro/testgres)
[![PyPI version](https://badge.fury.io/py/testgres.svg)](https://badge.fury.io/py/testgres)

[Documentation](https://postgrespro.github.io/testgres/)

# testgres

Utility for orchestrating temporary PostgreSQL clusters in Python tests. Supports Python 3.7.17 and newer.

## Installation

Install `testgres` from PyPI:

```sh
pip install testgres
```

Use a dedicated virtual environment for isolated test dependencies.

## Usage

### Environment

> Note: by default `testgres` invokes `initdb`, `pg_ctl`, and `psql` binaries found in `PATH`.

Specify a custom PostgreSQL installation in one of the following ways:

- Set the `PG_CONFIG` environment variable to point to the `pg_config` executable.
- Set the `PG_BIN` environment variable to point to the directory with PostgreSQL binaries.

Example:

```sh
export PG_BIN=$HOME/pg_16/bin
python my_tests.py
```

### Examples

Create a temporary node, run queries, and let `testgres` clean up automatically:

```python
# create a node with a random name, port, and data directory
with testgres.get_new_node() as node:

    # run initdb
    node.init()

    # start PostgreSQL
    node.start()

    # execute a query in the default database
    print(node.execute('select 1'))

# the node is stopped and its files are removed automatically
```

### Query helpers

`testgres` provides four helpers for executing queries against the node:

| Command | Description |
|---------|-------------|
| `node.psql(query, ...)` | Runs the query via `psql` and returns a tuple `(returncode, stdout, stderr)`. |
| `node.safe_psql(query, ...)` | Same as `psql()` but returns only `stdout` and raises if the command fails. |
| `node.execute(query, ...)` | Connects via `psycopg2` or `pg8000` (whichever is available) and returns a list of tuples. |
| `node.connect(dbname, ...)` | Returns a `NodeConnection` wrapper for executing multiple statements within a transaction. |

Example of transactional usage:

```python
with node.connect() as con:
    con.begin('serializable')
    print(con.execute('select %s', 1))
    con.rollback()
```

### Logging

By default `cleanup()` removes all temporary files (data directories, logs, and so on) created by the API. Call `configure_testgres(node_cleanup_full=False)` before starting nodes if you want to keep logs for inspection.

> Note: context managers (the `with` statement) call `stop()` and `cleanup()` automatically.

`testgres` integrates with the standard [Python logging](https://docs.python.org/3/library/logging.html) module, so you can aggregate logs from multiple nodes:

```python
import logging

# write everything to /tmp/testgres.log
logging.basicConfig(filename='/tmp/testgres.log')

# enable logging and create two nodes
testgres.configure_testgres(use_python_logging=True)
node1 = testgres.get_new_node().init().start()
node2 = testgres.get_new_node().init().start()

node1.execute('select 1')
node2.execute('select 2')

# disable logging
testgres.configure_testgres(use_python_logging=False)
```

See `tests/test_simple.py` for a complete logging example.

### Backup and replication

Creating backups and spawning replicas is straightforward:

```python
with testgres.get_new_node('master') as master:
    master.init().start()

    with master.backup() as backup:
        replica = backup.spawn_replica('replica').start()
        replica.catchup()

        print(replica.execute('postgres', 'select 1'))
```

### Benchmarks

Use `pgbench` through `testgres` to run quick benchmarks:

```python
with testgres.get_new_node('master') as master:
    master.init().start()

    result = master.pgbench_init(scale=2).pgbench_run(time=10)
    print(result)
```

### Custom configuration

`testgres` ships with sensible defaults. Adjust them as needed with `default_conf()` and `append_conf()`:

```python
extra_conf = "shared_preload_libraries = 'postgres_fdw'"

with testgres.get_new_node().init() as master:
    master.default_conf(fsync=True, allow_streaming=True)
    master.append_conf('postgresql.conf', extra_conf)
```

`default_conf()` is called by `init()` and rewrites the configuration file. Apply `append_conf()` afterwards to keep custom lines.

### Remote mode

You can provision nodes on a remote host (Linux only) by wiring `RemoteOperations` into the configuration:

```python
from testgres import ConnectionParams, RemoteOperations, TestgresConfig, get_remote_node

conn_params = ConnectionParams(
    host='example.com',
    username='postgres',
    ssh_key='/path/to/ssh/key'
)
os_ops = RemoteOperations(conn_params)

TestgresConfig.set_os_ops(os_ops=os_ops)

def test_basic_query():
    with get_remote_node(conn_params=conn_params) as node:
        node.init().start()
        assert node.execute('SELECT 1') == [(1,)]
```

### Pytest integration

Use fixtures to create and clean up nodes automatically when testing with `pytest`:

```python
import pytest
import testgres

@pytest.fixture
def pg_node():
    node = testgres.get_new_node().init().start()
    try:
        yield node
    finally:
        node.stop()
        node.cleanup()

def test_simple(pg_node):
    assert pg_node.execute('select 1')[0][0] == 1
```

This pattern keeps tests concise and ensures that every node is stopped and removed even if the test fails.

### Scaling tips

- Run tests in parallel with `pytest -n auto` (requires `pytest-xdist`). Ensure each node uses a distinct port by setting `PGPORT` in the fixture or by passing the `port` argument to `get_new_node()`.
- Always call `node.cleanup()` after each test, or rely on context managers/fixtures that do it for you, to avoid leftover data directories.
- Prefer `node.safe_psql()` for lightweight assertions that should fail fast; use `node.execute()` when you need structured Python results.

## Authors

[Ildar Musin](https://github.com/zilder)  
[Dmitry Ivanov](https://github.com/funbringer)  
[Ildus Kurbangaliev](https://github.com/ildus)  
[Yury Zhuravlev](https://github.com/stalkerg)
