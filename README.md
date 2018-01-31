[![Build Status](https://travis-ci.org/postgrespro/testgres.svg?branch=master)](https://travis-ci.org/postgrespro/testgres)
[![codecov](https://codecov.io/gh/postgrespro/testgres/branch/master/graph/badge.svg)](https://codecov.io/gh/postgrespro/testgres)
[![PyPI version](https://badge.fury.io/py/testgres.svg)](https://badge.fury.io/py/testgres)

# testgres

PostgreSQL testing utility. Both Python 2.7 and 3.3+ are supported.


## Installation

To install `testgres`, run:

```
pip install testgres
```

We encourage you to use `virtualenv` for your testing environment.


## Usage

### Environment

> Note: by default testgres runs `initdb`, `pg_ctl`, `psql` provided by `PATH`.

There are several ways to specify a custom postgres installation:

* export `PG_CONFIG` environment variable pointing to the `pg_config` executable;
* export `PG_BIN` environment variable pointing to the directory with executable files.

Example:

```bash
export PG_BIN=$HOME/pg_10/bin
python my_tests.py
```


### Logging

By default, `cleanup()` removes all temporary files (DB files, logs etc) that were created by testgres' API methods.
If you'd like to keep logs, execute `configure_testgres(node_cleanup_full=False)` before running any tests.

> Note: context managers (aka `with`) call `stop()` and `cleanup()` automatically.

testgres supports [python logging](https://docs.python.org/3.6/library/logging.html),
which means that you can aggregate logs from several nodes into one file:

```python
import logging

# write everything to /tmp/testgres.log
logging.basicConfig(filename='/tmp/testgres.log')

# create two different nodes with logging
node1 = testgres.get_new_node(use_logging=True).init().start()
node2 = testgres.get_new_node(use_logging=True).init().start()

# execute a few queries
node1.execute('postgres', 'select 1')
node2.execute('postgres', 'select 2')
```


### Examples

Here is an example of what you can do with `testgres`:

```python
with testgres.get_new_node('test') as node:
    node.init()  # run initdb
    node.start() # start PostgreSQL
    print(node.execute('postgres', 'select 1'))
    node.stop()  # stop PostgreSQL
```

Let's walk through the code. First, you create a new node using:

```python
with testgres.get_new_node('master') as node:
```

or

```python
with testgres.get_new_node('master', '/path/to/DB') as node:
```

where `master` is a node's application name. Name matters if you're testing something like replication.
Function `get_new_node()` only creates directory structure in specified directory (or somewhere in '/tmp' if
we did not specify base directory) for cluster. After that, we have to initialize the PostgreSQL cluster:

```python
node.init()
```

This function runs `initdb` command and adds some basic configuration to `postgresql.conf` and `pg_hba.conf` files.
Function `init()` accepts optional parameter `allows_streaming` which configures cluster for streaming replication (default is `False`).
Now we are ready to start:

```python
node.start()
```

Finally, our temporary cluster is able to process queries. There are four ways to run them:

| Command | Description |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `node.psql(dbname, query)` | Runs query via `psql` command and returns tuple `(error code, stdout, stderr)`. |
| `node.safe_psql(dbname, query)` | Same as `psql()` except that it returns only `stdout`. If an error occures during the execution, an exception will be thrown. |
| `node.execute(dbname, query)` | Connects to PostgreSQL using `psycopg2` or `pg8000` (depends on which one is installed in your system) and returns two-dimensional array with data. |
| `node.connect(dbname, username)` | Returns connection wrapper (`NodeConnection`) capable of running several queries within a single transaction. |

The last one is the most powerful: you can use `begin(isolation_level)`, `commit()` and `rollback()`:
```python
with node.connect() as con:
    con.begin('serializable')
    print(con.execute('select %s', 1))
    con.rollback()
```

To stop the server, run:

```python
node.stop()
```


### Backup & replication

It's quite easy to create a backup and start a new replica:

```python
with testgres.get_new_node('master') as master:
    master.init().start()
    with master.backup() as backup:
        # create and start a new replica
        replica = backup.spawn_replica('replica').start()

        # catch up with master node
        replica.catchup()

        # execute a dummy query
        print(replica.execute('postgres', 'select 1'))
```

### Benchmarks

`testgres` also can help you to make benchmarks using `pgbench` from postgres installation:

```python
with testgres.get_new_node('master') as master:
    # start a new node
    master.init().start()

    # initialize default DB and run bench for 10 seconds
    res = master.pgbench_init(scale=2).pgbench_run(time=10)
    print(res)
```


## Authors

[Ildar Musin](https://github.com/zilder) <i.musin(at)postgrespro.ru> Postgres Professional Ltd., Russia     
[Dmitry Ivanov](https://github.com/funbringer) <d.ivanov(at)postgrespro.ru> Postgres Professional Ltd., Russia   
[Ildus Kurbangaliev](https://github.com/ildus) <i.kurbangaliev(at)postgrespro.ru> Postgres Professional Ltd., Russia     
[Yury Zhuravlev](https://github.com/stalkerg) <stalkerg(at)gmail.com>
