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

By default, `cleanup()` removes all temporary files (DB files, logs etc) that were created by testgres' API methods. If you'd like to keep logs, execute `configure_testgres(node_cleanup_full=False)` before running any tests.

> Note: context managers (aka `with`) call `cleanup()` automatically.

Nodes support python logging system, so if you have configured logging
in your tests, you can use it to redirect postgres logs to yours.

To do that, just use `use_logging` argument:

```python
node = testgres.get_new_node('master', use_logging=True)
```

You can find working configuration example for logging in `tests/test_simple.py`.


### Examples

Here is an example of what you can do with `testgres`:

```python
import testgres

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

where `master` is a node's name, not a DB's name. Name matters if you're testing something like replication. Function `get_new_node()` only creates directory structure in specified directory (or somewhere in '/tmp' if we did not specify base directory) for cluster. After that, we have to initialize the PostgreSQL cluster:

```python
node.init()
```

This function runs `initdb` command and adds some basic configuration to `postgresql.conf` and `pg_hba.conf` files. Function `init()` accepts optional parameter `allows_streaming` which configures cluster for streaming replication (default is `False`).
Now we are ready to start:

```python
node.start()
```

Finally, our temporary cluster is able to process queries. There are four ways to run them:

| Command                                                     | Description                                                                                                                                         |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `node.psql(database, query)`                                | Runs query via `psql` command and returns tuple `(error code, stdout, stderr)`.                                                                     |
| `node.safe_psql(database, query)`                           | Same as `psql()` except that it returns only `stdout`. If an error occures during the execution, an exception will be thrown.                       |
| `node.execute(database, query, username=None, commit=True)` | Connects to PostgreSQL using `psycopg2` or `pg8000` (depends on which one is installed in your system) and returns two-dimensional array with data. |
| `node.connect(database='postgres')`                         | Returns connection wrapper (`NodeConnection`) capable of running several queries within a single transaction.                                       |

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
        replica = backup.spawn_replica('replica').start()
        print(replica.execute('postgres', 'select 1'))
```

> Note: you could take a look at [`pg_pathman`](https://github.com/postgrespro/pg_pathman) to get an idea of `testgres`' capabilities.

### Benchmarks

`testgres` also can help you to make benchmarks using `pgbench` from postgres installation:

```python
with testgres.get_new_node('master') as master:
    master.init().start()
    p = master.pg_bench_init(scale=10).pgbench(options=['-T', '60'])
    p.wait()
```

## Authors

[Ildar Musin](https://github.com/zilder) <i.musin(at)postgrespro.ru> Postgres Professional Ltd., Russia     
[Dmitry Ivanov](https://github.com/funbringer) <d.ivanov(at)postgrespro.ru> Postgres Professional Ltd., Russia   
[Ildus Kurbangaliev](https://github.com/ildus) <i.kurbangaliev(at)postgrespro.ru> Postgres Professional Ltd., Russia     
[Yury Zhuravlev](https://github.com/stalkerg) <stalkerg(at)gmail.com>
