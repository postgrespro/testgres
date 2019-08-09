[![Build Status](https://travis-ci.org/postgrespro/testgres.svg?branch=master)](https://travis-ci.org/postgrespro/testgres)
[![codecov](https://codecov.io/gh/postgrespro/testgres/branch/master/graph/badge.svg)](https://codecov.io/gh/postgrespro/testgres)
[![PyPI version](https://badge.fury.io/py/testgres.svg)](https://badge.fury.io/py/testgres)

[Documentation](https://postgrespro.github.io/testgres/)

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


### Examples

Here is an example of what you can do with `testgres`:

```python
# create a node with random name, port, etc
with testgres.get_new_node() as node:

    # run inidb
    node.init()

    # start PostgreSQL
    node.start()

    # execute a query in a default DB
    print(node.execute('select 1'))

# ... node stops and its files are about to be removed
```

There are four API methods for runnig queries:

| Command | Description |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `node.psql(query, ...)` | Runs query via `psql` command and returns tuple `(error code, stdout, stderr)`. |
| `node.safe_psql(query, ...)` | Same as `psql()` except that it returns only `stdout`. If an error occures during the execution, an exception will be thrown. |
| `node.execute(query, ...)` | Connects to PostgreSQL using `psycopg2` or `pg8000` (depends on which one is installed in your system) and returns two-dimensional array with data. |
| `node.connect(dbname, ...)` | Returns connection wrapper (`NodeConnection`) capable of running several queries within a single transaction. |

The last one is the most powerful: you can use `begin(isolation_level)`, `commit()` and `rollback()`:
```python
with node.connect() as con:
    con.begin('serializable')
    print(con.execute('select %s', 1))
    con.rollback()
```


### Logging

By default, `cleanup()` removes all temporary files (DB files, logs etc) that were created by testgres' API methods.
If you'd like to keep logs, execute `configure_testgres(node_cleanup_full=False)` before running any tests.

> Note: context managers (aka `with`) call `stop()` and `cleanup()` automatically.

`testgres` supports [python logging](https://docs.python.org/3.6/library/logging.html),
which means that you can aggregate logs from several nodes into one file:

```python
import logging

# write everything to /tmp/testgres.log
logging.basicConfig(filename='/tmp/testgres.log')

# enable logging, and create two different nodes
testgres.configure_testgres(use_python_logging=True)
node1 = testgres.get_new_node().init().start()
node2 = testgres.get_new_node().init().start()

# execute a few queries
node1.execute('select 1')
node2.execute('select 2')

# disable logging
testgres.configure_testgres(use_python_logging=False)
```

Look at `tests/test_simple.py` file for a complete example of the logging
configuration.


### Backup & replication

It's quite easy to create a backup and start a new replica:

```python
with testgres.get_new_node('master') as master:
    master.init().start()

    # create a backup
    with master.backup() as backup:

        # create and start a new replica
        replica = backup.spawn_replica('replica').start()

        # catch up with master node
        replica.catchup()

        # execute a dummy query
        print(replica.execute('postgres', 'select 1'))
```

### Benchmarks

`testgres` is also capable of running benchmarks using `pgbench`:

```python
with testgres.get_new_node('master') as master:
    # start a new node
    master.init().start()

    # initialize default DB and run bench for 10 seconds
    res = master.pgbench_init(scale=2).pgbench_run(time=10)
    print(res)
```


### Custom configuration

It's often useful to extend default configuration provided by `testgres`.

`testgres` has `default_conf()` function that helps control some basic
options. The `append_conf()` function can be used to add custom
lines to configuration lines:

```python
ext_conf = "shared_preload_libraries = 'postgres_fdw'"

# initialize a new node
with testgres.get_new_node().init() as master:

    # ... do something ...
	
    # reset main config file
    master.default_conf(fsync=True,
                        allow_streaming=True)

    # add a new config line
    master.append_conf('postgresql.conf', ext_conf)
```

Note that `default_conf()` is called by `init()` function; both of them overwrite
the configuration file, which means that they should be called before `append_conf()`.


## Authors

[Ildar Musin](https://github.com/zilder)  
[Dmitry Ivanov](https://github.com/funbringer)  
[Ildus Kurbangaliev](https://github.com/ildus)  
[Yury Zhuravlev](https://github.com/stalkerg)  
