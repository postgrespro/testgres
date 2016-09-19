[![Build Status](https://travis-ci.org/postgrespro/testgres.svg?branch=master)](https://travis-ci.org/postgrespro/testgres)
[![PyPI version](https://badge.fury.io/py/testgres.svg)](https://badge.fury.io/py/testgres)

# testgres

PostgreSQL testing utility.


## Installation

To install `testgres`, run:

```
pip install testgres
```

We encourage you to use `virtualenv` for your testing environment. Both Python 2.7 and 3.5 are supported.


## Usage

> Note: by default testgres runs `initdb`, `pg_ctl`, `psql` provided by `$PATH`. To specify a custom postgres installation, set the environment variable `$PG_CONFIG` pointing to the `pg_config` executable: `export PG_CONFIG=/path/to/pg_config`.

Here is an example of what you can do with `testgres`:

```python
import testgres

node = None
try:
    node = testgres.get_new_node('test').init().start()
    print(node.execute('postgres','127.0.0.1', 'select 1'))
except testgres.ClusterException as e:
    print(e)
finally:
    if node is not None:
        node.cleanup()
```

Let's walk through the code. First you create new node:

```python
node = testgres.get_new_node('master')
```

or:

```python
node = testgres.get_new_node('master', '/path/to/base')
```

`master` is a node's name, not the DB's name. The name matters if you're testing something like replication. Function `get_new_node()` only creates directory structure in specified directory (or somewhere in '/tmp' if we did not specify base directory) for cluster. After that, we have to initialize the PostgreSQL cluster:

```python
node.init()
```

This function runs `initdb` command and adds some basic configuration to `postgresql.conf` and `pg_hba.conf` files. Function `init()` accepts optional parameter `allows_streaming` which configures cluster for streaming replication (default is `False`).
Now we are ready to start:

```python
node.start()
```

Finally our temporary cluster is able to process queries. There are four ways to run them:

* `node.psql(database, query)` - runs query via `psql` command and returns tuple `(error code, stdout, stderr)`
* `node.safe_psql(database, query)` - same as `psql()` except that it returns only `stdout`. If an error occures during the execution, an exception will be thrown.
* `node.execute(database, host, query)` - connects to postgresql server using `psycopg2` or `pg8000` library (depends on which is installed in your system) and returns two-dimensional array with data.
* `node.connect(database='postgres')` - returns connection wrapper (`NodeConnection`) capable of running several queries within a single transaction.

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

It is essential to clean everything up, so make sure to call `node.cleanup()` once you've finished all of your tests.

Please see `testgres/tests` directory for replication configuration example.
> Note: you could take a look at [`pg_pathman`](https://github.com/postgrespro/pg_pathman) to get an idea of `testgres`' capabilities.


## Authors

Ildar Musin <i.musin@postgrespro.ru> Postgres Professional Ltd., Russia		
Dmitry Ivanov <d.ivanov@postgrespro.ru> Postgres Professional Ltd., Russia		
