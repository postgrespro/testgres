# testgres

Postgres testing utility

## Installation

To install `testgres` run:

```
pip install testgres
```

At current state it only works with Python 2.*

## Usage


> Node: by default testgres runs `initdb`, `pg_ctl`, `psql` commands from the `$PATH`. To specify custom postgres installation set environment variable `$PG_CONFIG` and point it to pg_config executable: `export PG_CONFIG=/path/to/pg_config`

Here is an example of how you can use `testgres`.

```python
from testgres import get_new_node

try:
    node = get_new_node('master')
    node.init()
    node.start()
    stdout = node.safe_psql('postgres', 'SELECT 1')
    print stdout
    node.stop()
except ClusterException, e:
    node.cleanup()
```

Let's walk through the code. First you create new node:

```python
node = get_new_node('master')
```

`master` here is a node's name, not the database's name. The name matters if you're testing replication. Function `get_new_node()` only creates directory structure in `/tmp` for cluster. Next line:

```python
node.init()
```

initializes cluster. On low level it runs `initdb` command and adds some basic configuration to `postgresql.conf` and `pg_hba.conf` files. Function `init()` accepts optional parameter `allows_streaming` which configures cluster for streaming replication (default is `False`).
Now we are ready to start:

```python
node.start()
```

After this you are able to run queries over the cluster. There are three functions to do that:

* `node.psql(database, query)` - runs query via `psql` command and returns tuple (error code, stdout, stderr)
* `node.safe_psql(database, query)` - the same as `psql()` except that it returns only `stdout`. If error occures during the execution then it will throw an exception;
* `node.execute(database, query)` - connects with postgresql server using `psycopg2` or `pg8000` library (depends on which is installed in your system) and returns two-dimensional array with data.

To stop server run:

```python
node.stop()
```

Please see `testgres/tests` directory for replication configuration example.
