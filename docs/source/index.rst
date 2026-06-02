
Testgres documentation
======================

Utility for orchestrating temporary PostgreSQL clusters in Python tests. Supports Python 3.7.17 and newer.

Installation
============

To install testgres, run:

.. code-block:: bash

    pip install testgres

We encourage you to use ``virtualenv`` for your testing environment.

Usage
=====

Environment
-----------

Note: by default ``testgres`` runs ``initdb``, ``pg_ctl``, and ``psql`` found in ``PATH``.

There are several ways to specify a custom PostgreSQL installation:

- export ``PG_CONFIG`` environment variable pointing to the ``pg_config`` executable;
- export ``PG_BIN`` environment variable pointing to the directory with executable files.

Example:

.. code-block:: bash

    export PG_BIN=$HOME/pg_16/bin
    python my_tests.py

Examples
--------

Create a temporary node, run queries, and let ``testgres`` clean up automatically:

.. code-block:: python

    # create a node with a random name, port, and data directory
    with testgres.get_new_node() as node:

        # run initdb
        node.init()

        # start PostgreSQL
        node.start()

        # execute a query in the default database
        print(node.execute('select 1'))

    # the node is stopped and its files are removed automatically

Query helpers
-------------

``testgres`` provides four helpers for executing queries against the node:

========================== =======================================================
Command                    Description
========================== =======================================================
``node.psql(query, ...)``  Runs the query via ``psql`` and returns ``(code, out, err)``.
``node.safe_psql(...)``    Returns only ``stdout`` and raises if the command fails.
``node.execute(...)``      Uses ``psycopg2``/``pg8000`` and returns a list of tuples.
``node.connect(...)``      Returns a ``NodeConnection`` for transactional usage.
========================== =======================================================

Example:

.. code-block:: python

    with node.connect() as con:
        con.begin('serializable')
        print(con.execute('select %s', 1))
        con.rollback()

Logging
-------

By default ``cleanup()`` removes all temporary files (data directories, logs, and so on) created by the API. Call ``configure_testgres(node_cleanup_full=False)`` before starting nodes if you want to keep logs for inspection.

Note: context managers (the ``with`` statement) call ``stop()`` and ``cleanup()`` automatically.

``testgres`` integrates with the standard `Python logging <https://docs.python.org/3/library/logging.html>`_ module, so you can aggregate logs from multiple nodes:

.. code-block:: python

    import logging

    logging.basicConfig(filename='/tmp/testgres.log')

    testgres.configure_testgres(use_python_logging=True)
    node1 = testgres.get_new_node().init().start()
    node2 = testgres.get_new_node().init().start()

    node1.execute('select 1')
    node2.execute('select 2')

    testgres.configure_testgres(use_python_logging=False)

Backup & replication
--------------------

It's quite easy to create a backup and start a new replica:

.. code-block:: python

    with testgres.get_new_node('master') as master:
        master.init().start()

        # create a backup
        with master.backup() as backup:

            # create and start a new replica
            replica = backup.spawn_replica('replica').start()

            replica.catchup()

            print(replica.execute('postgres', 'select 1'))

Benchmarks
----------

Use ``pgbench`` through ``testgres`` to run quick benchmarks:

.. code-block:: python

    with testgres.get_new_node('master') as master:
        master.init().start()

        result = master.pgbench_init(scale=2).pgbench_run(time=10)
        print(result)

Custom configuration
--------------------

``testgres`` ships with sensible defaults. Adjust them as needed with ``default_conf()`` and ``append_conf()``:

.. code-block:: python

    extra_conf = "shared_preload_libraries = 'postgres_fdw'"

    with testgres.get_new_node().init() as master:
        master.default_conf(fsync=True, allow_streaming=True)
        master.append_conf('postgresql.conf', extra_conf)

``default_conf()`` is called by ``init()`` and rewrites the configuration file. Apply ``append_conf()`` afterwards to keep custom lines.

Remote mode
-----------

Provision nodes on a remote host (Linux only) by wiring ``RemoteOperations`` into the configuration:

.. code-block:: python

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

Pytest integration
------------------

Use fixtures to create and clean up nodes automatically when testing with ``pytest``:

.. code-block:: python

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

Scaling tips
------------

* Run tests in parallel with ``pytest -n auto`` (requires ``pytest-xdist``). Set unique ports by passing ``port`` to ``get_new_node()`` or exporting ``PGPORT`` in the fixture.
* Always call ``node.cleanup()`` after each test, or rely on context managers/fixtures that do it for you, to avoid leftover data directories.
* Prefer ``safe_psql()`` for quick assertions, and ``execute()`` when you need Python data structures.

Modules
=======

.. toctree::
   :maxdepth: 2

   testgres


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
