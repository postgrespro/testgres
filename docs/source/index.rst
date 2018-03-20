
Testgres documentation
======================

Testgres is a PostgreSQL testing utility

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

Note: by default testgres runs ``initdb``, ``pg_ctl``, ``psql`` provided by ``PATH``.

There are several ways to specify a custom postgres installation:

- export ``PG_CONFIG`` environment variable pointing to the ``pg_config`` executable;
- export ``PG_BIN`` environment variable pointing to the directory with executable files.

Example:

.. code-block:: bash

    export PG_BIN=$HOME/pg_10/bin
    python my_tests.py

Examples
--------

Here is an example of what you can do with ``testgres``:

.. code-block:: python

    # create a node with random name, port, etc
    with testgres.get_new_node() as node:

        # run inidb
        node.init()

        # start PostgreSQL
        node.start()

        # execute a query in a default DB
        print(node.execute('select 1'))

    # ... node stops and its files are about to be removed

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

            # catch up with master node
            replica.catchup()

            # execute a dummy query
            print(replica.execute('postgres', 'select 1'))

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
