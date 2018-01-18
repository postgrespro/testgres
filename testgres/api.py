# coding: utf-8
"""
Testing framework for PostgreSQL and its extensions

This module was created under influence of Postgres TAP test feature
(PostgresNode.pm module). It can manage Postgres clusters: initialize,
edit configuration files, start/stop cluster, execute queries. The
typical flow may look like:

>>> with get_new_node() as node:
...     node.init().start()
...     result = node.safe_psql('postgres', 'select 1')
...     print(result.decode('utf-8').strip())
...     node.stop()
<testgres.node.PostgresNode object at 0x...>
1
<testgres.node.PostgresNode object at 0x...>

    Or:

>>> with get_new_node() as master:
...     master.init().start()
...     with master.backup() as backup:
...         with backup.spawn_replica() as replica:
...             replica = replica.start()
...             master.execute('postgres', 'create table test (val int4)')
...             master.execute('postgres', 'insert into test values (0), (1), (2)')
...             replica.catchup()  # wait until changes are visible
...             print(replica.execute('postgres', 'select count(*) from test'))
<testgres.node.PostgresNode object at 0x...>
[(3,)]

Copyright (c) 2016, Postgres Professional
"""

from .node import PostgresNode


def get_new_node(name=None, base_dir=None, use_logging=False):
    """
    Create a new node (select port automatically).

    Args:
        name: node's application name.
        base_dir: path to node's data directory.
        use_logging: enable python logging.

    Returns:
        An instance of PostgresNode.
    """

    return PostgresNode(name=name, base_dir=base_dir, use_logging=use_logging)
