# coding: utf-8
"""
Testing framework for PostgreSQL and its extensions

This module was created under influence of Postgres TAP test feature
(PostgresNode.pm module). It can manage Postgres clusters: initialize,
edit configuration files, start/stop cluster, execute queries. The
typical flow may look like:

    with get_new_node('test') as node:
        node.init()
        node.start()
        result = node.psql('postgres', 'SELECT 1')
        print(result)
        node.stop()

    Or:

    with get_new_node('node1') as node1:
        node1.init().start()
        with node1.backup() as backup:
            with backup.spawn_primary('node2') as node2:
                res = node2.start().execute('postgres', 'select 2')
                print(res)

Copyright (c) 2016, Postgres Professional
"""

from .node import PostgresNode


def get_new_node(name, base_dir=None, use_logging=False):
    """
    Create a new node (select port automatically).

    Args:
        name: node's name.
        base_dir: path to node's data directory.
        use_logging: enable python logging.

    Returns:
        An instance of PostgresNode.
    """

    return PostgresNode(name=name, base_dir=base_dir, use_logging=use_logging)
