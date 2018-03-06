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

from functools import wraps

from .node import PostgresNode


@wraps(PostgresNode.__init__)
def get_new_node(name=None, base_dir=None, **kwargs):
    # NOTE: leave explicit 'name' and 'base_dir' for compatibility
    return PostgresNode(name=name, base_dir=base_dir, **kwargs)
