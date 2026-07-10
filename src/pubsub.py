# coding: utf-8
"""
Unlike physical replication the logical replication allows users replicate only
specified databases and tables. It uses publish-subscribe model with possibly
multiple publishers and multiple subscribers. When initializing publisher's
node ``allow_logical=True`` should be passed to the :meth:`.PostgresNode.init()`
method to enable PostgreSQL to write extra information to the WAL needed by
logical replication.

To replicate table ``X`` from node A to node B the same table structure should
be defined on the subscriber's node as logical replication don't replicate DDL.
After that :meth:`~.PostgresNode.publish()` and :meth:`~.PostgresNode.subscribe()`
methods may be used to setup replication. Example:

>>> from testgres import get_new_node
>>> with get_new_node() as nodeA, get_new_node() as nodeB:
...     nodeA.init(allow_logical=True).start()
...     nodeB.init().start()
...
...     # create same table both on publisher and subscriber
...     create_table = 'create table test (a int, b int)'
...     nodeA.safe_psql(create_table)
...     nodeB.safe_psql(create_table)
...
...     # create publication
...     pub = nodeA.publish('mypub')
...     # create subscription
...     sub = nodeB.subscribe(pub, 'mysub')
...
...     # insert some data to the publisher's node
...     nodeA.execute('insert into test values (1, 1), (2, 2)')
...
...     # wait until changes apply on subscriber and check them
...     sub.catchup()
...
...     # read the data from subscriber's node
...     nodeB.execute('select * from test')
PostgresNode(name='...', port=..., base_dir='...')
PostgresNode(name='...', port=..., base_dir='...')
''
''
[(1, 1), (2, 2)]
"""

from six import raise_from

from .consts import LOGICAL_REPL_MAX_CATCHUP_ATTEMPTS
from .defaults import default_dbname, default_username
from .exceptions import CatchUpException
from .utils import options_string


class Publication(object):
    def __init__(self, name, node, tables=None, dbname=None, username=None):
        """
        Constructor. Use :meth:`.PostgresNode.publish()` instead of direct
        constructing publication objects.

        Args:
            name: publication name.
            node: publisher's node.
            tables: tables list or None for all tables.
            dbname: database name used to connect and perform subscription.
            username: username used to connect to the database.
        """
        assert type(name) is str
        assert node is not None
        assert node.os_ops is not None
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        self.name = name
        self.node = node
        self.dbname = dbname or default_dbname()
        self.username = username or default_username()

        # create publication in database
        t = "table " + ", ".join(tables) if tables else "all tables"
        query = "create publication {} for {}"
        self.node.execute(
            query.format(name, t),
            dbname=self.dbname,
            username=self.username,
        )

    def drop(self, dbname=None, username=None):
        """
        Drop publication
        """
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   dbname and username are ignored.
        #   We will use settings of our object.
        #
        assert dbname is None or dbname == self.dbname
        assert username is None or username == self.username

        self.node.execute(
            "drop publication {}".format(self.name),
            dbname=self.dbname,
            username=self.username,
        )

    def add_tables(self, tables, dbname=None, username=None):
        """
        Add tables to the publication. Cannot be used if publication was
        created with empty tables list.

        Args:
            tables: a list of tables to be added to the publication.
        """
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   dbname and username are ignored.
        #   We will use settings of our object.
        #
        assert dbname is None or dbname == self.dbname
        assert username is None or username == self.username

        if not tables:
            raise ValueError("Tables list is empty")

        query = "alter publication {} add table {}"
        self.node.execute(
            query.format(self.name, ", ".join(tables)),
            dbname=self.dbname,
            username=self.username,
        )


class Subscription(object):
    def __init__(self,
                 node,
                 publication,
                 name=None,
                 dbname=None,
                 username=None,
                 **params):
        """
        Constructor. Use :meth:`.PostgresNode.subscribe()` instead of direct
        constructing subscription objects.

        Args:
            name: subscription name.
            node: subscriber's node.
            publication: :class:`.Publication` object we are subscribing to
                (see :meth:`.PostgresNode.publish()`).
            dbname: database name used to connect and perform subscription.
            username: username used to connect to the database.
            params: subscription parameters (see documentation on `CREATE SUBSCRIPTION
                 <https://www.postgresql.org/docs/current/static/sql-createsubscription.html>`_
                 for details).
        """
        assert type(name) is str
        assert node is not None
        assert node.os_ops is not None
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        self.name = name
        self.node = node
        self.pub = publication
        self.dbname = dbname or default_dbname()
        self.username = username or default_username()

        # connection info
        conninfo = {
            "dbname": self.pub.dbname,
            "user": self.pub.username,
            "host": self.pub.node.host,
            "port": self.pub.node.port
        }

        query = (
            "create subscription {} connection '{}' publication {}").format(
                name, options_string(**conninfo), self.pub.name)

        # additional parameters
        if params:
            query += " with ({})".format(options_string(**params))

        # Note: cannot run 'create subscription' query in transaction mode
        self.node.execute(
            query,
            dbname=self.dbname,
            username=self.username,
        )

    def disable(self, dbname=None, username=None):
        """
        Disables the running subscription.
        """
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   dbname and username are ignored.
        #   We will use settings of our object.
        #
        assert dbname is None or dbname == self.dbname
        assert username is None or username == self.username

        query = "alter subscription {} disable"
        self.node.execute(
            query.format(self.name),
            dbname=self.dbname,
            username=self.username,
        )

    def enable(self, dbname=None, username=None):
        """
        Enables the previously disabled subscription.
        """
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   dbname and username were and are ignored.
        #   We will use settings of our object.
        #
        assert dbname is None or dbname == self.dbname
        assert username is None or username == self.username

        query = "alter subscription {} enable"

        self.node.execute(
            query.format(self.name),
            dbname=self.dbname,
            username=self.username,
        )

    def refresh(self, copy_data=True, dbname=None, username=None):
        """
        Disables the running subscription.
        """
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   dbname and username are ignored.
        #   We will use settings of our object.
        #
        assert dbname is None or dbname == self.dbname
        assert username is None or username == self.username

        query = "alter subscription {} refresh publication with (copy_data={})"
        self.node.execute(
            query.format(self.name, copy_data),
            dbname=self.dbname,
            username=self.username,
        )

    def drop(self, dbname=None, username=None):
        """
        Drops subscription
        """
        assert dbname is None or type(dbname) is str
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   dbname and username are ignored.
        #   We will use settings of our object.
        #
        assert dbname is None or dbname == self.dbname
        assert username is None or username == self.username

        self.node.execute(
            "drop subscription {}".format(self.name),
            dbname=self.dbname,
            username=self.username,
        )

    def catchup(self, username=None):
        """
        Wait until subscription catches up with publication.

        Args:
            username: remote node's user name.
        """
        assert username is None or type(username) is str

        #
        # [2026-07-10] [BUG FIX]
        #   username is ignored.
        #   We will use settings of objects.
        #
        assert username is None or username == self.username

        try:
            #
            # [2026-07-10]
            #   About dbname=None and username=None
            #   We will try to use self.pub.xxx the next time. OK?
            #
            pub_lsn = self.pub.node.execute(
                query="select pg_current_wal_lsn()",
                dbname=None,
                username=None,
            )[0][0]  # yapf: disable
            # create dummy xact, as LR replicates only on commit.
            self.pub.node.execute(
                query="select txid_current()",
                dbname=None,
                username=None,
            )
            query = """
            select '{}'::pg_lsn - replay_lsn <= 0
            from pg_catalog.pg_stat_replication where application_name = '{}'
            """.format(pub_lsn, self.name)

            # wait until this LSN reaches subscriber
            self.pub.node.poll_query_until(
                query=query,
                dbname=self.pub.dbname,
                username=self.pub.username,
                max_attempts=LOGICAL_REPL_MAX_CATCHUP_ATTEMPTS,
            )

            # Now, wait until there are no tablesync workers: probably
            # replay_lsn above was sent with changes of new tables just skipped;
            # they will be eaten by tablesync workers.
            query = """
            select count(*) = 0 from pg_subscription_rel where srsubstate != 'r'
            """
            self.node.poll_query_until(
                query=query,
                dbname=self.dbname,
                username=self.username,
                max_attempts=LOGICAL_REPL_MAX_CATCHUP_ATTEMPTS,
            )
        except Exception as e:
            raise_from(CatchUpException("Failed to catch up"), e)
