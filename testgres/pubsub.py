# coding: utf-8

from six import raise_from, iteritems

from .defaults import default_dbname, default_username
from .exceptions import CatchUpException
from .utils import pg_version_ge


class Publication(object):
    def __init__(self, pubname, node, tables=None, dbname=None, username=None):
        """
        Constructor

        Args:
            pubname: publication name
            node: publisher's node
            tables: tables list or None for all tables
            dbname: database name used to connect and perform subscription
            username: username used to connect to the database
        """
        self.name = pubname
        self.node = node
        self.dbname = dbname or default_dbname()
        self.username = username or default_username()

        # create publication in database
        t = 'table ' + ', '.join(tables) if tables else 'all tables'
        query = "create publication {} for {}"
        node.safe_psql(query.format(pubname, t),
                       dbname=dbname,
                       username=username)

    def close(self, dbname=None, username=None):
        """
        Drop publication
        """
        self.node.safe_psql("drop publication {}".format(self.name),
                            dbname=dbname, username=username)

    def add_tables(self, tables, dbname=None, username=None):
        """
        Add tables

        Args:
            tables: a list of tables to add to the publication
        """
        if not tables:
            raise ValueError("Tables list is empty")

        query = "alter publication {} add table {}"
        self.node.safe_psql(query.format(self.name, ', '.join(tables)),
                            dbname=dbname or self.dbname,
                            username=username or self.username)


class Subscription(object):
    def __init__(self,
                 subname,
                 node,
                 publication,
                 dbname=None,
                 username=None,
                 **kwargs):
        """
        Constructor

        Args:
            subname: subscription name
            node: subscriber's node
            publication: Publication object we are subscribing to
            dbname: database name used to connect and perform subscription
            username: username used to connect to the database
            **kwargs: subscription parameters (see CREATE SUBSCRIPTION
                in PostgreSQL documentation for more information)
        """
        self.name = subname
        self.node = node
        self.pub = publication

        # connection info
        conninfo = (
            u"dbname={} user={} host={} port={}"
        ).format(self.pub.dbname,
                 self.pub.username,
                 self.pub.node.host,
                 self.pub.node.port)

        query = (
            "create subscription {} connection '{}' publication {}"
        ).format(subname, conninfo, self.pub.name)

        # additional parameters
        if kwargs:
            params = ','.join('{}={}'.format(k, v) for k, v in iteritems(kwargs))
            query += " with ({})".format(params)

        node.safe_psql(query, dbname=dbname, username=username)

    def disable(self, dbname=None, username=None):
        """
        Disables the running subscription.
        """
        query = "alter subscription {} disable"
        self.node.safe_psql(query.format(self.name),
                            dbname=None,
                            username=None)

    def enable(self, dbname=None, username=None):
        """
        Enables the previously disabled subscription.
        """
        query = "alter subscription {} enable"
        self.node.safe_psql(query.format(self.name),
                            dbname=None,
                            username=None)

    def refresh(self, copy_data=True, dbname=None, username=None):
        """
        Disables the running subscription.
        """
        query = "alter subscription {} refresh publication with (copy_data={})"
        self.node.safe_psql(query.format(self.name, copy_data),
                            dbname=dbname,
                            username=username)

    def close(self, dbname=None, username=None):
        """
        Drops subscription
        """
        self.node.safe_psql("drop subscription {}".format(self.name),
                            dbname=dbname, username=username)

    def catchup(self, username=None):
        """
        Wait until subscription catches up with publication.

        Args:
            username: remote node's user name
        """
        query = (
            "select pg_current_wal_lsn() - replay_lsn = 0 "
            "from pg_stat_replication where application_name = '{}'"
        ).format(self.name)

        try:
            # wait until this LSN reaches subscriber
            self.pub.node.poll_query_until(
                query=query,
                dbname=self.pub.dbname,
                username=username or self.pub.username,
                max_attempts=60)
        except Exception as e:
            raise_from(CatchUpException("Failed to catch up", query), e)
