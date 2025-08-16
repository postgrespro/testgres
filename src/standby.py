# coding: utf-8

import six


@six.python_2_unicode_compatible
class First:
    """
    Specifies a priority-based synchronous replication and makes transaction
    commits wait until their WAL records are replicated to ``num_sync``
    synchronous standbys chosen based on their priorities.

    Args:
        sync_num (int): the number of standbys that transaction need to wait
            for replies from
        standbys (:obj:`list` of :class:`.PostgresNode`): the list of standby
            nodes
    """
    def __init__(self, sync_num, standbys):
        self.sync_num = sync_num
        self.standbys = standbys

    def __str__(self):
        return u"{} ({})".format(
            self.sync_num,
            u", ".join(u"\"{}\"".format(r.name) for r in self.standbys))


@six.python_2_unicode_compatible
class Any:
    """
    Specifies a quorum-based synchronous replication and makes transaction
    commits wait until their WAL records are replicated to at least ``num_sync``
    listed standbys. Only available for Postgres 10 and newer.

    Args:
        sync_num (int): the number of standbys that transaction need to wait
            for replies from
        standbys (:obj:`list` of :class:`.PostgresNode`): the list of standby
            nodes
    """
    def __init__(self, sync_num, standbys):
        self.sync_num = sync_num
        self.standbys = standbys

    def __str__(self):
        return u"ANY {} ({})".format(
            self.sync_num,
            u", ".join(u"\"{}\"".format(r.name) for r in self.standbys))
