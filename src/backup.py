# coding: utf-8

from six import raise_from

from .enums import XLogMethod

from .consts import \
    DATA_DIR, \
    TMP_NODE, \
    TMP_BACKUP, \
    PG_CONF_FILE, \
    BACKUP_LOG_FILE

from .exceptions import BackupException

from testgres.operations.os_ops import OsOperations

from .utils import \
    get_bin_path2, \
    execute_utility2, \
    clean_on_error


class NodeBackup(object):
    """
    Smart object responsible for backups
    """
    @property
    def log_file(self):
        assert self.os_ops is not None
        assert isinstance(self.os_ops, OsOperations)
        return self.os_ops.build_path(self.base_dir, BACKUP_LOG_FILE)

    def __init__(self,
                 node,
                 base_dir=None,
                 username=None,
                 xlog_method=XLogMethod.fetch,
                 options=None):
        """
        Create a new backup.

        Args:
            node: :class:`.PostgresNode` we're going to backup.
            base_dir: where should we store it?
            username: database user name.
            xlog_method: none | fetch | stream (see docs)
        """
        assert node.os_ops is not None
        assert isinstance(node.os_ops, OsOperations)

        if not options:
            options = []
        self.os_ops = node.os_ops
        if not node.status():
            raise BackupException('Node must be running')

        # Check arguments
        if not isinstance(xlog_method, XLogMethod):
            try:
                xlog_method = XLogMethod(xlog_method)
            except ValueError:
                msg = 'Invalid xlog_method "{}"'.format(xlog_method)
                raise BackupException(msg)

        # Set default arguments
        username = username or self.os_ops.get_user()
        base_dir = base_dir or self.os_ops.mkdtemp(prefix=TMP_BACKUP)

        # public
        self.original_node = node
        self.base_dir = base_dir
        self.username = username

        # private
        self._available = True

        data_dir = self.os_ops.build_path(self.base_dir, DATA_DIR)

        _params = [
            get_bin_path2(self.os_ops, "pg_basebackup"),
            "-p", str(node.port),
            "-h", node.host,
            "-U", username,
            "-D", data_dir,
            "-X", xlog_method.value
        ]  # yapf: disable
        _params += options
        execute_utility2(self.os_ops, _params, self.log_file)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()

    def _prepare_dir(self, destroy):
        """
        Provide a data directory for a copy of node.

        Args:
            destroy: should we convert this backup into a node?

        Returns:
            Path to data directory.
        """

        if not self._available:
            raise BackupException('Backup is exhausted')

        # Do we want to use this backup several times?
        available = not destroy

        if available:
            assert self.os_ops is not None
            assert isinstance(self.os_ops, OsOperations)

            dest_base_dir = self.os_ops.mkdtemp(prefix=TMP_NODE)

            data1 = self.os_ops.build_path(self.base_dir, DATA_DIR)
            data2 = self.os_ops.build_path(dest_base_dir, DATA_DIR)

            try:
                # Copy backup to new data dir
                self.os_ops.copytree(data1, data2)
            except Exception as e:
                raise_from(BackupException('Failed to copy files'), e)
        else:
            dest_base_dir = self.base_dir

        # Is this backup exhausted?
        self._available = available

        # Return path to new node
        return dest_base_dir

    def spawn_primary(self, name=None, destroy=True):
        """
        Create a primary node from a backup.

        Args:
            name: primary's application name.
            destroy: should we convert this backup into a node?

        Returns:
            New instance of :class:`.PostgresNode`.
        """

        # Prepare a data directory for this node
        base_dir = self._prepare_dir(destroy)

        # Build a new PostgresNode
        assert self.original_node is not None

        if (hasattr(self.original_node, "clone_with_new_name_and_base_dir")):
            node = self.original_node.clone_with_new_name_and_base_dir(name=name, base_dir=base_dir)
        else:
            # For backward compatibility
            NodeClass = self.original_node.__class__
            node = NodeClass(name=name, base_dir=base_dir, conn_params=self.original_node.os_ops.conn_params)

        assert node is not None
        assert type(node) == self.original_node.__class__  # noqa: E721

        with clean_on_error(node) as node:
            # Set a new port
            node.append_conf(filename=PG_CONF_FILE, line='\n')
            node.append_conf(filename=PG_CONF_FILE, port=node.port)

            return node

    def spawn_replica(self, name=None, destroy=True, slot=None):
        """
        Create a replica of the original node from a backup.

        Args:
            name: replica's application name.
            slot: create a replication slot with the specified name.
            destroy: should we convert this backup into a node?

        Returns:
            New instance of :class:`.PostgresNode`.
        """

        # Build a new PostgresNode
        node = self.spawn_primary(name=name, destroy=destroy)
        assert node is not None

        try:
            # Assign it a master and a recovery file (private magic)
            node._assign_master(self.original_node)
            node._create_recovery_conf(username=self.username, slot=slot)
        except:  # noqa: E722
            # TODO: Pass 'final=True' ?
            node.cleanup(release_resources=True)
            raise

        return node

    def cleanup(self):
        """
        Remove all files that belong to this backup.
        No-op if it's been converted to a PostgresNode (destroy=True).
        """

        if self._available:
            self._available = False
            self.os_ops.rmdirs(self.base_dir, ignore_errors=True)
