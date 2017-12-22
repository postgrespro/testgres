# coding: utf-8

import os
import shutil
import tempfile

from .consts import \
    DATA_DIR as _DATA_DIR, \
    BACKUP_LOG_FILE as _BACKUP_LOG_FILE, \
    DEFAULT_XLOG_METHOD as _DEFAULT_XLOG_METHOD

from .exceptions import BackupException

from .utils import \
    get_bin_path, \
    default_username as _default_username, \
    execute_utility as _execute_utility, \
    explain_exception as _explain_exception


class NodeBackup(object):
    """
    Smart object responsible for backups
    """

    @property
    def log_file(self):
        return os.path.join(self.base_dir, _BACKUP_LOG_FILE)

    def __init__(self,
                 node,
                 base_dir=None,
                 username=None,
                 xlog_method=_DEFAULT_XLOG_METHOD):

        if not node.status():
            raise BackupException('Node must be running')

        # Set default arguments
        username = username or _default_username()
        base_dir = base_dir or tempfile.mkdtemp()

        # public
        self.original_node = node
        self.base_dir = base_dir

        # private
        self._available = True

        data_dir = os.path.join(self.base_dir, _DATA_DIR)
        _params = [
            get_bin_path("pg_basebackup"),
            "-D{}".format(data_dir), "-p{}".format(node.port),
            "-U{}".format(username), "-X{}".format(xlog_method)
        ]
        _execute_utility(_params, self.log_file)

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
            dest_base_dir = tempfile.mkdtemp()

            data1 = os.path.join(self.base_dir, _DATA_DIR)
            data2 = os.path.join(dest_base_dir, _DATA_DIR)

            try:
                # Copy backup to new data dir
                shutil.copytree(data1, data2)
            except Exception as e:
                raise BackupException(_explain_exception(e))
        else:
            dest_base_dir = self.base_dir

        # Is this backup exhausted?
        self._available = available

        # Return path to new node
        return dest_base_dir

    def spawn_primary(self, name, destroy=True, use_logging=False):
        """
        Create a primary node from a backup.

        Args:
            name: name for a new node.
            destroy: should we convert this backup into a node?
            use_logging: enable python logging.

        Returns:
            New instance of PostgresNode.
        """

        base_dir = self._prepare_dir(destroy)

        # Build a new PostgresNode
        from .node import PostgresNode
        node = PostgresNode(
            name=name,
            base_dir=base_dir,
            master=self.original_node,
            use_logging=use_logging)

        # New nodes should always remove dir tree
        node._should_rm_dirs = True

        node.append_conf("postgresql.conf", "\n")
        node.append_conf("postgresql.conf", "port = {}".format(node.port))

        return node

    def spawn_replica(self, name, destroy=True, use_logging=False):
        """
        Create a replica of the original node from a backup.

        Args:
            name: name for a new node.
            destroy: should we convert this backup into a node?
            use_logging: enable python logging.

        Returns:
            New instance of PostgresNode.
        """

        node = self.spawn_primary(name, destroy, use_logging=use_logging)
        node._create_recovery_conf(self.original_node)

        return node

    def cleanup(self):
        if self._available:
            shutil.rmtree(self.base_dir, ignore_errors=True)
            self._available = False
