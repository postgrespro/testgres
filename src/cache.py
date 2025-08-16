# coding: utf-8

from six import raise_from

from .config import testgres_config

from .consts import XLOG_CONTROL_FILE

from .defaults import generate_system_id

from .exceptions import \
    InitNodeException, \
    ExecUtilException

from .utils import \
    get_bin_path2, \
    execute_utility2

from testgres.operations.local_ops import LocalOperations
from testgres.operations.os_ops import OsOperations


def cached_initdb(data_dir, logfile=None, params=None, os_ops: OsOperations = None, bin_path=None, cached=True):
    """
    Perform initdb or use cached node files.
    """

    assert os_ops is None or isinstance(os_ops, OsOperations)

    if os_ops is None:
        os_ops = LocalOperations.get_single_instance()

    assert isinstance(os_ops, OsOperations)

    def make_utility_path(name):
        assert name is not None
        assert type(name) == str  # noqa: E721

        if bin_path:
            return os_ops.build_path(bin_path, name)

        return get_bin_path2(os_ops, name)

    def call_initdb(initdb_dir, log=logfile):
        try:
            initdb_path = make_utility_path("initdb")
            _params = [initdb_path, "-D", initdb_dir, "-N"]
            execute_utility2(os_ops, _params + (params or []), log)
        except ExecUtilException as e:
            raise_from(InitNodeException("Failed to run initdb"), e)

    if params or not testgres_config.cache_initdb or not cached:
        call_initdb(data_dir, logfile)
    else:
        # Fetch cached initdb dir
        cached_data_dir = testgres_config.cached_initdb_dir

        # Initialize cached initdb

        if not os_ops.path_exists(cached_data_dir) or \
                not os_ops.listdir(cached_data_dir):
            call_initdb(cached_data_dir)

        try:
            # Copy cached initdb to current data dir
            os_ops.copytree(cached_data_dir, data_dir)

            # Assign this node a unique system id if asked to
            if testgres_config.cached_initdb_unique:
                # XXX: write new unique system id to control file
                # Some users might rely upon unique system ids, but
                # our initdb caching mechanism breaks this contract.
                pg_control = os_ops.build_path(data_dir, XLOG_CONTROL_FILE)
                system_id = generate_system_id()
                cur_pg_control = os_ops.read(pg_control, binary=True)
                new_pg_control = system_id + cur_pg_control[len(system_id):]
                os_ops.write(pg_control, new_pg_control, truncate=True, binary=True, read_and_write=True)

                # XXX: build new WAL segment with our system id
                _params = [make_utility_path("pg_resetwal"), "-D", data_dir, "-f"]
                execute_utility2(os_ops, _params, logfile)

        except ExecUtilException as e:
            msg = "Failed to reset WAL for system id"
            raise_from(InitNodeException(msg), e)

        except Exception as e:
            raise_from(InitNodeException("Failed to spawn a node"), e)
