# coding: utf-8
import os
import re
import subprocess
import pytest
import psutil
import platform
import logging

import src as testgres

from src import StartNodeException
from src import ExecUtilException
from src import NodeApp
from src import scoped_config
from src import get_new_node
from src import get_bin_path
from src import get_pg_config
from src import get_pg_version

# NOTE: those are ugly imports
from src.utils import bound_ports
from src.utils import PgVer
from src.node import ProcessProxy


def pg_version_ge(version):
    cur_ver = PgVer(get_pg_version())
    min_ver = PgVer(version)
    return cur_ver >= min_ver


def util_exists(util):
    def good_properties(f):
        return (os.path.exists(f) and  # noqa: W504
                os.path.isfile(f) and  # noqa: W504
                os.access(f, os.X_OK))  # yapf: disable

    # try to resolve it
    if good_properties(get_bin_path(util)):
        return True

    # check if util is in PATH
    for path in os.environ["PATH"].split(os.pathsep):
        if good_properties(os.path.join(path, util)):
            return True


def rm_carriage_returns(out):
    """
    In Windows we have additional '\r' symbols in output.
    Let's get rid of them.
    """
    if os.name == 'nt':
        if isinstance(out, (int, float, complex)):
            return out
        elif isinstance(out, tuple):
            return tuple(rm_carriage_returns(item) for item in out)
        elif isinstance(out, bytes):
            return out.replace(b'\r', b'')
        else:
            return out.replace('\r', '')
    else:
        return out


class TestTestgresLocal:
    def test_pg_config(self):
        # check same instances
        a = get_pg_config()
        b = get_pg_config()
        assert (id(a) == id(b))

        # save right before config change
        c1 = get_pg_config()

        # modify setting for this scope
        with scoped_config(cache_pg_config=False) as config:
            # sanity check for value
            assert not (config.cache_pg_config)

            # save right after config change
            c2 = get_pg_config()

            # check different instances after config change
            assert (id(c1) != id(c2))

            # check different instances
            a = get_pg_config()
            b = get_pg_config()
            assert (id(a) != id(b))

    def test_ports_management(self):
        assert bound_ports is not None
        assert type(bound_ports) == set  # noqa: E721

        if len(bound_ports) != 0:
            logging.warning("bound_ports is not empty: {0}".format(bound_ports))

        stage0__bound_ports = bound_ports.copy()

        with get_new_node() as node:
            assert bound_ports is not None
            assert type(bound_ports) == set  # noqa: E721

            assert node.port is not None
            assert type(node.port) == int  # noqa: E721

            logging.info("node port is {0}".format(node.port))

            assert node.port in bound_ports
            assert node.port not in stage0__bound_ports

            assert stage0__bound_ports <= bound_ports
            assert len(stage0__bound_ports) + 1 == len(bound_ports)

            stage1__bound_ports = stage0__bound_ports.copy()
            stage1__bound_ports.add(node.port)

            assert stage1__bound_ports == bound_ports

        # check that port has been freed successfully
        assert bound_ports is not None
        assert type(bound_ports) == set  # noqa: E721
        assert bound_ports == stage0__bound_ports

    def test_child_process_dies(self):
        # test for FileNotFound exception during child_processes() function
        cmd = ["timeout", "60"] if os.name == 'nt' else ["sleep", "60"]

        nAttempt = 0

        while True:
            if nAttempt == 5:
                raise Exception("Max attempt number is exceed.")

            nAttempt += 1

            logging.info("Attempt #{0}".format(nAttempt))

            with subprocess.Popen(cmd, shell=True) as process:  # shell=True might be needed on Windows
                r = process.poll()

                if r is not None:
                    logging.warning("process.pool() returns an unexpected result: {0}.".format(r))
                    continue

                assert r is None
                # collect list of processes currently running
                children = psutil.Process(os.getpid()).children()
                # kill a process, so received children dictionary becomes invalid
                process.kill()
                process.wait()
                # try to handle children list -- missing processes will have ptype "ProcessType.Unknown"
                [ProcessProxy(p) for p in children]
                break

    def test_upgrade_node(self):
        old_bin_dir = os.path.dirname(get_bin_path("pg_config"))
        new_bin_dir = os.path.dirname(get_bin_path("pg_config"))
        with get_new_node(prefix='node_old', bin_dir=old_bin_dir) as node_old:
            node_old.init()
            node_old.start()
            node_old.stop()
            with get_new_node(prefix='node_new', bin_dir=new_bin_dir) as node_new:
                node_new.init(cached=False)
                res = node_new.upgrade_from(old_node=node_old)
                node_new.start()
                assert (b'Upgrade Complete' in res)

    class tagPortManagerProxy:
        sm_prev_testgres_reserve_port = None
        sm_prev_testgres_release_port = None

        sm_DummyPortNumber = None
        sm_DummyPortMaxUsage = None

        sm_DummyPortCurrentUsage = None
        sm_DummyPortTotalUsage = None

        def __init__(self, dummyPortNumber, dummyPortMaxUsage):
            assert type(dummyPortNumber) == int  # noqa: E721
            assert type(dummyPortMaxUsage) == int  # noqa: E721
            assert dummyPortNumber >= 0
            assert dummyPortMaxUsage >= 0

            assert __class__.sm_prev_testgres_reserve_port is None
            assert __class__.sm_prev_testgres_release_port is None
            assert testgres.utils.reserve_port == testgres.utils.internal__reserve_port
            assert testgres.utils.release_port == testgres.utils.internal__release_port

            __class__.sm_prev_testgres_reserve_port = testgres.utils.reserve_port
            __class__.sm_prev_testgres_release_port = testgres.utils.release_port

            testgres.utils.reserve_port = __class__._proxy__reserve_port
            testgres.utils.release_port = __class__._proxy__release_port

            assert testgres.utils.reserve_port == __class__._proxy__reserve_port
            assert testgres.utils.release_port == __class__._proxy__release_port

            __class__.sm_DummyPortNumber = dummyPortNumber
            __class__.sm_DummyPortMaxUsage = dummyPortMaxUsage

            __class__.sm_DummyPortCurrentUsage = 0
            __class__.sm_DummyPortTotalUsage = 0

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            assert __class__.sm_DummyPortCurrentUsage == 0

            assert __class__.sm_prev_testgres_reserve_port is not None
            assert __class__.sm_prev_testgres_release_port is not None

            assert testgres.utils.reserve_port == __class__._proxy__reserve_port
            assert testgres.utils.release_port == __class__._proxy__release_port

            testgres.utils.reserve_port = __class__.sm_prev_testgres_reserve_port
            testgres.utils.release_port = __class__.sm_prev_testgres_release_port

            __class__.sm_prev_testgres_reserve_port = None
            __class__.sm_prev_testgres_release_port = None

        @staticmethod
        def _proxy__reserve_port():
            assert type(__class__.sm_DummyPortMaxUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortTotalUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortCurrentUsage) == int  # noqa: E721
            assert __class__.sm_DummyPortTotalUsage >= 0
            assert __class__.sm_DummyPortCurrentUsage >= 0

            assert __class__.sm_DummyPortTotalUsage <= __class__.sm_DummyPortMaxUsage
            assert __class__.sm_DummyPortCurrentUsage <= __class__.sm_DummyPortTotalUsage

            assert __class__.sm_prev_testgres_reserve_port is not None

            if __class__.sm_DummyPortTotalUsage == __class__.sm_DummyPortMaxUsage:
                return __class__.sm_prev_testgres_reserve_port()

            __class__.sm_DummyPortTotalUsage += 1
            __class__.sm_DummyPortCurrentUsage += 1
            return __class__.sm_DummyPortNumber

        @staticmethod
        def _proxy__release_port(dummyPortNumber):
            assert type(dummyPortNumber) == int  # noqa: E721

            assert type(__class__.sm_DummyPortMaxUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortTotalUsage) == int  # noqa: E721
            assert type(__class__.sm_DummyPortCurrentUsage) == int  # noqa: E721
            assert __class__.sm_DummyPortTotalUsage >= 0
            assert __class__.sm_DummyPortCurrentUsage >= 0

            assert __class__.sm_DummyPortTotalUsage <= __class__.sm_DummyPortMaxUsage
            assert __class__.sm_DummyPortCurrentUsage <= __class__.sm_DummyPortTotalUsage

            assert __class__.sm_prev_testgres_release_port is not None

            if __class__.sm_DummyPortCurrentUsage > 0 and dummyPortNumber == __class__.sm_DummyPortNumber:
                assert __class__.sm_DummyPortTotalUsage > 0
                __class__.sm_DummyPortCurrentUsage -= 1
                return

            return __class__.sm_prev_testgres_release_port(dummyPortNumber)

    def test_port_rereserve_during_node_start(self):
        assert testgres.PostgresNode._C_MAX_START_ATEMPTS == 5

        C_COUNT_OF_BAD_PORT_USAGE = 3

        with get_new_node() as node1:
            node1.init().start()
            assert (node1._should_free_port)
            assert (type(node1.port) == int)  # noqa: E721
            node1_port_copy = node1.port
            assert (rm_carriage_returns(node1.safe_psql("SELECT 1;")) == b'1\n')

            with __class__.tagPortManagerProxy(node1.port, C_COUNT_OF_BAD_PORT_USAGE):
                assert __class__.tagPortManagerProxy.sm_DummyPortNumber == node1.port
                with get_new_node() as node2:
                    assert (node2._should_free_port)
                    assert (node2.port == node1.port)

                    node2.init().start()

                    assert (node2.port != node1.port)
                    assert (node2._should_free_port)
                    assert (__class__.tagPortManagerProxy.sm_DummyPortCurrentUsage == 0)
                    assert (__class__.tagPortManagerProxy.sm_DummyPortTotalUsage == C_COUNT_OF_BAD_PORT_USAGE)
                    assert (node2.is_started)

                    assert (rm_carriage_returns(node2.safe_psql("SELECT 2;")) == b'2\n')

            # node1 is still working
            assert (node1.port == node1_port_copy)
            assert (node1._should_free_port)
            assert (rm_carriage_returns(node1.safe_psql("SELECT 3;")) == b'3\n')

    def test_port_conflict(self):
        assert testgres.PostgresNode._C_MAX_START_ATEMPTS > 1

        C_COUNT_OF_BAD_PORT_USAGE = testgres.PostgresNode._C_MAX_START_ATEMPTS

        with get_new_node() as node1:
            node1.init().start()
            assert (node1._should_free_port)
            assert (type(node1.port) == int)  # noqa: E721
            node1_port_copy = node1.port
            assert (rm_carriage_returns(node1.safe_psql("SELECT 1;")) == b'1\n')

            with __class__.tagPortManagerProxy(node1.port, C_COUNT_OF_BAD_PORT_USAGE):
                assert __class__.tagPortManagerProxy.sm_DummyPortNumber == node1.port
                with get_new_node() as node2:
                    assert (node2._should_free_port)
                    assert (node2.port == node1.port)

                    with pytest.raises(
                        expected_exception=StartNodeException,
                        match=re.escape("Cannot start node after multiple attempts.")
                    ):
                        node2.init().start()

                    assert (node2.port == node1.port)
                    assert (node2._should_free_port)
                    assert (__class__.tagPortManagerProxy.sm_DummyPortCurrentUsage == 1)
                    assert (__class__.tagPortManagerProxy.sm_DummyPortTotalUsage == C_COUNT_OF_BAD_PORT_USAGE)
                    assert not (node2.is_started)

                # node2 must release our dummyPort (node1.port)
                assert (__class__.tagPortManagerProxy.sm_DummyPortCurrentUsage == 0)

            # node1 is still working
            assert (node1.port == node1_port_copy)
            assert (node1._should_free_port)
            assert (rm_carriage_returns(node1.safe_psql("SELECT 3;")) == b'3\n')

    def test_simple_with_bin_dir(self):
        with get_new_node() as node:
            node.init().start()
            bin_dir = node.bin_dir

        app = NodeApp()
        with app.make_simple(base_dir=node.base_dir, bin_dir=bin_dir) as correct_bin_dir:
            correct_bin_dir.slow_start()
            correct_bin_dir.safe_psql("SELECT 1;")
            correct_bin_dir.stop()

        while True:
            try:
                app.make_simple(base_dir=node.base_dir, bin_dir="wrong/path")
            except FileNotFoundError:
                break  # Expected error
            except ExecUtilException:
                break  # Expected error

            raise RuntimeError("Error was expected.")  # We should not reach this

        return

    def test_set_auto_conf(self):
        # elements contain [property id, value, storage value]
        testData = [
            ["archive_command",
             "cp '%p' \"/mnt/server/archivedir/%f\"",
             "'cp \\'%p\\' \"/mnt/server/archivedir/%f\""],
            ["log_line_prefix",
             "'\n\r\t\b\\\"",
             "'\\\'\\n\\r\\t\\b\\\\\""],
            ["log_connections",
             True,
             "on"],
            ["log_disconnections",
             False,
             "off"],
            ["autovacuum_max_workers",
             3,
             "3"]
        ]
        if pg_version_ge('12'):
            testData.append(["restore_command",
                             'cp "/mnt/server/archivedir/%f" \'%p\'',
                             "'cp \"/mnt/server/archivedir/%f\" \\'%p\\''"])

        with get_new_node() as node:
            node.init().start()

            options = {}

            for x in testData:
                options[x[0]] = x[1]

            node.set_auto_conf(options)
            node.stop()
            node.slow_start()

            auto_conf_path = f"{node.data_dir}/postgresql.auto.conf"
            with open(auto_conf_path, "r") as f:
                content = f.read()

                for x in testData:
                    assert x[0] + " = " + x[2] in content

    @staticmethod
    def helper__skip_test_if_util_not_exist(name: str):
        assert type(name) == str  # noqa: E721

        if platform.system().lower() == "windows":
            name2 = name + ".exe"
        else:
            name2 = name

        if not util_exists(name2):
            pytest.skip('might be missing')
