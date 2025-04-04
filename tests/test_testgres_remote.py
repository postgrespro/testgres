# coding: utf-8
import os
import re
import subprocess

import pytest
import logging

from .helpers.global_data import PostgresNodeService
from .helpers.global_data import PostgresNodeServices

from .. import testgres

from ..testgres.exceptions import \
    InitNodeException, \
    ExecUtilException

from ..testgres.config import \
    TestgresConfig, \
    configure_testgres, \
    scoped_config, \
    pop_config, testgres_config

from ..testgres import \
    get_bin_path, \
    get_pg_config

# NOTE: those are ugly imports


def util_exists(util):
    def good_properties(f):
        return (testgres_config.os_ops.path_exists(f) and  # noqa: W504
                testgres_config.os_ops.isfile(f) and  # noqa: W504
                testgres_config.os_ops.is_executable(f))  # yapf: disable

    # try to resolve it
    if good_properties(get_bin_path(util)):
        return True

    # check if util is in PATH
    for path in testgres_config.os_ops.environ("PATH").split(testgres_config.os_ops.pathsep):
        if good_properties(os.path.join(path, util)):
            return True


class TestTestgresRemote:
    @pytest.fixture(autouse=True, scope="class")
    def implicit_fixture(self):
        cur_os_ops = PostgresNodeServices.sm_remote.os_ops
        assert cur_os_ops is not None

        prev_ops = testgres_config.os_ops
        assert prev_ops is not None
        testgres_config.set_os_ops(os_ops=cur_os_ops)
        assert testgres_config.os_ops is cur_os_ops
        yield
        assert testgres_config.os_ops is cur_os_ops
        testgres_config.set_os_ops(os_ops=prev_ops)
        assert testgres_config.os_ops is prev_ops

    def test_node_repr(self):
        with __class__.helper__get_node() as node:
            pattern = r"PostgresNode\(name='.+', port=.+, base_dir='.+'\)"
            assert re.match(pattern, str(node)) is not None

    def test_custom_init(self):
        with __class__.helper__get_node() as node:
            # enable page checksums
            node.init(initdb_params=['-k']).start()

        with __class__.helper__get_node() as node:
            node.init(
                allow_streaming=True,
                initdb_params=['--auth-local=reject', '--auth-host=reject'])

            hba_file = os.path.join(node.data_dir, 'pg_hba.conf')
            lines = node.os_ops.readlines(hba_file)

            # check number of lines
            assert (len(lines) >= 6)

            # there should be no trust entries at all
            assert not (any('trust' in s for s in lines))

    def test_init__LANG_С(self):
        # PBCKP-1744
        prev_LANG = os.environ.get("LANG")

        try:
            os.environ["LANG"] = "C"

            with __class__.helper__get_node() as node:
                node.init().start()
        finally:
            __class__.helper__restore_envvar("LANG", prev_LANG)

    def test_init__unk_LANG_and_LC_CTYPE(self):
        # PBCKP-1744
        prev_LANG = os.environ.get("LANG")
        prev_LANGUAGE = os.environ.get("LANGUAGE")
        prev_LC_CTYPE = os.environ.get("LC_CTYPE")
        prev_LC_COLLATE = os.environ.get("LC_COLLATE")

        try:
            # TODO: Pass unkData through test parameter.
            unkDatas = [
                ("UNKNOWN_LANG", "UNKNOWN_CTYPE"),
                ("\"UNKNOWN_LANG\"", "\"UNKNOWN_CTYPE\""),
                ("\\UNKNOWN_LANG\\", "\\UNKNOWN_CTYPE\\"),
                ("\"UNKNOWN_LANG", "UNKNOWN_CTYPE\""),
                ("\\UNKNOWN_LANG", "UNKNOWN_CTYPE\\"),
                ("\\", "\\"),
                ("\"", "\""),
            ]

            errorIsDetected = False

            for unkData in unkDatas:
                logging.info("----------------------")
                logging.info("Unk LANG is [{0}]".format(unkData[0]))
                logging.info("Unk LC_CTYPE is [{0}]".format(unkData[1]))

                os.environ["LANG"] = unkData[0]
                os.environ.pop("LANGUAGE", None)
                os.environ["LC_CTYPE"] = unkData[1]
                os.environ.pop("LC_COLLATE", None)

                assert os.environ.get("LANG") == unkData[0]
                assert not ("LANGUAGE" in os.environ.keys())
                assert os.environ.get("LC_CTYPE") == unkData[1]
                assert not ("LC_COLLATE" in os.environ.keys())

                assert os.getenv('LANG') == unkData[0]
                assert os.getenv('LANGUAGE') is None
                assert os.getenv('LC_CTYPE') == unkData[1]
                assert os.getenv('LC_COLLATE') is None

                exc: ExecUtilException = None
                with __class__.helper__get_node() as node:
                    try:
                        node.init()  # IT RAISES!
                    except InitNodeException as e:
                        exc = e.__cause__
                        assert exc is not None
                        assert isinstance(exc, ExecUtilException)

                if exc is None:
                    logging.warning("We expected an error!")
                    continue

                errorIsDetected = True

                assert isinstance(exc, ExecUtilException)

                errMsg = str(exc)
                logging.info("Error message is {0}: {1}".format(type(exc).__name__, errMsg))

                assert "warning: setlocale: LC_CTYPE: cannot change locale (" + unkData[1] + ")" in errMsg
                assert "initdb: error: invalid locale settings; check LANG and LC_* environment variables" in errMsg
                continue

            if not errorIsDetected:
                pytest.xfail("All the bad data are processed without errors!")

        finally:
            __class__.helper__restore_envvar("LANG", prev_LANG)
            __class__.helper__restore_envvar("LANGUAGE", prev_LANGUAGE)
            __class__.helper__restore_envvar("LC_CTYPE", prev_LC_CTYPE)
            __class__.helper__restore_envvar("LC_COLLATE", prev_LC_COLLATE)

    def test_pgbench(self):
        __class__.helper__skip_test_if_util_not_exist("pgbench")

        with __class__.helper__get_node().init().start() as node:
            # initialize pgbench DB and run benchmarks
            node.pgbench_init(scale=2, foreign_keys=True,
                              options=['-q']).pgbench_run(time=2)

            # run TPC-B benchmark
            proc = node.pgbench(stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                options=['-T3'])
            out = proc.communicate()[0]
            assert (b'tps = ' in out)

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

    def test_config_stack(self):
        # no such option
        with pytest.raises(expected_exception=TypeError):
            configure_testgres(dummy=True)

        # we have only 1 config in stack
        with pytest.raises(expected_exception=IndexError):
            pop_config()

        d0 = TestgresConfig.cached_initdb_dir
        d1 = 'dummy_abc'
        d2 = 'dummy_def'

        with scoped_config(cached_initdb_dir=d1) as c1:
            assert (c1.cached_initdb_dir == d1)

            with scoped_config(cached_initdb_dir=d2) as c2:
                stack_size = len(testgres.config.config_stack)

                # try to break a stack
                with pytest.raises(expected_exception=TypeError):
                    with scoped_config(dummy=True):
                        pass

                assert (c2.cached_initdb_dir == d2)
                assert (len(testgres.config.config_stack) == stack_size)

            assert (c1.cached_initdb_dir == d1)

        assert (TestgresConfig.cached_initdb_dir == d0)

    def test_unix_sockets(self):
        with __class__.helper__get_node() as node:
            node.init(unix_sockets=False, allow_streaming=True)
            node.start()

            res_exec = node.execute('select 1')
            res_psql = node.safe_psql('select 1')
            assert (res_exec == [(1,)])
            assert (res_psql == b'1\n')

            with node.replicate().start() as r:
                res_exec = r.execute('select 1')
                res_psql = r.safe_psql('select 1')
                assert (res_exec == [(1,)])
                assert (res_psql == b'1\n')

    @staticmethod
    def helper__get_node(name=None):
        svc = PostgresNodeServices.sm_remote

        assert isinstance(svc, PostgresNodeService)
        assert isinstance(svc.os_ops, testgres.OsOperations)
        assert isinstance(svc.port_manager, testgres.PostgresNodePortManager)

        return testgres.PostgresNode(
            name,
            conn_params=None,
            os_ops=svc.os_ops,
            port_manager=svc.port_manager)

    @staticmethod
    def helper__restore_envvar(name, prev_value):
        if prev_value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = prev_value

    @staticmethod
    def helper__skip_test_if_util_not_exist(name: str):
        assert type(name) == str  # noqa: E721
        if not util_exists(name):
            pytest.skip('might be missing')
