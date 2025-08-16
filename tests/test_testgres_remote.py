# coding: utf-8
import os

import pytest
import logging

from .helpers.global_data import PostgresNodeService
from .helpers.global_data import PostgresNodeServices

import src as testgres

from src.exceptions import InitNodeException
from src.exceptions import ExecUtilException

from src.config import scoped_config
from src.config import testgres_config

from src import get_bin_path
from src import get_pg_config

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

    @staticmethod
    def helper__get_node(name=None):
        svc = PostgresNodeServices.sm_remote

        assert isinstance(svc, PostgresNodeService)
        assert isinstance(svc.os_ops, testgres.OsOperations)
        assert isinstance(svc.port_manager, testgres.PortManager)

        return testgres.PostgresNode(
            name,
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
