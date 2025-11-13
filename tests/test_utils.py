from .helpers.global_data import OsOpsDescr
from .helpers.global_data import OsOpsDescrs
from .helpers.global_data import OsOperations

from src.utils import parse_pg_version
from src.utils import get_pg_config2
from src import scoped_config

import pytest
import typing


class TestUtils:
    sm_os_ops_descrs: typing.List[OsOpsDescr] = [
        OsOpsDescrs.sm_local_os_ops_descr,
        OsOpsDescrs.sm_remote_os_ops_descr
    ]

    @pytest.fixture(
        params=[descr.os_ops for descr in sm_os_ops_descrs],
        ids=[descr.sign for descr in sm_os_ops_descrs]
    )
    def os_ops(self, request: pytest.FixtureRequest) -> OsOperations:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, OsOperations)
        return request.param

    def test_parse_pg_version(self):
        # Linux Mint
        assert parse_pg_version("postgres (PostgreSQL) 15.5 (Ubuntu 15.5-1.pgdg22.04+1)") == "15.5"
        # Linux Ubuntu
        assert parse_pg_version("postgres (PostgreSQL) 12.17") == "12.17"
        # Windows
        assert parse_pg_version("postgres (PostgreSQL) 11.4") == "11.4"
        # Macos
        assert parse_pg_version("postgres (PostgreSQL) 14.9 (Homebrew)") == "14.9"

    def test_get_pg_config2(self, os_ops: OsOperations):
        assert isinstance(os_ops, OsOperations)

        # check same instances
        a = get_pg_config2(os_ops, None)
        b = get_pg_config2(os_ops, None)
        assert (id(a) == id(b))

        # save right before config change
        c1 = get_pg_config2(os_ops, None)

        # modify setting for this scope
        with scoped_config(cache_pg_config=False) as config:
            # sanity check for value
            assert not (config.cache_pg_config)

            # save right after config change
            c2 = get_pg_config2(os_ops, None)

            # check different instances after config change
            assert (id(c1) != id(c2))

            # check different instances
            a = get_pg_config2(os_ops, None)
            b = get_pg_config2(os_ops, None)
            assert (id(a) != id(b))
