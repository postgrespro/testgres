from src import TestgresConfig
from src import configure_testgres
from src import scoped_config
from src import pop_config

import src as testgres

import pytest


class TestConfig:
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
