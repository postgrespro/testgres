# coding: utf-8


class TestgresConfig:
    """
    Global config (override default settings).

    Attributes:
        cache_initdb:       shall we use cached initdb instance?
        cached_initdb_dir:  shall we create a temp dir for cached initdb?

        cache_pg_config:    shall we cache pg_config results?

        use_python_logging: use python logging configuration for all nodes.
        error_log_lines:    N of log lines to be included into exception (0=inf).

        node_cleanup_full:  shall we remove EVERYTHING (including logs)?
        node_cleanup_on_good_exit:  remove base_dir on nominal __exit__().
        node_cleanup_on_bad_exit:   remove base_dir on __exit__() via exception.
    """

    cache_initdb = True
    cached_initdb_dir = None

    cache_pg_config = True

    use_python_logging = False
    error_log_lines = 20

    node_cleanup_full = True
    node_cleanup_on_good_exit = True
    node_cleanup_on_bad_exit = False


def configure_testgres(**options):
    """
    Configure testgres.
    Look at TestgresConfig to check what can be changed.
    """

    for key, option in options.items():
        setattr(TestgresConfig, key, option)
