# coding: utf-8


class TestgresConfig:
    """
    Global config (override default settings).

    Attributes:
        cache_initdb:       shall we use cached initdb instance?
        cache_pg_config:    shall we cache pg_config results?
        cached_initdb_dir:  shall we create a temp dir for cached initdb?
        node_cleanup_full:  shall we remove EVERYTHING (including logs)?
        error_log_lines:    N of log lines to be included into exception (0=inf).
    """

    cache_initdb = True
    cache_pg_config = True
    cached_initdb_dir = None
    node_cleanup_full = True
    error_log_lines = 20


def configure_testgres(**options):
    """
    Configure testgres.
    Look at TestgresConfig to check what can be changed.
    """

    for key, option in options.items():
        setattr(TestgresConfig, key, option)
