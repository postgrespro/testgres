# coding: utf-8


class TestgresConfig:
    """
    Global config (override default settings).
    """

    # shall we cache pg_config results?
    cache_pg_config = True

    # shall we use cached initdb instance?
    cache_initdb = True

    # shall we create a temp dir for cached initdb?
    cached_initdb_dir = None

    # shall we remove EVERYTHING (including logs)?
    node_cleanup_full = True


def configure_testgres(**options):
    """
    Configure testgres.
    Look at TestgresConfig to check what can be changed.
    """

    for key, option in options.items():
        setattr(TestgresConfig, key, option)
