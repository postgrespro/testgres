import os
from pathlib import Path

import pytest


PG_BIN = os.environ.get("PG_BIN")
OS_USER = os.environ.get("PGLAB_OS_USER", os.environ.get("SUDO_USER", "postgres"))
DB_USER = os.environ.get("PGLAB_DB_USER", "postgres")
TEST_ROOT = Path(os.environ.get("PGLAB_TEST_ROOT", "/var/tmp"))

PGLAB_INTEGRATION_MARKS = [
    pytest.mark.skipif(
        os.environ.get("TESTGRES_PGLAB_TESTS") != "1",
        reason="set TESTGRES_PGLAB_TESTS=1 to run privileged PgLab tests",
    ),
    pytest.mark.skipif(
        os.geteuid() != 0,
        reason="PgLab integration tests require root",
    ),
    pytest.mark.skipif(
        not PG_BIN,
        reason="PG_BIN must point to the PostgreSQL bin directory",
    ),
]
