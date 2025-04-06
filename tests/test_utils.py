from ..testgres.utils import parse_pg_version


class TestUtils:
    def test_parse_pg_version(self):
        # Linux Mint
        assert parse_pg_version("postgres (PostgreSQL) 15.5 (Ubuntu 15.5-1.pgdg22.04+1)") == "15.5"
        # Linux Ubuntu
        assert parse_pg_version("postgres (PostgreSQL) 12.17") == "12.17"
        # Windows
        assert parse_pg_version("postgres (PostgreSQL) 11.4") == "11.4"
        # Macos
        assert parse_pg_version("postgres (PostgreSQL) 14.9 (Homebrew)") == "14.9"
