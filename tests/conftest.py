"""Shared test setup.

The unit tests need no database. Most of tests/test_api.py needs PostgreSQL
with the schema loaded (marked `db`), and some of those also need real
observation data (marked `real_data`). Both are skipped with a reason when
what they need is missing, so a fresh clone runs cleanly with plain `pytest`.
"""
import os
from pathlib import Path

import pytest

_ENV_FILE = Path(__file__).resolve().parent.parent / "backend" / ".env"

# config.py refuses to import without a database password. Unit tests never
# connect, so they get a placeholder unless a real password is configured.
if "TRAIN_DB_PASSWORD" not in os.environ and not _ENV_FILE.exists():
    os.environ["TRAIN_DB_PASSWORD"] = "unused-without-a-database"


def _database_state():
    """Return (reachable, has_observations) for the configured database."""
    import psycopg2
    from config import DB_CONFIG

    try:
        conn = psycopg2.connect(connect_timeout=2, **DB_CONFIG)
    except psycopg2.Error:
        return False, False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM delay_observations)")
            return True, cur.fetchone()[0]
    except psycopg2.Error:
        return True, False
    finally:
        conn.close()


def pytest_collection_modifyitems(config, items):
    needs_db = [i for i in items if i.get_closest_marker("db") or i.get_closest_marker("real_data")]
    if not needs_db:
        return
    reachable, has_data = _database_state()
    skip_db = pytest.mark.skip(reason="needs PostgreSQL with schema/schema.sql loaded")
    skip_data = pytest.mark.skip(reason="needs real observations in delay_observations")
    for item in needs_db:
        if not reachable:
            item.add_marker(skip_db)
        elif item.get_closest_marker("real_data") and not has_data:
            item.add_marker(skip_data)
