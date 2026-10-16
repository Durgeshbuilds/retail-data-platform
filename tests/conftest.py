import uuid
from datetime import date

import psycopg
import pytest

from retail import config
from retail.seed import seed

TODAY = date(2026, 10, 16)


@pytest.fixture(scope="session")
def spark():
    from retail.lake.spark import create_spark

    session = create_spark("retail-tests", master="local[2]", driver_memory="1g")
    yield session
    session.stop()


@pytest.fixture
def pg_dsn():
    """A freshly created and seeded database, dropped after the test."""
    try:
        admin = psycopg.connect(config.POSTGRES_DSN, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Postgres is not running (make up)")
    name = f"retail_test_{uuid.uuid4().hex[:8]}"
    admin.execute(f'CREATE DATABASE "{name}"')
    dsn = psycopg.conninfo.make_conninfo(config.POSTGRES_DSN, dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            for script in sorted((config.PROJECT_ROOT / "sql" / "init").glob("*.sql")):
                conn.execute(script.read_text())
            seed(conn, TODAY, history_months=2)
        yield dsn
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()
