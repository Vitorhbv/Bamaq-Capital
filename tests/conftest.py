import os

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url

from bamaq.adapters.database import make_engine, metadata, uow_factory
from bamaq.application import Transactions
from bamaq.bootstrap import UUIDs
from tests.fakes import FrozenClock, MemoryStore


@pytest.fixture
def clock():
    return FrozenClock()


@pytest.fixture
def memory():
    return MemoryStore()


@pytest.fixture
def service(memory, clock):
    return Transactions(memory, clock, UUIDs(), jitter=lambda *_: 0)


@pytest.fixture(scope="session")
def mysql_engine():
    url = os.getenv("BAMAQ_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set BAMAQ_TEST_DATABASE_URL to a dedicated MySQL *_test database")
    parsed = make_url(url)
    if parsed.get_backend_name() != "mysql" or not (parsed.database or "").endswith("_test"):
        pytest.fail("Integration tests require a dedicated MySQL database ending in _test")
    previous = os.environ.get("BAMAQ_DATABASE_URL")
    os.environ["BAMAQ_DATABASE_URL"] = url
    try:
        command.upgrade(Config("alembic.ini"), "head")
    finally:
        if previous is None:
            os.environ.pop("BAMAQ_DATABASE_URL", None)
        else:
            os.environ["BAMAQ_DATABASE_URL"] = previous
    engine = make_engine(url)
    yield engine
    engine.dispose()


@pytest.fixture
def sql_factory(mysql_engine):
    with mysql_engine.begin() as connection:
        for table in reversed(metadata.sorted_tables):
            connection.execute(table.delete())
    return uow_factory(mysql_engine)


@pytest.fixture
def sql_service(sql_factory, clock):
    return Transactions(sql_factory, clock, UUIDs(), jitter=lambda *_: 0)
