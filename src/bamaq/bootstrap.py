from datetime import UTC, datetime
from uuid import uuid4

from bamaq.adapters.database import make_engine, uow_factory
from bamaq.application import Transactions
from bamaq.config import Settings


class SystemClock:
    def now(self):
        return datetime.now(UTC)


class UUIDs:
    def new(self):
        return str(uuid4())


def build(settings: Settings | None = None):
    settings = settings or Settings()
    engine = make_engine(settings.database_url)
    factory = uow_factory(engine)
    service = Transactions(
        factory,
        SystemClock(),
        UUIDs(),
        max_attempts=settings.max_attempts,
        lease_seconds=settings.lease_seconds,
    )
    return engine, factory, service
