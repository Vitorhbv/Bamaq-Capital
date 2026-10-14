"""SQLAlchemy classical mappings keep infrastructure outside the domain."""

from datetime import UTC

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, registry, sessionmaker
from sqlalchemy.types import TypeDecorator

from bamaq.domain import (
    ConcurrentWrite,
    FailedMessage,
    Job,
    JobState,
    OutboxEvent,
    Status,
    Transaction,
)


class UTCDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(DATETIME(fsp=6) if dialect.name == "mysql" else DateTime())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("Expected timezone-aware UTC datetime")
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value, dialect):
        return value.replace(tzinfo=UTC) if value is not None else None


metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_name)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)
mapper_registry = registry(metadata=metadata)

transactions = Table(
    "transactions",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("customer_id", String(128), nullable=False),
    Column("value", Numeric(18, 2), nullable=False),
    Column("status", Enum(Status, native_enum=False, length=20), nullable=False),
    Column("version", Integer, nullable=False),
    Column("generation", Integer, nullable=False),
    Column("correlation_id", String(36), nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("updated_at", UTCDateTime(), nullable=False),
    Column(
        "idempotency_key",
        String(128).with_variant(
            String(128, collation="utf8mb4_bin"),
            "mysql",
        ),
        unique=True,
    ),
    Column("fingerprint", String(64)),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
jobs = Table(
    "processing_jobs",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("event_id", String(36), nullable=False, unique=True),
    Column("transaction_id", String(36), ForeignKey("transactions.id"), nullable=False),
    Column("generation", Integer, nullable=False),
    Column("status", Enum(JobState, native_enum=False, length=20), nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("next_attempt_at", UTCDateTime(), nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("updated_at", UTCDateTime(), nullable=False),
    Column("lease_token", String(36)),
    Column("lease_until", UTCDateTime()),
    Column("last_error", String(500)),
    UniqueConstraint("transaction_id", "generation", name="uq_job_generation"),
    Index("ix_jobs_ready", "status", "next_attempt_at"),
    Index("ix_jobs_expired", "status", "lease_until"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
outbox = Table(
    "outbox_events",
    metadata,
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    Column("event_id", String(36), nullable=False, unique=True),
    Column("transaction_id", String(36), ForeignKey("transactions.id")),
    Column("topic", String(128), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("published_at", UTCDateTime()),
    Index("ix_outbox_pending", "published_at", "id"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
failures = Table(
    "failed_messages",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("transaction_id", String(36), ForeignKey("transactions.id")),
    Column("generation", Integer),
    Column("reason", Text, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("source_topic", String(128)),
    Column("source_partition", Integer),
    Column("source_offset", BigInteger),
    Column("replayed_at", UTCDateTime()),
    Column("replay_event_id", String(36)),
    UniqueConstraint("source_topic", "source_partition", "source_offset", name="uq_failure_source"),
    Index("ix_failures_transaction", "transaction_id", "generation"),
    Index("ix_failures_created", "created_at"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
for entity, table in (
    (Transaction, transactions),
    (Job, jobs),
    (OutboxEvent, outbox),
    (FailedMessage, failures),
):
    mapper_registry.map_imperatively(entity, table)


def make_engine(url: str, **kwargs):
    options = {"pool_pre_ping": True}
    if url.startswith("mysql"):
        options.update(
            isolation_level="READ COMMITTED",
            connect_args={"connect_timeout": 5, "read_timeout": 10, "write_timeout": 10},
            pool_recycle=1800,
        )
    return create_engine(url, **(options | kwargs))


class SqlTransactions:
    def __init__(self, session: Session):
        self.session = session

    def get(self, transaction_id, *, lock=False):
        statement = select(Transaction).where(transactions.c.id == transaction_id)
        if lock:
            statement = statement.with_for_update()
        return self.session.scalar(statement)

    def by_key(self, key):
        return self.session.scalar(select(Transaction).where(transactions.c.idempotency_key == key))

    def add(self, transaction):
        self.session.add(transaction)
        # Establish FK parent before dependent inserts; still inside the same SQL transaction.
        self.session.flush()


class SqlJobs:
    def __init__(self, session: Session):
        self.session = session

    def by_event(self, event_id):
        return self.session.scalar(select(Job).where(jobs.c.event_id == event_id))

    def by_generation(self, transaction_id, generation):
        return self.session.scalar(
            select(Job).where(
                jobs.c.transaction_id == transaction_id,
                jobs.c.generation == generation,
            )
        )

    def get(self, job_id, *, lock=False):
        statement = select(Job).where(jobs.c.id == job_id)
        return self.session.scalar(statement.with_for_update() if lock else statement)

    def claimable(self, now):
        # Separate indexed scans: OR + filesort can lock all candidate rows before
        # LIMIT is applied, defeating SKIP LOCKED even under READ COMMITTED.
        # Recover expired leases first so a large fresh backlog cannot starve them.
        for status, due_column in (
            (JobState.RUNNING, jobs.c.lease_until),
            (JobState.READY, jobs.c.next_attempt_at),
        ):
            job = self.session.scalar(
                select(Job)
                .where(jobs.c.status == status, due_column <= now)
                .order_by(due_column, jobs.c.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if job is not None:
                return job
        return None

    def add(self, job):
        self.session.add(job)


class SqlOutbox:
    def __init__(self, session: Session):
        self.session = session

    def add(self, event):
        self.session.add(event)

    def oldest_pending(self):
        return self.session.scalar(
            select(OutboxEvent)
            .where(
                outbox.c.published_at.is_(None),
            )
            .order_by(outbox.c.id)
            .limit(1)
            .with_for_update()
        )


class SqlFailures:
    def __init__(self, session: Session):
        self.session = session

    def add(self, failure):
        self.session.add(failure)

    def get(self, failure_id, *, lock=False):
        statement = select(FailedMessage).where(failures.c.id == failure_id)
        return self.session.scalar(statement.with_for_update() if lock else statement)

    def by_source(self, topic, partition, offset):
        return self.session.scalar(
            select(FailedMessage).where(
                failures.c.source_topic == topic,
                failures.c.source_partition == partition,
                failures.c.source_offset == offset,
            )
        )

    def list_recent(self, limit):
        return list(
            self.session.scalars(
                select(FailedMessage).order_by(failures.c.created_at.desc()).limit(limit),
            )
        )

    def for_generation(self, transaction_id, generation):
        return list(
            self.session.scalars(
                select(FailedMessage).where(
                    failures.c.transaction_id == transaction_id,
                    failures.c.generation == generation,
                )
            )
        )


class SqlUnitOfWork:
    def __init__(self, sessions: sessionmaker):
        self.sessions = sessions

    def __enter__(self):
        self.session = self.sessions()
        self.transactions = SqlTransactions(self.session)
        self.jobs = SqlJobs(self.session)
        self.outbox = SqlOutbox(self.session)
        self.failures = SqlFailures(self.session)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Detach while attributes are loaded; rollback must not expire returned objects.
        self.session.expunge_all()
        self.session.rollback()
        self.session.close()
        if isinstance(exc_value, IntegrityError):
            code = getattr(exc_value.orig, "args", (None,))[0]
            if code == 1062 or "UNIQUE constraint failed" in str(exc_value.orig):
                raise ConcurrentWrite("Unique constraint race") from exc_value

    def commit(self):
        self.session.commit()


def uow_factory(engine):
    sessions = sessionmaker(engine, expire_on_commit=False)
    return lambda: SqlUnitOfWork(sessions)
