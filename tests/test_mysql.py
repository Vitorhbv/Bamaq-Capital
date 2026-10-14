from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from decimal import Decimal
from threading import Barrier

import pytest
from sqlalchemy import func, select

from bamaq.adapters.database import jobs, outbox, transactions
from bamaq.application import COMMANDS, Relay, Transactions
from bamaq.bootstrap import UUIDs
from bamaq.domain import OutboxEvent, Status, TransientRiskError
from tests.fakes import RecordingPublisher

pytestmark = pytest.mark.integration


def test_creation_and_outbox_rollback_together_in_real_mysql(sql_factory, clock):
    @contextmanager
    def failing_factory():
        with sql_factory() as uow:

            def crash():
                uow.session.flush()
                raise RuntimeError("Crash after SQL inserts before commit")

            uow.commit = crash
            yield uow

    service = Transactions(failing_factory, clock, UUIDs())
    with pytest.raises(RuntimeError):
        service.create("123", Decimal("1"))
    with sql_factory() as uow:
        assert uow.session.scalar(select(func.count()).select_from(transactions)) == 0
        assert uow.session.scalar(select(func.count()).select_from(outbox)) == 0


def command_for(factory, transaction_id):
    with factory() as uow:
        return uow.session.scalar(
            select(OutboxEvent)
            .where(
                outbox.c.transaction_id == transaction_id,
                outbox.c.topic == COMMANDS,
            )
            .order_by(outbox.c.id.desc())
        ).payload


def test_sql_end_to_end_decimal_and_status(sql_service, sql_factory):
    transaction = sql_service.create("123", Decimal("9999999999999999.99"))
    assert sql_service.get(transaction.id).value == Decimal("9999999999999999.99")
    assert sql_service.receive(command_for(sql_factory, transaction.id))
    claim = sql_service.claim()
    assert sql_service.complete(claim, result=Status.APPROVED)
    assert sql_service.get(transaction.id).status == Status.APPROVED
    with sql_factory() as uow:
        assert uow.session.scalar(select(func.count()).select_from(outbox)) == 4


def test_real_sql_rollback(sql_factory, sql_service):
    transaction = sql_service.create("123", Decimal("1"))
    with pytest.raises(RuntimeError), sql_factory() as uow:
        entity = uow.transactions.get(transaction.id, lock=True)
        entity.customer_id = "must-rollback"
        uow.outbox.add(
            OutboxEvent(
                event_id="rollback-event",
                transaction_id=transaction.id,
                topic=COMMANDS,
                payload={},
                created_at=entity.created_at,
            )
        )
        uow.session.flush()
        raise RuntimeError("Crash before commit")
    assert sql_service.get(transaction.id).customer_id == "123"
    with sql_factory() as uow:
        assert uow.session.scalar(select(func.count()).select_from(outbox)) == 2


def test_concurrent_post_idempotency(sql_service, sql_factory):
    barrier = Barrier(8)

    def create(_):
        barrier.wait()
        return sql_service.create("123", Decimal("1"), "concurrent-key").id

    with ThreadPoolExecutor(max_workers=8) as pool:
        identifiers = list(pool.map(create, range(8)))
    assert len(set(identifiers)) == 1
    with sql_factory() as uow:
        assert uow.session.scalar(select(func.count()).select_from(transactions)) == 1
        assert uow.session.scalar(select(func.count()).select_from(outbox)) == 2


def test_concurrent_consumers_deduplicate(sql_service, sql_factory):
    transaction = sql_service.create("123", Decimal("1"))
    event = command_for(sql_factory, transaction.id)
    barrier = Barrier(8)

    def consume(_):
        barrier.wait()
        return sql_service.receive(event)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(consume, range(8)))
    assert results.count(True) == 1
    with sql_factory() as uow:
        assert uow.session.scalar(select(func.count()).select_from(jobs)) == 1


def test_skip_locked_does_not_claim_locked_job(sql_service, sql_factory, clock):
    first = sql_service.create("first", Decimal("1"))
    sql_service.receive(command_for(sql_factory, first.id))
    clock.advance(1)
    second = sql_service.create("second", Decimal("1"))
    sql_service.receive(command_for(sql_factory, second.id))
    with sql_factory() as holder:
        locked = holder.jobs.claimable(clock.now())
        assert locked.transaction_id == first.id
        with ThreadPoolExecutor(max_workers=1) as pool:
            claim = pool.submit(sql_service.claim).result(timeout=5)
        assert claim.transaction.id == second.id


def test_concurrent_claim_has_single_owner(sql_service, sql_factory):
    transaction = sql_service.create("123", Decimal("1"))
    sql_service.receive(command_for(sql_factory, transaction.id))
    barrier = Barrier(6)

    def claim(_):
        barrier.wait()
        return sql_service.claim()

    with ThreadPoolExecutor(max_workers=6) as pool:
        claims = list(pool.map(claim, range(6)))
    assert len([c for c in claims if c is not None]) == 1


def test_lease_recovery_and_fencing_real_mysql(sql_service, sql_factory, clock):
    transaction = sql_service.create("123", Decimal("1"))
    sql_service.receive(command_for(sql_factory, transaction.id))
    old = sql_service.claim()
    clock.advance(31)
    new = sql_service.claim()
    assert not sql_service.complete(old, result=Status.REJECTED)
    assert sql_service.complete(new, result=Status.APPROVED)
    assert sql_service.get(transaction.id).status == Status.APPROVED


def test_sql_retry_exhaustion_and_replay(sql_service, sql_factory, clock):
    sql_service.max_attempts = 2
    transaction = sql_service.create("123", Decimal("1"))
    sql_service.receive(command_for(sql_factory, transaction.id))
    for _ in range(2):
        claim = sql_service.claim()
        sql_service.complete(claim, error=TransientRiskError("offline"))
        clock.advance(300)
    assert sql_service.get(transaction.id).status == Status.FAILED
    sql_service.replay(transaction.id)
    sql_service.receive(command_for(sql_factory, transaction.id))
    claim = sql_service.claim()
    assert claim.attempt == 1
    assert claim.transaction.generation == 2
    assert sql_service.complete(claim, result=Status.REJECTED)
    with sql_factory() as uow:
        history = uow.failures.for_generation(transaction.id, 1)
        assert len(history) == 1
        assert history[0].replayed_at is not None


def test_outbox_recovery_and_order_real_mysql(sql_service, sql_factory, clock):
    sql_service.create("123", Decimal("1"))
    publisher = RecordingPublisher()
    relay = Relay(sql_factory, publisher, clock)
    publisher.unavailable = True
    with pytest.raises(ConnectionError):
        relay.once()
    publisher.unavailable = False
    while relay.once():
        pass
    assert [e.payload["event_type"] for e in publisher.events] == [
        "transaction.processing_requested",
        "transaction.status_changed",
    ]
    with sql_factory() as uow:
        assert uow.outbox.oldest_pending() is None


def test_sql_quarantine_is_durable_and_deduplicated(sql_service, sql_factory):
    first = sql_service.quarantine(b"invalid", "Invalid JSON", COMMANDS, 1, 100)
    second = sql_service.quarantine(b"invalid", "Invalid JSON", COMMANDS, 1, 100)
    assert first == second
    with sql_factory() as uow:
        assert len(uow.failures.list_recent(100)) == 1
        assert uow.session.scalar(select(func.count()).select_from(outbox)) == 1
        assert uow.session.scalar(select(jobs.c.status)) is None
