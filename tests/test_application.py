import json
from decimal import Decimal
from uuid import uuid4

import pytest

from bamaq.adapters.kafka import MessageHandler
from bamaq.application import COMMANDS, DLQ, Relay
from bamaq.domain import (
    Conflict,
    InvalidEvent,
    JobState,
    PermanentRiskError,
    Status,
    TransientRiskError,
)
from tests.fakes import RecordingPublisher


def command_for(memory, transaction_id=None):
    commands = [e.payload for e in memory.rows["outbox"].values() if e.topic == COMMANDS]
    return next(
        e
        for e in reversed(commands)
        if transaction_id is None or e["transaction_id"] == transaction_id
    )


def pending_job(service, memory):
    transaction = service.create("123", Decimal("1500"))
    command = command_for(memory)
    service.receive(command)
    return transaction, command


@pytest.mark.parametrize("result", [Status.APPROVED, Status.REJECTED])
def test_business_outcomes(service, memory, result):
    transaction, _ = pending_job(service, memory)
    claim = service.claim()
    assert service.complete(claim, result=result)
    assert service.get(transaction.id).status == result
    assert next(iter(memory.rows["jobs"].values())).status == JobState.DONE
    statuses = [
        e.payload["data"]["status"]
        for e in memory.rows["outbox"].values()
        if e.payload["event_type"] == "transaction.status_changed"
    ]
    assert statuses == ["PENDING", "PROCESSING", result.value]


def test_duplicate_and_logical_duplicate(service, memory):
    transaction, command = pending_job(service, memory)
    assert service.receive(command) is False
    assert service.receive(command | {"event_id": str(uuid4())}) is False
    claim = service.claim()
    service.complete(claim, result=Status.APPROVED)
    assert not service.complete(claim, result=Status.REJECTED)
    assert not service.receive(command)
    assert len(memory.rows["jobs"]) == 1
    assert service.get(transaction.id).status == Status.APPROVED


def test_atomic_creation_rollback(service, memory):
    memory.fail_commit = True
    with pytest.raises(RuntimeError):
        service.create("123", Decimal("1"))
    assert not memory.rows["transactions"]
    assert not memory.rows["outbox"]


def test_request_idempotency(service):
    transaction = service.create("123", Decimal("1.0"), "same-key")
    assert service.create("123", Decimal("1.00"), "same-key").id == transaction.id
    with pytest.raises(Conflict):
        service.create("123", Decimal("2"), "same-key")


def test_temporary_failure_survives_restart(service, memory, clock):
    transaction, _ = pending_job(service, memory)
    claim = service.claim()
    service.complete(claim, error=TransientRiskError("offline"))
    assert service.get(transaction.id).status == Status.RETRY_PENDING
    assert service.claim() is None
    clock.advance(10)
    from bamaq.application import Transactions
    from bamaq.bootstrap import UUIDs

    restarted = Transactions(memory, clock, UUIDs(), jitter=lambda *_: 0)
    retry = restarted.claim()
    assert retry.attempt == 2
    restarted.complete(retry, result=Status.APPROVED)
    assert restarted.get(transaction.id).status == Status.APPROVED


def test_outage_of_thirty_minutes(service, memory, clock):
    transaction, _ = pending_job(service, memory)
    started = clock.now()
    while (clock.now() - started).total_seconds() < 1800:
        claim = service.claim()
        assert claim is not None
        service.complete(claim, error=TransientRiskError("offline"))
        job = next(iter(memory.rows["jobs"].values()))
        clock.advance((job.next_attempt_at - clock.now()).total_seconds())
    claim = service.claim()
    assert claim.attempt <= 16
    service.complete(claim, result=Status.APPROVED)
    assert service.get(transaction.id).status == Status.APPROVED


def test_exhaustion_dlq_and_replay(service, memory, clock):
    service.max_attempts = 3
    transaction, original = pending_job(service, memory)
    for _ in range(3):
        claim = service.claim()
        service.complete(claim, error=TransientRiskError("offline"))
        clock.advance(300)
    assert service.get(transaction.id).status == Status.FAILED
    assert len(memory.rows["failures"]) == 1
    assert len([e for e in memory.rows["outbox"].values() if e.topic == DLQ]) == 1
    replay = service.replay(transaction.id)
    assert replay.generation == 2
    assert replay.status == Status.PENDING
    assert not service.receive(original)
    service.receive(command_for(memory))
    claim = service.claim()
    assert claim.attempt == 1
    service.complete(claim, result=Status.APPROVED)
    assert service.get(transaction.id).status == Status.APPROVED
    assert next(iter(memory.rows["failures"].values())).replayed_at is not None
    with pytest.raises(Conflict):
        service.replay(transaction.id)


def test_permanent_failure_does_not_retry(service, memory):
    transaction, _ = pending_job(service, memory)
    service.complete(service.claim(), error=PermanentRiskError("invalid contract"))
    assert service.get(transaction.id).status == Status.FAILED
    assert service.claim() is None


def test_expired_lease_fences_old_executor(service, memory, clock):
    transaction, _ = pending_job(service, memory)
    old = service.claim()
    assert service.claim() is None
    clock.advance(31)
    recovered = service.claim()
    assert recovered.token != old.token
    assert recovered.attempt == 2
    assert not service.complete(old, result=Status.REJECTED)
    assert service.complete(recovered, result=Status.APPROVED)
    assert service.get(transaction.id).status == Status.APPROVED


def test_last_attempt_crash_goes_to_dlq(service, memory, clock):
    service.max_attempts = 1
    transaction, _ = pending_job(service, memory)
    service.claim()
    clock.advance(31)
    assert service.claim() is None
    assert service.get(transaction.id).status == Status.FAILED
    assert len(memory.rows["failures"]) == 1


def test_kafka_failure_keeps_outbox_and_republishes_same_event(service, memory, clock):
    service.create("123", Decimal("1"))
    publisher = RecordingPublisher()
    relay = Relay(memory, publisher, clock)
    publisher.unavailable = True
    with pytest.raises(ConnectionError):
        relay.once()
    assert all(e.published_at is None for e in memory.rows["outbox"].values())
    publisher.unavailable = False
    assert relay.once()
    first = publisher.events[-1]
    # Broker ack occurred, then SQL commit failed: delivery must be duplicated safely.
    memory.fail_commit = True
    with pytest.raises(RuntimeError):
        relay.once()
    uncommitted = publisher.events[-1]
    memory.fail_commit = False
    relay.once()
    assert publisher.events[-1].event_id == uncommitted.event_id
    assert first.event_id != uncommitted.event_id


@pytest.mark.parametrize("raw", [b"not json", b"{}", b"[]", b"\xff"])
def test_invalid_message_quarantine_is_idempotent(service, memory, raw):
    handler = MessageHandler(service)
    handler.handle(raw, COMMANDS, 0, 10)
    handler.handle(raw, COMMANDS, 0, 10)
    assert len(memory.rows["failures"]) == 1
    assert len(memory.rows["outbox"]) == 1
    assert next(iter(memory.rows["outbox"].values())).topic == DLQ


def test_unknown_version_and_corrected_quarantine_replay(service, memory):
    service.create("123", Decimal("1"))
    command = command_for(memory)
    raw = json.dumps(command | {"schema_version": 99}).encode()
    MessageHandler(service).handle(raw, COMMANDS, 0, 1)
    failure = next(iter(memory.rows["failures"].values()))
    event_id = service.replay_quarantine(failure.id, command)
    assert event_id != command["event_id"]
    assert service.receive(command_for(memory))
    with pytest.raises(Conflict):
        service.replay_quarantine(failure.id, command)


def test_event_additive_fields_are_compatible(service, memory):
    service.create("123", Decimal("1"))
    assert service.receive(command_for(memory) | {"future_optional_field": "allowed"})


def test_future_generation_is_quarantined(service, memory):
    service.create("123", Decimal("1"))
    with pytest.raises(InvalidEvent):
        service.receive(command_for(memory) | {"generation": 2})
