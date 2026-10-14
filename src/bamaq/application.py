"""Use cases and transactional boundaries, independent of transport and persistence."""

import base64
import hashlib
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from bamaq.domain import (
    ConcurrentWrite,
    Conflict,
    FailedMessage,
    InvalidEvent,
    Job,
    JobState,
    NotFound,
    OutboxEvent,
    PermanentRiskError,
    Status,
    Transaction,
    TransientRiskError,
)
from bamaq.ports import Clock, IdGenerator, Publisher, UnitOfWork, UowFactory

COMMANDS = "transactions.commands.v1"
STATUSES = "transactions.status.v1"
DLQ = "transactions.dlq.v1"


def validate_command(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise InvalidEvent("Event must be an object")
    if type(payload.get("schema_version")) is not int or payload["schema_version"] != 1:
        raise InvalidEvent("Unsupported schema_version")
    if payload.get("event_type") != "transaction.processing_requested":
        raise InvalidEvent("Unsupported event_type")
    for name in ("event_id", "transaction_id", "correlation_id"):
        try:
            UUID(payload[name])
        except (KeyError, ValueError, TypeError, AttributeError) as exc:
            raise InvalidEvent(f"Invalid {name}") from exc
    for name in ("generation", "aggregate_version"):
        if type(payload.get(name)) is not int or payload[name] < 1:
            raise InvalidEvent(f"Invalid {name}")
    try:
        timestamp = datetime.fromisoformat(payload["occurred_at"])
        if timestamp.tzinfo is None:
            raise ValueError("Missing timezone")
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidEvent("Invalid occurred_at") from exc
    if not isinstance(payload.get("data"), dict):
        raise InvalidEvent("Invalid data")
    return payload


@dataclass(frozen=True)
class Claim:
    job_id: str
    token: str
    transaction: Transaction
    attempt: int
    event_id: str


class Transactions:
    def __init__(
        self,
        uow: UowFactory,
        clock: Clock,
        ids: IdGenerator,
        *,
        max_attempts: int = 16,
        lease_seconds: int = 30,
        jitter: Any = random.uniform,
    ):
        self.uow = uow
        self.clock = clock
        self.ids = ids
        self.max_attempts = max_attempts
        self.lease_seconds = lease_seconds
        self.jitter = jitter

    def _event(
        self,
        uow: UnitOfWork,
        transaction: Transaction,
        topic: str,
        kind: str,
        data: dict[str, Any] | None = None,
    ) -> str:
        event_id = self.ids.new()
        now = self.clock.now()
        uow.outbox.add(
            OutboxEvent(
                event_id=event_id,
                transaction_id=transaction.id,
                topic=topic,
                created_at=now,
                payload={
                    "event_id": event_id,
                    "event_type": kind,
                    "schema_version": 1,
                    "transaction_id": transaction.id,
                    "generation": transaction.generation,
                    "aggregate_version": transaction.version,
                    "occurred_at": now.isoformat(),
                    "correlation_id": transaction.correlation_id,
                    "data": data or {},
                },
            )
        )
        return event_id

    def _status(self, uow: UnitOfWork, transaction: Transaction) -> None:
        self._event(
            uow,
            transaction,
            STATUSES,
            "transaction.status_changed",
            {"status": transaction.status.value},
        )

    def create(
        self,
        customer_id: str,
        value: Decimal,
        key: str | None = None,
    ) -> Transaction:
        if not customer_id.strip() or len(customer_id) > 128:
            raise ValueError("Invalid customer_id")
        if not value.is_finite() or value <= 0 or value >= Decimal("10000000000000000"):
            raise ValueError("Invalid value")
        if value != value.quantize(Decimal("0.01")):
            raise ValueError("value must have at most two decimal places")
        value = value.quantize(Decimal("0.01"))
        fingerprint = hashlib.sha256(
            json.dumps(
                [customer_id, str(value)],
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        # A unique-key race is resolved by reading the winner in a new transaction.
        for race_attempt in range(3):
            try:
                with self.uow() as uow:
                    existing = uow.transactions.by_key(key) if key else None
                    if existing:
                        if existing.fingerprint != fingerprint:
                            raise Conflict("Idempotency-Key reused with a different request")
                        return existing
                    now = self.clock.now()
                    transaction = Transaction(
                        id=self.ids.new(),
                        customer_id=customer_id,
                        value=value,
                        status=Status.PENDING,
                        version=1,
                        generation=1,
                        correlation_id=self.ids.new(),
                        created_at=now,
                        updated_at=now,
                        idempotency_key=key,
                        fingerprint=fingerprint,
                    )
                    uow.transactions.add(transaction)
                    self._event(uow, transaction, COMMANDS, "transaction.processing_requested")
                    self._status(uow, transaction)
                    uow.commit()
                    return transaction
            except ConcurrentWrite:
                if not key or race_attempt == 2:
                    raise
        raise AssertionError("Unreachable")

    def get(self, transaction_id: str) -> Transaction:
        with self.uow() as uow:
            transaction = uow.transactions.get(transaction_id)
            if transaction is None:
                raise NotFound("Transaction not found")
            return transaction

    def receive(self, payload: Any) -> bool:
        event = validate_command(payload)
        with self.uow() as uow:
            transaction = uow.transactions.get(event["transaction_id"], lock=True)
            if transaction is None:
                raise InvalidEvent("Unknown transaction")
            if uow.jobs.by_event(event["event_id"]):
                return False
            if event["generation"] < transaction.generation:
                return False
            if event["generation"] != transaction.generation:
                raise InvalidEvent("Unknown processing generation")
            if uow.jobs.by_generation(transaction.id, transaction.generation):
                return False
            if transaction.status != Status.PENDING:
                raise InvalidEvent("Transaction is not pending")
            if event["aggregate_version"] != transaction.version:
                raise InvalidEvent("Unexpected aggregate version")
            now = self.clock.now()
            uow.jobs.add(
                Job(
                    id=self.ids.new(),
                    event_id=event["event_id"],
                    transaction_id=transaction.id,
                    generation=transaction.generation,
                    status=JobState.READY,
                    attempts=0,
                    next_attempt_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            uow.commit()
            return True

    def quarantine(self, raw: bytes, reason: str, topic: str, partition: int, offset: int) -> str:
        with self.uow() as uow:
            existing = uow.failures.by_source(topic, partition, offset)
            if existing:
                return existing.id
            now = self.clock.now()
            failure = FailedMessage(
                id=self.ids.new(),
                transaction_id=None,
                generation=None,
                reason=reason[:500],
                payload={"raw_base64": base64.b64encode(raw).decode()},
                created_at=now,
                source_topic=topic,
                source_partition=partition,
                source_offset=offset,
            )
            uow.failures.add(failure)
            event_id = self.ids.new()
            uow.outbox.add(
                OutboxEvent(
                    event_id=event_id,
                    transaction_id=None,
                    topic=DLQ,
                    created_at=now,
                    payload={
                        "event_id": event_id,
                        "event_type": "message.quarantined",
                        "schema_version": 1,
                        "transaction_id": None,
                        "generation": None,
                        "aggregate_version": None,
                        "occurred_at": now.isoformat(),
                        "correlation_id": self.ids.new(),
                        "data": {
                            "failure_id": failure.id,
                            "reason": failure.reason,
                            "source_topic": topic,
                            "partition": partition,
                            "offset": offset,
                        },
                    },
                )
            )
            uow.commit()
            return failure.id

    def _dead(self, uow: UnitOfWork, job: Job, transaction: Transaction, reason: str) -> None:
        now = self.clock.now()
        job.status = JobState.DEAD
        job.updated_at = now
        job.lease_token = None
        job.lease_until = None
        job.last_error = reason[:500]
        transaction.transition(Status.FAILED, now)
        self._status(uow, transaction)
        failure = FailedMessage(
            id=self.ids.new(),
            transaction_id=transaction.id,
            generation=job.generation,
            reason=job.last_error,
            created_at=now,
            payload={"job_id": job.id, "event_id": job.event_id, "attempts": job.attempts},
        )
        uow.failures.add(failure)
        self._event(
            uow,
            transaction,
            DLQ,
            "transaction.processing_failed",
            {
                "failure_id": failure.id,
                "reason": job.last_error,
                "attempts": job.attempts,
            },
        )

    def claim(self) -> Claim | None:
        with self.uow() as uow:
            now = self.clock.now()
            job = uow.jobs.claimable(now)
            if job is None:
                return None
            transaction = uow.transactions.get(job.transaction_id, lock=True)
            if transaction is None:
                raise NotFound("Transaction not found for job")
            if job.generation != transaction.generation:
                job.status = JobState.DEAD
                job.lease_token = None
                job.lease_until = None
                uow.commit()
                return None
            if job.attempts >= self.max_attempts:
                self._dead(uow, job, transaction, "Execution lease expired on final attempt")
                uow.commit()
                return None
            job.attempts += 1
            job.status = JobState.RUNNING
            job.lease_token = self.ids.new()
            job.lease_until = now + timedelta(seconds=self.lease_seconds)
            job.updated_at = now
            if transaction.transition(Status.PROCESSING, now):
                self._status(uow, transaction)
            uow.commit()
            return Claim(job.id, job.lease_token, transaction, job.attempts, job.event_id)

    def complete(
        self,
        claim: Claim,
        *,
        result: Status | None = None,
        error: TransientRiskError | PermanentRiskError | None = None,
    ) -> bool:
        if (result is None) == (error is None):
            raise ValueError("Provide either a result or an error")
        if result is not None and result not in (Status.APPROVED, Status.REJECTED):
            raise ValueError("Invalid risk result")
        with self.uow() as uow:
            job = uow.jobs.get(claim.job_id, lock=True)
            now = self.clock.now()
            if (
                job is None
                or job.status != JobState.RUNNING
                or job.lease_token != claim.token
                or job.lease_until is None
                or job.lease_until <= now
            ):
                return False
            transaction = uow.transactions.get(job.transaction_id, lock=True)
            if transaction is None or transaction.generation != job.generation:
                return False
            if error:
                reason = f"{type(error).__name__}: {error}"[:500]
                if isinstance(error, PermanentRiskError) or job.attempts >= self.max_attempts:
                    self._dead(uow, job, transaction, reason)
                else:
                    delay = min(10 * 2 ** (job.attempts - 1), 300)
                    job.next_attempt_at = now + timedelta(
                        seconds=delay + self.jitter(0, delay * 0.2)
                    )
                    job.status = JobState.READY
                    job.last_error = reason
                    transaction.transition(Status.RETRY_PENDING, now)
                    self._status(uow, transaction)
            else:
                job.status = JobState.DONE
                transaction.transition(result, now)
                self._status(uow, transaction)
            job.lease_token = None
            job.lease_until = None
            job.updated_at = now
            uow.commit()
            return True

    def replay(self, transaction_id: str) -> Transaction:
        with self.uow() as uow:
            transaction = uow.transactions.get(transaction_id, lock=True)
            if transaction is None:
                raise NotFound("Transaction not found")
            if transaction.status != Status.FAILED:
                raise Conflict("Only FAILED transactions can be replayed")
            transaction.generation += 1
            transaction.transition(Status.PENDING, self.clock.now())
            event_id = self._event(
                uow,
                transaction,
                COMMANDS,
                "transaction.processing_requested",
            )
            self._status(uow, transaction)
            for failure in uow.failures.for_generation(transaction.id, transaction.generation - 1):
                if failure.replayed_at is None:
                    failure.replayed_at = self.clock.now()
                    failure.replay_event_id = event_id
            uow.commit()
            return transaction

    def replay_quarantine(self, failure_id: str, corrected: dict[str, Any]) -> str:
        event = validate_command(corrected)
        with self.uow() as uow:
            failure = uow.failures.get(failure_id, lock=True)
            if failure is None:
                raise NotFound("Failure not found")
            if failure.source_topic is None:
                raise Conflict("Use transaction replay for an exhausted job")
            if failure.replayed_at:
                raise Conflict("Failure has already been replayed")
            transaction = uow.transactions.get(event["transaction_id"], lock=True)
            if (
                transaction is None
                or transaction.status != Status.PENDING
                or transaction.generation != event["generation"]
                or transaction.version != event["aggregate_version"]
            ):
                raise Conflict("Corrected command must reference the current pending generation")
            if uow.jobs.by_generation(transaction.id, transaction.generation):
                raise Conflict("Generation already has a job")
            # Generate fresh transport identity; the logical generation remains unchanged.
            event_id = self._event(
                uow,
                transaction,
                COMMANDS,
                "transaction.processing_requested",
                event["data"],
            )
            failure.replayed_at = self.clock.now()
            failure.replay_event_id = event_id
            uow.commit()
            return event_id


class Relay:
    def __init__(self, uow: UowFactory, publisher: Publisher, clock: Clock):
        self.uow = uow
        self.publisher = publisher
        self.clock = clock

    def once(self) -> bool:
        # Row lock remains held until broker ack + SQL commit. No SKIP LOCKED here:
        # another relay must never publish a later row while this one is pending.
        with self.uow() as uow:
            event = uow.outbox.oldest_pending()
            if event is None:
                return False
            self.publisher.publish(event)
            event.published_at = self.clock.now()
            uow.commit()
            return True
