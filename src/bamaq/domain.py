"""Business objects. No framework, database or messaging dependencies."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRY_PENDING = "RETRY_PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class JobState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    DONE = "DONE"
    DEAD = "DEAD"


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


class InvalidEvent(Exception):
    pass


class TransientRiskError(Exception):
    pass


class PermanentRiskError(Exception):
    pass


class ConcurrentWrite(Exception):
    """Unique constraint race; retry the entire application transaction."""


@dataclass
class Transaction:
    id: str
    customer_id: str
    value: Decimal
    status: Status
    version: int
    generation: int
    correlation_id: str
    created_at: datetime
    updated_at: datetime
    idempotency_key: str | None = None
    fingerprint: str | None = None

    def transition(self, status: Status, now: datetime) -> bool:
        allowed = {
            Status.PENDING: {Status.PROCESSING},
            Status.PROCESSING: {
                Status.APPROVED,
                Status.REJECTED,
                Status.RETRY_PENDING,
                Status.FAILED,
            },
            Status.RETRY_PENDING: {Status.PROCESSING},
            Status.FAILED: {Status.PENDING},
        }
        if self.status == status:
            return False
        if status not in allowed.get(self.status, set()):
            raise Conflict(f"Invalid transition: {self.status} -> {status}")
        self.status = status
        self.version += 1
        self.updated_at = now
        return True


@dataclass
class Job:
    id: str
    event_id: str
    transaction_id: str
    generation: int
    status: JobState
    attempts: int
    next_attempt_at: datetime
    created_at: datetime
    updated_at: datetime
    lease_token: str | None = None
    lease_until: datetime | None = None
    last_error: str | None = None


@dataclass
class OutboxEvent:
    event_id: str
    transaction_id: str | None
    topic: str
    payload: dict[str, Any]
    created_at: datetime
    id: int | None = None
    published_at: datetime | None = None


@dataclass
class FailedMessage:
    id: str
    transaction_id: str | None
    generation: int | None
    reason: str
    payload: dict[str, Any]
    created_at: datetime
    source_topic: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None
    replayed_at: datetime | None = None
    replay_event_id: str | None = None
