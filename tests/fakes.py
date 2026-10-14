from copy import deepcopy
from datetime import UTC, datetime, timedelta

from bamaq.domain import JobState


class FrozenClock:
    def __init__(self):
        self.current = datetime(2026, 10, 13, 12, tzinfo=UTC)

    def now(self):
        return self.current

    def advance(self, seconds):
        self.current += timedelta(seconds=seconds)


class MemoryStore:
    def __init__(self):
        self.rows = {"transactions": {}, "jobs": {}, "outbox": {}, "failures": {}}
        self.fail_commit = False

    def __call__(self):
        return MemoryUow(self)


class MemoryUow:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.rows = deepcopy(self.db.rows)
        self.transactions = Transactions(self.rows["transactions"])
        self.jobs = Jobs(self.rows["jobs"])
        self.outbox = Outbox(self.rows["outbox"])
        self.failures = Failures(self.rows["failures"])
        return self

    def commit(self):
        if self.db.fail_commit:
            raise RuntimeError("Simulated database crash")
        self.db.rows = deepcopy(self.rows)

    def __exit__(self, *args):
        pass


class Repository:
    def __init__(self, rows):
        self.rows = rows

    def get(self, identifier, *, lock=False):
        return self.rows.get(identifier)

    def add(self, entity):
        self.rows[entity.id] = entity


class Transactions(Repository):
    def by_key(self, key):
        return next((t for t in self.rows.values() if t.idempotency_key == key), None)


class Jobs(Repository):
    def by_event(self, event_id):
        return next((j for j in self.rows.values() if j.event_id == event_id), None)

    def by_generation(self, transaction_id, generation):
        return next(
            (
                j
                for j in self.rows.values()
                if j.transaction_id == transaction_id and j.generation == generation
            ),
            None,
        )

    def claimable(self, now):
        return next(
            (
                j
                for j in self.rows.values()
                if (j.status == JobState.READY and j.next_attempt_at <= now)
                or (j.status == JobState.RUNNING and j.lease_until <= now)
            ),
            None,
        )


class Outbox(Repository):
    def add(self, event):
        event.id = len(self.rows) + 1
        super().add(event)

    def oldest_pending(self):
        return next((e for e in self.rows.values() if e.published_at is None), None)


class Failures(Repository):
    def by_source(self, topic, partition, offset):
        return next(
            (
                f
                for f in self.rows.values()
                if (
                    f.source_topic,
                    f.source_partition,
                    f.source_offset,
                )
                == (topic, partition, offset)
            ),
            None,
        )

    def list_recent(self, limit):
        return sorted(self.rows.values(), key=lambda f: f.created_at, reverse=True)[:limit]

    def for_generation(self, transaction_id, generation):
        return [
            f
            for f in self.rows.values()
            if f.transaction_id == transaction_id and f.generation == generation
        ]


class RecordingPublisher:
    def __init__(self):
        self.events = []
        self.unavailable = False

    def publish(self, event):
        if self.unavailable:
            raise ConnectionError("Simulated Kafka outage")
        self.events.append(deepcopy(event))
