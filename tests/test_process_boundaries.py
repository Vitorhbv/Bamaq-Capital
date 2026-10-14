import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from bamaq.adapters.kafka import consume
from bamaq.api import create_app
from bamaq.application import COMMANDS
from bamaq.domain import PermanentRiskError
from tests.test_application import command_for


class Stop:
    def __init__(self):
        self.stopped = False

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, delay):
        pass


def test_consumer_does_not_advance_or_ack_after_failed_handoff(monkeypatch):
    operations = []
    stop = Stop()
    message = SimpleNamespace(
        value=lambda: b"payload",
        topic=lambda: COMMANDS,
        partition=lambda: 0,
        offset=lambda: 5,
        error=lambda: None,
    )

    class Consumer:
        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            operations.append("poll")
            return message

        def commit(self, **kwargs):
            assert kwargs == {"message": message, "asynchronous": False}
            operations.append("commit")
            stop.set()

        def close(self):
            operations.append("close")

    class Handler:
        def handle(self, raw, topic, partition, offset):
            assert (raw, topic, partition, offset) == (b"payload", COMMANDS, 0, 5)
            if "failed" not in operations:
                operations.append("failed")
                raise ConnectionError("MySQL unavailable")
            operations.append("persisted")

    monkeypatch.setattr("bamaq.adapters.kafka.make_consumer", lambda *_: Consumer())
    settings = SimpleNamespace(kafka_bootstrap_servers="unused", kafka_group_id="unused")
    consume(settings, Handler(), stop, lambda: None)
    assert operations == ["poll", "failed", "persisted", "commit", "close"]


def test_consumer_commit_failure_exits_for_safe_redelivery(monkeypatch):
    closed = []
    message = SimpleNamespace(
        value=lambda: b"payload",
        topic=lambda: COMMANDS,
        partition=lambda: 0,
        offset=lambda: 5,
        error=lambda: None,
    )
    consumer = SimpleNamespace(
        subscribe=lambda *_: None,
        poll=lambda *_: message,
        close=lambda: closed.append(True),
    )

    def fail_commit(**kwargs):
        raise RuntimeError("Lost consumer-group membership")

    consumer.commit = fail_commit
    monkeypatch.setattr("bamaq.adapters.kafka.make_consumer", lambda *_: consumer)
    settings = SimpleNamespace(kafka_bootstrap_servers="unused", kafka_group_id="unused")
    with pytest.raises(RuntimeError):
        consume(settings, SimpleNamespace(handle=lambda *_: None), Stop(), lambda: None)
    assert closed == [True]


def test_api_database_outage_returns_503_without_driver_details(service):
    from fastapi.testclient import TestClient

    class BrokenEngine:
        def connect(self):
            raise OperationalError("SELECT secret", {"customer": "sensitive"}, Exception("offline"))

        def dispose(self):
            pass

    with TestClient(create_app(service=service, engine=BrokenEngine())) as client:
        assert client.get("/health/live").status_code == 200
        response = client.get("/health/ready")
        assert response.status_code == 503
        assert "sensitive" not in response.text
        assert "secret" not in response.text


def test_cli_failure_inspection_and_replay(service, memory, monkeypatch, capsys):
    from bamaq import cli

    transaction = service.create("sensitive-customer", Decimal("1"))
    service.receive(command_for(memory))
    service.complete(service.claim(), error=PermanentRiskError("Invalid risk response contract"))
    disposed = []
    engine = SimpleNamespace(dispose=lambda: disposed.append(True))
    monkeypatch.setattr(cli, "build", lambda *_: (engine, memory, service))
    monkeypatch.setattr(cli, "configure", lambda *_: None)
    assert cli.main(["failures"]) == 0
    failures = capsys.readouterr().out
    assert json.loads(failures)[0]["transaction_id"] == transaction.id
    assert "sensitive-customer" not in failures
    assert cli.main(["replay", transaction.id]) == 0
    assert json.loads(capsys.readouterr().out)["generation"] == 2
    assert cli.main(["replay", transaction.id]) == 2
    assert "Only FAILED" in capsys.readouterr().out
    assert len(disposed) == 3
