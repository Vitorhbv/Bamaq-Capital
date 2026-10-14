import logging
import signal
import tempfile
import time
from pathlib import Path
from threading import Event

from bamaq.adapters.kafka import KafkaPublisher, MessageHandler, consume
from bamaq.adapters.risk import HttpRisk
from bamaq.application import Relay
from bamaq.bootstrap import SystemClock, build
from bamaq.domain import PermanentRiskError, TransientRiskError


def heartbeat_path(role):
    return Path(tempfile.gettempdir()) / f"bamaq-{role}.heartbeat"


def healthy(role):
    try:
        return time.time() - heartbeat_path(role).stat().st_mtime < 30
    except FileNotFoundError:
        return False


def run(role, settings):
    log = logging.getLogger(role)
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    engine, factory, service = build(settings)
    heartbeat = lambda: heartbeat_path(role).touch()  # noqa: E731
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers) if role == "relay" else None
    risk = HttpRisk(settings.risk_url, settings.risk_timeout_seconds) if role == "worker" else None
    relay = Relay(factory, publisher, SystemClock()) if publisher else None
    try:
        if role == "consumer":
            consume(settings, MessageHandler(service), stop, heartbeat)
            return
        while not stop.is_set():
            heartbeat()
            try:
                if role == "relay":
                    worked = relay.once()
                else:
                    claim = service.claim()
                    worked = claim is not None
                    if claim:
                        context = {
                            "transaction_id": claim.transaction.id,
                            "event_id": claim.event_id,
                            "generation": claim.transaction.generation,
                            "attempt": claim.attempt,
                            "correlation_id": claim.transaction.correlation_id,
                        }
                        started = time.monotonic()
                        log.info("risk_analysis_started", extra=context)
                        try:
                            result = risk.analyze(claim.transaction)
                        except (TransientRiskError, PermanentRiskError) as exc:
                            applied = service.complete(claim, error=exc)
                            log.warning(
                                "risk_analysis_failed",
                                extra=context
                                | {
                                    "error_type": type(exc).__name__,
                                    "duration_ms": round((time.monotonic() - started) * 1000),
                                },
                            )
                        else:
                            applied = service.complete(claim, result=result)
                            log.info(
                                "risk_analysis_completed",
                                extra=context
                                | {
                                    "duration_ms": round((time.monotonic() - started) * 1000),
                                },
                            )
                        if not applied:
                            log.warning("stale_completion_discarded", extra=context)
                if not worked:
                    stop.wait(settings.poll_seconds)
            except Exception as exc:
                # The durable lease/outbox preserves work even for unexpected failures.
                log.error("iteration_failed", extra={"error_type": type(exc).__name__})
                stop.wait(1)
    finally:
        if publisher:
            publisher.close()
        engine.dispose()
        heartbeat_path(role).unlink(missing_ok=True)
