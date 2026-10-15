import argparse
import asyncio
import csv
import json
import os
from collections import Counter
from copy import deepcopy
from decimal import Decimal
from threading import Lock
from uuid import uuid4

import httpx
import pytest

from bamaq.domain import Status
from scripts.load_test import (
    DatabaseMonitor,
    KafkaMonitor,
    distribution,
    main,
    parse_stages,
    parser,
    run,
    write_reports,
)
from tests.fakes import RecordingPublisher
from tests.test_mysql import command_for


class MemoryMonitor:
    def __init__(self):
        self.rows = []
        self.lock = Lock()
        self.publish = True

    def create(self, sequence):
        row = {
            "id": str(uuid4()),
            "sequence": sequence,
            "value": "1500.00" if sequence % 2 == 0 else "6000.00",
            "status": "APPROVED" if sequence % 2 == 0 else "REJECTED",
            "expected_status": "APPROVED" if sequence % 2 == 0 else "REJECTED",
            "value_matches_request": True,
            "end_to_end_ms": 500,
        }
        with self.lock:
            self.rows.append(row)
        return row

    def snapshot(self):
        with self.lock:
            count = len(self.rows)
            statuses = Counter(row["status"] for row in self.rows)
        return {
            "created": count,
            "completed": count,
            "approved": statuses["APPROVED"],
            "rejected": statuses["REJECTED"],
            "failed": statuses["FAILED"],
            "pending": 0,
            "outbox_events": count * 4,
            "outbox_pending": 0 if self.publish else count * 4,
            "outbox_oldest_seconds": 0,
            "jobs_ready": 0,
            "jobs_running": 0,
            "jobs_dead": 0,
            "retries": 0,
        }

    def final_rows(self):
        with self.lock:
            return deepcopy(self.rows)


def options(*extra):
    args = parser().parse_args(
        [
            "--stages",
            "10:0.2",
            "--sample-interval",
            "0.01",
            "--drain-timeout",
            "0.3",
            "--quiet",
            *extra,
        ]
    )
    args.run_id = f"load-{uuid4()}"
    return args


def transport_for(monitor, *, status=202, delay=0, invalid=False):
    async def handle(request):
        if request.url.path == "/health/ready":
            return httpx.Response(200, json={"status": "ready"})
        if status != 202:
            return httpx.Response(status, json={"detail": "unavailable"})
        body = json.loads(request.content)
        sequence = int(request.headers["Idempotency-Key"].rsplit(":", 1)[1])
        row = monitor.create(sequence)
        if delay:
            await asyncio.sleep(delay)
        return httpx.Response(
            202,
            json={
                "id": row["id"],
                "customer_id": body["customer_id"],
                "value": "invalid" if invalid else row["value"],
                "status": "PENDING",
            },
        )

    return httpx.MockTransport(handle)


@pytest.mark.parametrize(
    "value", ["", "1", "1:2:3", "0:1", "1:0", "nan:1", "inf:1", "-1:2", "0.1:0.1"]
)
def test_invalid_stage_definitions(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_stages(value)


def test_case_rates_and_percentile_definition():
    from scripts.load_test import CASE_STAGES

    assert sum(s.requests for s in parse_stages(CASE_STAGES)) == 25290
    assert distribution([])["p95"] is None
    assert distribution(list(range(1, 101)))["p95"] == 95
    assert distribution([42])["p99"] == 42


def test_full_load_run_and_exported_reports(tmp_path):
    monitor = MemoryMonitor()
    args = options()
    report, requests, rows = asyncio.run(run(args, monitor, transport=transport_for(monitor)))
    assert report["passed"]
    assert report["summary"]["accepted"] == 2
    assert report["summary"]["statuses"] == {"APPROVED": 1, "REJECTED": 1}
    assert report["summary"]["end_to_end_latency_ms"]["p95"] == 500
    assert report["samples"][-1]["phase"] == "final"
    directory = write_reports(tmp_path, report, requests, rows)
    exported = json.loads((directory / "report.json").read_text(encoding="utf-8"))
    assert exported["passed"]
    assert "database_url" not in exported["config"]
    with (directory / "requests.csv").open(encoding="utf-8") as file:
        assert len(list(csv.DictReader(file))) == 2
    assert (directory / "transactions.csv").exists()
    assert (directory / "samples.csv").exists()


def test_http_errors_are_reported_as_failed_load(tmp_path):
    monitor = MemoryMonitor()
    report, requests, rows = asyncio.run(
        run(
            options(),
            monitor,
            transport=transport_for(monitor, status=503),
        )
    )
    assert not report["passed"]
    assert report["summary"]["request_errors"] == {"HTTP_503": 2}
    assert len(requests) == 2
    assert not report["checks"]["all_requests_accepted"]
    directory = write_reports(tmp_path, report, requests, rows)
    with (directory / "transactions.csv").open(encoding="utf-8") as file:
        reader = csv.DictReader(file)
        assert "id" in reader.fieldnames
        assert list(reader) == []


def test_timeout_after_commit_does_not_hide_persisted_transactions():
    monitor = MemoryMonitor()
    report, _, rows = asyncio.run(
        run(
            options("--request-timeout", "0.02"),
            monitor,
            transport=transport_for(monitor, delay=0.2),
        )
    )
    assert not report["passed"]
    assert len(rows) == 2
    assert report["summary"]["accepted"] == 0
    assert report["summary"]["request_errors"] == {"TimeoutError": 2}
    assert not report["checks"]["accepted_ids_match_persisted"]


def test_saturated_generator_drops_slots_instead_of_lowering_rate():
    monitor = MemoryMonitor()
    report, _, _ = asyncio.run(
        run(
            options("--stages", "100:0.1", "--concurrency", "1"),
            monitor,
            transport=transport_for(monitor, delay=0.2),
        )
    )
    assert not report["passed"]
    assert report["summary"]["sent"] < 10
    stage = report["stages"][0]
    assert stage["dropped_concurrency"] > 0
    assert stage["sent"] + stage["dropped_concurrency"] + stage["dropped_generator_lag"] == 10


def test_drain_timeout_keeps_pending_outbox_visible():
    monitor = MemoryMonitor()
    monitor.publish = False
    report, _, _ = asyncio.run(
        run(
            options("--drain-timeout", "0.05"),
            monitor,
            transport=transport_for(monitor),
        )
    )
    assert not report["passed"]
    assert report["summary"]["last_sample"]["outbox_pending"] == 8
    assert not report["checks"]["outbox_drained"]


def test_configured_latency_threshold_is_enforced():
    monitor = MemoryMonitor()
    report, _, _ = asyncio.run(
        run(
            options("--max-e2e-p95-ms", "100"),
            monitor,
            transport=transport_for(monitor),
        )
    )
    assert not report["passed"]
    assert not report["checks"]["end_to_end_p95_within_limit"]


def test_invalid_decimal_in_accepted_response_is_counted_as_contract_error():
    monitor = MemoryMonitor()
    report, _, _ = asyncio.run(
        run(options(), monitor, transport=transport_for(monitor, invalid=True))
    )
    assert not report["passed"]
    assert report["summary"]["request_errors"] == {"InvalidOperation": 2}


def test_maximum_request_budget_is_checked_before_starting():
    with pytest.raises(SystemExit) as exc:
        main(["--case", "--max-requests", "100"])
    assert exc.value.code == 2


@pytest.mark.integration
def test_mysql_monitor_scopes_run_and_measures_durable_completion(
    mysql_engine,
    sql_service,
    sql_factory,
    clock,
):
    from bamaq.application import Relay

    run_id = f"load-{uuid4()}"
    transaction = sql_service.create(run_id, Decimal("1500"), f"{run_id}:0")
    sql_service.create("unrelated", Decimal("1"), "unrelated-key")
    monitor = DatabaseMonitor(mysql_engine.url.render_as_string(hide_password=False), run_id)
    try:
        initial = monitor.snapshot()
        assert initial["created"] == 1
        assert initial["pending"] == 1
        assert initial["outbox_pending"] == 2
        sql_service.receive(command_for(sql_factory, transaction.id))
        assert monitor.snapshot()["jobs_ready"] == 1
        claim = sql_service.claim()
        clock.advance(1)
        sql_service.complete(claim, result=Status.APPROVED)
        final = monitor.snapshot()
        assert final["completed"] == 1
        assert final["outbox_events"] == 4
        rows = monitor.final_rows()
        assert len(rows) == 1
        assert rows[0]["end_to_end_ms"] == 1000
        assert rows[0]["value_matches_request"]
        relay = Relay(sql_factory, RecordingPublisher(), clock)
        while relay.once():
            pass
        assert monitor.snapshot()["outbox_pending"] == 0
    finally:
        monitor.close()


@pytest.mark.kafka
@pytest.mark.integration
def test_kafka_lag_monitor_reads_offsets_without_processing_messages():
    servers = os.getenv("BAMAQ_TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.skip("Set BAMAQ_TEST_KAFKA_BOOTSTRAP_SERVERS")
    monitor = KafkaMonitor(servers, f"load-observer-{uuid4()}")
    try:
        assert monitor.lag() >= 0
    finally:
        monitor.close()
