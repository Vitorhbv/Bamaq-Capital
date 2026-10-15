"""Local open-loop load generator with durable processing and outbox measurements.

Uses the project's pinned HTTPX, SQLAlchemy and Kafka clients. Does not delete data.
Run with --help or see docs/load-testing.md.
"""

import argparse
import asyncio
import csv
import json
import math
import os
import platform
import sys
import tempfile
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID, uuid4

import httpx
from sqlalchemy import text

from bamaq.adapters.database import make_engine
from bamaq.config import Settings

TERMINAL = {"APPROVED", "REJECTED", "FAILED"}
CASE_STAGES = "25:30,50:30,100:30,167:120"


@dataclass(frozen=True)
class Stage:
    rate: float
    seconds: float

    @property
    def requests(self):
        return math.floor(self.rate * self.seconds)


def parse_stages(value):
    try:
        stages = [Stage(*(float(v) for v in part.split(":"))) for part in value.split(",")]
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError("Use RPS:SECONDS, e.g. 5:10,10:10") from exc
    if not stages or any(
        not math.isfinite(s.rate)
        or not math.isfinite(s.seconds)
        or s.rate <= 0
        or s.seconds <= 0
        or s.requests < 1
        for s in stages
    ):
        raise argparse.ArgumentTypeError(
            "Each stage must have positive finite RPS/duration and >=1 request"
        )
    return stages


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Expected a positive finite number")
    return number


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return number


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "min": None, "p50": None, "p95": None, "p99": None, "max": None}
    return {
        "count": len(ordered),
        "min": round(ordered[0], 3),
        **{f"p{p}": round(ordered[math.ceil(p / 100 * len(ordered)) - 1], 3) for p in (50, 95, 99)},
        "max": round(ordered[-1], 3),
    }


@dataclass
class RequestResult:
    sequence: int
    stage: int
    status_code: int | None
    latency_ms: float
    transaction_id: str | None = None
    error: str | None = None


class DatabaseMonitor:
    """Read-only queries scoped to this run's indexed Idempotency-Key prefix."""

    def __init__(self, url, run_id):
        self.engine = make_engine(url)
        if self.engine.dialect.name != "mysql":
            raise ValueError("The load monitor requires the application's MySQL database")
        self.params = {"prefix": run_id + ":%"}

    def snapshot(self):
        with self.engine.connect() as connection:
            statuses = dict(
                connection.execute(
                    text(
                        "SELECT status, COUNT(*) FROM transactions "
                        "WHERE idempotency_key LIKE :prefix GROUP BY status"
                    ),
                    self.params,
                ).all()
            )
            events = (
                connection.execute(
                    text(
                        "SELECT COUNT(*) AS total, "
                        "COALESCE(SUM(o.published_at IS NULL), 0) AS pending, "
                        "MIN(CASE WHEN o.published_at IS NULL THEN o.created_at END) AS oldest "
                        "FROM outbox_events o JOIN transactions t ON t.id = o.transaction_id "
                        "WHERE t.idempotency_key LIKE :prefix"
                    ),
                    self.params,
                )
                .mappings()
                .one()
            )
            jobs = dict(
                connection.execute(
                    text(
                        "SELECT j.status, COUNT(*) FROM processing_jobs j "
                        "JOIN transactions t ON t.id = j.transaction_id "
                        "WHERE t.idempotency_key LIKE :prefix GROUP BY j.status"
                    ),
                    self.params,
                ).all()
            )
            retries = connection.execute(
                text(
                    "SELECT COALESCE(SUM(GREATEST(j.attempts - 1, 0)), 0) "
                    "FROM processing_jobs j JOIN transactions t ON t.id = j.transaction_id "
                    "WHERE t.idempotency_key LIKE :prefix"
                ),
                self.params,
            ).scalar_one()
        oldest = events["oldest"]
        return {
            "created": sum(statuses.values()),
            "completed": sum(statuses.get(s, 0) for s in TERMINAL),
            "approved": statuses.get("APPROVED", 0),
            "rejected": statuses.get("REJECTED", 0),
            "failed": statuses.get("FAILED", 0),
            "pending": sum(n for s, n in statuses.items() if s not in TERMINAL),
            "outbox_events": int(events["total"]),
            "outbox_pending": int(events["pending"]),
            "outbox_oldest_seconds": round(
                max(0, (datetime.now(UTC).replace(tzinfo=None) - oldest).total_seconds()), 3
            )
            if oldest
            else 0,
            "jobs_ready": jobs.get("READY", 0),
            "jobs_running": jobs.get("RUNNING", 0),
            "jobs_dead": jobs.get("DEAD", 0),
            "retries": int(retries),
        }

    def final_rows(self):
        with self.engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "SELECT id, idempotency_key, value, status, "
                        "TIMESTAMPDIFF(MICROSECOND, created_at, updated_at) / 1000.0 AS latency_ms "
                        "FROM transactions WHERE idempotency_key LIKE :prefix"
                    ),
                    self.params,
                )
                .mappings()
                .all()
            )
        result = []
        for row in rows:
            sequence = int(row["idempotency_key"].rsplit(":", 1)[1])
            result.append(
                {
                    "id": row["id"],
                    "sequence": sequence,
                    "value": str(row["value"]),
                    "status": row["status"],
                    "expected_status": "APPROVED" if sequence % 2 == 0 else "REJECTED",
                    "value_matches_request": row["value"]
                    == Decimal("1500.00" if sequence % 2 == 0 else "6000.00"),
                    "end_to_end_ms": float(row["latency_ms"])
                    if row["status"] in TERMINAL
                    else None,
                }
            )
        return sorted(result, key=lambda r: r["sequence"])

    def close(self):
        self.engine.dispose()


class KafkaMonitor:
    """Observe group offsets without joining the group or committing anything."""

    def __init__(self, servers, group):
        from confluent_kafka import Consumer

        self.client = Consumer(
            {
                "bootstrap.servers": servers,
                "group.id": group,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )

    def lag(self):
        from confluent_kafka import TopicPartition

        topic = "transactions.commands.v1"
        metadata = self.client.list_topics(topic=topic, timeout=3).topics[topic]
        if metadata.error:
            raise RuntimeError("Command topic unavailable")
        offsets = self.client.committed(
            [TopicPartition(topic, p) for p in metadata.partitions],
            timeout=3,
        )
        lag = 0
        for partition in offsets:
            if partition.error:
                raise RuntimeError("Consumer group offsets unavailable")
            low, high = self.client.get_watermark_offsets(partition, timeout=1)
            lag += max(0, high - max(low, partition.offset))
        return lag

    def close(self):
        self.client.close()


async def create_one(client, sequence, stage, run_id, timeout):
    started = time.perf_counter()
    status = None
    try:
        async with asyncio.timeout(timeout):
            response = await client.post(
                "/transactions",
                json={
                    "customer_id": run_id,
                    "value": "1500.00" if sequence % 2 == 0 else "6000.00",
                },
                headers={"Idempotency-Key": f"{run_id}:{sequence}"},
            )
            status = response.status_code
            if status != 202:
                return RequestResult(
                    sequence,
                    stage,
                    status,
                    (time.perf_counter() - started) * 1000,
                    error=f"HTTP_{status}",
                )
            body = response.json()
            identifier = str(UUID(body["id"]))
            expected_value = Decimal("1500.00" if sequence % 2 == 0 else "6000.00")
            if body["customer_id"] != run_id or Decimal(body["value"]) != expected_value:
                raise ValueError("Unexpected transaction data")
            if body["status"] not in {"PENDING", "PROCESSING", "RETRY_PENDING", *TERMINAL}:
                raise ValueError("Unexpected transaction state")
            return RequestResult(
                sequence,
                stage,
                status,
                (time.perf_counter() - started) * 1000,
                transaction_id=identifier,
            )
    except (
        TimeoutError,
        httpx.HTTPError,
        ValueError,
        KeyError,
        TypeError,
        InvalidOperation,
    ) as exc:
        return RequestResult(
            sequence,
            stage,
            status,
            (time.perf_counter() - started) * 1000,
            error=type(exc).__name__,
        )


async def inject(client, stages, run_id, concurrency, timeout):
    """Open-loop arrivals. Drop excess slots instead of silently lowering the offered rate."""
    results, stage_stats, active = [], [], set()
    sequence = 0

    async def send(number, index):
        results.append(await create_one(client, number, index, run_id, timeout))

    for index, stage in enumerate(stages):
        started = time.perf_counter()
        slot = dropped_busy = dropped_late = 0
        while slot < stage.requests:
            due = started + slot / stage.rate
            await asyncio.sleep(max(0, due - time.perf_counter()))
            lateness = time.perf_counter() - due
            if lateness > max(0.05, 1 / stage.rate):
                skipped = min(stage.requests - slot, max(1, math.floor(lateness * stage.rate)))
                dropped_late += skipped
                slot += skipped
                sequence += skipped
                continue
            if len(active) >= concurrency:
                dropped_busy += 1
            else:
                task = asyncio.create_task(send(sequence, index))
                active.add(task)
                task.add_done_callback(active.discard)
            slot += 1
            sequence += 1
        await asyncio.sleep(max(0, started + stage.seconds - time.perf_counter()))
        stage_stats.append(
            {
                "stage": index,
                "target_rps": stage.rate,
                "duration_seconds": stage.seconds,
                "planned": stage.requests,
                "dropped_concurrency": dropped_busy,
                "dropped_generator_lag": dropped_late,
            }
        )
    if active:
        await asyncio.gather(*active)
    for stage in stage_stats:
        requests = [r for r in results if r.stage == stage["stage"]]
        stage.update(
            {
                "sent": len(requests),
                "accepted": sum(r.transaction_id is not None for r in requests),
                "errors": sum(r.error is not None for r in requests),
                "api_latency_ms": distribution([r.latency_ms for r in requests]),
            }
        )
    return results, stage_stats


async def observe(database, kafka, samples, errors, stop, started, phase, interval, quiet):
    while not stop.is_set():
        try:
            sample = await asyncio.to_thread(database.snapshot)
            sample.update(
                {
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "phase": phase[0],
                    "kafka_group_lag": None,
                }
            )
            if kafka:
                try:
                    sample["kafka_group_lag"] = await asyncio.to_thread(kafka.lag)
                except Exception as exc:
                    errors[type(exc).__name__] += 1
            samples.append(sample)
            if not quiet:
                print(
                    f"[{sample['elapsed_seconds']:>6.1f}s] {sample['phase']}: "
                    f"created={sample['created']} completed={sample['completed']} "
                    f"pending={sample['pending']} outbox={sample['outbox_pending']} "
                    f"kafka_lag={sample['kafka_group_lag']}",
                    flush=True,
                )
        except Exception as exc:
            # Keep the failure class only: DB exceptions may include credentials or parameters.
            errors[type(exc).__name__] += 1
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass


def evaluate(results, rows, stages, samples, errors, args):
    planned = sum(s["planned"] for s in stages)
    accepted = [r.transaction_id for r in results if r.transaction_id]
    persisted = {row["id"] for row in rows}
    latest = samples[-1] if samples else {}
    api_latency = distribution([r.latency_ms for r in results])
    processing_latency = distribution(
        [row["end_to_end_ms"] for row in rows if row["end_to_end_ms"] is not None]
    )
    checks = {
        "all_planned_requests_sent": len(results) == planned,
        "all_requests_accepted": len(accepted) == planned and not any(r.error for r in results),
        "unique_response_ids": len(accepted) == len(set(accepted)),
        "accepted_ids_match_persisted": set(accepted) == persisted,
        "all_transactions_completed": len(rows) == planned
        and all(r["status"] in TERMINAL for r in rows),
        "expected_business_outcomes": bool(rows)
        and all(r["status"] == r["expected_status"] and r["value_matches_request"] for r in rows),
        "valid_processing_timestamps": all(
            row["end_to_end_ms"] is None or row["end_to_end_ms"] >= 0 for row in rows
        ),
        "outbox_drained": latest.get("outbox_pending") == 0,
        "no_monitor_errors": not errors,
    }
    for name, limit, actual in (
        ("api_p95_within_limit", args.max_api_p95_ms, api_latency["p95"]),
        ("end_to_end_p95_within_limit", args.max_e2e_p95_ms, processing_latency["p95"]),
        (
            "backlog_within_limit",
            args.max_backlog,
            max((s["pending"] for s in samples), default=None),
        ),
    ):
        if limit is not None:
            checks[name] = actual is not None and actual <= limit
    return checks, api_latency, processing_latency


async def run(args, database, kafka=None, transport=None):
    run_id = args.run_id
    planned = sum(s.requests for s in args.stages)
    samples, errors = [], Counter()
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    phase, stop = ["load"], asyncio.Event()
    limits = httpx.Limits(
        max_connections=args.concurrency, max_keepalive_connections=args.concurrency
    )
    async with httpx.AsyncClient(
        base_url=args.base_url.rstrip("/"),
        limits=limits,
        timeout=args.request_timeout,
        transport=transport,
    ) as client:
        async with asyncio.timeout(args.request_timeout):
            readiness = await client.get("/health/ready")
            readiness.raise_for_status()
        await asyncio.to_thread(database.snapshot)
        if kafka:
            await asyncio.to_thread(kafka.lag)
        # Readiness checks and connection warmup are not part of offered-load timing.
        started = time.perf_counter()
        started_at = datetime.now(UTC)
        observer = asyncio.create_task(
            observe(
                database,
                kafka,
                samples,
                errors,
                stop,
                started,
                phase,
                args.sample_interval,
                args.quiet,
            )
        )
        try:
            results, stages = await inject(
                client,
                args.stages,
                run_id,
                args.concurrency,
                args.request_timeout,
            )
            load_seconds = time.perf_counter() - started
            phase[0] = "drain"
            deadline = time.perf_counter() + args.drain_timeout
            accepted = sum(r.transaction_id is not None for r in results)
            while time.perf_counter() < deadline:
                latest = samples[-1] if samples else None
                if (
                    latest
                    and latest["phase"] == "drain"
                    and latest["created"] >= accepted
                    and latest["pending"] == 0
                    and latest["outbox_pending"] == 0
                    and latest["jobs_ready"] == 0
                    and latest["jobs_running"] == 0
                ):
                    break
                await asyncio.sleep(min(0.1, max(0, deadline - time.perf_counter())))
        finally:
            stop.set()
            await observer
    # A fresh final sample prevents stale pre-timeout samples from declaring the outbox drained.
    try:
        final = await asyncio.to_thread(database.snapshot)
        final.update(
            {
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "phase": "final",
                "kafka_group_lag": None,
            }
        )
        if kafka:
            try:
                final["kafka_group_lag"] = await asyncio.to_thread(kafka.lag)
            except Exception as exc:
                errors[type(exc).__name__] += 1
        samples.append(final)
    except Exception as exc:
        errors[type(exc).__name__] += 1
    try:
        rows = await asyncio.to_thread(database.final_rows)
    except Exception as exc:
        errors[type(exc).__name__] += 1
        rows = []
    elapsed = time.perf_counter() - started
    checks, api_latency, processing_latency = evaluate(results, rows, stages, samples, errors, args)
    report = {
        "schema_version": 1,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "passed": all(checks.values()),
        "config": {
            "base_url": args.base_url,
            "stages": [asdict(s) for s in args.stages],
            "concurrency": args.concurrency,
            "request_timeout_seconds": args.request_timeout,
            "drain_timeout_seconds": args.drain_timeout,
            "sample_interval_seconds": args.sample_interval,
            "max_api_p95_ms": args.max_api_p95_ms,
            "max_e2e_p95_ms": args.max_e2e_p95_ms,
            "max_backlog": args.max_backlog,
            "kafka_monitor_enabled": kafka is not None,
        },
        "generator": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "logical_cpus": os.cpu_count(),
        },
        "summary": {
            "planned": planned,
            "sent": len(results),
            "accepted": sum(r.transaction_id is not None for r in results),
            "http_statuses": dict(Counter(str(r.status_code) for r in results)),
            "request_errors": dict(Counter(r.error for r in results if r.error)),
            "persisted": len(rows),
            "terminal": sum(r["status"] in TERMINAL for r in rows),
            "statuses": dict(Counter(r["status"] for r in rows)),
            "load_seconds": round(load_seconds, 3),
            "total_seconds": round(elapsed, 3),
            "accepted_per_second": round(
                sum(r.transaction_id is not None for r in results) / load_seconds, 3
            ),
            "completed_per_second_whole_run": round(
                sum(r["status"] in TERMINAL for r in rows) / elapsed, 3
            ),
            "api_latency_ms": api_latency,
            "end_to_end_latency_ms": processing_latency,
            "peak_pending": max((s["pending"] for s in samples), default=None),
            "peak_outbox_pending": max((s["outbox_pending"] for s in samples), default=None),
            "last_sample": samples[-1] if samples else None,
            "monitor_errors": dict(errors),
        },
        "checks": checks,
        "stages": stages,
        "samples": samples,
        "notes": [
            "Rates are incoming transactions/second, not total Kafka events/second.",
            "A normal transaction emits four events; retries may emit more.",
            "End-to-end latency uses DB updated_at-created_at for terminal transactions.",
            "Kafka lag is for the entire consumer group; SQL measurements are scoped to this run.",
            "A passing result validates completion and configured limits; inspect backlog growth "
            "and sustained load duration before claiming steady-state capacity.",
            "HTTP timeouts can leave committed transactions; counts are reconciled with MySQL.",
            "Monitoring and the generator share resources with the local application.",
        ],
    }
    return report, results, rows


def write_reports(directory, report, results, rows):
    directory = directory / report["run_id"]
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    for name, records, columns in (
        (
            "requests",
            [asdict(r) for r in sorted(results, key=lambda r: r.sequence)],
            list(RequestResult.__dataclass_fields__),
        ),
        (
            "transactions",
            rows,
            [
                "id",
                "sequence",
                "value",
                "status",
                "expected_status",
                "value_matches_request",
                "end_to_end_ms",
            ],
        ),
        (
            "samples",
            report["samples"],
            [
                "created",
                "completed",
                "approved",
                "rejected",
                "failed",
                "pending",
                "outbox_events",
                "outbox_pending",
                "outbox_oldest_seconds",
                "jobs_ready",
                "jobs_running",
                "jobs_dead",
                "retries",
                "elapsed_seconds",
                "phase",
                "kafka_group_lag",
            ],
        ),
    ):
        with (directory / f"{name}.csv").open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(records)
    return directory


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-url", default=os.getenv("BAMAQ_LOAD_BASE_URL", "http://localhost:8000"))
    p.add_argument(
        "--database-url",
        default=os.getenv("BAMAQ_LOAD_DATABASE_URL"),
        help="Defaults to BAMAQ_LOAD_DATABASE_URL or the application's Settings",
    )
    stage = p.add_mutually_exclusive_group()
    stage.add_argument("--stages", type=parse_stages, default=parse_stages("5:10,10:10"))
    stage.add_argument("--case", action="store_true", help=f"Use stages {CASE_STAGES}")
    p.add_argument("--concurrency", type=positive_int, default=64)
    p.add_argument("--request-timeout", type=positive_float, default=10)
    p.add_argument("--drain-timeout", type=positive_float, default=300)
    p.add_argument("--sample-interval", type=positive_float, default=2)
    p.add_argument("--max-requests", type=positive_int, default=100000)
    p.add_argument("--max-api-p95-ms", type=positive_float)
    p.add_argument("--max-e2e-p95-ms", type=positive_float)
    p.add_argument("--max-backlog", type=int)
    p.add_argument(
        "--kafka-bootstrap-servers", default=os.getenv("BAMAQ_LOAD_KAFKA_BOOTSTRAP_SERVERS")
    )
    p.add_argument(
        "--kafka-group-id", default=os.getenv("BAMAQ_KAFKA_GROUP_ID", "bamaq-job-ingestion-v1")
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.getenv("BAMAQ_LOAD_OUTPUT_DIR", "reports/load-tests")),
    )
    p.add_argument("--quiet", action="store_true")
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    if args.case:
        args.stages = parse_stages(CASE_STAGES)
    if sum(s.requests for s in args.stages) > args.max_requests:
        p.error("Planned requests exceed --max-requests")
    if args.max_backlog is not None and args.max_backlog < 0:
        p.error("--max-backlog must be >=0")
    args.run_id = f"load-{uuid4()}"
    database = kafka = None
    print(
        f"Run {args.run_id}: {sum(s.requests for s in args.stages)} planned transactions",
        flush=True,
    )
    try:
        # Fail before creating transactions if the report mount is not writable.
        args.output_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=args.output_dir):
            pass
        database = DatabaseMonitor(args.database_url or Settings().database_url, args.run_id)
        if args.kafka_bootstrap_servers:
            kafka = KafkaMonitor(args.kafka_bootstrap_servers, args.kafka_group_id)
        report, results, rows = asyncio.run(run(args, database, kafka))
        directory = write_reports(args.output_dir, report, results, rows)
        print(
            json.dumps(
                {
                    "passed": report["passed"],
                    "summary": report["summary"],
                    "checks": report["checks"],
                    "report_directory": str(directory),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return 0 if report["passed"] else 1
    except KeyboardInterrupt:
        print("Interrupted; already created transactions remain in the database.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(
            f"Load test could not finish ({type(exc).__name__}). "
            "Check API/MySQL/Kafka availability and the output directory.",
            file=sys.stderr,
        )
        return 2
    finally:
        if kafka:
            kafka.close()
        if database:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
