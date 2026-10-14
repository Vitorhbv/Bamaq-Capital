import argparse
import json
import logging
from pathlib import Path

from bamaq.bootstrap import build
from bamaq.config import Settings
from bamaq.domain import Conflict, InvalidEvent, NotFound
from bamaq.logging import configure


def main(argv=None):
    parser = argparse.ArgumentParser(description="BAMAQ Capital processes and recovery")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("api", "risk-mock", "consumer", "relay", "worker"):
        sub.add_parser(name)
    failures = sub.add_parser("failures", help="List metadata; raw payloads are not printed")
    failures.add_argument(
        "--limit", type=int, default=20, choices=range(1, 1001), metavar="1..1000"
    )
    replay = sub.add_parser("replay", help="Start a new generation for a FAILED transaction")
    replay.add_argument("transaction_id")
    quarantine = sub.add_parser("replay-quarantine")
    quarantine.add_argument("failure_id")
    quarantine.add_argument("--payload-file", type=Path, required=True)
    health = sub.add_parser("health")
    health.add_argument("role", choices=("consumer", "relay", "worker"))
    args = parser.parse_args(argv)
    settings = Settings()
    configure(settings.log_level)
    if args.command == "health":
        from bamaq.runtime import healthy

        return 0 if healthy(args.role) else 1
    if args.command in ("api", "risk-mock"):
        import uvicorn

        target = "bamaq.api:app" if args.command == "api" else "bamaq.mock_risk:app"
        uvicorn.run(target, host="0.0.0.0", port=8000, log_config=None, access_log=False)
        return 0
    if args.command in ("consumer", "relay", "worker"):
        from bamaq.runtime import run

        try:
            run(args.command, settings)
            return 0
        except Exception as exc:
            logging.getLogger(args.command).error(
                "process_failed",
                extra={"error_type": type(exc).__name__},
            )
            return 1
    engine, factory, service = build(settings)
    try:
        if args.command == "failures":
            with factory() as uow:
                print(
                    json.dumps(
                        [
                            {
                                "id": f.id,
                                "transaction_id": f.transaction_id,
                                "generation": f.generation,
                                "reason": f.reason,
                                "created_at": f.created_at.isoformat(),
                                "replayed_at": f.replayed_at.isoformat() if f.replayed_at else None,
                            }
                            for f in uow.failures.list_recent(args.limit)
                        ],
                        indent=2,
                    )
                )
        elif args.command == "replay":
            transaction = service.replay(args.transaction_id)
            print(
                json.dumps(
                    {
                        "id": transaction.id,
                        "generation": transaction.generation,
                        "status": transaction.status,
                    }
                )
            )
        else:
            payload = json.loads(args.payload_file.read_text(encoding="utf-8"))
            event_id = service.replay_quarantine(args.failure_id, payload)
            print(json.dumps({"event_id": event_id}))
    except (Conflict, InvalidEvent, NotFound, ValueError, OSError) as exc:
        print(json.dumps({"error": str(exc)}))
        return 2
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
