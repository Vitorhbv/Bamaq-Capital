import json
import logging
import sys
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        for name in (
            "transaction_id",
            "event_id",
            "generation",
            "attempt",
            "duration_ms",
            "error_type",
            "failure_id",
            "topic",
            "partition",
            "offset",
            "correlation_id",
        ):
            if hasattr(record, name):
                data[name] = getattr(record, name)
        # Driver exception text may contain SQL parameters / personal data.
        if record.exc_info and record.exc_info[0]:
            data["error_type"] = record.exc_info[0].__name__
        return json.dumps(data, ensure_ascii=True)


def configure(level="INFO"):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
    logging.getLogger("httpx").setLevel(logging.WARNING)
