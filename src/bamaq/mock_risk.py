import asyncio
from decimal import Decimal
from typing import Annotated

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

from bamaq.config import Settings
from bamaq.schemas import CreateTransaction


def create_app(settings=None):
    settings = settings or Settings()
    app = FastAPI(title="Risk analysis mock")
    results = {}
    fingerprints = {}
    attempts = {}
    lock = asyncio.Lock()

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.post("/risk-analysis")
    async def analyze(
        body: CreateTransaction,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=128)],
    ):
        fingerprint = (body.customer_id, str(body.value.quantize(Decimal("0.01"))))
        async with lock:
            if idempotency_key in fingerprints and fingerprints[idempotency_key] != fingerprint:
                return JSONResponse(status_code=409, content={"detail": "Key conflict"})
            fingerprints[idempotency_key] = fingerprint
            if idempotency_key in results:
                saved_fingerprint, result = results[idempotency_key]
                if saved_fingerprint != fingerprint:
                    return JSONResponse(status_code=409, content={"detail": "Key conflict"})
                return result
            attempts[idempotency_key] = attempts.get(idempotency_key, 0) + 1
            attempt = attempts[idempotency_key]
        if settings.mock_mode == "unavailable" or attempt <= settings.mock_fail_first:
            return JSONResponse(status_code=503, content={"detail": "Simulated outage"})
        if settings.mock_delay_seconds:
            await asyncio.sleep(settings.mock_delay_seconds)
        if settings.mock_mode == "invalid":
            return {"unexpected": "contract"}
        result = {
            "result": "APPROVED"
            if body.value <= Decimal(settings.mock_approval_limit)
            else "REJECTED",
        }
        async with lock:
            results[idempotency_key] = (fingerprint, result)
        return result

    return app


app = create_app()
