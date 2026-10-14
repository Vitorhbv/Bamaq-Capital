import logging
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from bamaq.bootstrap import build
from bamaq.domain import ConcurrentWrite, Conflict, NotFound
from bamaq.schemas import CreateTransaction, TransactionView

log = logging.getLogger("api")


def create_app(service=None, engine=None):
    if service is None:
        engine, _, service = build()

    @asynccontextmanager
    async def lifespan(app):
        yield
        if engine is not None:
            engine.dispose()

    app = FastAPI(
        title="BAMAQ Capital",
        version="1.0.0",
        lifespan=lifespan,
        description="Transações assíncronas. Valores em BRL; respostas usam strings decimais.",
    )

    @app.exception_handler(NotFound)
    async def not_found(request, exc):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(Conflict)
    async def conflict(request, exc):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    async def unavailable(request, exc):
        log.error("persistence_unavailable", extra={"error_type": type(exc).__name__})
        return JSONResponse(
            status_code=503, content={"detail": "Persistence temporarily unavailable"}
        )

    app.add_exception_handler(SQLAlchemyError, unavailable)
    app.add_exception_handler(ConcurrentWrite, unavailable)

    @app.post("/transactions", status_code=202, response_model=TransactionView)
    def create(
        body: CreateTransaction,
        response: Response,
        idempotency_key: Annotated[
            str | None,
            Header(min_length=1, max_length=128, pattern=r"^[!-~]+$"),
        ] = None,
    ):
        transaction = service.create(body.customer_id, body.value, idempotency_key)
        response.headers["Location"] = f"/transactions/{transaction.id}"
        log.info(
            "transaction_accepted",
            extra={
                "transaction_id": transaction.id,
                "generation": transaction.generation,
                "correlation_id": transaction.correlation_id,
            },
        )
        return transaction

    @app.get("/transactions/{transaction_id}", response_model=TransactionView)
    def get(transaction_id: UUID):
        return service.get(str(transaction_id))

    @app.get("/health/live")
    def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    def ready():
        if engine is not None:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        return {"status": "ready"}

    return app


app = create_app()
