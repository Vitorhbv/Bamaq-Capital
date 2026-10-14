import asyncio
import logging
import time
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from bamaq.adapters.risk import HttpRisk
from bamaq.api import create_app
from bamaq.config import Settings
from bamaq.domain import PermanentRiskError, Status, TransientRiskError
from bamaq.logging import JsonFormatter
from bamaq.mock_risk import create_app as create_mock


def test_api_create_get_idempotency_and_openapi(service):
    with TestClient(create_app(service=service)) as client:
        response = client.post(
            "/transactions",
            json={"customer_id": "123", "value": 1500},
            headers={"Idempotency-Key": "request-1"},
        )
        assert response.status_code == 202
        body = response.json()
        assert body["value"] == "1500.00"
        assert body["status"] == "PENDING"
        assert response.headers["Location"] == f"/transactions/{body['id']}"
        assert client.get(response.headers["Location"]).json() == body
        assert (
            client.post(
                "/transactions",
                json={"customer_id": "123", "value": "1500.00"},
                headers={"Idempotency-Key": "request-1"},
            ).json()
            == body
        )
        assert (
            client.post(
                "/transactions",
                json={"customer_id": "123", "value": 2},
                headers={"Idempotency-Key": "request-1"},
            ).status_code
            == 409
        )
        assert client.get(f"/transactions/{uuid4()}").status_code == 404
        assert client.get("/transactions/not-a-uuid").status_code == 422
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200
        assert client.get("/openapi.json").status_code == 200


@pytest.mark.parametrize(
    "body",
    [
        {"customer_id": "", "value": 1},
        {"customer_id": "  ", "value": 1},
        {"customer_id": "a" * 129, "value": 1},
        {"customer_id": "123", "value": 0},
        {"customer_id": "123", "value": -1},
        {"customer_id": "123", "value": "1.001"},
        {"customer_id": "123", "value": "NaN"},
        {"customer_id": "123", "value": "Infinity"},
        {"customer_id": "123", "value": "10000000000000000"},
        {"customer_id": "123", "value": 1, "unexpected": True},
    ],
)
def test_invalid_api_input(service, memory, body):
    with TestClient(create_app(service=service)) as client:
        assert client.post("/transactions", json=body).status_code == 422
    assert not memory.rows["transactions"]


def test_exact_large_decimal(service):
    with TestClient(create_app(service=service)) as client:
        response = client.post(
            "/transactions",
            json={
                "customer_id": "123",
                "value": "9999999999999999.99",
            },
        )
        assert response.status_code == 202
        assert response.json()["value"] == "9999999999999999.99"


@pytest.mark.parametrize(
    "status_code,body,error",
    [
        (503, {}, TransientRiskError),
        (500, {}, TransientRiskError),
        (408, {}, TransientRiskError),
        (429, {}, TransientRiskError),
        (400, {}, PermanentRiskError),
        (401, {}, PermanentRiskError),
        (200, {"result": "UNKNOWN"}, PermanentRiskError),
        (200, {}, PermanentRiskError),
        (200, [], PermanentRiskError),
    ],
)
def test_risk_error_classification(service, status_code, body, error):
    transaction = service.create("123", Decimal("1"))
    transport = httpx.MockTransport(lambda request: httpx.Response(status_code, json=body))
    with pytest.raises(error):
        HttpRisk("http://risk", transport=transport).analyze(transaction)


def test_risk_sends_stable_key_and_interprets_result(service):
    transaction = service.create("123", Decimal("1"))
    keys = []

    def respond(request):
        keys.append(request.headers["Idempotency-Key"])
        assert request.headers["X-Correlation-ID"] == transaction.correlation_id
        return httpx.Response(200, json={"result": "APPROVED"})

    risk = HttpRisk("http://risk", transport=httpx.MockTransport(respond))
    assert risk.analyze(transaction) == Status.APPROVED
    assert risk.analyze(transaction) == Status.APPROVED
    assert keys == [f"{transaction.id}:1"] * 2


def test_total_http_deadline(service):
    async def slow_response(request):
        await asyncio.sleep(1)
        return httpx.Response(200, json={"result": "APPROVED"})

    transaction = service.create("123", Decimal("1"))
    started = time.monotonic()
    with pytest.raises(TransientRiskError):
        HttpRisk("http://risk", timeout=0.05, transport=httpx.MockTransport(slow_response)).analyze(
            transaction
        )
    assert time.monotonic() - started < 0.5


def test_transport_failure(service):
    def offline(request):
        raise httpx.ConnectError("offline", request=request)

    transaction = service.create("123", Decimal("1"))
    with pytest.raises(TransientRiskError):
        HttpRisk("http://risk", transport=httpx.MockTransport(offline)).analyze(transaction)


def test_mock_idempotency_and_transient_failures():
    with TestClient(create_mock(Settings(mock_fail_first=1))) as client:
        headers = {"Idempotency-Key": "transaction-1:1"}
        body = {"customer_id": "123", "value": "1500.00"}
        assert client.post("/risk-analysis", json=body, headers=headers).status_code == 503
        result = client.post("/risk-analysis", json=body, headers=headers)
        assert result.json() == {"result": "APPROVED"}
        assert client.post("/risk-analysis", json=body, headers=headers).json() == result.json()
        assert (
            client.post(
                "/risk-analysis", json=body | {"value": "6000"}, headers=headers
            ).status_code
            == 409
        )
        assert (
            client.post(
                "/risk-analysis",
                json=body | {"value": "6000"},
                headers={"Idempotency-Key": "another"},
            ).status_code
            == 503
        )
        assert client.post(
            "/risk-analysis", json=body | {"value": "6000"}, headers={"Idempotency-Key": "another"}
        ).json() == {"result": "REJECTED"}


def test_json_logs_include_ids_without_driver_parameters():
    try:
        raise ValueError("secret-customer-id")
    except ValueError:
        import sys

        record = logging.LogRecord("worker", logging.ERROR, "", 0, "failure", (), sys.exc_info())
    record.transaction_id = "txn"
    output = JsonFormatter().format(record)
    assert '"transaction_id": "txn"' in output
    assert "ValueError" in output
    assert "secret-customer-id" not in output
