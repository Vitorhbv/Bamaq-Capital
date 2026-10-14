import asyncio

import httpx

from bamaq.domain import PermanentRiskError, Status, TransientRiskError


class HttpRisk:
    def __init__(self, url: str, timeout: float = 5, transport=None):
        self.url = url.rstrip("/") + "/risk-analysis"
        self.timeout = timeout
        self.transport = transport

    def analyze(self, transaction):
        return asyncio.run(self._analyze(transaction))

    async def _analyze(self, transaction):
        try:
            # Total deadline, including body download, rather than just a socket-read timeout.
            async with asyncio.timeout(self.timeout):
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout, connect=min(1, self.timeout)),
                    transport=self.transport,
                ) as client:
                    response = await client.post(
                        self.url,
                        json={
                            "customer_id": transaction.customer_id,
                            "value": format(transaction.value, ".2f"),
                        },
                        headers={
                            "Idempotency-Key": f"{transaction.id}:{transaction.generation}",
                            "X-Correlation-ID": transaction.correlation_id,
                        },
                    )
        except (TimeoutError, httpx.TimeoutException, httpx.TransportError) as exc:
            raise TransientRiskError("Risk transport unavailable or deadline exceeded") from exc
        if response.status_code in (408, 429) or response.status_code >= 500:
            raise TransientRiskError(f"Risk HTTP {response.status_code}")
        if response.status_code != 200:
            raise PermanentRiskError(f"Unexpected risk HTTP {response.status_code}")
        try:
            data = response.json()
            result = Status(data["result"])
            if result not in (Status.APPROVED, Status.REJECTED):
                raise ValueError("Invalid result")
            return result
        except (ValueError, KeyError, TypeError) as exc:
            raise PermanentRiskError("Invalid risk response contract") from exc
