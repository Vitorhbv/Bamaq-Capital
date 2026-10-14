"""Exercise the complete deployed flow using only the Python standard library."""

import argparse
import json
import time
from urllib.request import Request, urlopen
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    args = parser.parse_args()
    for value, expected in (("1500.00", "APPROVED"), ("6000.00", "REJECTED")):
        body = json.dumps({"customer_id": "smoke-customer", "value": value}).encode()
        request = Request(
            f"{args.base_url}/transactions",
            data=body,
            headers={"Content-Type": "application/json", "Idempotency-Key": str(uuid4())},
        )
        with urlopen(request, timeout=10) as response:
            assert response.status == 202
            created = json.load(response)
            location = response.headers["Location"]
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with urlopen(f"{args.base_url}{location}", timeout=10) as response:
                current = json.load(response)
            if current["status"] in ("APPROVED", "REJECTED", "FAILED"):
                assert current["status"] == expected, current
                print(json.dumps({"id": created["id"], "status": current["status"]}))
                break
            time.sleep(0.25)
        else:
            raise AssertionError(f"Transaction {created['id']} did not complete within 60 seconds")


if __name__ == "__main__":
    main()
