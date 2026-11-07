"""End-to-end lost-response demo against the local Compose stack."""
import json
import time
import uuid
from urllib.request import Request, urlopen
from urllib.error import URLError

key = str(uuid.uuid4())
headers = {"Content-Type": "application/json", "X-Tenant": "smoke", "Idempotency-Key": key}
api = "http://localhost:8080"
warehouse = "http://localhost:8082"


def call(url, method="GET", body=None):
    payload = None if body is None else json.dumps(body).encode()
    with urlopen(Request(url, payload, headers, method=method), timeout=8) as response:
        return json.loads(data) if (data := response.read()) else None


def until(check, seconds=160):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(1)
    raise RuntimeError("Timed out waiting for " + str(check))


for host in (api, warehouse):
    until(lambda: call(host + "/actuator/health/liveness").get("status") == "UP")
call(warehouse + "/demo/slow/A", "PUT")
try:
    order = {"warehouse": "A", "sku": "demo-book", "quantity": 2}
    order_id = call(api + "/orders", "POST", order)["id"]
    assert call(api + "/orders", "POST", order)["id"] == order_id
    url = api + "/orders/" + order_id
    until(lambda: call(url + "/timeline") == ["ACCEPTED", "UNCONFIRMED", "CONFIRMED"])
    assert call(url)["state"] == "CONFIRMED"
    assert call(warehouse + "/shipments/" + order_id)["id"] == order_id
    print("Recovered order", order_id, call(url + "/timeline"))
finally:
    call(warehouse + "/demo/slow/A", "DELETE")
