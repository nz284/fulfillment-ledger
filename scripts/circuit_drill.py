"""Verify per-warehouse submit isolation against the local, single-worker Compose demo."""
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

API = "http://localhost:8080"
WAREHOUSE = "http://localhost:8082"
RUN = "circuit-" + uuid.uuid4().hex


def compose(*args):
    return subprocess.check_output(["docker", "compose", *args], text=True).strip()


def sql(database, query):
    return compose("exec", "-T", "postgres", "psql", "-U", "fulfillment", "-d", database,
                   "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", query)


def request(url, method="GET", body=None, phase=""):
    headers = {"X-Tenant": RUN + phase, "Idempotency-Key": str(uuid.uuid4()), "Content-Type": "application/json"}
    data = None if body is None else json.dumps(body).encode()
    with urlopen(Request(url, data=data, headers=headers, method=method), timeout=10) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def submit_pairs(phase, count):
    orders = {}
    start = time.monotonic()
    for i in range(count):
        for site in ("A", "B"):
            order = request(API + "/orders", "POST", {"warehouse": site, "sku": RUN, "quantity": 1}, phase)
            orders[str(uuid.UUID(order["id"]))] = site
        time.sleep(max(0, start + (i+1)*0.25 - time.monotonic()))
    return orders


def verify(phase, orders):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        found = sql("fulfillment", f"""
            SELECT o.id FROM fulfillment_order o JOIN timeline t ON o.id=t.order_id
            WHERE o.tenant='{RUN+phase}' AND o.state='CONFIRMED' AND t.state='CONFIRMED'
        """).splitlines()
        if len(found) == len(orders) and set(found) == orders.keys():
            break
        time.sleep(1)
    else:
        raise TimeoutError(f"{phase}: only {len(found)}/{len(orders)} confirmations and timelines")


def logs(service, since):
    raw = compose("logs", "--no-log-prefix", "--since", since, service)
    return [json.loads(line) for line in raw.splitlines() if line.startswith("{")]


def b_p95(phase):
    return float(sql("fulfillment", f"""
        SELECT percentile_cont(.95) WITHIN GROUP (ORDER BY extract(epoch FROM h.created_at-o.created_at)*1000)
        FROM fulfillment_order o JOIN history h ON h.order_id=o.id AND h.state='CONFIRMED'
        WHERE o.tenant='{RUN+phase}' AND o.warehouse='B'
    """))


def run():
    require(len(compose("ps", "-q", "worker").splitlines()) == 1, "run this drill with one worker")
    baseline = submit_pairs("-normal", 48)
    verify("-normal", baseline)
    since = datetime.now(timezone.utc).isoformat()
    switch = WAREHOUSE + "/demo/submit-unavailable/A"
    request(switch, "PUT")
    try:
        outage = submit_pairs("-fault", 72)
        # Snapshot before restoring A: fresh orders should remain durable and unattempted.
        untouched = int(sql("fulfillment", f"""
            SELECT count(*) FROM fulfillment_order o JOIN work_item w ON w.order_id=o.id
            WHERE o.tenant='{RUN}-fault' AND w.kind='SHIP_A' AND w.attempts=0 AND o.state='ACCEPTED'
        """))
        require(untouched > 0, "expected unclaimed A orders during the open interval")
    finally:
        request(switch, "DELETE")
    verify("-fault", outage)
    expected = {**baseline, **outage}
    stored = sql("warehouse", f"SELECT id,warehouse FROM shipment WHERE sku='{RUN}' AND quantity=1").splitlines()
    require(dict(row.split("|") for row in stored) == expected, "warehouse payload mismatch")

    worker = logs("worker", since)
    transitions = [r for r in worker if r.get("event") == "warehouse.circuit.transition"]
    require(all((r["warehouse_id"],r["operation"]) == ("A","submit") for r in transitions),
            "another circuit changed state")
    states = [r["state"] for r in transitions]
    require(states[:3] == ["OPEN","HALF_OPEN","OPEN"] and states[-2:] == ["HALF_OPEN","CLOSED"],
            f"expected a failed probe followed by recovery: {states}")
    rejects = [r for r in logs("warehouse", since) if r.get("event") == "warehouse.submit.rejected"]
    lookups = [r for r in worker if r.get("event") == "warehouse.lookup.missing"]
    def instant(r):
        # All timestamps come from the single Compose host. Parse nanoseconds at microsecond precision.
        import re
        stamp = re.sub(r"\.(\d+)", lambda m: "." + m[1][:6].ljust(6, "0"), r["@timestamp"])
        return datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()

    lookup_during_open = 0
    for left, right in zip(transitions,transitions[1:]):
        lo, hi = instant(left), instant(right)
        attempts = sum(lo < instant(r) < hi for r in rejects)
        if left["state"] == "OPEN":
            require(hi-lo >= 9.9 and attempts == 0, "A submitted while its circuit was open")
            lookup_during_open += sum(lo < instant(r) < hi for r in lookups)
        elif right["state"] == "OPEN":
            require(attempts == 1, "expected exactly one rejected half-open probe")
    require(lookup_during_open > 0, "no independent lookup observed during open interval")
    normal, fault = b_p95("-normal"), b_p95("-fault")
    budget = 2*normal + 150
    require(fault <= budget, f"B fault P95 {fault:.1f}ms exceeds budget {budget:.1f}ms")
    report = dict(orders_reconciled=len(expected), normal_b_samples=48, fault_b_samples=72,
                  normal_b_p95_ms=round(normal,2), fault_b_p95_ms=round(fault,2),
                  b_budget_ms=round(budget,2), a_circuit_transitions=states,
                  a_unattempted_before_restore=untouched, a_lookups_while_open=lookup_during_open,
                  a_rejected_http_requests=len(rejects))
    print(json.dumps(report, indent=2))
    if path := os.environ.get("CIRCUIT_REPORT"):
        Path(path).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    run()
