"""Crash the lease owner after warehouse commit in a disposable two-worker stack."""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import takeover_latency
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "ledger-takeover-" + uuid.uuid4().hex[:12]
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", "compose.yaml", "-f", "ops/takeover.compose.yaml"]
LOCK = 730021
TENANT = "takeover-test"


def command(*argv, capture=True):
    result = subprocess.run(argv, cwd=ROOT, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else None


def compose(*args, **kwargs):
    return command(*COMPOSE, *args, **kwargs)


def sql(statement, database="fulfillment"):
    return compose("exec", "-T", "postgres", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1",
                   "-U", "fulfillment", "-d", database, "-c", statement)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def until(label, check, timeout=120, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(label)


def http(url, body=None):
    headers = {"X-Tenant": TENANT, "Content-Type": "application/json"}
    if body is not None:
        headers["Idempotency-Key"] = str(uuid.uuid4())
    data = json.dumps(body).encode() if body is not None else None
    with urlopen(Request(url, data=data, headers=headers), timeout=10) as response:
        return json.load(response)


def ready(base):
    try:
        return http(base + "/actuator/health/readiness")["status"] == "UP"
    except (URLError, ConnectionError, TimeoutError):
        return False


def log_has(container, event, order_id):
    for line in command("docker", "logs", container).splitlines():
        if order_id in line and line.startswith("{"):
            if json.loads(line).get("event") == event:
                yield line


def install_gate():
    # Only the initial attempt blocks. Hibernate has not committed the CONFIRMED
    # transaction. PostgreSQL releases its row locks when the owner disappears.
    sql(f"""
        CREATE FUNCTION takeover_gate() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock({LOCK});
          RETURN NEW;
        END $$;
        CREATE TRIGGER stop_initial_confirmation BEFORE UPDATE ON work_item
        FOR EACH ROW WHEN (OLD.kind='SHIP_A' AND OLD.attempts=1 AND NOT OLD.done AND NEW.done)
        EXECUTE FUNCTION takeover_gate();
    """)
    compose("exec", "-d", "-e", "PGAPPNAME=takeover-gate-holder", "postgres", "psql",
            "-U", "fulfillment", "-d", "fulfillment", "-c",
            f"SELECT pg_advisory_lock({LOCK}); SELECT pg_sleep(240)")
    until("test gate was not acquired",
          lambda: sql(f"SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid={LOCK} AND granted"))


def run():
    compose("up", "--build", "-d", "--scale", "worker=2", capture=False)
    api = "http://" + compose("port", "api", "8080")
    warehouse = "http://" + compose("port", "warehouse", "8080")
    for base in (api, warehouse):
        until("service readiness: " + base, lambda: ready(base))

    workers = compose("ps", "-q", "worker").splitlines()
    require(len(workers) == 2, "expected two running workers")
    by_ip = {}
    for worker in workers:
        info = json.loads(command("docker", "inspect", worker))[0]
        network = info["NetworkSettings"]["Networks"][PROJECT + "_default"]
        by_ip[network["IPAddress"]] = worker
        until("Kafka listener startup", lambda: "partitions assigned" in command("docker", "logs", worker))
    install_gate()

    def submit(site, sku=None):
        payload = {"warehouse": site, "sku": sku or "takeover-" + site, "quantity": 2}
        order_id = str(uuid.UUID(http(api + "/orders", payload)["id"]))
        return order_id, {"id": order_id, **payload}

    warm, _ = submit("B")
    until("preflight confirmed", lambda: http(api + "/orders/" + warm)["state"] == "CONFIRMED")
    takeover_latency.install(sql)
    with takeover_latency.Traffic(submit) as traffic:
        time.sleep(12)
        a, expected = submit("A")
        owner_ip = until("first worker reaching the confirmation gate", lambda: sql(f"""
            SELECT host(s.client_addr) FROM pg_stat_activity s JOIN pg_locks l ON l.pid=s.pid
            WHERE l.locktype='advisory' AND l.objid={LOCK} AND NOT l.granted
        """))
        owner = by_ip[owner_ip]
        survivor = next(worker for worker in workers if worker != owner)
        require(http(warehouse + "/shipments/" + a) == expected, "warehouse A did not commit the payload")
        require(http(api + "/orders/" + a)["state"] == "ACCEPTED", "platform confirmed before crash")
        task = f"FROM work_item WHERE order_id='{a}' AND kind='SHIP_A'"
        require(sql("SELECT attempts " + task) == "1", "expected first lease")
        lease_end = sql("SELECT extract(epoch FROM available_at) " + task)
        crash = sql("SELECT extract(epoch FROM clock_timestamp())")
        crash_clock = time.monotonic()
        command("docker", "kill", "--signal=KILL", owner)
        require(command("docker", "inspect", "-f", "{{.State.ExitCode}}", owner) == "137",
                "owner did not exit on SIGKILL")
        require(sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE application_name='takeover-gate-holder'") == "t", "gate holder missing")
        b, expected_b = submit("B")
        time.sleep(max(0, crash_clock + 50 - time.monotonic()))
    for order_id in (a, b):
        until("confirmation for " + order_id, lambda: http(api + "/orders/" + order_id)["state"] == "CONFIRMED")
        until("timeline for " + order_id, lambda: http(api + "/orders/" + order_id + "/timeline")
              == ["ACCEPTED", "CONFIRMED"])
    require(compose("ps", "-q", "worker") == survivor, "unexpected surviving worker set")
    require(sql("SELECT attempts " + task) == "2", "expected exactly one reclaim")
    require(http(warehouse + "/shipments/" + b) == expected_b, "warehouse B payload mismatch")
    require(sql(f"SELECT count(*) FROM shipment WHERE id='{a}' AND warehouse='A' "
                "AND sku='takeover-A' AND quantity=2", "warehouse") == "1", "warehouse A mismatch")
    require(sql(f"SELECT count(*) FROM history WHERE order_id='{a}' AND state='CONFIRMED'") == "1",
            "duplicate or missing confirmation")
    require(any(log_has(survivor, "warehouse.lookup.matched", a)), "survivor lookup was not observed")
    require(len(list(log_has(compose("ps", "-q", "warehouse"), "shipment.submit.committed", a))) == 1,
            "warehouse A should receive one committed single-shipment submission")

    def confirmed_at(order_id):
        return float(sql(f"SELECT extract(epoch FROM created_at) FROM history WHERE order_id='{order_id}' "
                         "AND state='CONFIRMED'"))

    a_done, b_done = confirmed_at(a), confirmed_at(b)
    require(a_done >= float(lease_end), "survivor confirmed before the original lease expired")
    b_start = float(sql(f"SELECT extract(epoch FROM created_at) FROM fulfillment_order WHERE id='{b}'"))
    report = {"warehouse_a_order": a, "warehouse_b_order": b, "killed_worker": owner[:12],
              "surviving_worker": survivor[:12], "lease_attempts": 2, "warehouse_a_rows": 1,
              "crash_to_confirmation_s": round(a_done - float(crash), 3),
              "remaining_lease_at_crash_s": round(float(lease_end) - float(crash), 3),
              "warehouse_b_confirmation_s": round(b_done - b_start, 3),
              "warehouse_b_confirmed_before_recovery": b_done < a_done}
    latency = takeover_latency.report(traffic.ids, crash, sql,
                                      {worker: command("docker", "logs", worker) for worker in workers}, until)
    report["b_latency"] = {phase: latency[phase] for phase in ("normal", "fault")}
    print(json.dumps(report, indent=2), flush=True)
    report["b_trace"] = latency
    if path := os.environ.get("TAKEOVER_REPORT"):
        Path(path).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if budget := os.environ.get("B_FAULT_P95_BUDGET_MS"):
        observed = latency["fault"]["admitted_to_confirmed"]["p95_ms"]
        require(observed <= float(budget), f"B fault-window P95 {observed} ms exceeds {budget} ms")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        compose("logs", "--tail=25", "worker", "warehouse", capture=False)
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans", capture=False)
