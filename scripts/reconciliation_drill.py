"""Run against the disposable/local Compose demo with the reconcile profile enabled."""
import json
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OPS = "http://localhost:8083/reconciliation"
WAREHOUSE = "http://localhost:8082"
tenant = "reconciliation-drill-" + uuid.uuid4().hex


def sql(db, statement):
    return subprocess.check_output(["docker", "compose", "exec", "-T", "postgres", "psql",
                                   "-U", "fulfillment", "-d", db, "-XAtq", "-v", "ON_ERROR_STOP=1",
                                   "-c", statement], text=True).strip()


def http(url, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = Request(url, data=data, headers={"Content-Type": "application/json"}, method=method)
    with urlopen(req, timeout=10) as resp:
        content = resp.read()
        return json.loads(content) if content else None


def until(label, check):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except (URLError, ConnectionError):
            pass
        time.sleep(.25)
    raise RuntimeError("Timed out: " + label)


def require(ok, reason):
    if not ok:
        raise RuntimeError(reason)


def done(run):
    result = http(OPS + "/runs/" + run)
    return result if result["phase"] == "DONE" else None


def issues(order):
    return http(OPS + "/issues?" + urlencode({"orderId": order}))


def main():
    until("reconciler readiness", lambda: http("http://localhost:8083/actuator/health/readiness")["status"] == "UP")
    ids = {name: str(uuid.uuid4()) for name in
           ("remote_only", "local_only", "quantity", "safe", "late_ship", "early_order", "recent", "pending")}
    start = datetime(2005, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=uuid.uuid4().int % 100000000)
    end = start + timedelta(minutes=1)
    stamp = (start + timedelta(seconds=5)).isoformat()
    query = urlencode({"from": start.isoformat(), "to": end.isoformat()})

    for name, order in ids.items():
        if name != "remote_only":
            state = {"safe":"UNCONFIRMED", "recent":"ACCEPTED", "pending":"ACCEPTED"}.get(name,"CONFIRMED")
            created = "now()" if name == "recent" else "'" + (
                (start - timedelta(minutes=1)).isoformat() if name == "early_order" else stamp) + "'"
            sql("fulfillment", f"""INSERT INTO fulfillment_order(id,tenant,request_key,sku,quantity,warehouse,state,created_at)
                VALUES ('{order}','{tenant}','{name}','book',2,'A','{state}',{created})""")
        if name != "local_only":
            qty = 3 if name == "quantity" else 2
            at = (end + timedelta(minutes=1)).isoformat() if name == "late_ship" else stamp
            sql("warehouse", f"""INSERT INTO shipment(id,warehouse,sku,quantity,recorded_at)
                VALUES ('{order}','A','book',{qty},'{at}')""")

    # Equal timestamps force the second component of the keyset cursor to break ties.
    first = http(WAREHOUSE + "/shipments/manifest?" + query + "&limit=1")[0]
    continuation = urlencode({"afterTime":first["recordedAt"], "afterId":first["shipment"]["id"]})
    second = http(WAREHOUSE + "/shipments/manifest?" + query + "&limit=1&" + continuation)[0]
    require(first["shipment"]["id"] != second["shipment"]["id"], "manifest cursor repeated an ID")

    run = http(OPS + "/runs?" + query, "POST")["id"]
    require(http(OPS + "/runs?" + query, "POST")["id"] == run, "duplicate window")
    summary = until("completed comparison", lambda: done(run))
    expected = {"remote_only":"PLATFORM_MISSING", "local_only":"WAREHOUSE_MISSING", "quantity":"PAYLOAD_MISMATCH",
                "safe":"UNCONFIRMED_MATCH", "pending":"STATE_PENDING"}
    seen = {}
    for name, order in ids.items():
        found = issues(order)
        require(len(found) == int(name in expected), f"{name}: unexpected issues {found}")
        if found:
            require(found[0]["kind"] == expected[name], name + ": wrong category")
            seen[name] = found[0]["id"]
    issue = issues(ids["quantity"])[0]
    require(issue["firstEvidence"]["platform"]["quantity"] == 2 and
            issue["firstEvidence"]["warehouse"]["quantity"] == 3, "mismatch evidence lost")
    require(issues(ids["safe"])[0]["status"] == "AUTO_REPAIRED", "missing repair receipt")
    safe = ids["safe"]
    require(sql("fulfillment", f"SELECT state FROM fulfillment_order WHERE id='{safe}'") == "CONFIRMED", "repair failed")
    require(sql("fulfillment", f"SELECT done FROM work_item WHERE id='{safe}'") == "t", "dispatch not fenced")
    require(sql("warehouse", f"SELECT quantity FROM shipment WHERE id='{ids['quantity']}'") == "3", "warehouse mutated")

    review = OPS + "/issues/" + seen["quantity"]
    http(review + "/review","POST",{"actor":"demo-operator","note":"Compare the signed packing slip"})
    require(http(review + "/reviews")[0]["actor"] == "demo-operator", "review not recorded")
    http(OPS + "/runs/" + run + "/rerun","POST")
    until("idempotent rerun", lambda: done(run))
    require(all(issues(ids[name])[0]["id"] == issue_id for name, issue_id in seen.items()), "duplicate issue")
    require(issues(ids["quantity"])[0]["status"] == "REVIEWED", "unchanged review lost")
    require(sql("fulfillment", f"SELECT count(*) FROM history WHERE order_id='{safe}' AND state='CONFIRMED'") == "1",
            "duplicate confirmation")
    # Model an operator correcting the source after checking the evidence.
    sql("warehouse", f"UPDATE shipment SET quantity=2 WHERE id='{ids['quantity']}'")
    http(OPS + "/runs/" + run + "/rerun","POST")
    until("rechecked correction", lambda: done(run))
    cleared = issues(ids["quantity"])[0]
    require(cleared["status"] == "CLEARED" and cleared["firstEvidence"]["warehouse"]["quantity"] == 3,
            "case history lost on clearance")
    # Keep the interval short so this assertion exercises the grace period itself.
    now = datetime.now(timezone.utc)
    recent = urlencode({"from":(now-timedelta(seconds=10)).isoformat(),"to":now.isoformat()})
    try:
        http(OPS + "/runs?" + recent, "POST")
    except HTTPError as error:
        require(error.code == 400, "expected grace-window rejection")
    else:
        raise RuntimeError("recent window was accepted")
    print(json.dumps({"run":run,"initial_pages":summary["pages"],"cases":expected,"checks":"passed"},indent=2))


if __name__ == "__main__":
    main()
