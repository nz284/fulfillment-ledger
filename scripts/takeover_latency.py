"""Stage timing and a fixed-rate B probe for the disposable takeover drill."""
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

SKU = "continuous-B"
STAGES = ("admitted", "acked", "received", "task_created", "claimed", "confirmed")


def install(sql):
    sql("""
        CREATE TABLE probe_times(order_id uuid PRIMARY KEY, task_created timestamptz, claimed timestamptz);
        CREATE FUNCTION record_probe_stage() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP='INSERT' THEN
            INSERT INTO probe_times VALUES (NEW.order_id,clock_timestamp(),NULL);
          ELSIF NEW.attempts=1 AND OLD.attempts=0 THEN
            UPDATE probe_times SET claimed=clock_timestamp() WHERE order_id=NEW.order_id;
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER probe_ship_b AFTER INSERT OR UPDATE ON work_item
        FOR EACH ROW WHEN (NEW.kind='SHIP_B') EXECUTE FUNCTION record_probe_stage();
    """)


class Traffic:
    """One submission every 0.5s; the crash controller runs independently."""
    def __init__(self, submit):
        self.submit = submit
        self.stop = threading.Event()

    def __enter__(self):
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future = self.pool.submit(self.collect)
        return self

    def collect(self):
        ids = []
        deadline = time.monotonic()
        while not self.stop.is_set():
            ids.append(self.submit("B", SKU)[0])
            deadline += 0.5
            self.stop.wait(max(0, deadline - time.monotonic()))
        return ids

    def __exit__(self, kind, value, tb):
        self.stop.set()
        self.pool.shutdown(wait=True)
        self.ids = self.future.result()


def summarize(rows):
    def stats(values):
        ordered = sorted(values)
        return {"p95_ms": round(ordered[math.ceil(0.95*len(ordered))-1], 2),
                "max_ms": round(ordered[-1], 2)}

    pairs = list(zip(STAGES, STAGES[1:])) + [("admitted", "confirmed")]
    return {"samples": len(rows), **{
        f"{start}_to_{end}": stats([(row[end]-row[start])*1000 for row in rows])
        for start, end in pairs}}


def report(ids, crash, sql, logs, until):
    cutoff = float(crash)
    until("all B probes confirmed and projected", lambda: int(sql(f"""
        SELECT count(*) FROM fulfillment_order o JOIN timeline t ON t.order_id=o.id
        WHERE o.sku='{SKU}' AND o.state='CONFIRMED' AND t.state='CONFIRMED'
    """)) == len(ids))
    raw = sql(f"""
        SELECT o.id,extract(epoch FROM o.created_at),extract(epoch FROM p.task_created),
               extract(epoch FROM p.claimed),extract(epoch FROM h.created_at)
        FROM fulfillment_order o JOIN probe_times p ON p.order_id=o.id
        JOIN history h ON h.order_id=o.id AND h.state='CONFIRMED'
        WHERE o.sku='{SKU}' ORDER BY o.created_at
    """)
    rows = {}
    for line in raw.splitlines():
        order, admitted, created, claimed, confirmed = line.split("|")
        rows[order] = dict(id=order, admitted=float(admitted), task_created=float(created),
                           claimed=float(claimed), confirmed=float(confirmed))
    if set(rows) != set(ids):
        raise RuntimeError("missing B timing or unexpected B order")
    shipped = sql(f"SELECT id FROM shipment WHERE warehouse='B' AND sku='{SKU}' AND quantity=2", "warehouse")
    if set(shipped.splitlines()) != set(ids):
        raise RuntimeError("B warehouse records differ from submitted probes")
    rebalances = []
    for worker, records in logs.items():
        for line in records.splitlines():
            if not line.startswith("{"):
                continue
            entry = json.loads(line)
            order, event = entry.get("fulfillment_id"), entry.get("event")
            timestamp = entry.get("@timestamp")
            message = entry.get("message", "")
            if "partitions assigned" in message or "partitions revoked" in message or "partitions lost" in message:
                rebalances.append(dict(time=timestamp, worker=worker[:12], message=message))
            if order not in rows or event not in ("dispatch.acked", "dispatch.received"):
                continue
            stage = event.split(".")[1]
            normalized = re.sub(r"\.(\d+)", lambda m: "." + m[1][:6].ljust(6, "0"), timestamp)
            instant = datetime.fromisoformat(normalized.replace("Z", "+00:00")).timestamp()
            if stage not in rows[order] or instant < rows[order][stage]:
                rows[order][stage] = instant
                if stage == "acked":
                    rows[order]["partition"] = entry["partition"]
    for row in rows.values():
        if any(stage not in row for stage in STAGES):
            raise RuntimeError(f"incomplete stage trace for {row['id']}: {row}")
    normal = [r for r in rows.values() if r["admitted"] < cutoff]
    fault = [r for r in rows.values() if r["admitted"] >= cutoff]
    if len(normal)<20 or len(fault)<90:
        raise RuntimeError(f"insufficient B traffic: normal={len(normal)}, fault={len(fault)}")
    return {"normal": summarize(normal), "fault": summarize(fault),
            "crash_utc": datetime.fromtimestamp(cutoff, timezone.utc).isoformat(),
            "rebalances": sorted(rebalances,key=lambda r:r["time"]), "orders": list(rows.values())}
