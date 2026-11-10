"""End-to-end Elasticsearch recovery drill. All fault injection targets a fresh private stack."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "ledger-search-" + uuid.uuid4().hex[:12]
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", "compose.yaml", "-f",
           "ops/search-drill.compose.yaml", "--profile", "search"]
TOPIC = "reconciliation.issue-changes.v1"
GROUP = "reconciliation-issue-search-v1"


def compose(*args, stdin=None, capture=True):
    result = subprocess.run([*COMPOSE, *args], cwd=ROOT, check=True, input=stdin, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else None


def sql(db, statement):
    return compose("exec", "-T", "postgres", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1",
                   "-U", "fulfillment", "-d", db, "-c", statement)


def http(url, method="GET", payload=None, headers=None):
    req = Request(url, data=None if payload is None else json.dumps(payload).encode(), method=method,
                  headers={"Content-Type":"application/json", **(headers or {})})
    with urlopen(req, timeout=12) as response:
        return json.loads(raw) if (raw := response.read()) else None


def until(label, check, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except HTTPError as error:
            if error.code != 503:
                raise
        except (URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(.3)
    raise TimeoutError(label)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def run():
    compose("up","--build","-d","api","worker","warehouse","reconciler","kafka","redis",capture=False)
    host = lambda name: "http://" + compose("port",name,"8080")
    ops = host("reconciler") + "/reconciliation"
    api = host("api")
    until("reconciler readiness",lambda: http(ops+"/runs") is not None)
    sample_sku = "DRILL-BOOK"
    def seed_shipment(site, sku):
        order = str(uuid.uuid4())
        sql("warehouse",f"""INSERT INTO shipment(id,warehouse,sku,quantity,recorded_at)
            VALUES ('{order}','{site}','{sku}',2,'2020-01-01T00:10Z')""")
        return order

    first_order = seed_shipment("A",sample_sku)
    second_order = seed_shipment("B",sample_sku)
    for _ in range(12):
        seed_shipment("A","FILLER")
    window = urlencode({"from":"2020-01-01T00:00:00Z","to":"2020-01-01T01:00:00Z"})
    job = http(ops+"/runs?"+window,"POST")["id"]
    until("initial comparison",lambda: http(ops+"/runs/"+job)["phase"]=="DONE")
    first = http(ops+"/issues?orderId="+first_order)[0]["id"]
    second = http(ops+"/issues?orderId="+second_order)[0]["id"]
    def review(issue, note):
        http(ops+"/issues/"+issue+"/review","POST",{"actor":"search-drill","note":note})
    review(first,"Signed PACKING slip checked against the carrier receipt and albarán")

    compose("up","--build","-d","elasticsearch","issue-search",capture=False)
    search = host("issue-search")+"/issue-search"
    def find(**params):
        return http(search+"?"+urlencode(params))
    def buckets(result, field):
        return {b["key"]:b["doc_count"] for b in result["aggregations"][field]["buckets"]}
    until("pre-existing notes indexed",lambda: find(q="packing signed",warehouse="A",sku=sample_sku,
          kind="PLATFORM_MISSING",status="REVIEWED")["total"]==1)
    require(find(q="albarán",sku=sample_sku)["total"]==1,"UTF-8 review text lost")
    result = find(sku=sample_sku,limit=1)
    require(result["total"]==2 and len(result["issues"])==1 and buckets(result,"warehouse")=={"A":1,"B":1},
            "filtered totals or top warehouse counts differ")
    require(find(sku=sample_sku, to="2001-01-01T00:00:00Z")["total"]==0, "discovery-time filter ignored")

    compose("stop","elasticsearch",capture=False)
    try:
        try:
            find(q="packing")
            raise RuntimeError("search should report unavailable")
        except HTTPError as err:
            require(err.code==503,"expected 503 while search store is offline")
        hdr = {"X-Tenant":PROJECT,"Idempotency-Key":str(uuid.uuid4())}
        admitted = http(api+"/orders","POST",{"warehouse":"B","sku":"ONLINE","quantity":1},hdr)
        until("fulfillment while ES is offline",lambda: http(api+"/orders/"+admitted["id"],headers=hdr)["state"]=="CONFIRMED")
        review(second,"Packing slip for west warehouse verified")
        seed_shipment("A",sample_sku)
        http(ops+"/runs/"+job+"/rerun","POST")
        until("reconciliation while ES is offline",lambda: http(ops+"/runs/"+job)["phase"]=="DONE")
    finally:
        compose("start","elasticsearch",capture=False)
    until("offline changes indexed",lambda: find(q="packing slip",sku=sample_sku)["total"]==2
          and find(sku=sample_sku)["total"]==3)
    result = find(q="packing slip",sku=sample_sku)
    require(buckets(result,"warehouse")=={"A":1,"B":1} and buckets(result,"status")=={"REVIEWED":2},
            "note matches or aggregations are wrong")
    require(find(q="carrier",warehouse="B")["total"]==0,"filter escaped its warehouse")
    source = http(ops+"/issues/"+second)
    require(source["status"]=="REVIEWED","authoritative detail missing")

    # Send real duplicate and stale notifications and wait for their group offsets to advance.
    version = int(sql("fulfillment",f"SELECT search_version FROM reconciliation_issue WHERE id='{second}'"))
    events = "".join(f"{second}|"+json.dumps({"issueId":second,"version":v})+"\n" for v in (version,1,version))
    compose("exec","-T","kafka","/opt/kafka/bin/kafka-console-producer.sh","--bootstrap-server","kafka:9092",
            "--topic",TOPIC,"--property","parse.key=true","--property","key.separator=|",stdin=events)
    def caught_up():
        output=compose("exec","-T","kafka","/opt/kafka/bin/kafka-consumer-groups.sh","--bootstrap-server","kafka:9092",
                       "--describe","--group",GROUP)
        rows=[line.split() for line in output.splitlines() if line.split()[:2]==[GROUP,TOPIC]]
        return len(rows)==3 and all(r[5]=="0" for r in rows)
    until("replayed offsets committed",caught_up)
    require(find(q="west verified",sku=sample_sku)["total"]==1,"replay duplicated or rewound document")

    # Update an already-scanned document while the new generation is still being built.
    current=http(search+"/generation")["active"]
    pending=http(search+"/rebuild","POST")["building"]
    require(pending and http(search+"/rebuild","POST")["building"]==pending,"concurrent rebuild duplicated")
    filler_ids=sorted(i["issueId"] for i in find(sku="FILLER")["issues"])
    def scanned_filler():
        state=http(search+"/generation")
        return state["building"]==pending and state["cursor"] and state["cursor"]>=filler_ids[0]
    until("first rebuilt filler page",scanned_filler)
    require(find(sku=sample_sku)["total"]==3,"old alias stopped serving during backfill")
    review(filler_ids[0],"Concurrent backfill inspection")
    until("atomic alias switch",lambda: http(search+"/generation")["active"]==pending)
    until("review made during rebuild",lambda: find(q="backfill inspection")["total"]==1)
    require(current!=pending,"generation did not change")

    # Only indexes created by this disposable project are exposed to this command.
    compose("exec","-T","elasticsearch","curl","-fsS","-X","DELETE","http://localhost:9200/"+pending)
    until("replacement for lost index",lambda: (a:=http(search+"/generation")["active"]) and a!=pending)
    until("rebuild from PostgreSQL after complete index loss",lambda: find(q="packing slip")["total"]==2
          and find(q="backfill inspection")["total"]==1 and find(sku=sample_sku)["total"]==3)
    total=int(sql("fulfillment","SELECT count(*) FROM reconciliation_issue"))
    require(find()["total"]==total,"reconstructed total differs from PostgreSQL")
    report={"reconciled_issues":total,"packing_slip_matches":2,"warehouse_buckets":buckets(result,"warehouse"),
            "normal_order_confirmed_during_outage":True,"reordered_notifications_consumed":3,
            "concurrent_review_preserved":True,"deleted_index_rebuilt":True}
    print(json.dumps(report,indent=2),flush=True)
    if path:=os.environ.get("SEARCH_REPORT"):
        Path(path).write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        compose("logs","--tail=40","issue-search","elasticsearch","reconciler",capture=False)
        raise
    finally:
        compose("down","-v","--remove-orphans",capture=False)
