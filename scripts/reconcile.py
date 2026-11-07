"""Compare accepted IDs with durable warehouse rows for one k6 run."""
import argparse
import subprocess
import time


def query(database, sql):
    result = subprocess.run(["docker", "compose", "exec", "-T", "postgres", "psql",
                             "-U", "fulfillment", "-d", database, "-v", "ON_ERROR_STOP=1",
                             "-At", "-F", "|", "-c", sql], capture_output=True, text=True, check=True)
    return [line.split("|") for line in result.stdout.splitlines() if line]


parser = argparse.ArgumentParser()
parser.add_argument("run_id")
parser.add_argument("--wait", type=int, default=300)
args = parser.parse_args()
if not args.run_id.replace("-", "").replace("_", "").isalnum() or len(args.run_id) > 100:
    parser.error("Use a short alphanumeric run ID with optional hyphens or underscores")
deadline = time.monotonic() + args.wait
while True:
    orders = dict(query("fulfillment", f"SELECT id,state FROM fulfillment_order WHERE tenant='{args.run_id}'"))
    shipped = {row[0] for row in query("warehouse", "SELECT id FROM shipment")}
    missing = [order for order, state in orders.items() if state != "CONFIRMED" or order not in shipped]
    if orders and not missing:
        print(f"{len(orders)} unique accepted orders matched confirmed warehouse shipments")
        break
    if time.monotonic() >= deadline:
        raise SystemExit(f"accepted={len(orders)} unresolved={len(missing)} sample={missing[:10]}")
    time.sleep(2)
