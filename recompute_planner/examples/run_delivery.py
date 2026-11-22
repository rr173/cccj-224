"""End-to-end demo: one consistent cross-day attribution delivery.

Three order days are materialized directly (this feature never creates plans
or registers data).  On the two most recent days the newest versions read the
same touch versions and fit together; the older day's newest reads a different
touch version, so the consistent optimum drops it and keeps the newer two.

Run:  python3 examples/run_delivery.py
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import DeliveryError, RecomputePlanner  # noqa: E402

ATTR_GRAPH = {"nodes": {
    "orders": {"output": "day"},
    "touches": {"output": "day"},
    "attr": {"output": "day", "materialize": "attribution",
             "inputs": [{"node": "orders"},
                        {"node": "touches", "window": 7, "cross_day": True}]},
}}


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def window(day):
    from datetime import date, timedelta
    base = date.fromisoformat(day)
    return [(base - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]


def materialize(p, day, version, order_version, touch_version, results):
    p.conn.execute(
        "INSERT OR IGNORE INTO published (node, partition, version, evidence_json,"
        " source, created_at) VALUES ('orders',?,?,'{}','demo',"
        " '2026-11-22T00:00:00+00:00')", (day, order_version))
    touches = {t: touch_version for t in window(day)}
    for tday, tver in touches.items():
        p.conn.execute(
            "INSERT OR IGNORE INTO published (node, partition, version, evidence_json,"
            " source, created_at) VALUES ('touches',?,?,'{}','demo',"
            " '2026-11-22T00:00:00+00:00')", (tday, tver))
        p.conn.execute(
            "INSERT OR IGNORE INTO node_snapshots (node, partition, version, kind,"
            " digest, row_count, registered_at) VALUES ('touches',?,?,'touches',"
            " 'x',0,'2026-11-22T00:00:00+00:00')", (tday, tver))
    p.conn.execute(
        "INSERT OR IGNORE INTO node_snapshots (node, partition, version, kind,"
        " digest, row_count, registered_at) VALUES ('orders',?,?,'orders','x',0,"
        " '2026-11-22T00:00:00+00:00')", (day, order_version))
    p.conn.execute(
        "INSERT OR IGNORE INTO published (node, partition, version, evidence_json,"
        " source, created_at) VALUES ('attr',?,?,'{}','demo',"
        " '2026-11-22T00:00:00+00:00')", (day, version))
    inputs = {"orders": {day: order_version}, "touches": touches}
    p.conn.execute(
        "INSERT INTO attribution_runs (node, partition, version, plan_id, mode,"
        " baseline_version, input_versions_json, report_json, created_at)"
        " VALUES ('attr',?,?,'demo','full',NULL,?,'{}',"
        " '2026-11-22T00:00:00+00:00')", (day, version, canon(inputs)))
    for r in results:
        p.conn.execute(
            "INSERT INTO attribution_results (node, partition, version, order_id,"
            " customer_id, order_ts, touch_date, touch_id, channel)"
            " VALUES ('attr',?,?,?,?,?,?,?,?)",
            (day, version, r["order_id"], r["customer_id"],
             "2026-11-10T00:00:00.000000Z", r.get("touch_date"),
             r.get("touch_id"), r.get("channel")))


def main():
    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "delivery.db")
    target = os.path.join(tmp, "delivery.jsonl")
    with RecomputePlanner(db) as p:
        p.compile_graph(ATTR_GRAPH)
        days = ["2026-11-10", "2026-11-11", "2026-11-12"]
        # order day -> (attribution version, order version, touch version)
        layout = {
            days[0]: [("v1", "v4", "v1"), ("v2", "v1", "v9")],  # newest clashes
            days[1]: [("v1", "v2", "v2"), ("v2", "v1", "v1")],  # newest = all v1
            days[2]: [("v1", "v2", "v2"), ("v2", "v1", "v1")],  # newest = all v1
        }
        for day, options in layout.items():
            for version, order_version, touch_version in options:
                materialize(p, day, version, order_version, touch_version, [
                    {"order_id": "o1", "customer_id": "c1",
                     "touch_date": day, "touch_id": f"t-{day}", "channel": "email"},
                    {"order_id": "o2", "customer_id": "c2"},      # unattributed
                ])
        p.conn.commit()

        run = p.deliver_attribution_range("attr", days[0], days[2], target)
        print("selected:", [(e["date"], e["attribution_version"]) for e in run["days"]])
        print(f"wrote {run['rows']} order rows to {target}\n")
        with open(target, encoding="utf-8") as fh:
            for line in fh:
                print(line.rstrip()[:160])

        # a day with no usable version fails the whole delivery, never skips it
        try:
            p.deliver_attribution_range("attr", "2026-11-12", "2026-11-13", target)
        except DeliveryError as exc:
            print(f"\nrange delivery failed ({exc.code}): {exc}")


if __name__ == "__main__":
    main()
