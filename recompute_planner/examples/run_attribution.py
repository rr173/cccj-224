"""End-to-end demo: order-attribution materialization on top of a recompute plan.

Graph:  orders (day) + touches (day, 7-day window) -> attr (day, materialized)
A late orders correction triggers the first materialization; a late touches
revision then shows customer-local incremental recompute.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import RecomputePlanner  # noqa: E402
from tests.test_attribution import (ATTR_DAY, ATTR_GRAPH, ORDERS_V2,  # noqa: E402
                                    ORDERS_V3, TOUCHES_V1, TOUCHES_V2_1031,
                                    days)

ORD_DAYS = days("2026-10-31", 7)
TOUCH_DAYS = days("2026-10-25", 13)


def show(p, version):
    print(f"\nattr/{ATTR_DAY}@{version} -- per-order results:")
    for r in p.read_attribution("attr", ATTR_DAY, version):
        hit = (f"{r['touch_date']} / {r['touch_id']} / {r['channel']}"
               if r["touch_id"] else "(unattributed)")
        print(f"  {r['order_id']}  customer={r['customer_id']:<3} -> {hit}")


def main():
    db = os.path.join(tempfile.mkdtemp(), "attr.db")
    with RecomputePlanner(db) as p:
        p.compile_graph(ATTR_GRAPH)
        # upstream day partitions are published, then their row sets registered
        # (empty partitions are registered explicitly)
        for d in ORD_DAYS:
            p.seed_published("orders", d, "v1", {"origin": "bootstrap"})
            p.register_snapshot("orders", d, "v1", [])
        for d in TOUCH_DAYS:
            p.seed_published("touches", d, "v1", {"origin": "bootstrap"})
            p.register_snapshot("touches", d, "v1", TOUCHES_V1.get(d, []))

        # --- first materialization: late correction of the orders partition
        p.ingest_event("e1", "orders", ATTR_DAY, 1, "orders-v2")
        pid = p.create_plan()
        print("plan:", [f"{i['node']}/{i['partition']}->{i['new_version']}"
                        for i in p.list_plan_items(pid, limit=100)["items"]])
        p.advance(pid, "orders", ATTR_DAY, "success")          # upstream: original path
        p.register_snapshot("orders", ATTR_DAY, "v2", ORDERS_V2)
        try:
            p.advance(pid, "attr", ATTR_DAY, "success")        # refused for attr nodes
        except Exception as exc:
            print("advance(success) on attr node refused:", exc)
        rep = p.run_attribution_item(pid, "attr", ATTR_DAY)
        print(f"materialized attr/{ATTR_DAY}@{rep['version']} mode={rep['mode']} "
              f"rows={rep['rows_total']}")
        show(p, "v1")

        # --- late touches revision + order removal: incremental recompute
        p.ingest_event("e2", "touches", "2026-10-31", 2, "touches-v2")
        p.ingest_event("e3", "orders", ATTR_DAY, 3, "orders-v3")
        pid2 = p.create_plan()
        p.advance(pid2, "touches", "2026-10-31", "success")
        p.advance(pid2, "orders", ATTR_DAY, "success")
        p.register_snapshot("touches", "2026-10-31", "v2", TOUCHES_V2_1031)
        p.register_snapshot("orders", ATTR_DAY, "v3", ORDERS_V3)
        rep = p.run_attribution_item(pid2, "attr", ATTR_DAY)
        print(f"\nre-materialized attr/{ATTR_DAY}@{rep['version']} mode={rep['mode']} "
              f"baseline={rep['baseline_version']}")
        print("  changed inputs:",
              [f"{c['node']}/{c['partition']} {c['from']}->{c['to']}"
               for c in rep["changed_inputs"]])
        print("  affected customers:", rep["affected_customers"],
              f"(copied {rep['rows_copied']} rows, recomputed {rep['rows_recomputed']})")
        show(p, "v2")

        # remaining attr partitions materialize (empty order days -> empty results)
        for it in p.list_plan_items(pid2, node="attr", limit=100)["items"]:
            if it["status"] != "success":
                p.run_attribution_item(pid2, it["node"], it["partition"])
        print("\nplan2 status:", p.get_plan(pid2)["status"])
        print("stats:", p.stats())


if __name__ == "__main__":
    main()
