"""End-to-end demo: two weekly reward receipts, hand-checkable numbers.

A first delivery (v1 attribution) is approved and posted as receipt R-1.
A late touch then moves one order from email to ads and removes another order;
the corrected whole-week delivery is approved and posted as R-2.  The second
receipt is a differential: the old channel is clawed back, the new channel paid
in, and orders whose attribution did not change still move a few cents because
their channel now has a different participant count and remainder ordering.

Run:  python3 examples/run_rewards.py
"""

import hashlib
import json
import os
import sys
import tempfile
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import RecomputePlanner, RewardLedger  # noqa: E402

ATTR_GRAPH = {"nodes": {
    "orders": {"output": "day"},
    "touches": {"output": "day"},
    "attr": {"output": "day", "materialize": "attribution",
             "inputs": [{"node": "orders"},
                        {"node": "touches", "window": 7, "cross_day": True}]},
}}

WEEK = ["2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08",
        "2026-10-09", "2026-10-10", "2026-10-11"]
POOLS = {"email": 100, "ads": 50, "social": 30}

V1 = {
    "2026-10-05": [("a1", "email"), ("a2", "email"), ("a3", "email"),
                   ("b1", "email")],
    "2026-10-06": [("c1", "email"), ("d1", "ads")],
    "2026-10-07": [("e1", "ads"), ("e2", "social")],
}
V2 = {
    "2026-10-05": [("a2", "email"), ("a3", "email")],
    "2026-10-06": [("c1", "email"), ("d1", "ads"), ("a1", "ads")],
    "2026-10-07": [("e1", "ads"), ("e2", "social")],
}


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def window(day):
    base = date.fromisoformat(day)
    return [(base - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]


def seed_day(p, day, version, pairs):
    """Publish + materialize one attribution day (touches all v1)."""
    evidence = canon({"seeded": True})
    p.conn.execute(
        "INSERT OR IGNORE INTO published VALUES ('orders',?,?,?,'seed',"
        " '2026-11-22T00:00:00+00:00')", (day, version, evidence))
    for tday in window(day):
        p.conn.execute(
            "INSERT OR IGNORE INTO published VALUES ('touches',?,?,'{}','seed',"
            " '2026-11-22T00:00:00+00:00')", (tday, "v1"))
        p.conn.execute(
            "INSERT OR IGNORE INTO node_snapshots VALUES ('touches',?,?,'touches',"
            " 'x',0,'2026-11-22T00:00:00+00:00')", (tday, "v1"))
    p.conn.execute(
        "INSERT OR IGNORE INTO node_snapshots VALUES ('orders',?,?,'orders','x',0,"
        " '2026-11-22T00:00:00+00:00')", (day, version))
    p.conn.execute(
        "INSERT OR IGNORE INTO published VALUES ('attr',?,?,?,'seed',"
        " '2026-11-22T00:00:00+00:00')", (day, version, evidence))
    inputs = {"orders": {day: version},
              "touches": {t: "v1" for t in window(day)}}
    p.conn.execute(
        "INSERT INTO attribution_runs VALUES ('attr',?,?,'demo','full',NULL,?,"
        "'{}','2026-11-22T00:00:00+00:00')", (day, version, canon(inputs)))
    for order_id, channel in pairs:
        p.conn.execute(
            "INSERT INTO attribution_results VALUES ('attr',?,?,?,'c',?,?,?,?)",
            (day, version, order_id, "2026-10-01T00:00:00.000000Z",
             day if channel else None, f"t-{order_id}" if channel else None,
             channel))


def build(work, name, layout):
    p = RecomputePlanner(os.path.join(work, f"{name}.db"))
    p.compile_graph(ATTR_GRAPH)
    for index, day in enumerate(WEEK):
        seed_day(p, day, f"v{index + 1}", layout.get(day, []))
    p.conn.commit()
    target = os.path.join(work, f"{name}.jsonl")
    p.deliver_attribution_range("attr", WEEK[0], WEEK[-1], target)
    p.close()
    return target


def digest(path):
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def main():
    work = tempfile.mkdtemp()
    first = build(work, "v1", V1)
    second = build(work, "v2", V2)

    with RewardLedger(os.path.join(work, "rewards.db")) as ledger:
        print("approved v1:", digest(first))
        r1 = ledger.post_weekly_rewards("attr", WEEK[0], first, digest(first),
                                        "R-2026-41-1", None, POOLS)
        print(f"R1 seq={r1['seq']} payout={r1['payout']}")
        for line in ledger.iter_reward_lines("attr", WEEK[0], "R-2026-41-1"):
            print(f"  +{line['amount']:4d}  {line['order_date']} {line['order_id']}"
                  f" {line['channel']}")

        print("\napproved v2:", digest(second))
        r2 = ledger.post_weekly_rewards("attr", WEEK[0], second, digest(second),
                                        "R-2026-41-2", "R-2026-41-1", POOLS)
        print(f"R2 seq={r2['seq']} payout={r2['payout']} (nets to zero)")
        for line in ledger.iter_reward_lines("attr", WEEK[0], "R-2026-41-2"):
            print(f"  {line['amount']:+4d}  {line['order_date']} {line['order_id']}"
                  f" {line['channel']}")

        print("\nsummary:", ledger.reward_week_summary("attr", WEEK[0]))
        # an exact retry is idempotent and returns the stored receipt
        again = ledger.post_weekly_rewards("attr", WEEK[0], first, digest(first),
                                           "R-2026-41-1", None, POOLS)
        print("retry R1 identical:", again == r1)


if __name__ == "__main__":
    main()
