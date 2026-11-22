"""End-to-end tests for order-attribution materialization.

Graph:  orders (day) + touches (day) -> attr (day, materialize="attribution")
attr/D reads orders/D and touches/[D-6 .. D] (same day + six preceding days).

Cross-month dataset (Oct/Nov 2026). An independent per-order enumeration
(enum_attribute, shares no code with planner/) cross-checks the full result
of the initial materialization and of every late revision.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import (GraphError, PlanError, RecomputePlanner, SnapshotError,  # noqa: E402
                     StateError)

ATTR_GRAPH = {"nodes": {
    "orders": {"output": "day"},
    "touches": {"output": "day"},
    "attr": {"output": "day", "materialize": "attribution",
             "inputs": [{"node": "orders"},
                        {"node": "touches", "window": 7, "cross_day": True}]},
}}

ATTR_DAY = "2026-11-01"


def days(start, n):
    base = date.fromisoformat(start)
    return [(base + timedelta(days=i)).isoformat() for i in range(n)]


ORD_DAYS = days("2026-10-31", 7)     # order partitions the tests may need
TOUCH_DAYS = days("2026-10-25", 13)  # touch windows of attr/2026-10-31..11-06

# v1 touches (only listed days are non-empty); crosses the Oct/Nov boundary
TOUCHES_V1 = {
    "2026-10-26": [{"id": "t-c5", "customer_id": "c5",
                    "ts": "2026-10-26T23:59:00Z", "channel": "search"}],
    "2026-10-28": [{"id": "t-cU", "customer_id": "cU",
                    "ts": "2026-10-28T07:00:00Z", "channel": "email"}],
    "2026-10-29": [{"id": "t-cT", "customer_id": "cT",
                    "ts": "2026-10-29T12:00:00Z", "channel": "email"}],
    "2026-10-30": [{"id": "t1", "customer_id": "c1",
                    "ts": "2026-10-30T08:00:00Z", "channel": "email"}],
    "2026-10-31": [{"id": "t2", "customer_id": "c1",
                    "ts": "2026-10-31T09:00:00Z", "channel": "ads"},
                   {"id": "t3", "customer_id": "c1",
                    "ts": "2026-10-31T09:00:00Z", "channel": "social"},
                   {"id": "t4", "customer_id": "c2",
                    "ts": "2026-10-31T07:30:00Z", "channel": "email"},
                   {"id": "t5", "customer_id": "c2",
                    "ts": "2026-10-31T20:00:00Z", "channel": "display"}],
    "2026-11-01": [{"id": "t7", "customer_id": "c2",
                    "ts": "2026-11-01T09:00:00Z", "channel": "push"}],
}

# orders published as v2 by the first plan (v1 was registered empty)
ORDERS_V2 = [
    {"id": "o1", "customer_id": "c1", "ts": "2026-11-01T10:00:00Z"},
    {"id": "o2", "customer_id": "c2", "ts": "2026-11-01T08:00:00Z"},
    {"id": "o3", "customer_id": "c3", "ts": "2026-11-01T12:00:00Z"},
    {"id": "o4", "customer_id": "c5", "ts": "2026-11-01T00:30:00Z"},
    {"id": "o5", "customer_id": "cU", "ts": "2026-11-01T09:00:00Z"},
    {"id": "o6", "customer_id": "c9", "ts": "2026-11-01T18:00:00Z"},
]

# late revision of touches/2026-10-31: t2 deleted, t4 moved c2->c9, t5 retimed
TOUCHES_V2_1031 = [
    {"id": "t3", "customer_id": "c1", "ts": "2026-10-31T09:00:00Z", "channel": "social"},
    {"id": "t4", "customer_id": "c9", "ts": "2026-10-31T07:30:00Z", "channel": "email"},
    {"id": "t5", "customer_id": "c2", "ts": "2026-10-31T07:00:00Z", "channel": "display"},
]

# late revision of orders: o3 removed
ORDERS_V3 = [o for o in ORDERS_V2 if o["id"] != "o3"]


def _canon(ts):
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def window_touches(touch_by_day, attr_day):
    """(day, touch) pairs of the seven touch partitions read by attr/attr_day."""
    base = date.fromisoformat(attr_day)
    out = []
    for i in range(6, -1, -1):
        d = (base - timedelta(days=i)).isoformat()
        for t in touch_by_day.get(d, []):
            out.append((d, t))
    return out


def enum_attribute(orders, touches):
    """Independent per-order enumeration (no planner code).

    An order takes the same-customer touch with the latest timestamp not
    after the order time; ties go to the lexicographically smallest id.
    """
    rows = []
    for o in orders:
        ots = datetime.fromisoformat(o["ts"].replace("Z", "+00:00"))
        best = None
        for day, t in touches:
            if t["customer_id"] != o["customer_id"]:
                continue
            tts = datetime.fromisoformat(t["ts"].replace("Z", "+00:00"))
            if tts > ots:
                continue
            if best is None or tts > best[0] or (tts == best[0] and t["id"] < best[1]):
                best = (tts, t["id"], day, t["channel"])
        _, tid, tday, tch = best if best else (None, None, None, None)
        rows.append({"order_id": o["id"], "customer_id": o["customer_id"],
                     "order_ts": _canon(o["ts"]), "touch_date": tday,
                     "touch_id": tid, "channel": tch})
    return sorted(rows, key=lambda r: r["order_id"])


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "planner.db")
        self.p = RecomputePlanner(self.db)

    def tearDown(self):
        try:
            self.p.close()
        except Exception:
            pass
        self.tmp.cleanup()

    def make_seeded(self, skip_touch_registrations=()):
        p = self.p
        p.compile_graph(ATTR_GRAPH)
        for d in ORD_DAYS:
            p.seed_published("orders", d, "v1", {"origin": "bootstrap"})
            p.register_snapshot("orders", d, "v1", [])
        for d in TOUCH_DAYS:
            p.seed_published("touches", d, "v1", {"origin": "bootstrap"})
            if d not in skip_touch_registrations:
                p.register_snapshot("touches", d, "v1", TOUCHES_V1.get(d, []))

    def run_first_plan(self):
        """Late correction of orders/2026-11-01 -> first attr materialization."""
        p = self.p
        p.ingest_event("e1", "orders", ATTR_DAY, 1, "orders-v2")
        pid = p.create_plan()
        items = p.list_plan_items(pid, limit=100)["items"]
        self.assertEqual([(i["node"], i["partition"]) for i in items],
                         [("orders", ATTR_DAY), ("attr", ATTR_DAY)])
        self.assertEqual(items[1]["new_version"], "v1")
        self.assertEqual(items[1]["input_versions"],
                         {"orders": {ATTR_DAY: "v2"},
                          "touches": {d: "v1" for d in days("2026-10-26", 7)}})
        p.advance(pid, "orders", ATTR_DAY, "success")
        p.register_snapshot("orders", ATTR_DAY, "v2", ORDERS_V2)
        return pid

    def finish_attr_items(self, pid):
        for it in self.p.list_plan_items(pid, node="attr", limit=100)["items"]:
            if it["status"] != "success":
                self.p.run_attribution_item(pid, it["node"], it["partition"])


class TestInitialMaterialization(Base):
    def test_full_mode_and_attribution_semantics(self):
        self.make_seeded()
        p = self.p
        pid = self.run_first_plan()

        # the materialized node cannot sneak past result writing via advance()
        with self.assertRaises(PlanError):
            p.advance(pid, "attr", ATTR_DAY, "success")
        # non-publish outcomes still use the original path
        self.assertEqual(p.advance(pid, "attr", ATTR_DAY, "running")["status"], "running")

        report = p.run_attribution_item(pid, "attr", ATTR_DAY)
        self.assertEqual(report["mode"], "full")
        self.assertIsNone(report["baseline_version"])
        self.assertEqual(report["version"], "v1")
        self.assertEqual(report["rows_total"], 6)
        self.assertEqual(report["rows_copied"], 0)
        # full mode reads all eight input partitions completely
        self.assertEqual(len(report["reads"]), 8)
        self.assertTrue(all(r["customers"] == "all" for r in report["reads"]))
        self.assertEqual(p.get_plan(pid)["status"], "completed")

        got = p.read_attribution("attr", ATTR_DAY, "v1")
        expected = enum_attribute(ORDERS_V2, window_touches(TOUCHES_V1, ATTR_DAY))
        self.assertEqual(got, expected)

        by_id = {r["order_id"]: r for r in got}
        # same-instant tie -> lexicographically smallest touch id
        self.assertEqual((by_id["o1"]["touch_date"], by_id["o1"]["touch_id"],
                          by_id["o1"]["channel"]), ("2026-10-31", "t2", "ads"))
        # t7 (11-01 09:00) did not exist yet when o2 (08:00) happened
        self.assertEqual(by_id["o2"]["touch_id"], "t5")
        # no candidate: order kept with empty attribution
        self.assertIsNone(by_id["o3"]["touch_id"])
        self.assertIsNone(by_id["o3"]["touch_date"])
        self.assertIsNone(by_id["o3"]["channel"])
        # oldest window day (cross-month boundary) is eligible
        self.assertEqual(by_id["o4"]["touch_date"], "2026-10-26")
        self.assertEqual(by_id["o4"]["touch_id"], "t-c5")
        # customer with touches but no orders produces no row
        self.assertNotIn("cT", {r["customer_id"] for r in got})
        self.assertIsNone(by_id["o6"]["touch_id"])  # c9 has no touches yet

        info = p.attribution_run_info("attr", ATTR_DAY, "v1")
        self.assertEqual(info["mode"], "full")
        self.assertEqual(info["input_versions"]["orders"], {ATTR_DAY: "v2"})
        self.assertEqual(info["plan_id"], pid)
        # exact-version reads: v2 was never materialized
        with self.assertRaises(StateError):
            p.read_attribution("attr", ATTR_DAY, "v2")


class TestIncrementalRevision(Base):
    def test_touch_and_order_revision_recomputes_only_affected_customers(self):
        self.make_seeded()
        p = self.p
        pid1 = self.run_first_plan()
        p.run_attribution_item(pid1, "attr", ATTR_DAY)
        v1_rows = {r["order_id"]: r for r in p.read_attribution("attr", ATTR_DAY, "v1")}

        p.ingest_event("e2", "touches", "2026-10-31", 2, "touches-v2")
        p.ingest_event("e3", "orders", ATTR_DAY, 3, "orders-v3")
        pid2 = p.create_plan()
        items = p.list_plan_items(pid2, limit=100)["items"]
        self.assertEqual([(i["node"], i["partition"]) for i in items],
                         [("touches", "2026-10-31"), ("orders", ATTR_DAY)]
                         + [("attr", d) for d in days("2026-10-31", 7)])
        attr_item = next(i for i in items
                         if i["node"] == "attr" and i["partition"] == ATTR_DAY)
        self.assertEqual(attr_item["new_version"], "v2")
        self.assertEqual(attr_item["input_versions"]["orders"], {ATTR_DAY: "v3"})
        self.assertEqual(attr_item["input_versions"]["touches"]["2026-10-31"], "v2")
        self.assertEqual(set(attr_item["waits_on"]),
                         {"orders/2026-11-01", "touches/2026-10-31"})

        p.advance(pid2, "touches", "2026-10-31", "success")
        p.advance(pid2, "orders", ATTR_DAY, "success")
        p.register_snapshot("touches", "2026-10-31", "v2", TOUCHES_V2_1031)
        p.register_snapshot("orders", ATTR_DAY, "v3", ORDERS_V3)

        report = p.run_attribution_item(pid2, "attr", ATTR_DAY)
        self.assertEqual(report["mode"], "incremental")
        self.assertEqual(report["baseline_version"], "v1")
        self.assertEqual(report["version"], "v2")
        # only version-changed upstream partitions were diffed
        self.assertEqual(report["changed_inputs"], [
            {"node": "orders", "partition": ATTR_DAY, "from": "v2", "to": "v3"},
            {"node": "touches", "partition": "2026-10-31", "from": "v1", "to": "v2"},
        ])
        # deleted winner (c1), retimed + moved touch (c2, c9), removed order (c3)
        self.assertEqual(report["affected_customers"], ["c1", "c2", "c3", "c9"])
        self.assertEqual(report["rows_copied"], 2)      # o4 (c5), o5 (cU)
        self.assertEqual(report["rows_recomputed"], 3)  # o1, o2, o6; o3 is gone
        self.assertEqual(report["rows_total"], 5)

        # read boundary: unchanged partitions were probed per affected customer
        # only -- unrelated customers in them were never scanned
        changed_keys = {("orders", ATTR_DAY), ("touches", "2026-10-31")}
        unchanged = [r for r in report["reads"]
                     if (r["node"], r["partition"]) not in changed_keys]
        self.assertTrue(unchanged)
        self.assertTrue(all(r["customers"] != "all" for r in unchanged))
        probed = {c for r in unchanged for c in r["customers"]}
        self.assertLessEqual(probed, {"c1", "c2", "c3", "c9"})
        self.assertNotIn("cU", probed)
        self.assertNotIn("c5", probed)
        self.assertNotIn("cT", probed)
        # changed partitions were read in full (old + new version) for the diff
        full_diffs = [r for r in report["reads"]
                      if (r["node"], r["partition"]) in changed_keys
                      and r["customers"] == "all"]
        self.assertEqual({(r["node"], r["partition"], r["version"]) for r in full_diffs},
                         {("orders", ATTR_DAY, "v2"), ("orders", ATTR_DAY, "v3"),
                          ("touches", "2026-10-31", "v1"), ("touches", "2026-10-31", "v2")})

        # the incremental result equals a from-scratch independent enumeration
        touches_now = dict(TOUCHES_V1)
        touches_now["2026-10-31"] = TOUCHES_V2_1031
        expected = enum_attribute(ORDERS_V3, window_touches(touches_now, ATTR_DAY))
        got = p.read_attribution("attr", ATTR_DAY, "v2")
        self.assertEqual(got, expected)

        by_id = {r["order_id"]: r for r in got}
        self.assertNotIn("o3", by_id)                    # deleted order removed
        self.assertEqual(by_id["o1"]["touch_id"], "t3")  # winner deleted -> runner-up
        self.assertEqual(by_id["o1"]["channel"], "social")
        self.assertEqual(by_id["o2"]["touch_id"], "t5")  # retimed touch, still eligible
        self.assertEqual((by_id["o6"]["touch_id"], by_id["o6"]["channel"]),
                         ("t4", "email"))                # touch moved to c9
        # copied rows are identical to the baseline
        self.assertEqual(by_id["o4"], v1_rows["o4"])
        self.assertEqual(by_id["o5"], v1_rows["o5"])
        # the old version is still readable with its original content
        self.assertEqual({r["order_id"] for r in p.read_attribution("attr", ATTR_DAY, "v1")},
                         {"o1", "o2", "o3", "o4", "o5", "o6"})

        # the other attr partitions (empty order days) materialize as empty
        self.finish_attr_items(pid2)
        self.assertEqual(p.get_plan(pid2)["status"], "completed")
        self.assertEqual(p.read_attribution("attr", "2026-11-03", "v1"), [])
        info = p.attribution_run_info("attr", "2026-11-03", "v1")
        self.assertEqual(info["mode"], "full")


class TestPinnedVersions(Base):
    def test_reads_pinned_version_not_runtime_latest(self):
        self.make_seeded()
        p = self.p
        pid1 = self.run_first_plan()
        p.run_attribution_item(pid1, "attr", ATTR_DAY)

        p.ingest_event("e2", "touches", "2026-10-31", 2, "touches-v2")
        pid2 = p.create_plan()
        attr_item = next(i for i in p.list_plan_items(pid2, node="attr", limit=100)["items"]
                         if i["partition"] == ATTR_DAY)
        self.assertEqual(attr_item["input_versions"]["touches"]["2026-10-31"], "v2")
        p.advance(pid2, "touches", "2026-10-31", "success")
        touches_v2 = TOUCHES_V1["2026-10-31"] + [
            {"id": "t8", "customer_id": "c1",
             "ts": "2026-10-31T10:30:00Z", "channel": "sms"}]
        p.register_snapshot("touches", "2026-10-31", "v2", touches_v2)

        # a newer version appears AFTER the plan was created (out-of-band)
        p.seed_published("touches", "2026-10-31", "v3", {"origin": "racing publisher"})
        touches_v3 = touches_v2 + [
            {"id": "t9", "customer_id": "c1",
             "ts": "2026-10-31T23:59:00Z", "channel": "video"}]
        p.register_snapshot("touches", "2026-10-31", "v3", touches_v3)

        report = p.run_attribution_item(pid2, "attr", ATTR_DAY)
        # the run read the pinned v2, not the runtime-latest v3
        self.assertEqual(report["input_versions"]["touches"]["2026-10-31"], "v2")
        touches_now = dict(TOUCHES_V1)
        touches_now["2026-10-31"] = touches_v2
        expected = enum_attribute(ORDERS_V2, window_touches(touches_now, ATTR_DAY))
        self.assertEqual(p.read_attribution("attr", ATTR_DAY, "v2"), expected)
        by_id = {r["order_id"]: r for r in p.read_attribution("attr", ATTR_DAY, "v2")}
        self.assertEqual(by_id["o1"]["touch_id"], "t8")  # t9 from v3 must not leak in

        self.finish_attr_items(pid2)
        self.assertEqual(p.get_plan(pid2)["status"], "completed")


class TestRegistration(Base):
    def test_validation_is_atomic_and_snapshots_are_immutable(self):
        p = self.p
        p.compile_graph(ATTR_GRAPH)
        p.seed_published("touches", "2026-10-27", "v1", {"origin": "bootstrap"})
        p.seed_published("touches", "2026-10-28", "v1", {"origin": "bootstrap"})
        p.seed_published("orders", "2026-11-01", "v1", {"origin": "bootstrap"})

        def bad(rows, node="touches", partition="2026-10-27", version="v1"):
            with self.assertRaises(SnapshotError):
                p.register_snapshot(node, partition, version, rows)
            # no half-written snapshot survives
            self.assertIsNone(p.conn.execute(
                "SELECT 1 FROM node_snapshots WHERE node=? AND partition=? AND version=?",
                (node, partition, version)).fetchone())
            self.assertEqual(p.conn.execute(
                "SELECT COUNT(*) c FROM snapshot_rows"
                " WHERE node=? AND partition=? AND version=?",
                (node, partition, version)).fetchone()["c"], 0)

        good = {"id": "a", "customer_id": "c1",
                "ts": "2026-10-27T01:00:00Z", "channel": "email"}
        bad([good, dict(good, customer_id="c2")])                    # duplicate id
        bad([dict(good, ts="2026-10-28T00:00:00Z")])                 # outside the day
        bad([dict(good, ts="2026-10-27T01:00:00+08:00")])            # not UTC
        bad([dict(good, ts="2026-10-27T01:00:00")])                  # no UTC offset
        bad([dict(good, ts="not-a-ts")])                             # unparseable
        bad([dict(good, id="")])                                     # empty id
        bad([dict(good, customer_id="")])                            # empty customer
        bad([{k: v for k, v in good.items() if k != "channel"}])     # missing channel
        bad([dict(good, extra=1)])                                   # unexpected field
        bad([{"id": "a", "customer_id": "c1", "ts": "2026-11-01T01:00:00Z",
              "channel": "x"}], node="orders", partition="2026-11-01")  # channel on orders
        bad("not-a-list")
        bad([], version="v9")            # version not published yet
        bad([], node="attr")             # not an upstream node
        bad([], partition="2026-11")     # not a day partition
        bad([], version="1")             # bad version format

        # explicit empty snapshot; identical re-registration is a no-op
        r = p.register_snapshot("touches", "2026-10-27", "v1", [])
        self.assertEqual((r["result"], r["rows"]), ("registered", 0))
        self.assertEqual(p.register_snapshot("touches", "2026-10-27", "v1", [])["result"],
                         "duplicate")
        # ... but replacing it with different content is rejected
        with self.assertRaises(SnapshotError):
            p.register_snapshot("touches", "2026-10-27", "v1", [good])
        self.assertEqual(p.conn.execute(
            "SELECT row_count c FROM node_snapshots"
            " WHERE node='touches' AND partition='2026-10-27' AND version='v1'")
            .fetchone()["c"], 0)

        # timestamps are canonicalized (offset form, fractional seconds)
        p.register_snapshot("touches", "2026-10-28", "v1",
                            [{"id": "b", "customer_id": "c1",
                              "ts": "2026-10-28T07:00:00.5+00:00", "channel": "email"}])
        ts = p.conn.execute("SELECT ts FROM snapshot_rows WHERE row_id='b'").fetchone()["ts"]
        self.assertEqual(ts, "2026-10-28T07:00:00.500000Z")

    def test_missing_snapshot_blocks_run_until_registered(self):
        self.make_seeded(skip_touch_registrations={"2026-10-27"})
        p = self.p
        pid = self.run_first_plan()
        # a missing snapshot is not treated as an empty one: the item cannot run
        with self.assertRaises(PlanError) as ctx:
            p.run_attribution_item(pid, "attr", ATTR_DAY)
        self.assertIn("touches/2026-10-27@v1", str(ctx.exception))
        self.assertIsNone(p.current_version("attr", ATTR_DAY))
        self.assertEqual(p.stats()["attribution_runs"], 0)
        self.assertEqual(p.list_plan_items(pid, node="attr")["items"][0]["status"], "pending")
        # explicit (empty) registration unblocks the run
        p.register_snapshot("touches", "2026-10-27", "v1", [])
        report = p.run_attribution_item(pid, "attr", ATTR_DAY)
        self.assertEqual(report["rows_total"], 6)
        self.assertEqual(p.get_plan(pid)["status"], "completed")


class TestCommitAtomicity(Base):
    def test_failed_commit_rolls_back_and_item_can_retry(self):
        self.make_seeded()
        p = self.p
        # v1 already materialized: old results must survive a failed v2 commit
        pid1 = self.run_first_plan()
        p.run_attribution_item(pid1, "attr", ATTR_DAY)
        v1_rows = p.read_attribution("attr", ATTR_DAY, "v1")

        p.ingest_event("e2", "touches", "2026-10-31", 2, "touches-v2")
        pid2 = p.create_plan()
        p.advance(pid2, "touches", "2026-10-31", "success")
        p.register_snapshot("touches", "2026-10-31", "v2", TOUCHES_V2_1031)

        # a racing publisher grabs the item's new version before the run commits
        p.seed_published("attr", ATTR_DAY, "v2", {"origin": "racing publisher"})
        with self.assertRaises(sqlite3.IntegrityError):
            p.run_attribution_item(pid2, "attr", ATTR_DAY)
        # nothing leaked: v2 has no results/run record, item still runnable,
        # and the old version is completely untouched
        with self.assertRaises(StateError):
            p.read_attribution("attr", ATTR_DAY, "v2")
        self.assertEqual(p.read_attribution("attr", ATTR_DAY, "v1"), v1_rows)
        self.assertEqual(p.stats()["attribution_runs"], 1)
        item = next(i for i in p.list_plan_items(pid2, node="attr", limit=100)["items"]
                    if i["partition"] == ATTR_DAY)
        self.assertEqual(item["status"], "pending")
        self.assertEqual(p.get_plan(pid2)["status"], "active")

        # clear the racing row; the retry publishes results + version atomically
        p.conn.execute(
            "DELETE FROM published WHERE node='attr' AND partition=? AND version='v2'",
            (ATTR_DAY,))
        p.conn.commit()
        report = p.run_attribution_item(pid2, "attr", ATTR_DAY)
        self.assertEqual(report["version"], "v2")
        self.assertEqual(report["mode"], "incremental")
        self.assertEqual(p.read_attribution("attr", ATTR_DAY, "v1"), v1_rows)
        # a succeeded item cannot publish again while the plan is retained
        with self.assertRaises(PlanError):
            p.run_attribution_item(pid2, "attr", ATTR_DAY)
        self.finish_attr_items(pid2)
        self.assertEqual(p.get_plan(pid2)["status"], "completed")


class TestGraphValidation(Base):
    def test_attribution_node_shape(self):
        p = self.p
        inputs = [{"node": "orders"}, {"node": "touches", "window": 7, "cross_day": True}]
        p.compile_graph({"nodes": {
            "orders": {}, "touches": {},
            "attr": {"materialize": "attribution", "inputs": inputs}}})

        def bad(g):
            with self.assertRaises(GraphError):
                p.compile_graph(g)

        bad({"nodes": {"orders": {}, "touches": {},
                       "attr": {"materialize": "nope", "inputs": inputs}}})
        bad({"nodes": {"orders": {},
                       "attr": {"materialize": "attribution",
                                "inputs": [{"node": "orders"}]}}})  # no 7-day window
        bad({"nodes": {"orders": {}, "touches": {},
                       "attr": {"materialize": "attribution",
                                "inputs": [{"node": "orders"},
                                           {"node": "touches", "window": 6,
                                            "cross_day": True}]}}})
        bad({"nodes": {"orders": {}, "other": {},
                       "attr": {"materialize": "attribution",
                                "inputs": [{"node": "orders"}, {"node": "other"}]}}})
        bad({"nodes": {"orders": {}, "touches": {},
                       "attr": {"output": "month", "materialize": "attribution",
                                "inputs": inputs}}})
        bad({"nodes": {"orders": {},
                       "attr": {"materialize": "attribution",
                                "inputs": [{"node": "orders"},
                                           {"node": "orders", "window": 7,
                                            "cross_day": True}]}}})  # same node twice
        # one node cannot be the orders upstream here and the touches upstream there
        bad({"nodes": {"orders": {}, "touches": {},
                       "a1": {"materialize": "attribution", "inputs": inputs},
                       "a2": {"materialize": "attribution",
                              "inputs": [{"node": "touches"},
                                         {"node": "orders", "window": 7,
                                          "cross_day": True}]}}})


class TestBlockedPrerequisite(Base):
    def test_failed_upstream_blocks_attribution_item(self):
        self.make_seeded()
        p = self.p
        p.ingest_event("e1", "orders", ATTR_DAY, 1, "orders-v2")
        pid = p.create_plan()
        res = p.advance(pid, "orders", ATTR_DAY, "failed")
        self.assertEqual(res["blocked"], ["attr/2026-11-01"])
        # a blocked item cannot run and publishes nothing
        with self.assertRaises(PlanError):
            p.run_attribution_item(pid, "attr", ATTR_DAY)
        self.assertIsNone(p.current_version("attr", ATTR_DAY))
        self.assertEqual(p.stats()["attribution_runs"], 0)
        self.assertEqual(p.get_plan(pid)["status"], "completed_with_failures")
        # dirty debt retries exactly the failed/blocked pair in the next plan
        pid2 = p.create_plan()
        self.assertEqual(
            [(i["node"], i["partition"]) for i in p.list_plan_items(pid2, limit=100)["items"]],
            [("orders", ATTR_DAY), ("attr", ATTR_DAY)])


class TestPersistence(Base):
    def test_reopen_reads_old_versions(self):
        self.make_seeded()
        pid = self.run_first_plan()
        self.p.run_attribution_item(pid, "attr", ATTR_DAY)
        v1_rows = self.p.read_attribution("attr", ATTR_DAY, "v1")
        self.p.close()

        p2 = RecomputePlanner(self.db)
        self.assertEqual(p2.read_attribution("attr", ATTR_DAY, "v1"), v1_rows)
        info = p2.attribution_run_info("attr", ATTR_DAY, "v1")
        self.assertEqual(info["mode"], "full")
        self.assertEqual(info["input_versions"]["orders"][ATTR_DAY], "v2")
        with self.assertRaises(StateError):
            p2.read_attribution("attr", ATTR_DAY, "v9")
        p2.close()


if __name__ == "__main__":
    unittest.main()
