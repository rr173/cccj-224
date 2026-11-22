"""End-to-end tests for the recompute planner.

Scenario graph (hand-computed expectations):

    A (source, day) -> B (day, same-day) -> C (day, same-day) -> M (month, global)
    A -> W (day, 7-day cross-day window)
    X1 -> X2   (unrelated branch)

Late events on A/2026-11-05 and A/2026-10-31 must dirty exactly:
    A,B,C x {2026-10-31, 2026-11-05}
    W x {2026-10-31 .. 2026-11-11}   (7-day windows covering a dirty day)
    M x {2026-10, 2026-11}           (months containing a dirty day)
and nothing on the X branch.
"""

import os
import sys
import tempfile
import unittest
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import (EventError, GraphError, PlanError, RecomputePlanner,  # noqa: E402
                     StateError)
from reference.ref_impl import reference_impact  # noqa: E402


def build_graph():
    return {"nodes": {
        "A": {"output": "day"},
        "B": {"output": "day", "inputs": [{"node": "A"}]},
        "C": {"output": "day", "inputs": [{"node": "B"}]},
        "W": {"output": "day", "inputs": [{"node": "A", "window": 7, "cross_day": True}]},
        "M": {"output": "month", "inputs": [{"node": "C", "global": True}]},
        "X1": {"output": "day"},
        "X2": {"output": "day", "inputs": [{"node": "X1"}]},
    }}


DAYS = [(date(2026, 10, 1) + timedelta(days=i)).isoformat() for i in range(51)]  # 10-01..11-20


def seed_all(p):
    for n in ("A", "B", "C", "W", "X1", "X2"):
        for d in DAYS:
            p.seed_published(n, d, "v1", {"origin": "bootstrap"})
    for m in ("2026-09", "2026-10", "2026-11"):
        p.seed_published("M", m, "v1", {"origin": "bootstrap"})


def run_plan(p, pid):
    """Advance every item to success in publish order."""
    for it in p.list_plan_items(pid, limit=100000)["items"]:
        p.advance(pid, it["node"], it["partition"], "success")


# Hand-computed expected publish order for events A/2026-11-05 + A/2026-10-31.
EXPECTED_MAIN_ORDER = [
    ("A", "2026-10-31"), ("A", "2026-11-05"),
    ("B", "2026-10-31"), ("W", "2026-10-31"),
    ("W", "2026-11-01"), ("W", "2026-11-02"), ("W", "2026-11-03"), ("W", "2026-11-04"),
    ("B", "2026-11-05"), ("W", "2026-11-05"),
    ("W", "2026-11-06"), ("W", "2026-11-07"), ("W", "2026-11-08"),
    ("W", "2026-11-09"), ("W", "2026-11-10"), ("W", "2026-11-11"),
    ("C", "2026-10-31"), ("C", "2026-11-05"),
    ("M", "2026-10"), ("M", "2026-11"),
]


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

    def make_seeded(self):
        self.p.compile_graph(build_graph())
        seed_all(self.p)


class TestMainScenario(Base):
    def test_hand_computed_minimal_set_order_and_evidence(self):
        self.make_seeded()
        p = self.p
        p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        p.ingest_event("e2", "A", "2026-10-31", 2, "d2")
        pid = p.create_plan()
        self.assertIsNotNone(pid)

        items = p.list_plan_items(pid, limit=100)["items"]
        self.assertEqual([(i["node"], i["partition"]) for i in items], EXPECTED_MAIN_ORDER)
        self.assertEqual([i["publish_order"] for i in items], list(range(1, 21)))
        self.assertTrue(all(i["new_version"] == "v2" for i in items))

        by_key = {(i["node"], i["partition"]): i for i in items}
        # 7-day window item: exact input versions and prerequisites
        w = by_key[("W", "2026-11-05")]
        self.assertEqual(w["input_versions"]["A"], {
            "2026-10-30": "v1", "2026-10-31": "v2", "2026-11-01": "v1",
            "2026-11-02": "v1", "2026-11-03": "v1", "2026-11-04": "v1",
            "2026-11-05": "v2"})
        self.assertEqual(w["waits_on"], ["A/2026-10-31", "A/2026-11-05"])
        # cross-month boundary: window of W/2026-10-31 spans Oct->Nov dirty day
        w31 = by_key[("W", "2026-10-31")]
        self.assertEqual(w31["input_versions"]["A"]["2026-10-25"], "v1")
        self.assertEqual(w31["input_versions"]["A"]["2026-10-31"], "v2")
        self.assertEqual(w31["waits_on"], ["A/2026-10-31"])
        # global monthly aggregate: evidence covers every day of the month
        m = by_key[("M", "2026-11")]
        self.assertEqual(len(m["input_versions"]["C"]), 30)
        self.assertEqual(m["input_versions"]["C"]["2026-11-05"], "v2")
        self.assertEqual(m["input_versions"]["C"]["2026-11-04"], "v1")
        self.assertEqual(m["waits_on"], ["C/2026-11-05"])
        m10 = by_key[("M", "2026-10")]
        self.assertEqual(len(m10["input_versions"]["C"]), 31)
        self.assertEqual(m10["input_versions"]["C"]["2026-10-31"], "v2")
        # chain evidence
        c = by_key[("C", "2026-11-05")]
        self.assertEqual(c["waits_on"], ["B/2026-11-05"])
        self.assertEqual(c["input_versions"], {"B": {"2026-11-05": "v2"}})
        # reasons cite the triggering events
        self.assertTrue(any("e1" in r for r in by_key[("A", "2026-11-05")]["reasons"]))
        self.assertTrue(any("e2" in r for r in by_key[("A", "2026-10-31")]["reasons"]))
        self.assertTrue(any("window" in r for r in w["reasons"]))
        self.assertTrue(any("global month 2026-11" in r for r in m["reasons"]))

        # safely skippable partitions
        skip = p.get_skippable(pid, limit=1000)["items"]
        skip_keys = {(s["node"], s["partition"]) for s in skip}
        for k in [("X1", "2026-11-05"), ("X2", "2026-10-31"), ("C", "2026-11-06"),
                  ("W", "2026-11-12"), ("W", "2026-10-30"), ("M", "2026-09")]:
            self.assertIn(k, skip_keys)
        self.assertNotIn(("W", "2026-11-05"), skip_keys)
        x1 = next(s for s in skip if s["node"] == "X1")
        self.assertIn("unaffected", x1["reason"])

        # execute in order; versions advance; old versions retained
        run_plan(p, pid)
        self.assertEqual(p.get_plan(pid)["status"], "completed")
        self.assertEqual(p.current_version("A", "2026-11-05"), "v2")
        self.assertEqual(p.current_version("M", "2026-11"), "v2")
        self.assertEqual([h["version"] for h in p.version_history("A", "2026-11-05")],
                         ["v1", "v2"])
        ev = p.version_history("A", "2026-11-05")[1]["evidence"]
        self.assertEqual(ev["plan_id"], pid)

        # replay by plan id
        rep = p.replay_plan(pid)
        self.assertTrue(rep["match"], rep["diffs"])
        self.assertEqual(rep["item_count"], 20)

    def test_reference_implementation_agrees(self):
        self.make_seeded()
        p = self.p
        p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        p.ingest_event("e2", "A", "2026-10-31", 2, "d2")
        pid = p.create_plan()
        items = p.list_plan_items(pid, limit=100)["items"]

        ref = reference_impact(build_graph(), [("A", "2026-11-05"), ("A", "2026-10-31")])
        self.assertEqual(set(ref["dirty"]),
                         {f"{i['node']}/{i['partition']}" for i in items})
        edge_map = {}
        for u, v in ref["edges"]:
            edge_map.setdefault(v, set()).add(u)
        for i in items:
            key = f"{i['node']}/{i['partition']}"
            self.assertEqual(set(i["waits_on"]), edge_map.get(key, set()), key)

    def test_pagination_by_node(self):
        self.make_seeded()
        self.p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        self.p.ingest_event("e2", "A", "2026-10-31", 2, "d2")
        pid = self.p.create_plan()
        page1 = self.p.list_plan_items(pid, node="W", limit=5, offset=0)
        self.assertEqual(page1["total"], 12)
        self.assertEqual([i["partition"] for i in page1["items"]],
                         ["2026-10-31", "2026-11-01", "2026-11-02",
                          "2026-11-03", "2026-11-04"])
        page3 = self.p.list_plan_items(pid, node="W", limit=5, offset=10)
        self.assertEqual([i["partition"] for i in page3["items"]],
                         ["2026-11-10", "2026-11-11"])
        self.assertEqual(self.p.list_plan_items(pid, node="X1", limit=5)["total"], 0)


class TestEvents(Base):
    def test_duplicate_and_content_change_revision(self):
        self.make_seeded()
        p = self.p
        r1 = p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        self.assertEqual(r1, {"result": "accepted", "event_id": "e1", "revision": 1})
        r2 = p.ingest_event("e1", "A", "2026-11-05", 2, "d1")  # re-delivery
        self.assertEqual(r2["result"], "duplicate")
        r3 = p.ingest_event("e1", "A", "2026-11-05", 3, "d1b")  # content changed
        self.assertEqual((r3["result"], r3["revision"]), ("revision", 2))
        # only the latest revision stays pending and drives the plan
        pid = p.create_plan()
        a = next(i for i in p.list_plan_items(pid, limit=100)["items"] if i["node"] == "A")
        self.assertTrue(any("rev2" in r for r in a["reasons"]))
        self.assertTrue(all("rev1" not in r for r in a["reasons"]))

    def test_event_validation(self):
        self.make_seeded()
        with self.assertRaises(EventError):
            self.p.ingest_event("e", "NOPE", "2026-11-05", 1, "d")
        with self.assertRaises(EventError):
            self.p.ingest_event("e", "A", "2026-13-40", 1, "d")
        with self.assertRaises(EventError):
            self.p.ingest_event("e", "M", "2026-11-05", 1, "d")  # month node
        with self.assertRaises(EventError):
            self.p.ingest_event("e", "A", "2026-11-05", 1, "")

    def test_no_pending_events_no_plan(self):
        self.make_seeded()
        self.assertIsNone(self.p.create_plan())
        self.p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        pid = self.p.create_plan()
        self.assertIsNotNone(pid)
        # a second attempt while active is refused, not silently recomputed
        with self.assertRaises(PlanError):
            self.p.create_plan()
        run_plan(self.p, pid)
        # events were consumed: no full-history recompute on the next call
        self.assertIsNone(self.p.create_plan())

    def test_preview_impact_prunes(self):
        self.make_seeded()
        self.p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        dirty = set(self.p.preview_impact()["dirty"])
        self.assertIn("W/2026-11-11", dirty)
        self.assertIn("M/2026-11", dirty)
        self.assertNotIn("W/2026-11-12", dirty)   # window [11-06..11-12] excludes 11-05
        self.assertNotIn("M/2026-10", dirty)
        self.assertNotIn("X1/2026-11-05", dirty)  # unrelated branch pruned
        self.p.create_plan()
        self.assertEqual(self.p.preview_impact()["dirty"], {})


class TestGraphValidation(Base):
    def test_missing_dependency_rejected(self):
        with self.assertRaises(GraphError):
            self.p.compile_graph({"nodes": {
                "A": {"output": "day", "inputs": [{"node": "GHOST"}]}}})

    def test_cycle_rejected(self):
        with self.assertRaises(GraphError):
            self.p.compile_graph({"nodes": {
                "A": {"inputs": [{"node": "C"}]},
                "B": {"inputs": [{"node": "A"}]},
                "C": {"inputs": [{"node": "B"}]},
            }})

    def test_range_contradictions_rejected(self):
        with self.assertRaises(GraphError):  # window without cross_day
            self.p.compile_graph({"nodes": {
                "A": {}, "W": {"inputs": [{"node": "A", "window": 7}]}}})
        with self.assertRaises(GraphError):  # global but daily output
            self.p.compile_graph({"nodes": {
                "A": {}, "M": {"output": "day", "inputs": [{"node": "A", "global": True}]}}})
        with self.assertRaises(GraphError):  # day->month without global
            self.p.compile_graph({"nodes": {
                "A": {}, "M": {"output": "month", "inputs": [{"node": "A"}]}}})
        with self.assertRaises(GraphError):  # window < 1
            self.p.compile_graph({"nodes": {
                "A": {}, "B": {"inputs": [{"node": "A", "window": 0, "cross_day": True}]}}})

    def test_published_state_missing_evidence_rejected(self):
        self.make_seeded()
        self.p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        self.p.conn.execute(
            "INSERT INTO published (node, partition, version, evidence_json, source, created_at)"
            " VALUES ('A', '2026-12-01', 'v1', NULL, 'bootstrap', '2026-11-22T00:00:00+00:00')")
        self.p.conn.commit()
        with self.assertRaises(StateError):
            self.p.create_plan()


class TestFailureSemantics(Base):
    def test_node_failure_blocks_successors_and_keeps_old_versions(self):
        self.make_seeded()
        p = self.p
        p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        pid = p.create_plan()
        self.assertEqual(p.list_plan_items(pid, limit=100)["total"], 11)

        p.advance(pid, "A", "2026-11-05", "success")
        self.assertEqual(p.current_version("A", "2026-11-05"), "v2")
        res = p.advance(pid, "B", "2026-11-05", "failed")
        self.assertEqual(res["status"], "failed")
        self.assertEqual(set(res["blocked"]), {"C/2026-11-05", "M/2026-11"})
        # blocked items refuse to advance and never publish
        with self.assertRaises(PlanError):
            p.advance(pid, "C", "2026-11-05", "success")
        # independent W branch still completes
        for d in range(5, 12):
            p.advance(pid, "W", f"2026-11-{d:02d}", "success")
        plan = p.get_plan(pid)
        self.assertEqual(plan["status"], "completed_with_failures")
        # successors stayed unpublished; old versions untouched
        self.assertEqual(p.current_version("C", "2026-11-05"), "v1")
        self.assertEqual(p.current_version("M", "2026-11"), "v1")
        self.assertEqual(p.current_version("W", "2026-11-07"), "v2")
        self.assertEqual([h["version"] for h in p.version_history("C", "2026-11-05")], ["v1"])

        # dirty debt retries exactly the failed/blocked set in the next plan
        pid2 = p.create_plan()
        items2 = p.list_plan_items(pid2, limit=100)["items"]
        self.assertEqual([(i["node"], i["partition"]) for i in items2],
                         [("B", "2026-11-05"), ("C", "2026-11-05"), ("M", "2026-11")])
        self.assertEqual(items2[0]["new_version"], "v2")  # failed attempt consumed no version
        self.assertTrue(any("carry-over" in r for r in items2[0]["reasons"]))
        run_plan(p, pid2)
        self.assertEqual(p.current_version("C", "2026-11-05"), "v2")
        hist = p.version_history("C", "2026-11-05")
        self.assertEqual([h["version"] for h in hist], ["v1", "v2"])
        self.assertEqual(hist[1]["evidence"]["inputs"], {"B": {"2026-11-05": "v2"}})

    def test_prerequisite_not_complete(self):
        self.make_seeded()
        self.p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        pid = self.p.create_plan()
        with self.assertRaises(PlanError):
            self.p.advance(pid, "B", "2026-11-05", "success")  # A not done yet
        with self.assertRaises(PlanError):
            self.p.advance(pid, "A", "2026-11-05", "bogus")
        with self.assertRaises(PlanError):
            self.p.advance(pid, "A", "2026-11-06", "success")  # not in plan
        with self.assertRaises(PlanError):
            self.p.advance("P-nope", "A", "2026-11-05", "success")


class TestInterleaving(Base):
    def test_interleaved_ingest_and_advance_no_leak_no_overreach(self):
        self.make_seeded()
        p = self.p
        p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        pid1 = p.create_plan()
        self.assertEqual(p.list_plan_items(pid1, limit=100)["total"], 11)
        p.advance(pid1, "A", "2026-11-05", "success")
        p.advance(pid1, "B", "2026-11-05", "success")

        # event arrives while plan1 is still active
        p.ingest_event("e3", "A", "2026-11-06", 2, "d3")
        with self.assertRaises(PlanError):
            p.create_plan()  # one active plan at a time

        p.advance(pid1, "C", "2026-11-05", "failed")  # blocks M/2026-11
        for d in range(5, 12):
            p.advance(pid1, "W", f"2026-11-{d:02d}", "success")
        self.assertEqual(p.get_plan(pid1)["status"], "completed_with_failures")
        self.assertEqual(p.current_version("C", "2026-11-05"), "v1")
        self.assertEqual(p.current_version("M", "2026-11"), "v1")

        # plan2 = new event e3 + carry-over debt {C/11-05, M/2026-11}
        pid2 = p.create_plan()
        got = [(i["node"], i["partition"])
               for i in p.list_plan_items(pid2, limit=100)["items"]]
        self.assertEqual(got, [
            ("A", "2026-11-06"), ("B", "2026-11-06"),
            ("W", "2026-11-06"), ("W", "2026-11-07"), ("W", "2026-11-08"),
            ("W", "2026-11-09"), ("W", "2026-11-10"), ("W", "2026-11-11"),
            ("W", "2026-11-12"),
            ("C", "2026-11-05"), ("C", "2026-11-06"), ("M", "2026-11"),
        ])
        keys = set(got)
        # no over-propagation: W/11-05 was republished by plan1 and is clean now
        self.assertNotIn(("W", "2026-11-05"), keys)
        # no full-history recompute: e1 already consumed
        self.assertNotIn(("A", "2026-11-05"), keys)
        # unrelated branch never leaks in
        self.assertFalse(any(n.startswith("X") for n, _ in keys))
        # no missed pruning: window [11-06..11-12] must include the new dirty day
        self.assertIn(("W", "2026-11-12"), keys)
        by_key = {(i["node"], i["partition"]): i
                  for i in p.list_plan_items(pid2, limit=100)["items"]}
        self.assertEqual(by_key[("M", "2026-11")]["waits_on"],
                         ["C/2026-11-05", "C/2026-11-06"])
        w06 = by_key[("W", "2026-11-06")]["input_versions"]["A"]
        self.assertEqual(w06["2026-11-05"], "v2")  # republished by plan1
        self.assertEqual(w06["2026-11-06"], "v2")  # planned now
        run_plan(p, pid2)

        # duplicate re-delivery is a no-op; no plan is generated
        self.assertEqual(p.ingest_event("e1", "A", "2026-11-05", 3, "d1")["result"],
                         "duplicate")
        self.assertIsNone(p.create_plan())

        # content change forms a new revision and triggers recompute
        r = p.ingest_event("e1", "A", "2026-11-05", 4, "d1b")
        self.assertEqual((r["result"], r["revision"]), ("revision", 2))
        pid3 = p.create_plan()
        items3 = p.list_plan_items(pid3, limit=100)["items"]
        self.assertEqual({(i["node"], i["partition"]) for i in items3},
                         {("A", "2026-11-05"), ("B", "2026-11-05"), ("C", "2026-11-05"),
                          ("M", "2026-11")}
                         | {("W", f"2026-11-{d:02d}") for d in range(5, 12)})
        a3 = next(i for i in items3 if i["node"] == "A")
        self.assertEqual(a3["new_version"], "v3")  # v1 bootstrap, v2 from plan1
        run_plan(p, pid3)
        self.assertEqual([h["version"] for h in p.version_history("A", "2026-11-05")],
                         ["v1", "v2", "v3"])


class TestReplayAndBinding(Base):
    def test_replay_match_and_tamper_detection(self):
        self.make_seeded()
        p = self.p
        p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        pid = p.create_plan()
        self.assertTrue(p.replay_plan(pid)["match"])
        p.conn.execute(
            "UPDATE plan_items SET new_version='v99'"
            " WHERE plan_id=? AND node='A' AND partition='2026-11-05'", (pid,))
        p.conn.commit()
        rep = p.replay_plan(pid)
        self.assertFalse(rep["match"])
        self.assertTrue(rep["diffs"])

    def test_plan_id_binds_graph_events_and_published_state(self):
        ids = []
        for extra in (False, False, True):
            with tempfile.TemporaryDirectory() as tmp:
                p = RecomputePlanner(os.path.join(tmp, "x.db"))
                p.compile_graph(build_graph())
                seed_all(p)
                if extra:
                    p.seed_published("A", "2026-11-21", "v1", {"origin": "bootstrap"})
                p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
                ids.append(p.create_plan())
                p.close()
        self.assertEqual(ids[0], ids[1])       # identical inputs -> identical plan id
        self.assertNotEqual(ids[0], ids[2])    # different published state -> different id

    def test_replay_survives_event_pruning(self):
        p = RecomputePlanner(self.db, max_events=1, max_plans=10)
        p.compile_graph(build_graph())
        seed_all(p)
        for i in range(3):
            p.ingest_event(f"e{i}", "A", f"2026-11-{5 + i:02d}", i, f"d{i}")
            pid = p.create_plan()
            run_plan(p, pid)
        kept = p.conn.execute("SELECT plan_id FROM plans ORDER BY created_at").fetchall()
        self.assertEqual(len(kept), 3)
        # events were pruned to the retention bound, replay still works from bound inputs
        self.assertLessEqual(p.stats()["events_total"], 1 + 1)  # <= max_events consumed + pending
        self.assertTrue(p.replay_plan(kept[0]["plan_id"])["match"])
        p.close()


class TestPersistenceAndRetention(Base):
    def test_reopen_after_process_exit(self):
        self.make_seeded()
        self.p.ingest_event("e1", "A", "2026-11-05", 1, "d1")
        self.p.close()
        p2 = RecomputePlanner(self.db)
        pid = p2.create_plan()
        self.assertIsNotNone(pid)
        self.assertEqual(p2.list_plan_items(pid, limit=100)["total"], 11)
        p2.close()

    def test_bounded_retention(self):
        p = RecomputePlanner(self.db, max_events=3, max_plans=2)
        p.compile_graph(build_graph())
        seed_all(p)
        pids = []
        for i in range(5):
            p.ingest_event(f"e{i}", "A", f"2026-11-{10 + i:02d}", i, f"d{i}")
            pid = p.create_plan()
            run_plan(p, pid)
            pids.append(pid)
        stats = p.stats()
        self.assertLessEqual(stats["events_total"], 3)      # consumed events pruned
        self.assertLessEqual(stats["plans_total"], 2)       # old plans pruned
        with self.assertRaises(PlanError):
            p.get_plan(pids[0])                              # oldest plan is gone
        self.assertTrue(p.replay_plan(pids[-1])["match"])    # newest still replayable
        p.close()


if __name__ == "__main__":
    unittest.main()
