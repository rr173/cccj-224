"""Cross-day consistent attribution delivery.

Every test seeds the database directly (published rows + registered snapshots
+ a materialized run/results triple) because this feature must never create a
plan, register upstream data or recompute attribution.  Selection expectations
are fixed by hand or by the independent brute-force reference at the bottom of
the file.
"""

import itertools
import json
import os
import random
import sys
import tempfile
import unittest
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import DeliveryError, RecomputePlanner  # noqa: E402
from planner import delivery as delivery_mod  # noqa: E402

ATTR_GRAPH = {"nodes": {
    "orders": {"output": "day"},
    "touches": {"output": "day"},
    "attr": {"output": "day", "materialize": "attribution",
             "inputs": [{"node": "orders"},
                        {"node": "touches", "window": 7, "cross_day": True}]},
}}

BASE = date(2026, 10, 1)


def d(offset):
    return (BASE + timedelta(days=offset)).isoformat()


def vkey(version):
    return (int(version[1:]), version)


def window(day):
    base = date.fromisoformat(day)
    return [(base - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]


def touch_versions(day, default="v1", **overrides):
    """Seven touch-day versions of one order day's window."""
    out = {tday: default for tday in window(day)}
    for key, value in overrides.items():
        assert key in out, f"{key} is not in {day}'s window"
        out[key] = value
    return out


def result(order_id, customer="c", touch_day=None, touch_id=None, channel=None):
    return {"order_id": order_id, "customer_id": customer, "touch_date": touch_day,
            "touch_id": touch_id, "channel": channel}


def _canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def read_jsonl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def read_bytes(path):
    with open(path, "rb") as fh:
        return fh.read()


# ----------------------------------------------------------------------
# seeding helpers (direct SQL, no planner workflow)
# ----------------------------------------------------------------------
def add_published(p, node, day, version):
    p.conn.execute(
        "INSERT OR IGNORE INTO published (node, partition, version, evidence_json,"
        " source, created_at) VALUES (?,?,?,?, 'test', '2026-11-22T00:00:00+00:00')",
        (node, day, version, _canon({"seeded": True})))


def add_order_snapshot(p, day, version, rows=()):
    p.conn.execute(
        "INSERT OR IGNORE INTO node_snapshots (node, partition, version, kind, digest,"
        " row_count, registered_at) VALUES ('orders', ?, ?, 'orders', 'x', ?,"
        " '2026-11-22T00:00:00+00:00')", (day, version, len(rows)))
    for row in rows:
        p.conn.execute(
            "INSERT INTO snapshot_rows (node, partition, version, row_id,"
            " customer_id, ts, channel) VALUES ('orders',?,?,?,?,?,NULL)"
            " ON CONFLICT DO NOTHING",
            (day, version, row["id"], row["customer_id"], row["ts"]))


def materialize(p, day, version, orders_version, touches, results,
                register_touches=True):
    """Publish + run record + results (+ registered snapshots) for one day.

    ``touches``: {touch_day: version} for the seven days of ``day``'s window.
    """
    add_published(p, "orders", day, orders_version)
    add_order_snapshot(p, day, orders_version)
    for tday, tver in touches.items():
        add_published(p, "touches", tday, tver)
        if register_touches:
            p.conn.execute(
                "INSERT OR IGNORE INTO node_snapshots (node, partition, version, kind,"
                " digest, row_count, registered_at) VALUES ('touches', ?, ?,"
                " 'touches', 'x', 0, '2026-11-22T00:00:00+00:00')", (tday, tver))
    add_published(p, "attr", day, version)
    inputs = {"orders": {day: orders_version}, "touches": dict(touches)}
    p.conn.execute(
        "INSERT INTO attribution_runs (node, partition, version, plan_id, mode,"
        " baseline_version, input_versions_json, report_json, created_at)"
        " VALUES ('attr', ?, ?, 'seeded', 'full', NULL, ?, '{}',"
        " '2026-11-22T00:00:00+00:00')",
        (day, version, _canon(inputs)))
    for r in results:
        p.conn.execute(
            "INSERT INTO attribution_results (node, partition, version, order_id,"
            " customer_id, order_ts, touch_date, touch_id, channel)"
            " VALUES ('attr',?,?,?,?,?,?,?,?)",
            (day, version, r["order_id"], r["customer_id"],
             r.get("order_ts", "2026-10-01T00:00:00.000000Z"),
             r["touch_date"], r["touch_id"], r["channel"]))


# ----------------------------------------------------------------------
# independent selection reference (brute force over the Cartesian product)
# ----------------------------------------------------------------------
def ref_choose(days, runs):
    """Best list of (day, entry) by exhaustive search, or None."""
    best = None
    for combo in itertools.product(*[runs[day] for day in days]):
        ok = True
        for i in range(len(days)):
            for j in range(i + 1, len(days)):
                a, b = combo[i], combo[j]
                for tday in set(a["touches"]) & set(b["touches"]):
                    if a["touches"][tday] != b["touches"][tday]:
                        ok = False
                        break
                if not ok:
                    break
            if not ok:
                break
        if not ok:
            continue
        key = tuple(vkey(e["version"]) for e in reversed(combo))
        if best is None or key > best[0]:
            best = (key, list(zip(days, combo)))
    return best


class Base(unittest.TestCase):
    def setUp(self):
        self._tmps = []
        self._planners = []
        self.p, self.work = self.new_planner()
        self.p.compile_graph(ATTR_GRAPH)
        self.p.conn.commit()
        self.target = os.path.join(self.work, "delivery.jsonl")

    def new_planner(self):
        tmp = tempfile.TemporaryDirectory()
        self._tmps.append(tmp)
        path = os.path.join(tmp.name, "planner.db")
        p = self.open_on(path)
        return p, tmp.name

    def open_on(self, db_path):
        """Another connection to the same database file.

        WAL mode is set once on creation; reopening must not change it."""
        p = RecomputePlanner(db_path)
        self._planners.append(p)
        return p

    def tearDown(self):
        for p in self._planners:
            try:
                p.close()
            except Exception:
                pass
        for tmp in self._tmps:
            tmp.cleanup()

    def commit(self):
        self.p.conn.commit()

    def deliver(self, start, end, target=None, planner=None):
        p = planner or self.p
        return p.deliver_attribution_range("attr", start, end,
                                           target or self.target)


# ----------------------------------------------------------------------
# hand-computed scenario
# ----------------------------------------------------------------------
class TestHandComputedFallback(Base):
    """Three consecutive order days 10-29, 10-30, 10-31.

    Their 7-day windows overlap on 10-24..10-29 (29/30), 10-25..10-30 (30/31)
    and 10-25..10-29 (29/31).

    * 10-30@v2 (newest) and 10-31@v2 (newest) both read all-v1 touches and
      are compatible with each other;
    * 10-29@v2 (newest) reads 10-28@v9, so it clashes with both;
    * 10-29@v1 reads all-v1 touches and combines with either.

    Enumerating the 8 combinations leaves (v1, v2, v2) as the unique optimum
    (read latest order day first): the two newer days keep their newest and
    the older day falls back.  Picking 10-29's newest and "padding" cannot work.
    """

    def seed(self):
        p = self.p
        day1, day2, day3 = d(28), d(29), d(30)          # 10-29, 10-30, 10-31
        # day 1: newest conflicts on a shared day; runner-up is all-v1
        materialize(p, day1, "v1", "v5", touch_versions(day1, "v1"),
                    [result("a2", "c1", d(27), "t-z", "print")])
        materialize(p, day1, "v2", "v2",
                    touch_versions(day1, "v1", **{d(23): "v9"}),
                    [result("a2", "c1", d(27), "t-z", "print")])
        # day 2: newest all-v1 (compatible with day 3), runner-up all-v2
        materialize(p, day2, "v1", "v1", touch_versions(day2, "v2"),
                    [result("b1", "c2", day2, "t-b", "ads")])
        materialize(p, day2, "v2", "v1", touch_versions(day2, "v1"),
                    [result("b1", "c2", day2, "t-b", "ads")])
        # day 3: newest all-v1, runner-up all-v2
        materialize(p, day3, "v1", "v1", touch_versions(day3, "v2"),
                    [result("c1", "c3", day3, "t-c", "social")])
        materialize(p, day3, "v2", "v1", touch_versions(day3, "v1"),
                    [result("c1", "c3", day3, "t-c", "social")])
        self.commit()
        self.days = [day1, day2, day3]

    def test_fallback_manifest_orders_and_distinct_order_versions(self):
        self.seed()
        day1, day2, day3 = self.days
        # an older delivery already exists and is replaced only by a complete one
        with open(self.target, "w", encoding="utf-8") as fh:
            fh.write('{"old":true}\n')
        run = self.deliver(day1, day3)
        self.assertEqual(run["rows"], 3)

        lines = read_jsonl(self.target)
        manifest = lines[0]
        self.assertEqual(manifest["kind"], "attribution-delivery-manifest")
        self.assertEqual(manifest["node"], "attr")
        self.assertEqual(manifest["date_range"], {"start": day1, "end": day3})
        self.assertEqual(
            [(e["date"], e["attribution_version"]) for e in manifest["days"]],
            [(day1, "v1"), (day2, "v2"), (day3, "v2")])
        # exact versions actually read, including touch days that win nothing
        self.assertEqual(manifest["days"][0]["input_versions"],
                         {"orders": {day1: "v5"},
                          "touches": touch_versions(day1, "v1")})
        self.assertEqual(manifest["days"][1]["input_versions"],
                         {"orders": {day2: "v1"},
                          "touches": touch_versions(day2, "v1")})
        # every shared touch day carries exactly one version across the file
        shared = {}
        for entry in manifest["days"]:
            for tday, tver in entry["input_versions"]["touches"].items():
                shared.setdefault(tday, set()).add(tver)
        self.assertTrue(all(len(v) == 1 for v in shared.values()), shared)
        for tday, versions in shared.items():
            if tday in window(day1) and tday in window(day2):
                self.assertEqual(versions, {"v1"})

        rows = lines[1:]
        self.assertEqual([(r["order_date"], r["order_id"]) for r in rows],
                         [(day1, "a2"), (day2, "b1"), (day3, "c1")])
        self.assertEqual((rows[0]["touch_date"], rows[0]["touch_id"], rows[0]["channel"]),
                         (d(27), "t-z", "print"))
        # 10-29's order version (v5) differs from the other days' (v1)
        self.assertEqual([(r["attribution_version"], r["orders_version"]) for r in rows],
                         [("v1", "v5"), ("v2", "v1"), ("v2", "v1")])

    def test_single_day_range_keeps_newest(self):
        self.seed()
        day1, _, day3 = self.days
        run = self.deliver(day1, day1)
        manifest = read_jsonl(self.target)[0]
        self.assertEqual(manifest["days"][0]["attribution_version"], "v2")
        self.assertEqual(manifest["days"][0]["input_versions"]["orders"][day1], "v2")
        self.assertEqual(run["rows"], 1)
        self.assertEqual(read_jsonl(self.target)[1]["touch_id"], "t-z")

        run = self.deliver(day3, day3, os.path.join(self.work, "d3.jsonl"))
        self.assertEqual(run["days"][0]["attribution_version"], "v2")

    def test_middle_day_must_move_with_its_neighbour(self):
        self.seed()
        day1, day2, day3 = self.days
        # From day 2 on, the newest of day 2 and day 3 are all-v1 and compatible;
        # the range [day2..day3] keeps both newest.
        run = self.deliver(day2, day3)
        self.assertEqual([e["attribution_version"] for e in run["days"]], ["v2", "v2"])
        # Extending to day 1 forces day 1 to fall back, never the other direction.
        run = self.deliver(day1, day3)
        self.assertEqual([e["attribution_version"] for e in run["days"]],
                         ["v1", "v2", "v2"])


class TestCandidateEligibility(Base):
    def test_published_without_materialization_is_skipped(self):
        p = self.p
        materialize(p, d(5), "v1", "v1", touch_versions(d(5)), [result("a")])
        materialize(p, d(5), "v2", "v2", touch_versions(d(5)), [result("a")])
        add_published(p, "attr", d(5), "v3")      # published, never materialized
        self.commit()
        run = self.deliver(d(5), d(5))
        self.assertEqual(run["days"][0]["attribution_version"], "v2")

    def test_materialized_empty_day_differs_from_unmaterialized(self):
        p = self.p
        materialize(p, d(6), "v1", "v1", touch_versions(d(6)), [])
        add_published(p, "attr", d(6), "v2")      # looks newer, no run record
        self.commit()
        run = self.deliver(d(6), d(6))
        self.assertEqual(run["days"][0]["attribution_version"], "v1")
        self.assertEqual(run["rows"], 0)
        self.assertEqual(len(read_jsonl(self.target)), 1)  # manifest only

    def test_run_record_must_pin_exactly_the_required_inputs(self):
        p = self.p
        day = d(7)
        # a run that also claims an unrelated partition is ambiguous, not a
        # candidate; the delivery never reads upstream rows to guess
        materialize(p, day, "v1", "v1", touch_versions(day), [])
        bad = {"orders": {day: "v1"}, "touches": touch_versions(day)}
        bad["touches"][d(0)] = "v4"           # 10-01 is not in day's window
        p.conn.execute(
            "UPDATE attribution_runs SET input_versions_json=?"
            " WHERE node='attr' AND partition=? AND version='v1'",
            (_canon(bad), day))
        self.commit()
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(day, day)
        self.assertEqual(ctx.exception.code, "no_candidate")
        self.assertFalse(os.path.exists(self.target))

    def test_run_record_missing_a_required_input_is_not_a_candidate(self):
        p = self.p
        day = d(7)
        materialize(p, day, "v1", "v1", touch_versions(day), [])
        partial = {"orders": {day: "v1"},
                   "touches": {t: "v1" for t in window(day)[1:]}}
        p.conn.execute(
            "UPDATE attribution_runs SET input_versions_json=?"
            " WHERE node='attr' AND partition=? AND version='v1'",
            (_canon(partial), day))
        self.commit()
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(day, day)
        self.assertEqual(ctx.exception.code, "no_candidate")

    def test_run_must_be_published(self):
        materialize(self.p, d(8), "v1", "v1", touch_versions(d(8)), [])
        self.p.conn.execute("DELETE FROM published WHERE node='attr' AND partition=?",
                            (d(8),))
        self.commit()
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(d(8), d(8))
        self.assertEqual(ctx.exception.code, "no_candidate")

    def test_day_without_candidates_is_a_failure_not_a_skip(self):
        materialize(self.p, d(9), "v1", "v1", touch_versions(d(9)), [])
        self.commit()
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(d(9), d(10))
        self.assertEqual(ctx.exception.code, "no_candidate")
        self.assertIn(d(10), str(ctx.exception))
        self.assertFalse(os.path.exists(self.target))

    def test_rejects_bad_node_and_bad_range(self):
        materialize(self.p, d(9), "v1", "v1", touch_versions(d(9)), [])
        self.commit()
        with self.assertRaises(DeliveryError) as ctx:
            self.p.deliver_attribution_range("orders", d(9), d(9), self.target)
        self.assertEqual(ctx.exception.code, "not_attribution")
        with self.assertRaises(DeliveryError) as ctx:
            self.p.deliver_attribution_range("ghost", d(9), d(9), self.target)
        self.assertEqual(ctx.exception.code, "unknown_node")
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(d(10), d(9))
        self.assertEqual(ctx.exception.code, "invalid_range")
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(d(9), d(9), os.path.join(self.work, "nope", "x.jsonl"))
        self.assertEqual(ctx.exception.code, "invalid_target")

    def test_works_from_records_without_a_current_graph(self):
        # a separate database with no compiled graph at all: eligibility must
        # come from the materialized records, not from the current graph
        p = RecomputePlanner(os.path.join(self.work, "nograph.db"))
        self._planners.append(p)
        target = os.path.join(self.work, "nograph.jsonl")
        materialize(p, d(12), "v1", "v1", touch_versions(d(12)), [result("a")])
        materialize(p, d(13), "v1", "v3", touch_versions(d(13)), [result("b")])
        p.conn.commit()
        run = p.deliver_attribution_range("attr", d(12), d(13), target)
        self.assertEqual([e["date"] for e in run["days"]], [d(12), d(13)])
        self.assertEqual(run["orders_node"], "orders")
        self.assertEqual(run["touches_node"], "touches")
        self.assertEqual([r["orders_version"] for r in read_jsonl(target)[1:]],
                         ["v1", "v3"])

    def test_explicit_upstream_nodes(self):
        materialize(self.p, d(14), "v1", "v1", touch_versions(d(14)), [result("a")])
        self.commit()
        run = self.p.deliver_attribution_range(
            "attr", d(14), d(14), self.target,
            orders_node="orders", touches_node="touches")
        self.assertEqual(run["days"][0]["attribution_version"], "v1")
        with self.assertRaises(DeliveryError) as ctx:
            self.p.deliver_attribution_range(
                "attr", d(14), d(14), self.target, orders_node="orders")
        self.assertEqual(ctx.exception.code, "invalid_inputs")

    def test_does_not_plan_register_or_recompute(self):
        materialize(self.p, d(11), "v1", "v1", touch_versions(d(11)), [result("a")])
        before = self.p.stats()
        self.commit()
        self.deliver(d(11), d(11))
        stats = self.p.stats()
        self.assertEqual(stats["plans_total"], 0)
        self.assertEqual(stats["events_total"], 0)
        self.assertEqual(stats["snapshots_registered"], before["snapshots_registered"])
        self.assertEqual(stats["attribution_runs"], before["attribution_runs"])
        self.assertEqual(stats["attribution_rows"], before["attribution_rows"])


# ----------------------------------------------------------------------
# generated multi-day scenarios
# ----------------------------------------------------------------------
def generated_scenario(seed, n_days, n_versions, touch_pool, order_pool,
                       backbone_prob, guarantee_solution):
    """runs: {day: [{version, orders_version, touches, results}]}.

    The first candidate of each day is always fully on a random backbone when
    ``guarantee_solution`` is set, so a consistent chain exists; the other
    candidates deviate on a random subset of touch days, creating many
    equivalent local choices.  Distinct order versions per day create further
    ties that the selection must resolve deterministically.
    """
    rng = random.Random(seed)
    days = [d(100 + i) for i in range(n_days)]
    all_touch_days = sorted({t for day in days for t in window(day)})
    backbone = {t: f"v{rng.randint(1, touch_pool)}" for t in all_touch_days}

    runs = {}
    for day in days:
        versions = [f"v{n}" for n in rng.sample(range(1, n_versions + 1), n_versions)]
        runs[day] = []
        for index, version in enumerate(versions):
            touches = {}
            for tday in window(day):
                if guarantee_solution and index == 0:
                    touches[tday] = backbone[tday]
                elif rng.random() < backbone_prob:
                    touches[tday] = backbone[tday]
                else:
                    touches[tday] = f"v{rng.randint(1, touch_pool)}"
            order_version = f"v{rng.randint(1, order_pool)}"
            rows = [result(f"{day}-{version}-{k}") for k in range(2)]
            runs[day].append({"version": version, "orders_version": order_version,
                              "touches": touches, "results": rows})
    return days, runs


class TestGeneratedScenarios(Base):
    def seed_runs(self, days, runs):
        for day in days:
            for entry in runs[day]:
                materialize(self.p, day, entry["version"], entry["orders_version"],
                            entry["touches"], entry["results"])
        self.commit()

    def check_against_reference(self, days, runs):
        self.seed_runs(days, runs)
        expected = ref_choose(days, runs)
        self.assertIsNotNone(expected, "generator failed to guarantee a solution")
        run = self.deliver(days[0], days[-1])
        lines = read_jsonl(self.target)
        manifest = lines[0]
        self.assertEqual([e["date"] for e in manifest["days"]], days)
        self.assertEqual([e["attribution_version"] for e in manifest["days"]],
                         [entry["version"] for _, entry in expected[1]])
        for entry, (day, run_entry) in zip(manifest["days"], expected[1]):
            self.assertEqual(entry["input_versions"]["orders"], {day: run_entry["orders_version"]})
            self.assertEqual(entry["input_versions"]["touches"], run_entry["touches"])
        expected_rows = [(day, r["order_id"])
                         for day, entry in expected[1] for r in entry["results"]]
        self.assertEqual([(r["order_date"], r["order_id"]) for r in lines[1:]],
                         expected_rows)
        self.assertEqual(run["rows"], len(expected_rows))

    def test_many_equivalent_choices_matches_reference(self):
        for seed in (1, 2, 3):
            planner, work = self.new_planner()
            planner.compile_graph(ATTR_GRAPH)
            planner.conn.commit()
            self.p, self.target = planner, os.path.join(work, "delivery.jsonl")
            with self.subTest(seed=seed):
                days, runs = generated_scenario(
                    seed, n_days=5, n_versions=6, touch_pool=6, order_pool=4,
                    backbone_prob=0.6, guarantee_solution=True)
                self.check_against_reference(days, runs)

    def test_every_day_has_a_version_but_none_combine(self):
        planner, work = self.new_planner()
        planner.compile_graph(ATTR_GRAPH)
        planner.conn.commit()
        self.p, self.target = planner, os.path.join(work, "delivery.jsonl")
        days, runs = generated_scenario(
            7, n_days=5, n_versions=4, touch_pool=40, order_pool=3,
            backbone_prob=0.0, guarantee_solution=False)
        self.assertIsNone(ref_choose(days, runs))
        self.seed_runs(days, runs)
        # every day is individually deliverable
        for day in days:
            self.deliver(day, day, os.path.join(work, f"{day}.jsonl"))
        with open(self.target, "w", encoding="utf-8") as fh:
            fh.write('{"previous":true}\n')
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(days[0], days[-1])
        self.assertEqual(ctx.exception.code, "no_solution")
        self.assertEqual(read_bytes(self.target), b'{"previous":true}\n')

    def test_random_solvable_and_unsolvable_match_reference(self):
        for seed in (11, 12, 13, 14):
            planner, work = self.new_planner()
            planner.compile_graph(ATTR_GRAPH)
            planner.conn.commit()
            self.p, self.target = planner, os.path.join(work, "delivery.jsonl")
            with self.subTest(seed=seed):
                days, runs = generated_scenario(
                    seed, n_days=4, n_versions=5, touch_pool=6, order_pool=3,
                    backbone_prob=0.4, guarantee_solution=(seed % 2 == 1))
                expected = ref_choose(days, runs)
                self.seed_runs(days, runs)
                if expected is None:
                    with self.assertRaises(DeliveryError) as ctx:
                        self.deliver(days[0], days[-1])
                    self.assertEqual(ctx.exception.code, "no_solution")
                else:
                    self.deliver(days[0], days[-1])
                    got = [e["attribution_version"]
                           for e in read_jsonl(self.target)[0]["days"]]
                    self.assertEqual(got, [entry["version"] for _, entry in expected[1]])


    def test_selection_never_enumerates_the_cartesian_product(self):
        planner, work = self.new_planner()
        planner.compile_graph(ATTR_GRAPH)
        planner.conn.commit()
        self.p, self.target = planner, os.path.join(work, "delivery.jsonl")
        days, runs = generated_scenario(
            21, n_days=6, n_versions=4, touch_pool=6, order_pool=3,
            backbone_prob=0.5, guarantee_solution=True)
        # 4^6 = 4096 combinations: an exhaustive search would call the pairwise
        # compatibility test at least that often; the chain DP stays near 6*4*4
        calls = {"n": 0}
        real = delivery_mod._compatible

        def counting(a, b):
            calls["n"] += 1
            return real(a, b)

        delivery_mod._compatible = counting
        try:
            self.seed_runs(days, runs)
            self.deliver(days[0], days[-1])
        finally:
            delivery_mod._compatible = real
        self.assertLess(calls["n"], 6 * 4 * 4 + 1)
        expected = ref_choose(days, runs)
        self.assertEqual([e["attribution_version"]
                          for e in read_jsonl(self.target)[0]["days"]],
                         [entry["version"] for _, entry in expected[1]])


    def test_rows_are_streamed_day_by_day(self):
        planner, work = self.new_planner()
        planner.compile_graph(ATTR_GRAPH)
        planner.conn.commit()
        self.p, self.target = planner, os.path.join(work, "delivery.jsonl")
        days, runs = generated_scenario(
            31, n_days=4, n_versions=1, touch_pool=3, order_pool=2,
            backbone_prob=1.0, guarantee_solution=True)
        per_day = {}
        for day in days:
            entry = runs[day][0]
            entry["results"] = [result(f"{day}-o{k}") for k in range(200)]
            per_day[day] = len(entry["results"])
        self.seed_runs(days, runs)

        from planner import api as api_mod

        writes = {"n": 0}
        opened = []
        real_write = delivery_mod.AtomicTarget.write
        real_iter = api_mod.iter_result_rows

        def counting_write(target, line):
            writes["n"] += 1
            real_write(target, line)

        def observing_iter(conn, node, day, version):
            # when this day's result cursor is first touched, exactly the
            # manifest plus every earlier day's rows must have been written
            opened.append((day, writes["n"]))
            yield from real_iter(conn, node, day, version)

        delivery_mod.AtomicTarget.write = counting_write
        api_mod.iter_result_rows = observing_iter
        try:
            run = self.deliver(days[0], days[-1])
        finally:
            delivery_mod.AtomicTarget.write = real_write
            api_mod.iter_result_rows = real_iter
        self.assertEqual(run["rows"], sum(per_day.values()))
        expected = 1
        for day, written_before in opened:
            self.assertEqual(written_before, expected, day)
            expected += per_day[day]
        self.assertEqual([day for day, _ in opened], days)

# ----------------------------------------------------------------------
# atomic target, io failures, racing publication
# ----------------------------------------------------------------------
class TestAtomicTarget(Base):
    def seed(self, extra_day=False):
        materialize(self.p, d(20), "v1", "v1", touch_versions(d(20)),
                    [result("a", "c", d(20), "t", "email")])
        materialize(self.p, d(21), "v1", "v1", touch_versions(d(21)),
                    [result("b", "c", d(21), "t2", "ads")])
        if extra_day:
            pass
        self.commit()

    def test_missing_day_failure_keeps_previous_file(self):
        self.seed()
        self.deliver(d(20), d(21))
        old = read_bytes(self.target)
        with self.assertRaises(DeliveryError) as ctx:
            self.deliver(d(20), d(22))         # 10-22 has no candidates
        self.assertEqual(ctx.exception.code, "no_candidate")
        self.assertEqual(read_bytes(self.target), old)
        self.assertEqual([n for n in os.listdir(self.work)
                          if n.startswith(".delivery-")], [])

    def test_io_failure_after_rows_are_written_rolls_back(self):
        self.seed()
        self.deliver(d(20), d(21))
        old = read_bytes(self.target)

        real_write = delivery_mod.AtomicTarget.write
        state = {"n": 0}

        def flaky_write(target, line):
            if state["n"] >= 2:               # manifest + one row succeeded
                raise OSError("disk full")
            state["n"] += 1
            real_write(target, line)

        delivery_mod.AtomicTarget.write = flaky_write
        try:
            with self.assertRaises(DeliveryError) as ctx:
                self.deliver(d(20), d(21))
        finally:
            delivery_mod.AtomicTarget.write = real_write
        self.assertEqual(ctx.exception.code, "read")
        self.assertEqual(read_bytes(self.target), old)
        self.assertEqual([n for n in os.listdir(self.work)
                          if n.startswith(".delivery-")], [])

    def test_publication_during_selection_does_not_leak_in(self):
        self.seed()
        self.deliver(d(20), d(21))
        before = read_jsonl(self.target)

        # a second connection publishes a newer materialized version on the
        # second day *in the middle of the candidate scan* (between reading
        # day 20 and day 21)
        other = self.open_on(os.path.join(self.work, "planner.db"))
        from planner import api as api_mod

        real_window = api_mod.touch_window
        state = {"n": 0}

        def racing_window(day):
            state["n"] += 1
            if state["n"] == 1:
                # day 20 was already scanned; this must not join the delivery
                materialize(other, d(20), "v9", "v9", touch_versions(d(20)),
                            [result("a", "c", d(20), "t-new", "video")])
                other.conn.commit()
            return real_window(day)

        api_mod.touch_window = racing_window
        try:
            run = self.deliver(d(20), d(21))
        finally:
            api_mod.touch_window = real_window
        # the racing v9 is visible afterwards ...
        self.assertEqual(other.current_version("attr", d(20)), "v9")
        # ... but the shared read view kept it out of this selection
        self.assertEqual([e["attribution_version"] for e in run["days"]], ["v1", "v1"])
        after = read_jsonl(self.target)
        self.assertEqual(after[0]["days"], before[0]["days"])
        self.assertEqual([r["order_id"] for r in after[1:]],
                         [r["order_id"] for r in before[1:]])
        self.assertNotIn("t-new", {r.get("touch_id") for r in after[1:]})
        # a later delivery does see the newly published version
        later = self.deliver(d(20), d(20), os.path.join(self.work, "later.jsonl"))
        self.assertEqual(later["days"][0]["attribution_version"], "v9")

    def test_target_not_created_when_no_solution(self):
        self.seed()
        with self.assertRaises(DeliveryError):
            self.deliver(d(20), d(22))
        self.assertFalse(os.path.exists(self.target))


if __name__ == "__main__":
    unittest.main()
