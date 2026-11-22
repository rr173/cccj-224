"""Weekly channel reward posting.

Inputs are produced by the real ``deliver_attribution_range`` entry point on a
directly seeded attribution database, so the JSONL format under test is the
shipped one.  Expected settlement amounts are hand-computed in the test
docstrings; an independent per-order reference recomputes the same arithmetic
for the generated larger sample.

Run:  python3 -m unittest tests.test_rewards -v
"""

import hashlib
import json
import os
import random
import sys
import tempfile
import unittest
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from planner import RewardError, RewardLedger, RecomputePlanner  # noqa: E402

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
ZERO = {"email": 0, "ads": 0, "social": 0}


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def window(day):
    base = date.fromisoformat(day)
    return [(base - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]


def sha256(path):
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def read_lines(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


# ----------------------------------------------------------------------
# input production through the shipped delivery entry point
# ----------------------------------------------------------------------
def seed_day(p, day, version, pairs):
    """Publish + materialize one attribution day; touches are all v1."""
    evidence = canon({"seeded": True})
    p.conn.execute(
        "INSERT OR IGNORE INTO published (node, partition, version, evidence_json,"
        " source, created_at) VALUES ('orders',?,?,?,'seed',"
        " '2026-11-22T00:00:00+00:00')", (day, version, evidence))
    touches = {t: "v1" for t in window(day)}
    for tday, tver in touches.items():
        p.conn.execute(
            "INSERT OR IGNORE INTO published (node, partition, version,"
            " evidence_json, source, created_at) VALUES ('touches',?,?,?,'seed',"
            " '2026-11-22T00:00:00+00:00')", (tday, tver, evidence))
        p.conn.execute(
            "INSERT OR IGNORE INTO node_snapshots (node, partition, version, kind,"
            " digest, row_count, registered_at) VALUES ('touches',?,?,'touches',"
            " 'x',0,'2026-11-22T00:00:00+00:00')", (tday, tver))
    p.conn.execute(
        "INSERT OR IGNORE INTO node_snapshots (node, partition, version, kind,"
        " digest, row_count, registered_at) VALUES ('orders',?,?,'orders','x',0,"
        " '2026-11-22T00:00:00+00:00')", (day, version))
    p.conn.execute(
        "INSERT OR IGNORE INTO published (node, partition, version, evidence_json,"
        " source, created_at) VALUES ('attr',?,?,?,'seed',"
        " '2026-11-22T00:00:00+00:00')", (day, version, evidence))
    inputs = {"orders": {day: version}, "touches": touches}
    p.conn.execute(
        "INSERT INTO attribution_runs (node, partition, version, plan_id, mode,"
        " baseline_version, input_versions_json, report_json, created_at)"
        " VALUES ('attr',?,?,'seed','full',NULL,?,'{}',"
        " '2026-11-22T00:00:00+00:00')", (day, version, canon(inputs)))
    for order_id, channel in pairs:
        p.conn.execute(
            "INSERT INTO attribution_results (node, partition, version, order_id,"
            " customer_id, order_ts, touch_date, touch_id, channel)"
            " VALUES ('attr',?,?,?,?,?,?,?,?)",
            (day, version, order_id, "c", "2026-10-01T00:00:00.000000Z",
             day if channel else None, f"t-{order_id}" if channel else None,
             channel))


def build_week(work, name, layout):
    """Produce a real delivery JSONL for one whole week.

    ``layout``: {day: [(order_id, channel|None), ...]}; missing days are empty.
    Returns the file path.  Attribution versions differ from orders versions
    to prove the two are read from the right places.
    """
    db = os.path.join(work, f"{name}.db")
    target = os.path.join(work, f"{name}.jsonl")
    p = RecomputePlanner(db)
    p.compile_graph(ATTR_GRAPH)
    for index, day in enumerate(WEEK):
        seed_day(p, day, f"v{index + 1}", layout.get(day, []))
    p.conn.commit()
    run = p.deliver_attribution_range("attr", WEEK[0], WEEK[-1], target)
    p.close()
    assert run["rows"] == sum(len(v) for v in layout.values())
    return target


def rewrite(path, transform):
    """Write a modified copy next to ``path`` and return the new path."""
    lines = read_lines(path)
    out = [lines[0]] + [transform(dict(row)) for row in lines[1:]]
    target = path.replace(".jsonl", ".mod.jsonl")
    with open(target, "w", encoding="utf-8") as fh:
        for obj in out:
            fh.write(canon(obj) + "\n")
    return target


# ----------------------------------------------------------------------
# independent reference for the split + differential
# ----------------------------------------------------------------------
def ref_entitlements(rows, pools, week):
    """Plain per-order reference for one whole-week snapshot."""
    participants = {}
    for row in rows:
        channel = row["channel"]
        if channel in pools:
            key = (row["order_date"], row["order_id"])
            participants.setdefault(channel, []).append(key)
    out = {}
    for channel, pool in pools.items():
        keys = sorted(set(participants.get(channel, [])))
        if not keys:
            continue
        base, remainder = divmod(pool, len(keys))
        for index, key in enumerate(keys):
            out[(key[0], key[1], channel)] = base + (1 if index < remainder else 0)
    return out


def ref_delta(old, new):
    out = {}
    for key in set(old) | set(new):
        amount = new.get(key, 0) - old.get(key, 0)
        if amount:
            out[key] = amount
    return out


class Base(unittest.TestCase):
    def setUp(self):
        self._tmps = []
        self._ledgers = []
        self.work = self._tmpdir()
        self.led = self.new_ledger()

    def _tmpdir(self):
        tmp = tempfile.TemporaryDirectory()
        self._tmps.append(tmp)
        return tmp.name

    def new_ledger(self, path=None):
        path = path or os.path.join(self.work, "rewards.db")
        led = RewardLedger(path)
        self._ledgers.append(led)
        return led

    def tearDown(self):
        for led in self._ledgers:
            try:
                led.close()
            except Exception:
                pass
        for tmp in self._tmps:
            tmp.cleanup()

    def post(self, path, receipt, prev=None, pools=None, ledger=None,
             digest=None, week=None, **kwargs):
        return (ledger or self.led).post_weekly_rewards(
            "attr", week or WEEK[0], path, digest or sha256(path), receipt,
            prev, pools or POOLS, **kwargs)

    def lines(self, receipt, ledger=None):
        return {(r["order_date"], r["order_id"], r["channel"]): r["amount"]
                for r in (ledger or self.led).iter_reward_lines(
                    "attr", WEEK[0], receipt)}

    def assert_reward_error(self, code, fn, *args, **kwargs):
        with self.assertRaises(RewardError) as ctx:
            fn(*args, **kwargs)
        self.assertEqual(ctx.exception.code, code, str(ctx.exception))
        return ctx.exception


# ----------------------------------------------------------------------
# hand-computed first receipt and correction
# ----------------------------------------------------------------------
class TestHandComputed(Base):
    """Week 2026-10-05..2026-10-11, pools email=100, ads=50, social=30.

    First receipt (v1 attribution):
      10-05 email: a1, a2, a3, b1
      10-06 email: c1 ; ads: d1
      10-07 ads: e1 ; social: e2
      (headers: a1 a2 a3 b1 c1 d1 e1 e2)

    email has 5 participants -> 100 = 20 each.
    ads has 2 -> 50 = 25 each.
    social has 1 -> 30.

    Correction (new attribution): a1 moves email -> ads, b1 disappears.
      10-05 email: a2, a3
      10-06 email: c1 ; ads: a1, d1
      10-07 ads: e1 ; social: e2

    email now 3 -> 100 = 33 rem 1 -> 10-05/a2 gets the extra cent:
      a2=34, a3=33, c1=33.
    ads now 3 -> 50 = 16 rem 2 -> the two earliest (10-05/a1, 10-06/d1):
      a1=17, d1=17, e1=16.
    social unchanged: e2=30.

    Deltas:
      email: a1 -20 (old channel clawed back), b1 -20 (vanished order),
             a2 +14 (34-20), a3 +13, c1 +13  -> 0 net
      ads:   a1 +17 (new channel, on 10-06), d1 17-25 = -8,
             e1 16-25 = -9 -> 0 net
      social: none (e2 30-30 = 0 is not a line)
    """

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

    def test_first_receipt_and_correction(self):
        first = build_week(self.work, "v1", self.V1)
        second = build_week(self.work, "v2", self.V2)

        r1 = self.post(first, "R-1")
        self.assertEqual(r1["seq"], 1)
        self.assertIsNone(r1["prev_receipt_no"])
        self.assertEqual(r1["payout"], {"email": 100, "ads": 50, "social": 30})
        self.assertEqual(self.lines("R-1"), {
            ("2026-10-05", "a1", "email"): 20,
            ("2026-10-05", "a2", "email"): 20,
            ("2026-10-05", "a3", "email"): 20,
            ("2026-10-05", "b1", "email"): 20,
            ("2026-10-06", "c1", "email"): 20,
            ("2026-10-06", "d1", "ads"): 25,
            ("2026-10-07", "e1", "ads"): 25,
            ("2026-10-07", "e2", "social"): 30,
        })

        r2 = self.post(second, "R-2", prev="R-1")
        self.assertEqual(r2["seq"], 2)
        self.assertEqual(r2["prev_receipt_no"], "R-1")
        self.assertEqual(r2["payout"], ZERO)      # corrections net to zero
        self.assertEqual(r2["entitlement"],
                         {"email": 100, "ads": 50, "social": 30})
        self.assertEqual(self.lines("R-2"), {
            ("2026-10-05", "a1", "email"): -20,   # old channel clawed back
            ("2026-10-06", "a1", "ads"): 17,      # same order, new channel
            ("2026-10-05", "b1", "email"): -20,   # order vanished
            ("2026-10-05", "a2", "email"): 14,    # unchanged channel, re-split
            ("2026-10-05", "a3", "email"): 13,
            ("2026-10-06", "c1", "email"): 13,
            ("2026-10-06", "d1", "ads"): -8,      # unchanged channel, re-split
            ("2026-10-07", "e1", "ads"): -9,
        })

        # independent reference agrees, per receipt and per channel
        ref1 = ref_entitlements(read_lines(first)[1:], POOLS, WEEK)
        ref2 = ref_entitlements(read_lines(second)[1:], POOLS, WEEK)
        expect2 = ref_delta(ref1, ref2)
        self.assertEqual(self.lines("R-2"), {
            (d, o, c): a for (d, o, c), a in expect2.items()})

        # cumulative signed lines reproduce the current entitlement exactly
        cumulative = {}
        for receipt in ("R-1", "R-2"):
            for key, amount in self.lines(receipt).items():
                cumulative[key] = cumulative.get(key, 0) + amount
        cumulative = {k: v for k, v in cumulative.items() if v}
        self.assertEqual(cumulative, {k: v for k, v in ref2.items() if v})

    def test_receipt_chain_is_persisted_and_readable_after_reopen(self):
        first = build_week(self.work, "v1", self.V1)
        second = build_week(self.work, "v2", self.V2)
        self.post(first, "R-1")
        self.post(second, "R-2", prev="R-1")
        self.led.close()
        self._ledgers.remove(self.led)

        reopened = self.new_ledger()
        chain = reopened.list_reward_receipts("attr", WEEK[0])
        self.assertEqual([r["receipt_no"] for r in chain], ["R-1", "R-2"])
        # the approved digest and predecessor survive for audit
        self.assertEqual(chain[1]["approved_digest"], sha256(second))
        self.assertEqual(chain[1]["prev_receipt_no"], "R-1")
        page = reopened.read_reward_receipt("attr", WEEK[0], "R-2")
        self.assertEqual(page["line_count"], 8)
        self.assertEqual(page["manifest"]["days"][0]["date"], WEEK[0])
        self.assertEqual(reopened.reward_week_summary("attr", WEEK[0]),
                         {"book_id": "attr|2026-10-05", "pools": POOLS,
                          "entitlement": {"email": 100, "ads": 50, "social": 30},
                          "open_receipt_no": "R-2", "receipts": 2})

    def test_zero_delta_still_records_receipt_and_basis(self):
        first = build_week(self.work, "v1", self.V1)
        self.post(first, "R-1")
        # same content, new number: an explicit no-op correction
        r = self.post(first, "R-2", prev="R-1")
        self.assertEqual(r["line_count"], 0)
        self.assertEqual(r["payout"], ZERO)
        self.assertEqual(r["file_sha256"], sha256(first))
        self.assertEqual(list(self.led.iter_reward_lines("attr", WEEK[0], "R-2")), [])
        self.assertEqual(self.led.reward_week_summary("attr", WEEK[0])
                         ["open_receipt_no"], "R-2")

    def test_unattributed_and_unconfigured_channels_earn_nothing(self):
        path = build_week(self.work, "u", {
            "2026-10-05": [("x1", None), ("x2", "print"), ("x3", "ads")],
        })
        r = self.post(path, "R-1")
        self.assertEqual(r["payout"], {"email": 0, "ads": 50, "social": 0})
        self.assertEqual(self.lines("R-1"),
                         {("2026-10-05", "x3", "ads"): 50})
        # an entirely empty week is allowed and posts no lines; it must be its
        # own book (a different Monday), not a second R-1 precursor
        empty = build_week(self.work, "empty", {})
        next_week = "2026-10-12"
        target = os.path.join(self.work, "empty-week.jsonl")
        p = RecomputePlanner(os.path.join(self.work, "empty.db"))
        p.compile_graph(ATTR_GRAPH)
        for day in [(date.fromisoformat(next_week) + timedelta(days=i)).isoformat()
                    for i in range(7)]:
            seed_day(p, day, "v1", [])
        p.conn.commit()
        p.deliver_attribution_range("attr", next_week,
                                    (date.fromisoformat(next_week)
                                     + timedelta(days=6)).isoformat(), target)
        p.close()
        r = self.post(target, "E-1", week=next_week)
        self.assertEqual(r["line_count"], 0)
        self.assertEqual(r["payout"], ZERO)


# ----------------------------------------------------------------------
# week / book validation
# ----------------------------------------------------------------------
class TestWeekRules(Base):
    def test_rejects_non_monday_and_partial_week(self):
        path = build_week(self.work, "v1", TestHandComputed.V1)
        self.assert_reward_error("invalid_week", self.post, path, "R-1",
                                 week="2026-10-06")
        day_path = os.path.join(self.work, "day.jsonl")
        p = RecomputePlanner(os.path.join(self.work, "day.db"))
        p.compile_graph(ATTR_GRAPH)
        seed_day(p, WEEK[0], "v1", [("a1", "email")])
        p.conn.commit()
        p.deliver_attribution_range("attr", WEEK[0], WEEK[0], day_path)
        p.close()
        self.assert_reward_error("invalid_week", self.post, day_path, "R-1")

    def test_manifest_must_cover_every_day_exactly_once(self):
        path = build_week(self.work, "v1", TestHandComputed.V1)

        def drop_day(manifest):
            manifest["days"] = manifest["days"][:6]
            manifest["date_range"]["end"] = manifest["days"][-1]["date"]
            return manifest

        def duplicate_day(manifest):
            manifest["days"][-1] = dict(manifest["days"][-2])
            return manifest

        def wrong_node(manifest):
            manifest["node"] = "other"
            return manifest

        for mutate, code in ((drop_day, "invalid_week"),
                             (duplicate_day, "invalid_week"),
                             (wrong_node, "invalid_delivery")):
            lines = read_lines(path)
            target = os.path.join(self.work, f"bad-{mutate.__name__}.jsonl")
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(canon(mutate(lines[0])) + "\n")
                for row in lines[1:]:
                    fh.write(canon(row) + "\n")
            self.assert_reward_error(code, self.post, target, "R-1")

    def test_rows_outside_the_week_duplicates_and_partial_attribution(self):
        path = build_week(self.work, "v1", TestHandComputed.V1)

        def out_of_week(row):
            row["order_date"] = "2026-10-12"
            return row

        def partial(row):
            row["touch_id"] = None
            return row

        def duplicate(row):
            return dict(row)

        cases = [("out", out_of_week, "invalid_row"),
                 ("partial", partial, "invalid_row"),
                 ("dup", duplicate, "invalid_row")]
        for name, mutate, code in cases:
            lines = read_lines(path)
            target = os.path.join(self.work, f"row-{name}.jsonl")
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(canon(lines[0]) + "\n")
                for row in lines[1:]:
                    fh.write(canon(mutate(dict(row))) + "\n")
                if name == "dup":
                    fh.write(canon(lines[1]) + "\n")
            self.assert_reward_error(code, self.post, target, f"R-{name}")

    def test_partial_attribution_is_not_treated_as_unattributed(self):
        path = build_week(self.work, "v1", TestHandComputed.V1)

        def only_channel(row):
            row["touch_date"] = None
            row["touch_id"] = None
            return row

        target = rewrite(path, only_channel)
        target = os.path.join(self.work, "partial2.jsonl")
        lines = read_lines(path)
        with open(target, "w", encoding="utf-8") as fh:
            fh.write(canon(lines[0]) + "\n")
            for row in lines[1:]:
                fh.write(canon(only_channel(dict(row))) + "\n")
        self.assert_reward_error("invalid_row", self.post, target, "R-1")

    def test_row_version_must_match_the_manifest(self):
        path = build_week(self.work, "v1", TestHandComputed.V1)

        for field, value in (("attribution_version", "v9"),
                             ("orders_version", "v9")):
            def mutate(row, field=field, value=value):
                row[field] = value
                return row
            target = os.path.join(self.work, f"ver-{field}.jsonl")
            lines = read_lines(path)
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(canon(lines[0]) + "\n")
                for row in lines[1:]:
                    fh.write(canon(mutate(dict(row))) + "\n")
            self.assert_reward_error("version_mismatch", self.post, target, "R-1")

    def test_pools_are_immutable_after_the_first_receipt(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        second = build_week(self.work, "v2", TestHandComputed.V2)
        self.post(first, "R-1")
        self.assert_reward_error(
            "immutable_pools", self.post, second, "R-2", prev="R-1",
            pools={"email": 99, "ads": 50, "social": 30})
        # non-negative SQLite integers only (validated before touching the book)
        for i, bad in enumerate(({"email": -1}, {"email": 1.5}, {"email": True},
                                 {"email": 2 ** 63})):
            self.assert_reward_error("invalid_pools", self.post, first,
                                     f"R-x{i}", prev="R-1", pools=bad)


# ----------------------------------------------------------------------
# idempotency, staleness and atomicity
# ----------------------------------------------------------------------
class TestIdempotency(Base):
    def test_exact_retry_returns_original_even_after_newer_receipts(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        second = build_week(self.work, "v2", TestHandComputed.V2)
        original = self.post(first, "R-1")
        self.post(second, "R-2", prev="R-1")
        before = self.lines("R-1")

        again = self.post(first, "R-1")          # prev=None, same pools/content
        self.assertEqual(again, original)
        self.assertEqual(self.lines("R-1"), before)
        self.assertEqual(self.led.reward_week_summary("attr", WEEK[0])
                         ["open_receipt_no"], "R-2")
        # a retry of the latest receipt is also safe
        self.post(second, "R-2", prev="R-1")

    def test_same_number_different_content_or_parameters_is_rejected(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        second = build_week(self.work, "v2", TestHandComputed.V2)
        self.post(first, "R-1")
        self.assert_reward_error("receipt_conflict", self.post, second, "R-1")
        self.assert_reward_error("receipt_conflict", self.post, first, "R-1",
                                 pools={"email": 1})
        # approved bytes are verified before the receipt is consulted
        self.assert_reward_error("digest_mismatch", self.post, first, "R-1",
                                 digest="0" * 64)

    def test_stale_predecessor_is_rejected(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        second = build_week(self.work, "v2", TestHandComputed.V2)
        third = build_week(self.work, "v3", TestHandComputed.V1)
        self.post(first, "R-1")
        self.post(second, "R-2", prev="R-1")
        # R-1 is no longer the open receipt
        self.assert_reward_error("stale_predecessor", self.post, third, "R-3",
                                 prev="R-1")
        self.assert_reward_error("stale_predecessor", self.post, third, "R-3",
                                 prev=None)

    def test_two_receipts_cannot_consume_the_same_predecessor(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        second = build_week(self.work, "v2", TestHandComputed.V2)
        self.post(first, "R-1")
        self.post(second, "R-2", prev="R-1")
        self.assert_reward_error("stale_predecessor", self.post, second, "R-3",
                                 prev="R-1")
        self.assertEqual([r["receipt_no"] for r in self.led.list_reward_receipts(
            "attr", WEEK[0])], ["R-1", "R-2"])

    def test_approved_digest_must_match_the_bytes(self):
        path = build_week(self.work, "v1", TestHandComputed.V1)
        self.assert_reward_error("digest_mismatch", self.post, path, "R-1",
                                 digest="0" * 64)
        self.assert_reward_error("invalid_digest", self.post, path, "R-1",
                                 digest="not-a-digest")
        # a file changed after approval is refused even under the same number
        approved = sha256(path)
        with open(path, "ab") as fh:
            fh.write(b"\n")
        self.assert_reward_error("digest_mismatch", self.post, path, "R-1",
                                 digest=approved)

    def test_failure_while_writing_lines_leaves_nothing_and_retry_succeeds(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        # 4th line fails after the receipt row was already written in the tx
        self.assert_reward_error(
            "write", self.post, first, "R-1", _fail_after_lines=4)
        self.assertEqual(self.led.list_reward_receipts("attr", WEEK[0]), [])
        self.assertEqual(self.led.reward_week_summary("attr", WEEK[0])
                         ["open_receipt_no"], None)
        self.assert_reward_error("unknown_receipt", self.led.read_reward_receipt,
                                 "attr", WEEK[0], "R-1")
        self.assert_reward_error("unknown_receipt", lambda: list(
            self.led.iter_reward_lines("attr", WEEK[0], "R-1")))
        # the number and the predecessor were never consumed
        r = self.post(first, "R-1")
        self.assertEqual(r["seq"], 1)
        self.assertEqual(len(self.lines("R-1")), 8)

        # same for a correction: the previous receipt stays open
        second = build_week(self.work, "v2", TestHandComputed.V2)
        self.assert_reward_error(
            "write", self.post, second, "R-2", prev="R-1", _fail_after_lines=2)
        self.assertEqual(self.led.reward_week_summary("attr", WEEK[0])
                         ["open_receipt_no"], "R-1")
        r = self.post(second, "R-2", prev="R-1")
        self.assertEqual(r["line_count"], 8)

        # and a failure while reading the delivery consumes nothing either
        self.assert_reward_error(
            "write", self.post, second, "R-3", prev="R-2", _fail_after_lines=-1)
        self.assertEqual(self.led.reward_week_summary("attr", WEEK[0])
                         ["open_receipt_no"], "R-2")

    def test_another_connection_cannot_consume_the_same_predecessor(self):
        first = build_week(self.work, "v1", TestHandComputed.V1)
        second = build_week(self.work, "v2", TestHandComputed.V2)
        self.post(first, "R-1")
        other = self.new_ledger(os.path.join(self.work, "rewards.db"))
        self.post(second, "R-2", prev="R-1", ledger=other)
        self.assert_reward_error("stale_predecessor", self.post, second, "R-3",
                                 prev="R-1")
        self.assertEqual([r["receipt_no"] for r in other.list_reward_receipts(
            "attr", WEEK[0])], ["R-1", "R-2"])


# ----------------------------------------------------------------------
# generated sample: bounded memory + independent cross-check
# ----------------------------------------------------------------------
class TestGeneratedSample(Base):
    def make_week(self, n_orders, seed=7):
        rng = random.Random(seed)
        channels = ["email", "ads", "social", None, "print"]
        layout = {}
        chosen = set()
        for index in range(n_orders):
            day = WEEK[index % 7]
            # the same order id may repeat on a different day; never twice
            # within one day
            order_id = f"o{rng.randrange(max(2, n_orders // 3))}"
            key = (day, order_id)
            if key in chosen:
                continue
            chosen.add(key)
            layout.setdefault(day, []).append(
                (order_id, rng.choice(channels)))
        return build_week(self.work, f"gen{n_orders}", layout), layout

    def test_generated_week_matches_independent_reference(self):
        first, layout1 = self.make_week(300, seed=11)
        # correction: drop some, change some channels, keep most
        rng = random.Random(12)
        changed = {}
        for day, pairs in layout1.items():
            keep = []
            for order_id, channel in pairs:
                roll = rng.random()
                if roll < 0.1:
                    continue
                if roll < 0.3:
                    channel = rng.choice(["email", "ads", "social", None])
                keep.append((order_id, channel))
            changed[day] = keep
        second = build_week(self.work, "gen2", changed)

        pools = {"email": 10007, "ads": 5003, "social": 2999}
        self.post(first, "R-1", pools=pools)
        self.post(second, "R-2", prev="R-1", pools=pools)

        ref1 = ref_entitlements(read_lines(first)[1:], pools, WEEK)
        ref2 = ref_entitlements(read_lines(second)[1:], pools, WEEK)
        self.assertEqual(self.lines("R-1"), ref1)
        self.assertEqual(self.lines("R-2"), ref_delta(ref1, ref2))
        # both receipts land exactly on their pools
        self.assertEqual(sum(self.lines("R-1").values()), sum(pools.values()))
        self.assertEqual(sum(self.lines("R-2").values()), 0)

    def test_one_channel_covering_most_orders_is_not_held_in_memory(self):
        """A single channel with many participants is split in SQLite.

        The test asserts the observable consequence: correct split + remainder
        order, without the caller ever materialising the participant list.
        (Implementation detail: temp tables use ``temp_store=FILE``.)
        """
        n = 2000
        path = build_week(self.work, "single", {
            "2026-10-05": [(f"k{i:05d}", "email") for i in range(n)],
        })
        r = self.post(path, "R-1", pools={"email": 1000})
        self.assertEqual(r["line_count"], 1000)   # 1000 cents: 0 or 1 each
        lines = self.lines("R-1")
        self.assertEqual(sum(lines.values()), 1000)
        winners = [k for k, v in lines.items() if v == 1]
        self.assertEqual(len(winners), 1000)
        self.assertEqual(winners[0], (WEEK[0], "k00000", "email"))
        self.assertEqual(winners[-1], (WEEK[0], "k00999", "email"))


class TestSharedDatabase(Base):
    def test_ledger_can_share_the_planner_database(self):
        """Both features use the same schema ``connect``; one file is enough."""
        path = build_week(self.work, "v1", TestHandComputed.V1)
        db = os.path.join(self.work, "shared.db")
        with RecomputePlanner(db) as p:
            p.compile_graph(ATTR_GRAPH)
        with RewardLedger(db) as led:
            r = led.post_weekly_rewards("attr", WEEK[0], path, sha256(path),
                                        "R-1", None, POOLS)
            self.assertEqual(r["payout"], {"email": 100, "ads": 50, "social": 30})
        # and the planner still works on the same file afterwards
        with RecomputePlanner(db) as p:
            self.assertEqual(p.current_graph()["graph_id"][:2], "G-")


# ----------------------------------------------------------------------
# malformed deliveries never escape as raw exceptions
# ----------------------------------------------------------------------
class TestMalformedInputs(Base):
    def test_unparseable_and_top_level_shapes(self):
        good = build_week(self.work, "v1", TestHandComputed.V1)
        cases = {"not-json": b"{not json}\n",
                 "array": b"[1,2,3]\n",
                 "empty": b""}
        for name, payload in cases.items():
            path = os.path.join(self.work, f"{name}.jsonl")
            with open(path, "wb") as fh:
                fh.write(payload)
            self.assert_reward_error("invalid_delivery", self.post, path,
                                     f"R-{name}")
        self.assertTrue(os.path.exists(good))

    def test_structural_mutations_are_rejected_not_crashed(self):
        """A deterministic set of row/manifest mutations raises RewardError."""
        path = build_week(self.work, "v1", TestHandComputed.V1)
        lines = read_lines(path)

        def write(name, mutate):
            target = os.path.join(self.work, f"mut-{name}.jsonl")
            head, body = mutate(lines[0], lines[1:])
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(canon(head) + "\n")
                for row in body:
                    fh.write(canon(row) + "\n")
            return target

        def drop_channel(row):
            row["channel"] = None
            return row

        def bad_touch(row):
            row["touch_date"] = "2026-01-01"
            return row

        def missing_field(row):
            row.pop("orders_version")
            return row

        mutations = {
            "drop-channel": (lambda h, b: (h, [drop_channel(dict(r)) for r in b]),
                             "invalid_row"),
            "bad-touch": (lambda h, b: (h, [bad_touch(dict(r)) for r in b]),
                          "invalid_row"),
            "missing-field": (lambda h, b: (h, [missing_field(dict(r)) for r in b]),
                              "invalid_row"),
            "manifest-kind": (lambda h, b: ({**h, "kind": "x"}, b),
                              "invalid_delivery"),
            "manifest-range": (lambda h, b: ({**h, "date_range":
                                              {"start": "2026-10-01",
                                               "end": WEEK[-1]}}, b),
                               "invalid_week"),
            "manifest-days-short": (lambda h, b: ({**h, "days": h["days"][:3]}, b),
                                    "invalid_week"),
        }
        for name, (mutate, code) in mutations.items():
            target = write(name, mutate)
            self.assert_reward_error(code, self.post, target, f"R-{name}")

    def test_book_key_rejects_ambiguous_node_names(self):
        self.assert_reward_error(
            "invalid_inputs", self.led.post_weekly_rewards, "a|b", WEEK[0],
            os.path.join(self.work, "x.jsonl"), "0" * 64, "R-1", None, POOLS)

    def test_missing_file_is_a_read_error(self):
        self.assert_reward_error(
            "read", self.post, os.path.join(self.work, "nope.jsonl"), "R-1",
            digest="0" * 64)


if __name__ == "__main__":
    unittest.main()
