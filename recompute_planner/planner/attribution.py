"""Order-attribution materialization: snapshot validation, per-order
attribution and incremental (customer-local) recompute.

Pure row logic lives here (no plan/DB orchestration -- that is api.py).

Data flow (the only one supported): orders are day-partitioned, touches are
day-partitioned; an attribution partition reads same-day orders and touches
from the same day plus the six preceding day partitions.

Attribution rule: an order attributes to the same-customer touch with the
latest timestamp not after the order time; ties on timestamp are broken by
the lexicographically smallest touch id. Orders without a candidate are
kept with empty (NULL) attribution fields.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone

from .errors import SnapshotError

_TOUCH_WINDOW = 7  # same day + six preceding day partitions


def canon_ts(value, day):
    """Validate and canonicalize a UTC timestamp that must fall within ``day``.

    Returns a fixed-width ISO string ('YYYY-MM-DDTHH:MM:SS.ffffffZ') so that
    lexicographic order equals chronological order. Raises SnapshotError.
    """
    if not isinstance(value, str) or not value.strip():
        raise SnapshotError(f"timestamp must be a non-empty string, got {value!r}")
    s = value.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise SnapshotError(f"unparseable timestamp {value!r}")
    if dt.tzinfo is None:
        raise SnapshotError(f"timestamp {value!r} carries no UTC offset")
    if dt.utcoffset() != timedelta(0):
        raise SnapshotError(f"timestamp {value!r} is not UTC")
    dt = dt.astimezone(timezone.utc)
    if dt.date().isoformat() != day:
        raise SnapshotError(f"timestamp {value!r} does not fall within day {day}")
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate_rows(kind, rows, day):
    """Validate one complete snapshot; return normalized rows sorted by id.

    kind: 'orders' (id, customer_id, ts) or 'touches' (+ channel).
    Any invalid row or duplicate id aborts the whole snapshot (nothing is
    stored by the caller). Raises SnapshotError.
    """
    if kind not in ("orders", "touches"):
        raise SnapshotError(f"unknown snapshot kind {kind!r}")
    if not isinstance(rows, (list, tuple)):
        raise SnapshotError("rows must be a list (use [] for an explicitly empty snapshot)")
    required = {"id", "customer_id", "ts"} | ({"channel"} if kind == "touches" else set())
    seen, out = set(), []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            raise SnapshotError(f"row {i}: must be an object, got {r!r}")
        if set(r) != required:
            raise SnapshotError(
                f"row {i}: fields must be exactly {sorted(required)}, got {sorted(r)}")
        rid, cid = r["id"], r["customer_id"]
        if not isinstance(rid, str) or not rid:
            raise SnapshotError(f"row {i}: id must be a non-empty string, got {rid!r}")
        if not isinstance(cid, str) or not cid:
            raise SnapshotError(f"row {i}: customer_id must be a non-empty string, got {cid!r}")
        if rid in seen:
            raise SnapshotError(f"duplicate row id {rid!r} in snapshot")
        seen.add(rid)
        ts = canon_ts(r["ts"], day)
        row = {"id": rid, "customer_id": cid, "ts": ts, "channel": None}
        if kind == "touches":
            ch = r["channel"]
            if not isinstance(ch, str) or not ch:
                raise SnapshotError(f"row {i}: channel must be a non-empty string, got {ch!r}")
            row["channel"] = ch
        out.append(row)
    out.sort(key=lambda r: r["id"])
    return out


def rows_digest(rows):
    """Content digest of a validated snapshot (idempotent re-registration)."""
    canon = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def attribute_customer(order_rows, touch_rows):
    """Attribute one customer's orders against their touches.

    touch_rows carry an extra ``date`` field (their partition day).
    Returns one result row per order.
    """
    out = []
    for o in order_rows:
        best = None
        for t in touch_rows:
            if t["ts"] > o["ts"]:
                continue  # touch did not exist yet when the order happened
            if best is None or t["ts"] > best["ts"] or (
                    t["ts"] == best["ts"] and t["id"] < best["id"]):
                best = t
        out.append({
            "order_id": o["id"],
            "customer_id": o["customer_id"],
            "order_ts": o["ts"],
            "touch_date": best["date"] if best else None,
            "touch_id": best["id"] if best else None,
            "channel": best["channel"] if best else None,
        })
    return out


def attribute_all(order_rows, touch_rows):
    """Full compute: attribute every order (grouped per customer)."""
    touches_by_customer = {}
    for t in touch_rows:
        touches_by_customer.setdefault(t["customer_id"], []).append(t)
    orders_by_customer = {}
    for o in order_rows:
        orders_by_customer.setdefault(o["customer_id"], []).append(o)
    rows = []
    for cust in sorted(orders_by_customer):
        rows += attribute_customer(orders_by_customer[cust],
                                   touches_by_customer.get(cust, ()))
    return rows


def _row_content(row):
    return (row["customer_id"], row["ts"], row["channel"])


def diff_customers(old_rows, new_rows):
    """Customers directly affected by the diff of two snapshot versions.

    Inserts/deletes affect their customer; a content change (including a
    customer change) affects both the old and the new customer.
    """
    old_by_id = {r["id"]: r for r in old_rows}
    new_by_id = {r["id"]: r for r in new_rows}
    affected = set()
    for rid, r in old_by_id.items():
        if rid not in new_by_id:
            affected.add(r["customer_id"])
    for rid, r in new_by_id.items():
        old = old_by_id.get(rid)
        if old is None:
            affected.add(r["customer_id"])
        elif _row_content(old) != _row_content(r):
            affected.add(old["customer_id"])
            affected.add(r["customer_id"])
    return affected


class SnapshotReader:
    """Reads registered snapshot rows and records the read boundary of every
    query (which partition at which version, all customers or which subset) --
    the evidence that an incremental run never scans unrelated customers in
    unchanged partitions."""

    def __init__(self, conn):
        self.conn = conn
        self.reads = []

    def rows(self, node, partition, version, customers=None):
        if customers is None:
            cur = self.conn.execute(
                "SELECT row_id, customer_id, ts, channel FROM snapshot_rows"
                " WHERE node=? AND partition=? AND version=?",
                (node, partition, version))
            self.reads.append({"node": node, "partition": partition, "version": version,
                               "customers": "all"})
        else:
            customers = sorted(customers)
            if not customers:
                return []
            self.reads.append({"node": node, "partition": partition, "version": version,
                               "customers": customers})
            marks = ",".join("?" * len(customers))
            cur = self.conn.execute(
                "SELECT row_id, customer_id, ts, channel FROM snapshot_rows"
                f" WHERE node=? AND partition=? AND version=? AND customer_id IN ({marks})",
                (node, partition, version, *customers))
        return [{"id": r["row_id"], "customer_id": r["customer_id"],
                 "ts": r["ts"], "channel": r["channel"]} for r in cur.fetchall()]
