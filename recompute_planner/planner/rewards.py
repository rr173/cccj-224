"""Weekly channel reward posting: an approved attribution delivery JSONL ->
per-order, per-channel differential settlement lines.

This module is separate from graph planning and attribution recomputation.  It
consumes a file produced by ``deliver_attribution_range`` that the business has
approved (identified by its SHA-256), and turns it into immutable, auditable
postings.  It never re-reads live attribution, never re-verifies touch
versions, and never talks to a payment system.

Semantics
---------
* Book key: ``(attribution node, UTC Monday)``.  Only ISO weeks (Monday through
  Sunday) are accepted, and the file must carry exactly one manifest entry for
  every day of that week.  A book's pools are fixed by its first receipt.
* Orders are keyed by ``(order_date, order_id)``; same ids on different days
  are distinct orders.
* Every order with an attribution channel present in the pool participates in
  that channel's split.  The pool is divided equally among participants using
  SQLite integer division; the remaining cents go one each to the participants
  ordered by ``(order_date, order_id)`` ascending.  No participants, no split.
* A correction file describes the **whole** week.  For every
  ``(order, channel)`` the delta is ``current entitlement - previously posted
  entitlement``; the first receipt starts from all zeros.  Orders that vanished
  get a negative clawback.  There is no "did the channel change?" shortcut:
  unchanged attributions can still move cents when the participant set or the
  remainder ordering changes.
* Receipt + settlement lines + the baseline used by later corrections commit in
  one transaction.  A failed write leaves no book, no receipt, no line and
  consumes no receipt number.
* Receipts are immutable.  The exact same receipt (same number, file digest,
  predecessor and pools) returns the original receipt even when newer receipts
  exist; the same number with different content or parameters is rejected.
* Participant lists and deltas are built in SQLite temporary tables (on disk):
  the whole week's orders and lines are never accumulated in Python memory.
"""

import hashlib
import json
import os
import re
import sqlite3
from datetime import date, timedelta

from .dates import is_day
from .db import connect
from .errors import RewardError

MANIFEST_KIND = "attribution-delivery-manifest"
_ROW_FIELDS = {"order_date", "order_id", "attribution_version", "orders_version",
               "touch_date", "touch_id", "channel"}
_MANIFEST_FIELDS = {"kind", "node", "orders_node", "touches_node",
                    "date_range", "days"}
_MANIFEST_DAY_FIELDS = {"date", "attribution_version", "input_versions"}
_HEX_RE = re.compile(r"[0-9a-fA-F]{64}$")

# Temporary tables live in SQLite's temp database with temp_store=FILE, so they
# are backed by disk.  Huge channels are partitioned/updated entirely in SQL.
_TEMP_TABLES = {
    "pool_map": "CREATE TEMP TABLE IF NOT EXISTS pool_map ("
                " channel TEXT PRIMARY KEY, pool INTEGER NOT NULL)",
    "participants": "CREATE TEMP TABLE IF NOT EXISTS participants ("
                    " channel TEXT NOT NULL, order_date TEXT NOT NULL,"
                    " order_id TEXT NOT NULL, amount INTEGER NOT NULL DEFAULT 0,"
                    " PRIMARY KEY (channel, order_date, order_id))",
    "deltas": "CREATE TEMP TABLE IF NOT EXISTS deltas ("
              " channel TEXT NOT NULL, order_date TEXT NOT NULL,"
              " order_id TEXT NOT NULL, amount INTEGER NOT NULL,"
              " PRIMARY KEY (channel, order_date, order_id))",
    "payout": "CREATE TEMP TABLE IF NOT EXISTS payout ("
              " channel TEXT PRIMARY KEY, delta INTEGER NOT NULL,"
              " entitlement INTEGER NOT NULL)",
    "seen": "CREATE TEMP TABLE IF NOT EXISTS seen ("
            " order_date TEXT NOT NULL, order_id TEXT NOT NULL,"
            " PRIMARY KEY (order_date, order_id))",
}


def _canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def hash_file(path):
    """SHA-256 and byte size of ``path`` (the approved-bytes check)."""
    digest = hashlib.sha256()
    size = 0
    try:
        with open(os.fspath(path), "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise RewardError(f"cannot read delivery file: {exc}", code="read") from exc
    return digest.hexdigest(), size


def monday_of(day_key):
    d = date.fromisoformat(day_key)
    return (d - timedelta(days=d.weekday())).isoformat()


def week_days(monday):
    base = date.fromisoformat(monday)
    return [(base + timedelta(days=i)).isoformat() for i in range(7)]


def _parse_pools(pools):
    if not isinstance(pools, dict):
        raise RewardError("pools must be an object {channel: integer cents}",
                          code="invalid_pools")
    out = {}
    for channel, amount in pools.items():
        if not isinstance(channel, str) or not channel:
            raise RewardError(
                f"channel must be a non-empty string, got {channel!r}",
                code="invalid_pools")
        if (not isinstance(amount, int) or isinstance(amount, bool)
                or amount < 0 or amount > 2 ** 63 - 1):
            raise RewardError(
                f"pool for {channel!r} must be a non-negative SQLite integer,"
                f" got {amount!r}", code="invalid_pools")
        out[channel] = amount
    return out


class DeliveryFile:
    """One approved delivery JSONL: validation pass + streaming row pass.

    The first pass validates structure, manifest/week agreement, ordering,
    duplicate orders and per-row attribution/version agreement, while hashing
    the exact bytes.  Only the small manifest is retained; rows are streamed
    again by :meth:`iter_rows`.
    """

    def __init__(self, path, node, week_start):
        self.path = os.fspath(path)
        self.node = node
        self.week_start = week_start
        self.days = week_days(week_start)
        self._day_set = set(self.days)
        self.touch_min = (date.fromisoformat(week_start)
                          - timedelta(days=6)).isoformat()
        self._orders_versions = {}
        self._scan()

    # -- line helpers -------------------------------------------------
    @staticmethod
    def _parse(line, lineno):
        try:
            obj = json.loads(line)
        except (ValueError, RecursionError) as exc:
            raise RewardError(f"line {lineno}: invalid JSON: {exc}",
                              code="invalid_delivery")
        if not isinstance(obj, dict):
            raise RewardError(f"line {lineno}: expected a JSON object",
                              code="invalid_delivery")
        return obj

    def _scan(self):
        # The approved-bytes check wins over structural errors: a file that is
        # not the approved one must be reported as a digest mismatch even if
        # the tampering also made it unparseable.
        self.digest, self.size = hash_file(self.path)
        manifest = None
        rows = 0
        try:
            with open(self.path, "rb") as fh:
                first = True
                for lineno, raw in enumerate(fh, 1):
                    if not raw.strip():
                        raise RewardError(f"line {lineno}: blank line in delivery",
                                          code="invalid_delivery")
                    try:
                        text = raw.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise RewardError(f"line {lineno}: invalid UTF-8: {exc}",
                                          code="invalid_delivery")
                    obj = self._parse(text, lineno)
                    if first:
                        manifest = self._manifest(obj, lineno)
                        self.manifest = manifest
                        self._manifest_days = {entry["date"]: entry
                                               for entry in manifest["days"]}
                        self._orders_versions = dict(
                            self._pinned_orders_versions(manifest))
                        first = False
                        continue
                    self._row(obj, lineno)
                    rows += 1
                if first:
                    raise RewardError("delivery file is empty", code="invalid_delivery")
        except OSError as exc:
            raise RewardError(f"cannot read delivery file: {exc}", code="read") from exc
        self.manifest = manifest
        self.rows = rows

    def _manifest(self, obj, lineno):
        missing = _MANIFEST_FIELDS - set(obj)
        if missing:
            raise RewardError(f"line {lineno}: manifest is missing {sorted(missing)}",
                              code="invalid_delivery")
        if obj["kind"] != MANIFEST_KIND:
            raise RewardError(
                f"line {lineno}: kind {obj['kind']!r} is not an attribution"
                " delivery manifest", code="invalid_delivery")
        if obj["node"] != self.node:
            raise RewardError(
                f"line {lineno}: manifest node {obj['node']!r} does not match the"
                f" bookkeeping node {self.node!r}", code="invalid_delivery")
        span = obj["date_range"]
        if not isinstance(span, dict) or set(span) != {"start", "end"}:
            raise RewardError(f"line {lineno}: malformed date_range",
                              code="invalid_delivery")
        if span["start"] != self.days[0] or span["end"] != self.days[-1]:
            raise RewardError(
                f"line {lineno}: range {span['start']}..{span['end']} is not the"
                f" whole UTC week {self.days[0]}..{self.days[-1]}",
                code="invalid_week")
        days = obj["days"]
        if not isinstance(days, list) or len(days) != 7:
            raise RewardError(
                f"line {lineno}: manifest must list exactly seven days for"
                f" {self.days[0]}..{self.days[-1]}", code="invalid_week")
        parsed, seen = {}, set()
        for entry in days:
            if not isinstance(entry, dict) or not _MANIFEST_DAY_FIELDS.issubset(entry):
                raise RewardError(f"line {lineno}: malformed manifest day entry",
                                  code="invalid_delivery")
            day = entry["date"]
            if day not in self._day_set:
                raise RewardError(
                    f"line {lineno}: manifest day {day!r} is outside the week"
                    f" {self.days[0]}..{self.days[-1]}", code="invalid_week")
            if day in seen:
                raise RewardError(f"line {lineno}: manifest lists day {day} twice",
                                  code="invalid_week")
            if not isinstance(entry["attribution_version"], str) or not \
                    entry["attribution_version"]:
                raise RewardError(
                    f"line {lineno}: bad attribution_version for {day}",
                    code="invalid_delivery")
            if not isinstance(entry["input_versions"], dict):
                raise RewardError(f"line {lineno}: bad input_versions for {day}",
                                  code="invalid_delivery")
            parsed[day] = {"date": day,
                           "attribution_version": entry["attribution_version"],
                           "input_versions": entry["input_versions"]}
            seen.add(day)
        if seen != self._day_set:
            raise RewardError(
                f"line {lineno}: manifest days do not cover every day of"
                f" {self.days[0]}..{self.days[-1]}", code="invalid_week")
        return {
            "node": self.node,
            "orders_node": obj["orders_node"],
            "touches_node": obj["touches_node"],
            "date_range": {"start": self.days[0], "end": self.days[-1]},
            "days": [parsed[day] for day in self.days],
        }

    def _pinned_orders_versions(self, manifest):
        """{day: orders version} pinned by an accepted manifest."""
        out = {}
        for entry in manifest["days"]:
            out[entry["date"]] = self._pinned_orders_version(
                entry["date"], entry["input_versions"],
                manifest["orders_node"], 1)
        return out

    @staticmethod
    def _pinned_orders_version(day, inputs, orders_node, lineno):
        """The one version the manifest pins for the orders partition of ``day``.

        ``orders_node`` is used when the manifest names it; otherwise the
        upstream whose entry is exactly ``{day: version}`` identifies the
        orders role (the same convention the delivery itself uses).
        """
        if isinstance(orders_node, str):
            parts = inputs.get(orders_node)
            if isinstance(parts, dict) and set(parts) == {day}:
                value = parts[day]
                if isinstance(value, str) and value:
                    return value
        found = [parts[day] for parts in inputs.values()
                 if isinstance(parts, dict) and set(parts) == {day}]
        if len(found) != 1 or not isinstance(found[0], str) or not found[0]:
            raise RewardError(
                f"line {lineno}: manifest must pin exactly one orders version"
                f" for {day}", code="invalid_delivery")
        return found[0]

    def _row(self, obj, lineno):
        if set(obj) != _ROW_FIELDS:
            raise RewardError(
                f"line {lineno}: row fields must be exactly {sorted(_ROW_FIELDS)},"
                f" got {sorted(obj)}", code="invalid_row")
        day, oid = obj["order_date"], obj["order_id"]
        if not isinstance(day, str) or day not in self._day_set:
            raise RewardError(
                f"line {lineno}: order_date {day!r} is outside the delivered week"
                f" {self.days[0]}..{self.days[-1]}", code="invalid_row")
        if not isinstance(oid, str) or not oid:
            raise RewardError(f"line {lineno}: order_id must be a non-empty string",
                              code="invalid_row")
        touch = (obj["touch_date"], obj["touch_id"], obj["channel"])
        if any(v is None for v in touch) and not all(v is None for v in touch):
            raise RewardError(
                f"line {lineno}: touch_date/touch_id/channel must be all null or"
                " all set", code="invalid_row")
        if not all(v is None for v in touch):
            if not all(isinstance(v, str) and v for v in touch):
                raise RewardError(
                    f"line {lineno}: attribution fields must be non-empty strings",
                    code="invalid_row")
            if not is_day(obj["touch_date"]):
                raise RewardError(
                    f"line {lineno}: touch_date {obj['touch_date']!r} is not a"
                    " YYYY-MM-DD day", code="invalid_row")
            if not (self.touch_min <= obj["touch_date"] <= self.days[-1]):
                raise RewardError(
                    f"line {lineno}: touch_date {obj['touch_date']!r} is outside"
                    f" the touch window {self.touch_min}..{self.days[-1]} that"
                    " can feed this week", code="invalid_row")
        for field in ("attribution_version", "orders_version"):
            if not isinstance(obj[field], str) or not obj[field]:
                raise RewardError(f"line {lineno}: {field} must be a non-empty string",
                                  code="invalid_row")
        expected = self._manifest_days[day]
        if obj["attribution_version"] != expected["attribution_version"]:
            raise RewardError(
                f"line {lineno}: attribution_version"
                f" {obj['attribution_version']!r} differs from the manifest's"
                f" {expected['attribution_version']!r} for {day}",
                code="version_mismatch")
        if obj["orders_version"] != self._orders_versions[day]:
            raise RewardError(
                f"line {lineno}: orders_version {obj['orders_version']!r} differs"
                f" from the manifest's {self._orders_versions[day]!r} for {day}",
                code="version_mismatch")
        return day, oid, obj["channel"]

    # -- streaming feed ------------------------------------------------
    def iter_rows(self):
        """Yield (order_date, order_id, channel|None) in file order.

        Rows are re-validated against the accepted manifest while streaming, so
        a file swapped between the validation pass and the settlement pass is
        rejected instead of silently posted.
        """
        digest = hashlib.sha256()
        try:
            with open(self.path, "rb") as fh:
                for lineno, raw in enumerate(fh, 1):
                    digest.update(raw)
                    if lineno == 1:
                        continue
                    try:
                        text = raw.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise RewardError(
                            f"line {lineno}: invalid UTF-8: {exc}", code="read")
                    yield self._row(self._parse(text, lineno), lineno)
            if digest.hexdigest() != self.digest:
                raise RewardError(
                    "delivery file changed between validation and settlement;"
                    " the bytes no longer match the approved digest",
                    code="digest_mismatch")
        except OSError as exc:
            raise RewardError(f"cannot re-read delivery file: {exc}",
                              code="read") from exc


# ----------------------------------------------------------------------
# ledger
# ----------------------------------------------------------------------
class RewardLedger:
    """SQLite-backed weekly reward receipt chain (one receipt per book step)."""

    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.conn = connect(self.db_path)
        self.conn.isolation_level = None  # explicit transactions
        self.conn.execute("PRAGMA temp_store = FILE")

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # posting
    # ------------------------------------------------------------------
    def post_weekly_rewards(self, node, week_start, delivery_path,
                            approved_digest, receipt_no, prev_receipt_no,
                            pools, *, _fail_after_lines=None):
        """Post one approved delivery as a differential settlement receipt.

        Parameters
        ----------
        node, week_start
            Book key: materialized attribution node and the UTC Monday.
        delivery_path
            File written by ``deliver_attribution_range`` for the whole week.
        approved_digest
            Hex SHA-256 of the exact bytes the business approved.
        receipt_no, prev_receipt_no
            This receipt's number and the previously posted one (``None`` for
            the first receipt of the book).
        pools
            ``{channel: non-negative integer cents}``; immutable after the
            first receipt of the book.

        Returns the stored receipt metadata (no lines; stream them with
        :meth:`iter_reward_lines`).  The call is transactional: on failure the
        database is unchanged.  ``_fail_after_lines`` is a test seam that
        aborts the line-writing loop after that many rows.
        """
        pools = _parse_pools(pools)
        node = self._check_node(node)
        week_start = self._check_monday(week_start)
        receipt_no = self._check_receipt_no(receipt_no, "receipt_no")
        if prev_receipt_no is not None:
            prev_receipt_no = self._check_receipt_no(prev_receipt_no,
                                                     "prev_receipt_no")
        if not isinstance(approved_digest, str) or not _HEX_RE.match(approved_digest):
            raise RewardError(
                "approved_digest must be the hex SHA-256 approved by the business",
                code="invalid_digest")
        approved_digest = approved_digest.lower()

        actual_digest, _ = hash_file(delivery_path)
        if actual_digest != approved_digest:
            raise RewardError(
                f"delivery digest {actual_digest} does not match the approved"
                f" digest {approved_digest}", code="digest_mismatch")
        delivery = DeliveryFile(delivery_path, node, week_start)

        book_id = f"{node}|{week_start}"
        pools_json = _canon(pools)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            response = self._post(delivery, book_id, node, week_start, receipt_no,
                                  prev_receipt_no, pools, pools_json,
                                  approved_digest, _fail_after_lines)
            self.conn.commit()
        except BaseException:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self._drop_temp()
        return response

    def _post(self, delivery, book_id, node, week_start, receipt_no,
              prev_receipt_no, pools, pools_json, approved_digest,
              fail_after_lines):
        existing = self._receipt_row(book_id, receipt_no)
        if existing is not None:
            if (existing["file_sha256"] != delivery.digest
                    or existing["prev_receipt_no"] != prev_receipt_no
                    or existing["approved_digest"] != approved_digest
                    or existing["pools_json"] != pools_json):
                raise RewardError(
                    f"receipt {receipt_no!r} already exists for book {book_id}"
                    " with different file content, predecessor or pools;"
                    " receipts are immutable", code="receipt_conflict")
            return self._receipt_public(existing)

        book = self.conn.execute(
            "SELECT pools_json FROM reward_books WHERE book_id=?", (book_id,)
        ).fetchone()
        if book is not None and book["pools_json"] != pools_json:
            raise RewardError(
                f"pools for book {book_id} are fixed by its first receipt and"
                " cannot change", code="immutable_pools")
        previous = self._open_receipt(book_id)
        open_no = previous["receipt_no"] if previous is not None else None
        if prev_receipt_no != open_no:
            raise RewardError(
                f"stale predecessor for book {book_id}: the open receipt is"
                f" {open_no!r}, not {prev_receipt_no!r}; a different receipt may"
                " already have consumed it", code="stale_predecessor")

        self._reset_temp()
        payout, entitlement, line_count = self._settle(
            book_id, delivery, pools, fail_after_lines)

        receipt = {
            "book_id": book_id, "receipt_no": receipt_no,
            "seq": 1 if previous is None else previous["seq"] + 1,
            "node": node, "week_start": week_start,
            "prev_receipt_no": prev_receipt_no,
            "file_sha256": delivery.digest, "file_size": delivery.size,
            "approved_digest": approved_digest,
            "manifest_json": _canon(delivery.manifest),
            "pools_json": pools_json,
            "payout_json": _canon({"delta": payout, "entitlement": entitlement}),
            "line_count": line_count,
        }
        self.conn.execute(
            "INSERT INTO reward_receipts (book_id, receipt_no, seq, node,"
            " week_start, prev_receipt_no, file_sha256, file_size,"
            " approved_digest, manifest_json, pools_json, payout_json,"
            " line_count, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))",
            (receipt["book_id"], receipt["receipt_no"], receipt["seq"],
             receipt["node"], receipt["week_start"], receipt["prev_receipt_no"],
             receipt["file_sha256"], receipt["file_size"],
             receipt["approved_digest"], receipt["manifest_json"],
             receipt["pools_json"], receipt["payout_json"],
             receipt["line_count"]))
        if previous is None:
            self.conn.execute(
                "INSERT INTO reward_books (book_id, node, week_start, pools_json,"
                " created_at) VALUES (?,?,?,?, datetime('now'))",
                (book_id, node, week_start, pools_json))

        self._write_lines(book_id, receipt_no, line_count, fail_after_lines)
        self._replace_state(book_id)
        return self._receipt_public(self._receipt_row(book_id, receipt_no))

    # -- settlement (all in SQLite temp tables) ------------------------
    def _settle(self, book_id, delivery, pools, fail_after_lines):
        self.conn.executemany(
            "INSERT INTO temp.pool_map (channel, pool) VALUES (?,?)",
            sorted(pools.items()))

        # 1. every row is recorded once (duplicate orders are rejected);
        #    attributed rows whose channel has a pool become participants.
        seen_batch, part_batch = [], []
        try:
            for day, oid, channel in delivery.iter_rows():
                seen_batch.append((day, oid))
                if len(seen_batch) >= 500:
                    self.conn.executemany(
                        "INSERT INTO temp.seen (order_date, order_id) VALUES (?,?)",
                        seen_batch)
                    seen_batch = []
                if channel is not None and channel in pools:
                    part_batch.append((channel, day, oid))
                    if len(part_batch) >= 500:
                        self.conn.executemany(
                            "INSERT INTO temp.participants"
                            " (channel, order_date, order_id) VALUES (?,?,?)",
                            part_batch)
                        part_batch = []
            if seen_batch:
                self.conn.executemany(
                    "INSERT INTO temp.seen (order_date, order_id) VALUES (?,?)",
                    seen_batch)
            if part_batch:
                self.conn.executemany(
                    "INSERT INTO temp.participants"
                    " (channel, order_date, order_id) VALUES (?,?,?)", part_batch)
        except sqlite3.IntegrityError as exc:
            raise RewardError(
                f"duplicate order in delivery (each (order_date, order_id) may"
                f" appear once): {exc}", code="invalid_row") from exc
        if fail_after_lines == -1:
            raise RewardError("injected failure while reading the delivery",
                              code="write")

        # 2. split every pool; remainder cents follow (order_date, order_id)
        for channel in sorted(pools):
            count = self.conn.execute(
                "SELECT COUNT(*) FROM temp.participants WHERE channel=?",
                (channel,)).fetchone()[0]
            if not count:
                continue
            base, remainder = divmod(pools[channel], count)
            self.conn.execute(
                "UPDATE temp.participants AS p SET amount = ? + (r.rn <= ?)"
                " FROM (SELECT order_date, order_id,"
                "         ROW_NUMBER() OVER (ORDER BY order_date, order_id) AS rn"
                "       FROM temp.participants WHERE channel=?) AS r"
                " WHERE p.channel=? AND p.order_date=r.order_date"
                "   AND p.order_id=r.order_id",
                (base, remainder, channel, channel))
        self.conn.execute("DELETE FROM temp.participants WHERE amount = 0")

        # 3. differential: current entitlement minus what is already posted
        self.conn.execute(
            "INSERT INTO temp.deltas (channel, order_date, order_id, amount)"
            " SELECT p.channel, p.order_date, p.order_id,"
            "        p.amount - COALESCE(s.amount, 0)"
            " FROM temp.participants p"
            " LEFT JOIN reward_state s ON s.book_id=?"
            "   AND s.order_date=p.order_date AND s.order_id=p.order_id"
            "   AND s.channel=p.channel"
            " WHERE p.amount != COALESCE(s.amount, 0)"
            " UNION ALL"
            " SELECT s.channel, s.order_date, s.order_id, -s.amount"
            " FROM reward_state s WHERE s.book_id=?"
            "   AND NOT EXISTS (SELECT 1 FROM temp.participants p"
            "     WHERE p.channel=s.channel AND p.order_date=s.order_date"
            "       AND p.order_id=s.order_id)"
            "   AND s.amount != 0", (book_id, book_id))
        line_count = self.conn.execute(
            "SELECT COUNT(*) FROM temp.deltas").fetchone()[0]

        self.conn.execute(
            "INSERT INTO temp.payout (channel, delta, entitlement)"
            " SELECT m.channel,"
            "        COALESCE((SELECT SUM(d.amount) FROM temp.deltas d"
            "                   WHERE d.channel=m.channel), 0),"
            "        COALESCE((SELECT SUM(p.amount) FROM temp.participants p"
            "                   WHERE p.channel=m.channel), 0)"
            " FROM temp.pool_map m")
        payout = {r[0]: r[1] for r in self.conn.execute(
            "SELECT channel, delta FROM temp.payout")}
        entitlement = {r[0]: r[1] for r in self.conn.execute(
            "SELECT channel, entitlement FROM temp.payout")}
        return payout, entitlement, line_count

    def _write_lines(self, book_id, receipt_no, line_count, fail_after_lines):
        cursor = self.conn.execute(
            "SELECT channel, order_date, order_id, amount FROM temp.deltas"
            " ORDER BY order_date, order_id, channel")
        written = 0
        while True:
            chunk = cursor.fetchmany(500)
            if not chunk:
                break
            if fail_after_lines is not None and written + len(chunk) > fail_after_lines:
                take = max(0, fail_after_lines - written)
                self.conn.executemany(
                    "INSERT INTO reward_lines (book_id, receipt_no, order_date,"
                    " order_id, channel, amount) VALUES (?,?,?,?,?,?)",
                    ((book_id, receipt_no, d, o, c, a)
                     for c, d, o, a in chunk[:take]))
                raise RewardError(
                    f"injected failure after {written + take} settlement lines",
                    code="write")
            self.conn.executemany(
                "INSERT INTO reward_lines (book_id, receipt_no, order_date,"
                " order_id, channel, amount) VALUES (?,?,?,?,?,?)",
                ((book_id, receipt_no, d, o, c, a) for c, d, o, a in chunk))
            written += len(chunk)
        if written != line_count:
            raise RewardError(
                f"settlement line count mismatch: computed {line_count}, wrote"
                f" {written}", code="state")

    def _replace_state(self, book_id):
        """The baseline for the next correction is this receipt's full
        entitlement (deltas were computed against the previous one)."""
        self.conn.execute("DELETE FROM reward_state WHERE book_id=?", (book_id,))
        self.conn.execute(
            "INSERT INTO reward_state (book_id, order_date, order_id, channel, amount)"
            " SELECT ?, order_date, order_id, channel, amount"
            " FROM temp.participants", (book_id,))

    # -- temp table lifecycle -----------------------------------------
    def _reset_temp(self):
        for name, ddl in _TEMP_TABLES.items():
            self.conn.execute(ddl)
            self.conn.execute(f"DELETE FROM temp.{name}")

    def _drop_temp(self):
        for name in _TEMP_TABLES:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS temp.{name}")
            except sqlite3.Error:
                pass

    # ------------------------------------------------------------------
    # reading
    # ------------------------------------------------------------------
    def read_reward_receipt(self, node, week_start, receipt_no, limit=500,
                            offset=0):
        """One receipt's metadata plus a page of its signed settlement lines."""
        book_id = self._book_id(node, week_start)
        row = self._receipt_row(book_id, self._check_receipt_no(
            receipt_no, "receipt_no"))
        if row is None:
            raise RewardError(
                f"no receipt {receipt_no!r} for book {book_id}",
                code="unknown_receipt")
        out = self._receipt_public(row)
        out["offset"] = offset
        out["limit"] = limit
        out["lines"] = [dict(r) for r in self.conn.execute(
            "SELECT order_date, order_id, channel, amount FROM reward_lines"
            " WHERE book_id=? AND receipt_no=?"
            " ORDER BY order_date, order_id, channel LIMIT ? OFFSET ?",
            (book_id, receipt_no, limit, offset))]
        return out

    def iter_reward_lines(self, node, week_start, receipt_no):
        """Stream every signed line of one receipt without materializing it."""
        book_id = self._book_id(node, week_start)
        if self._receipt_row(book_id, receipt_no) is None:
            raise RewardError(
                f"no receipt {receipt_no!r} for book {book_id}",
                code="unknown_receipt")
        cur = self.conn.execute(
            "SELECT order_date, order_id, channel, amount FROM reward_lines"
            " WHERE book_id=? AND receipt_no=?"
            " ORDER BY order_date, order_id, channel", (book_id, receipt_no))
        for r in cur:
            yield {"order_date": r["order_date"], "order_id": r["order_id"],
                   "channel": r["channel"], "amount": r["amount"]}

    def list_reward_receipts(self, node, week_start):
        """The immutable receipt chain of one book, oldest first."""
        book_id = self._book_id(node, week_start)
        return [self._receipt_public(r) for r in self.conn.execute(
            "SELECT * FROM reward_receipts WHERE book_id=? ORDER BY seq",
            (book_id,))]

    def reward_week_summary(self, node, week_start):
        """Current pools, entitlement totals and open receipt of one book."""
        book_id = self._book_id(node, week_start)
        book = self.conn.execute(
            "SELECT pools_json FROM reward_books WHERE book_id=?", (book_id,)
        ).fetchone()
        if book is None:
            return {"book_id": book_id, "pools": None, "entitlement": None,
                    "open_receipt_no": None, "receipts": 0}
        pools = json.loads(book["pools_json"])
        entitlement = self.conn.execute(
            "SELECT channel, SUM(amount) FROM reward_state WHERE book_id=?"
            " GROUP BY channel", (book_id,)).fetchall()
        totals = {channel: 0 for channel in pools}
        for channel, amount in entitlement:
            totals[channel] = amount
        open_row = self._open_receipt(book_id)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM reward_receipts WHERE book_id=?",
            (book_id,)).fetchone()[0]
        return {"book_id": book_id, "pools": pools, "entitlement": totals,
                "open_receipt_no": open_row["receipt_no"] if open_row else None,
                "receipts": count}

    # -- helpers -------------------------------------------------------
    def _book_id(self, node, week_start):
        node = self._check_node(node)
        return f"{node}|{self._check_monday(week_start)}"

    @staticmethod
    def _check_node(node):
        if not isinstance(node, str) or not node:
            raise RewardError("node must be a non-empty string",
                              code="invalid_inputs")
        if "|" in node:
            raise RewardError("node must not contain '|' (book key separator)",
                              code="invalid_inputs")
        return node

    @staticmethod
    def _check_monday(week_start):
        if not isinstance(week_start, str):
            raise RewardError("week_start must be a YYYY-MM-DD UTC Monday",
                              code="invalid_week")
        try:
            if monday_of(week_start) != week_start:
                raise ValueError
        except (TypeError, ValueError):
            raise RewardError(
                f"week_start {week_start!r} is not a UTC Monday", code="invalid_week")
        return week_start

    @staticmethod
    def _check_receipt_no(value, name):
        if not isinstance(value, str) or not value.strip():
            raise RewardError(f"{name} must be a non-empty string",
                              code="invalid_inputs")
        return value

    def _receipt_row(self, book_id, receipt_no):
        return self.conn.execute(
            "SELECT * FROM reward_receipts WHERE book_id=? AND receipt_no=?",
            (book_id, receipt_no)).fetchone()

    def _open_receipt(self, book_id):
        return self.conn.execute(
            "SELECT * FROM reward_receipts WHERE book_id=? ORDER BY seq DESC"
            " LIMIT 1", (book_id,)).fetchone()

    @staticmethod
    def _receipt_public(row):
        payout = json.loads(row["payout_json"])
        return {
            "book_id": row["book_id"],
            "node": row["node"],
            "week_start": row["week_start"],
            "receipt_no": row["receipt_no"],
            "seq": row["seq"],
            "prev_receipt_no": row["prev_receipt_no"],
            "file_sha256": row["file_sha256"],
            "file_size": row["file_size"],
            "approved_digest": row["approved_digest"],
            "pools": json.loads(row["pools_json"]),
            "payout": payout["delta"],
            "entitlement": payout["entitlement"],
            "line_count": row["line_count"],
            "created_at": row["created_at"],
            "manifest": json.loads(row["manifest_json"]),
        }
