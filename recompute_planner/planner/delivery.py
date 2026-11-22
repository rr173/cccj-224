"""Consistent cross-day delivery of materialized attribution results.

A delivery covers an inclusive UTC day range and one materialized attribution
node.  For every order day in the range exactly one already-published,
actually-materialized attribution version is selected.  Two selected days may
not disagree about the version of a touch-day partition they both read; the
touch-day version participates even when no touch in that partition wins an
order.

Selection is a chain constraint problem.  Day ``d`` reads touch days
``[d-6 .. d]``; consecutive order days overlap on six touch days, and pair-wise
agreement between consecutive days propagates to every pair of days (any two
windows are linked through the run of days between them), so only adjacent
days need to be compared.  The chosen tuple, read latest order day first, is
maximised lexicographically on the attribution version (numeric ``vN`` order,
then version string); differing per-day order versions are only a tie-break.

Nothing here writes to the database or recomputes attribution.
"""

import json
import os
import re
import tempfile
from datetime import date, timedelta
from functools import cmp_to_key

from .errors import DeliveryError

TOUCH_WINDOW = 7  # same day + six preceding day partitions
_VERSION_RE = re.compile(r"v[0-9]+$")


# ----------------------------------------------------------------------
# date helpers
# ----------------------------------------------------------------------
def day_range(start, end):
    """Inclusive list of UTC day keys; DeliveryError on bad/reversed range."""
    try:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
    except (TypeError, ValueError):
        raise DeliveryError(
            f"date range must be two YYYY-MM-DD keys, got {start!r}..{end!r}",
            code="invalid_range")
    if first > last:
        raise DeliveryError(
            f"date range {start}..{end} is empty (from must be <= to)",
            code="invalid_range")
    out, cur = [], first
    while cur <= last:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def touch_window(day):
    """Touch-day partitions read by an order day, ascending (7 days)."""
    base = date.fromisoformat(day)
    return [(base - timedelta(days=i)).isoformat() for i in range(TOUCH_WINDOW - 1, -1, -1)]


def version_key(version):
    return (int(version[1:]), version)


# ----------------------------------------------------------------------
# candidates and selection
# ----------------------------------------------------------------------
class Candidate:
    """One usable attribution version of one order day.

    ``version`` is the attribution version, ``orders_version`` the exact order
    partition version and ``touches`` the exact version of each of the seven
    touch days the run recorded reading.
    """

    __slots__ = ("day", "version", "orders_version", "touches")

    def __init__(self, day, version, orders_version, touches):
        self.day = day
        self.version = version
        self.orders_version = orders_version
        self.touches = dict(touches)


def _compatible(later, earlier):
    """Consecutive order days agree on every touch day both of them read."""
    for day, version in later.touches.items():
        other = earlier.touches.get(day)
        if other is not None and other != version:
            return False
    return True


def _pref_cmp(a, b):
    """Deterministic per-day order: newest attribution version first, then
    newest order version, then the touch versions as a final tie-break."""
    ka, kb = version_key(a.version), version_key(b.version)
    if ka != kb:
        return -1 if ka > kb else 1
    oa, ob = version_key(a.orders_version), version_key(b.orders_version)
    if oa != ob:
        return -1 if oa > ob else 1
    ta = json.dumps(sorted(a.touches.items()), separators=(",", ":"))
    tb = json.dumps(sorted(b.touches.items()), separators=(",", ":"))
    if ta != tb:
        return -1 if ta > tb else 1
    return 0


def choose_versions(days, candidates_by_day):
    """Pick one candidate per day (ascending ``days``) maximising the tuple of
    attribution versions read latest order day first.

    Dynamic programme over the day chain: for every candidate it keeps the best
    version tuple of a consistent prefix ending at that candidate.  Cost is
    quadratic in the per-day candidate count per day, never the Cartesian
    product of all days.  Raises DeliveryError (code ``no_candidate`` or
    ``no_solution``).

    Only adjacent days are compared: any two order days in the range are linked
    by the run of days between them, so pairwise agreement propagates.
    """
    ordered = []
    for day in days:
        cands = sorted(candidates_by_day.get(day, ()), key=cmp_to_key(_pref_cmp))
        if not cands:
            raise DeliveryError(
                f"{day}: no usable attribution version (needs a published version"
                " with a materialized run whose recorded inputs pin orders/"
                f"{day} and touches/{touch_window(day)[0]}..{day})",
                code="no_candidate")
        ordered.append(cands)

    # key(c) = (attribution versions, order versions, touch tie-break), each
    # read latest order day first -- a deterministic lexicographic order
    def key_of(cand, prev_key, index):
        touches = tuple(sorted(cand.touches.items()))
        return ((version_key(cand.version),) + (prev_key[0] if prev_key else ()),
                (version_key(cand.orders_version),) + (prev_key[1] if prev_key else ()),
                (touches, -index) + (prev_key[2] if prev_key else ()))

    best, parent = {}, {}
    for index, cand in enumerate(ordered[0]):
        best[id(cand)] = key_of(cand, None, index)
        parent[id(cand)] = None
    for index in range(1, len(ordered)):
        for cand in ordered[index]:
            options = [prev for prev in ordered[index - 1]
                       if best[id(prev)] is not None and _compatible(cand, prev)]
            if not options:
                best[id(cand)] = None
                continue
            prev = max(options, key=lambda p: best[id(p)])
            best[id(cand)] = key_of(cand, best[id(prev)], index)
            parent[id(cand)] = prev

    last = [c for c in ordered[-1] if best[id(c)] is not None]
    if not last:
        raise DeliveryError(
            "every day has candidates but they cannot be combined into one"
            " consistent range: shared touch-day versions differ between"
            f" {days[0]}..{days[-1]}",
            code="no_solution")

    chosen = []
    cur = max(last, key=lambda c: best[id(c)])
    while cur is not None:
        chosen.append(cur)
        cur = parent[id(cur)]
    chosen.reverse()
    return chosen


# ----------------------------------------------------------------------
# streaming reads and atomic target
# ----------------------------------------------------------------------
def iter_result_rows(conn, node, day, version):
    """Yield per-order rows of one materialized version, streamed in order.

    Kept as a module function so a caller can observe an in-flight delivery;
    it never materialises the whole result set in memory.
    """
    cur = conn.execute(
        "SELECT order_id, customer_id, touch_date, touch_id, channel"
        " FROM attribution_results"
        " WHERE node=? AND partition=? AND version=? ORDER BY order_id",
        (node, day, version))
    for r in cur:
        yield {"order_id": r["order_id"], "customer_id": r["customer_id"],
               "touch_date": r["touch_date"], "touch_id": r["touch_id"],
               "channel": r["channel"]}


class AtomicTarget:
    """Write next to the target and replace it only on a complete file.

    A failure leaves neither a half-written target nor a temporary file, and
    the previous target (if any) is untouched until the replacement.
    """

    def __init__(self, target):
        self.target = os.fspath(target)
        directory = os.path.dirname(os.path.abspath(self.target))
        fd, self.tmp = tempfile.mkstemp(
            dir=directory, prefix=".delivery-", suffix=".tmp")
        self._file = os.fdopen(fd, "w", encoding="utf-8", newline="\n")

    def write(self, line):
        self._file.write(line)
        self._file.write("\n")

    def commit(self):
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            os.replace(self.tmp, self.target)
        except BaseException:
            self.abort()
            raise
        self.tmp = None

    def abort(self):
        try:
            self._file.close()
        except Exception:
            pass
        if self.tmp and os.path.exists(self.tmp):
            try:
                os.unlink(self.tmp)
            except OSError:
                pass
        self.tmp = None


def canon_line(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
