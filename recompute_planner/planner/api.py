"""Facade API: event ingestion, dependency compilation, impact pruning,
plan query and state advancement, persisted in a small SQLite database.

Semantics
---------
* Late events are deduplicated by (event_id, digest); a changed digest forms
  a new revision which supersedes the previous pending one.
* A plan is computed only from *pending* events and *pending* dirty debt
  (carry-overs from failed/blocked items) -- never from full history.
* plan_id binds graph version + source event set + published-state snapshot.
* Published versions are immutable and append-only; a plan never overwrites
  old versions. If a prerequisite fails, dependents become blocked and stay
  unpublished instead of silently reusing the old version.
* Only one active plan at a time; state is bounded by retention pruning.
"""

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone

from .attribution import (SnapshotReader, attribute_all, attribute_customer,
                          diff_customers, rows_digest, validate_rows)
from .dates import gran_of, is_day
from .db import connect
from .errors import EventError, PlanError, SnapshotError, StateError
from .graph import attribution_roles, normalize, upstream_roles
from .impact import build_plan_items, compute_impact

_VERSION_RE = re.compile(r"v[0-9]+$")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _evidence_hash(evidence_json):
    return hashlib.sha1(evidence_json.encode("utf-8")).hexdigest()[:10]


def _event_reason(event_id, revision, arrival_seq, digest, node, date):
    return (f"late event {event_id} rev{revision} "
            f"(arrival {arrival_seq}, digest {digest[:8]}) on {node}/{date}")


class RecomputePlanner:
    """Late-event driven minimal recompute planner backed by SQLite."""

    def __init__(self, db_path, max_events=1000, max_plans=50):
        self.db_path = str(db_path)
        self.max_events = int(max_events)
        self.max_plans = int(max_plans)
        self.conn = connect(self.db_path)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # dependency compilation
    # ------------------------------------------------------------------
    def compile_graph(self, graph_def):
        """Validate and store a graph version; make it current. Raises GraphError."""
        norm = normalize(graph_def)
        gid = "G-" + _hash(_canon(norm))[:12]
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO graphs (graph_id, def_json, created_at) VALUES (?,?,?)",
                (gid, _canon(norm), _now()))
            self.conn.execute(
                "INSERT INTO meta (key, value) VALUES ('current_graph', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (gid,))
        return gid

    def current_graph(self):
        gid, gdef = self._current_graph()
        return {"graph_id": gid, "def": gdef}

    def _current_graph(self):
        row = self.conn.execute("SELECT value FROM meta WHERE key='current_graph'").fetchone()
        if not row:
            raise StateError("no dependency graph compiled yet")
        grow = self.conn.execute(
            "SELECT def_json FROM graphs WHERE graph_id=?", (row["value"],)).fetchone()
        return row["value"], json.loads(grow["def_json"])

    def _plan_graph(self, plan_row):
        """Graph definition the plan is bound to (not necessarily current)."""
        grow = self.conn.execute(
            "SELECT def_json FROM graphs WHERE graph_id=?",
            (plan_row["graph_id"],)).fetchone()
        return json.loads(grow["def_json"])

    # ------------------------------------------------------------------
    # event ingestion
    # ------------------------------------------------------------------
    def ingest_event(self, event_id, source_node, business_date, arrival_seq, digest):
        """Receive a late event. Idempotent on (event_id, digest); a changed
        digest forms a new revision that supersedes the pending previous one."""
        gid, gdef = self._current_graph()
        node = gdef["nodes"].get(source_node)
        if node is None:
            raise EventError(f"unknown source node {source_node!r} in graph {gid}")
        if node["output"] != "day":
            raise EventError(
                f"node {source_node!r} is {node['output']}-partitioned; "
                "late events require a day-partitioned source node")
        if not is_day(business_date):
            raise EventError(f"invalid business_date {business_date!r}; expected YYYY-MM-DD")
        if not isinstance(arrival_seq, int) or isinstance(arrival_seq, bool) or arrival_seq < 0:
            raise EventError("arrival_seq must be a non-negative integer")
        if not digest:
            raise EventError("content digest is required")
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id=? ORDER BY revision DESC LIMIT 1",
            (event_id,)).fetchone()
        if row and row["digest"] == digest:
            return {"result": "duplicate", "event_id": event_id, "revision": row["revision"]}
        rev = 1 if row is None else row["revision"] + 1
        with self.conn:
            if row and row["status"] == "pending":
                self.conn.execute(
                    "UPDATE events SET status='superseded' WHERE event_id=? AND revision=?",
                    (event_id, row["revision"]))
            self.conn.execute(
                "INSERT INTO events (event_id, revision, source_node, business_date,"
                " arrival_seq, digest, status, consumed_by, received_at)"
                " VALUES (?,?,?,?,?,?, 'pending', NULL, ?)",
                (event_id, rev, source_node, business_date, arrival_seq, digest, _now()))
        return {"result": "accepted" if rev == 1 else "revision",
                "event_id": event_id, "revision": rev}

    # ------------------------------------------------------------------
    # published state (immutable versions)
    # ------------------------------------------------------------------
    def seed_published(self, node, partition, version="v1", evidence=None):
        """Bootstrap a published partition. Evidence is mandatory; rows are immutable."""
        _, gdef = self._current_graph()
        spec = gdef["nodes"].get(node)
        if spec is None:
            raise StateError(f"unknown node {node!r}")
        if evidence is None:
            raise StateError("evidence is required for published state")
        if not _VERSION_RE.match(version):
            raise StateError(f"version must look like 'vN', got {version!r}")
        if gran_of(partition) != spec["output"]:
            raise StateError(
                f"partition {partition!r} does not match node {node!r} output {spec['output']!r}")
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO published (node, partition, version, evidence_json, source, created_at)"
                    " VALUES (?,?,?,?, 'bootstrap', ?)",
                    (node, partition, version, _canon(evidence), _now()))
        except sqlite3.IntegrityError:
            raise StateError(
                f"published {node}/{partition}@{version} already exists; versions are immutable")

    def current_version(self, node, partition):
        entry = self._published_state().get((node, partition))
        return entry[1]["version"] if entry else None

    def version_history(self, node, partition):
        rows = self.conn.execute(
            "SELECT * FROM published WHERE node=? AND partition=?"
            " ORDER BY CAST(SUBSTR(version, 2) AS INTEGER)", (node, partition)).fetchall()
        return [{"node": r["node"], "partition": r["partition"], "version": r["version"],
                 "evidence": json.loads(r["evidence_json"]) if r["evidence_json"] else None,
                 "source": r["source"], "created_at": r["created_at"]} for r in rows]

    def _published_state(self):
        """{(node, partition): (version_int, row)} of current (max) versions."""
        best = {}
        for r in self.conn.execute("SELECT * FROM published").fetchall():
            key = (r["node"], r["partition"])
            n = int(r["version"][1:])
            if key not in best or n > best[key][0]:
                best[key] = (n, r)
        return best

    def _validate_published_evidence(self):
        rows = self.conn.execute(
            "SELECT node, partition, version FROM published"
            " WHERE evidence_json IS NULL LIMIT 5").fetchall()
        if rows:
            bad = ", ".join(f"{r['node']}/{r['partition']}@{r['version']}" for r in rows)
            raise StateError(f"published state missing evidence: {bad}")

    # ------------------------------------------------------------------
    # impact pruning (no writes)
    # ------------------------------------------------------------------
    def preview_impact(self):
        """Compute the dirty set from pending events/debt without creating a plan."""
        gid, gdef = self._current_graph()
        seeds, ev_rows, debt_rows = self._pending_seeds(gdef)
        dirty = compute_impact(gdef, seeds) if seeds else {}
        return {
            "graph_id": gid,
            "pending_events": len(ev_rows),
            "pending_debt": len(debt_rows),
            "dirty": {f"{n}/{p}": sorted(rs) for (n, p), rs in sorted(dirty.items())},
        }

    def _pending_seeds(self, gdef):
        seeds = {}
        ev_rows = self.conn.execute(
            "SELECT * FROM events WHERE status='pending' ORDER BY event_id, revision").fetchall()
        for e in ev_rows:
            if e["source_node"] not in gdef["nodes"]:
                raise StateError(
                    f"pending event {e['event_id']} references node {e['source_node']!r}"
                    " absent from current graph")
            key = (e["source_node"], e["business_date"])
            seeds.setdefault(key, []).append(_event_reason(
                e["event_id"], e["revision"], e["arrival_seq"], e["digest"],
                e["source_node"], e["business_date"]))
        debt_rows = self.conn.execute(
            "SELECT * FROM dirty_debt WHERE status='pending' ORDER BY id").fetchall()
        for d in debt_rows:
            if d["node"] not in gdef["nodes"]:
                raise StateError(
                    f"pending debt references node {d['node']!r} absent from current graph")
            seeds.setdefault((d["node"], d["partition"]), []).append(d["reason"])
        return seeds, ev_rows, debt_rows

    # ------------------------------------------------------------------
    # plan creation
    # ------------------------------------------------------------------
    def create_plan(self):
        """Compile a minimal recompute plan from pending events + dirty debt.

        Returns the plan id, or None when nothing is pending. Raises PlanError
        if another plan is still active, StateError on invalid published state.
        """
        gid, gdef = self._current_graph()
        active = self.conn.execute(
            "SELECT plan_id FROM plans WHERE status='active'").fetchone()
        if active:
            raise PlanError(
                f"plan {active['plan_id']} still active; finish or close it before replanning")
        self._validate_published_evidence()
        seeds, ev_rows, debt_rows = self._pending_seeds(gdef)
        if not seeds:
            return None
        state = self._published_state()
        versions = {f"{n}|{p}": r["version"] for (n, p), (_, r) in state.items()}
        snapshot = {f"{n}|{p}": {"v": r["version"], "e": _evidence_hash(r["evidence_json"])}
                    for (n, p), (_, r) in sorted(state.items())}
        items = build_plan_items(gdef, seeds, versions)
        event_refs = [{"event_id": e["event_id"], "revision": e["revision"],
                       "node": e["source_node"], "date": e["business_date"],
                       "digest": e["digest"], "arrival_seq": e["arrival_seq"]}
                      for e in ev_rows]
        debt_refs = [{"node": d["node"], "partition": d["partition"], "reason": d["reason"],
                      "released_by_plan": d["released_by_plan"]} for d in debt_rows]
        pid = "P-" + _hash(gid + _canon(event_refs) + _canon(debt_refs) + _canon(snapshot))[:12]
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO plans (plan_id, graph_id, event_refs_json, debt_refs_json,"
                    " snapshot_json, status, created_at) VALUES (?,?,?,?,?,?,?)",
                    (pid, gid, _canon(event_refs), _canon(debt_refs), _canon(snapshot),
                     "active", _now()))
                for it in items:
                    self.conn.execute(
                        "INSERT INTO plan_items (plan_id, node, partition, publish_order,"
                        " new_version, reason_json, input_versions_json, waits_on_json, status)"
                        " VALUES (?,?,?,?,?,?,?,?, 'pending')",
                        (pid, it["node"], it["partition"], it["publish_order"], it["new_version"],
                         _canon(it["reasons"]), _canon(it["input_versions"]),
                         _canon(it["waits_on"])))
                for e in ev_rows:
                    self.conn.execute(
                        "UPDATE events SET status='consumed', consumed_by=?"
                        " WHERE event_id=? AND revision=?",
                        (pid, e["event_id"], e["revision"]))
                for d in debt_rows:
                    self.conn.execute(
                        "UPDATE dirty_debt SET status='consumed', consumed_by=? WHERE id=?",
                        (pid, d["id"]))
        except sqlite3.IntegrityError:
            return pid  # identical inputs produced an identical plan already
        return pid

    # ------------------------------------------------------------------
    # plan query
    # ------------------------------------------------------------------
    def _require_plan(self, plan_id):
        row = self.conn.execute("SELECT * FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise PlanError(f"unknown plan {plan_id}")
        return row

    def get_plan(self, plan_id):
        row = self._require_plan(plan_id)
        counts = {r["status"]: r["c"] for r in self.conn.execute(
            "SELECT status, COUNT(*) c FROM plan_items WHERE plan_id=? GROUP BY status",
            (plan_id,))}
        return {
            "plan_id": row["plan_id"], "graph_id": row["graph_id"],
            "status": row["status"], "created_at": row["created_at"],
            "event_refs": json.loads(row["event_refs_json"]),
            "debt_refs": json.loads(row["debt_refs_json"]),
            "published_snapshot": json.loads(row["snapshot_json"]),
            "item_counts": counts, "item_total": sum(counts.values()),
        }

    def list_plan_items(self, plan_id, node=None, limit=50, offset=0):
        """Paginated plan items, ordered by expected publish order."""
        self._require_plan(plan_id)
        where = "plan_id=?" + (" AND node=?" if node else "")
        args = [plan_id] + ([node] if node else [])
        total = self.conn.execute(
            f"SELECT COUNT(*) c FROM plan_items WHERE {where}", args).fetchone()["c"]
        rows = self.conn.execute(
            f"SELECT * FROM plan_items WHERE {where}"
            " ORDER BY publish_order LIMIT ? OFFSET ?", args + [limit, offset]).fetchall()
        return {"plan_id": plan_id, "total": total, "limit": limit, "offset": offset,
                "items": [self._item_dict(r) for r in rows]}

    def get_skippable(self, plan_id, node=None, limit=50, offset=0):
        """Published partitions the plan safely skips (pruned by declared ranges)."""
        self._require_plan(plan_id)
        rows = self.conn.execute(
            "SELECT node, partition FROM plan_items WHERE plan_id=?", (plan_id,)).fetchall()
        item_keys = {(r["node"], r["partition"]) for r in rows}
        plan_nodes = {r["node"] for r in rows}
        skip = []
        for (n, pk), (_, r) in sorted(self._published_state().items()):
            if (n, pk) in item_keys or (node and n != node):
                continue
            if n in plan_nodes:
                reason = (f"node {n} is impacted but partition {pk} has no dirty input"
                          " within its declared ranges")
            else:
                reason = (f"node {n} unaffected by this plan's events"
                          " (pruned via declared dependencies)")
            skip.append({"node": n, "partition": pk, "version": r["version"], "reason": reason})
        return {"plan_id": plan_id, "total": len(skip), "limit": limit, "offset": offset,
                "items": skip[offset:offset + limit]}

    @staticmethod
    def _item_dict(r):
        return {"node": r["node"], "partition": r["partition"],
                "publish_order": r["publish_order"], "new_version": r["new_version"],
                "reasons": json.loads(r["reason_json"]),
                "input_versions": json.loads(r["input_versions_json"]),
                "waits_on": json.loads(r["waits_on_json"]),
                "status": r["status"]}

    # ------------------------------------------------------------------
    # plan replay (deterministic re-derivation from bound inputs)
    # ------------------------------------------------------------------
    def replay_plan(self, plan_id):
        """Recompute the plan from its bound inputs and compare with what is stored."""
        row = self._require_plan(plan_id)
        grow = self.conn.execute(
            "SELECT def_json FROM graphs WHERE graph_id=?", (row["graph_id"],)).fetchone()
        gdef = json.loads(grow["def_json"])
        seeds = {}
        for e in json.loads(row["event_refs_json"]):
            seeds.setdefault((e["node"], e["date"]), []).append(_event_reason(
                e["event_id"], e["revision"], e["arrival_seq"], e["digest"],
                e["node"], e["date"]))
        for d in json.loads(row["debt_refs_json"]):
            seeds.setdefault((d["node"], d["partition"]), []).append(d["reason"])
        snapshot = json.loads(row["snapshot_json"])
        versions = {k: v["v"] for k, v in snapshot.items()}
        expected = build_plan_items(gdef, seeds, versions)
        stored = [self._item_dict(r) for r in self.conn.execute(
            "SELECT * FROM plan_items WHERE plan_id=? ORDER BY publish_order",
            (plan_id,)).fetchall()]
        diffs = []
        if len(expected) != len(stored):
            diffs.append(f"item count: recomputed {len(expected)} != stored {len(stored)}")
        for exp, got in zip(expected, stored):
            for field in ("node", "partition", "publish_order", "new_version",
                          "reasons", "input_versions", "waits_on"):
                if exp[field] != got[field]:
                    diffs.append(f"item {got['node']}/{got['partition']} field {field}:"
                                 f" recomputed {exp[field]!r} != stored {got[field]!r}")
        return {"plan_id": plan_id, "graph_id": row["graph_id"],
                "match": not diffs, "diffs": diffs, "item_count": len(stored)}

    # ------------------------------------------------------------------
    # state advancement
    # ------------------------------------------------------------------
    def advance(self, plan_id, node, partition, outcome):
        """Advance one item: outcome in {'running', 'success', 'failed'}.

        On success a new immutable version is published with the item's
        evidence. On failure all transitive dependents become blocked and stay
        unpublished; their dirty debt is released for the next plan.
        """
        if outcome not in ("running", "success", "failed"):
            raise PlanError(f"outcome must be running|success|failed, got {outcome!r}")
        plan = self._require_plan(plan_id)
        if plan["status"] != "active":
            raise PlanError(f"plan {plan_id} is {plan['status']}; cannot advance items")
        item = self.conn.execute(
            "SELECT * FROM plan_items WHERE plan_id=? AND node=? AND partition=?",
            (plan_id, node, partition)).fetchone()
        if item is None:
            raise PlanError(f"no item {node}/{partition} in plan {plan_id}")
        if outcome == "failed":
            if item["status"] != "pending" and item["status"] != "running":
                raise PlanError(f"cannot fail item in status {item['status']}")
            with self.conn:
                self.conn.execute(
                    "UPDATE plan_items SET status='failed'"
                    " WHERE plan_id=? AND node=? AND partition=?",
                    (plan_id, node, partition))
                blocked = self._block_dependents(plan_id, [(node, partition)])
            self._finalize_if_done(plan_id)
            return {"status": "failed", "blocked": [f"{n}/{p}" for n, p in blocked]}
        # running / success
        if item["status"] not in ("pending", "running"):
            raise PlanError(f"cannot mark {outcome} on item in status {item['status']}")
        if outcome == "success":
            spec = self._plan_graph(plan)["nodes"].get(node)
            if spec is not None and spec.get("materialize") == "attribution":
                raise PlanError(
                    f"node {node!r} is attribution-materialized; use "
                    "run_attribution_item() so results, input versions and the "
                    "success publication commit atomically")
        waits = json.loads(item["waits_on_json"])
        prereq = self._prereq_statuses(plan_id, waits)
        bad = [w for w, s in prereq.items() if s in ("failed", "blocked", "abandoned")]
        if bad:
            with self.conn:
                self.conn.execute(
                    "UPDATE plan_items SET status='blocked'"
                    " WHERE plan_id=? AND node=? AND partition=?",
                    (plan_id, node, partition))
                blocked = self._block_dependents(plan_id, [(node, partition)])
            self._finalize_if_done(plan_id)
            return {"status": "blocked", "reason": f"prerequisite failed/blocked: {bad}",
                    "blocked": [f"{n}/{p}" for n, p in blocked]}
        incomplete = [w for w, s in prereq.items() if s != "success"]
        if incomplete:
            raise PlanError(f"prerequisites not complete for {node}/{partition}: {incomplete}")
        if outcome == "running":
            with self.conn:
                self.conn.execute(
                    "UPDATE plan_items SET status='running'"
                    " WHERE plan_id=? AND node=? AND partition=?",
                    (plan_id, node, partition))
            return {"status": "running"}
        evidence = {"inputs": json.loads(item["input_versions_json"]),
                    "reasons": json.loads(item["reason_json"]),
                    "plan_id": plan_id}
        with self.conn:
            self.conn.execute(
                "INSERT INTO published (node, partition, version, evidence_json, source, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (node, partition, item["new_version"], _canon(evidence), plan_id, _now()))
            self.conn.execute(
                "UPDATE plan_items SET status='success'"
                " WHERE plan_id=? AND node=? AND partition=?",
                (plan_id, node, partition))
        self._finalize_if_done(plan_id)
        return {"status": "success", "version": item["new_version"]}

    def _prereq_statuses(self, plan_id, waits):
        prereq = {}
        for w in waits:
            wn, wp = w.rsplit("/", 1)
            r = self.conn.execute(
                "SELECT status FROM plan_items WHERE plan_id=? AND node=? AND partition=?",
                (plan_id, wn, wp)).fetchone()
            prereq[w] = r["status"]
        return prereq

    def close_plan(self, plan_id):
        """Abandon an active plan; unfinished items become dirty debt."""
        plan = self._require_plan(plan_id)
        if plan["status"] != "active":
            raise PlanError(f"plan {plan_id} is {plan['status']}; cannot close")
        with self.conn:
            self.conn.execute(
                "UPDATE plan_items SET status='abandoned'"
                " WHERE plan_id=? AND status IN ('pending','running')", (plan_id,))
            self.conn.execute(
                "UPDATE plans SET status='abandoned' WHERE plan_id=?", (plan_id,))
            self._release_debt(plan_id)
        self._prune()
        return {"plan_id": plan_id, "status": "abandoned"}

    def _block_dependents(self, plan_id, start_keys):
        rows = self.conn.execute(
            "SELECT node, partition, waits_on_json, status FROM plan_items WHERE plan_id=?",
            (plan_id,)).fetchall()
        dependents, status = {}, {}
        for r in rows:
            key = (r["node"], r["partition"])
            status[key] = r["status"]
            for w in json.loads(r["waits_on_json"]):
                dependents.setdefault(tuple(w.rsplit("/", 1)), []).append(key)
        blocked, queue = [], list(start_keys)
        while queue:
            x = queue.pop()
            for dep in dependents.get(x, ()):
                if status[dep] in ("pending", "running"):
                    status[dep] = "blocked"
                    blocked.append(dep)
                    queue.append(dep)
        for n, p in blocked:
            self.conn.execute(
                "UPDATE plan_items SET status='blocked'"
                " WHERE plan_id=? AND node=? AND partition=?", (plan_id, n, p))
        return blocked

    def _finalize_if_done(self, plan_id):
        counts = {r["status"]: r["c"] for r in self.conn.execute(
            "SELECT status, COUNT(*) c FROM plan_items WHERE plan_id=? GROUP BY status",
            (plan_id,))}
        if counts.get("pending") or counts.get("running"):
            return False
        ok = set(counts) <= {"success"}
        with self.conn:
            self.conn.execute(
                "UPDATE plans SET status=? WHERE plan_id=?",
                ("completed" if ok else "completed_with_failures", plan_id))
            if not ok:
                self._release_debt(plan_id)
        self._prune()
        return True

    def _release_debt(self, plan_id):
        rows = self.conn.execute(
            "SELECT node, partition, status FROM plan_items"
            " WHERE plan_id=? AND status IN ('failed','blocked','abandoned')",
            (plan_id,)).fetchall()
        for r in rows:
            self.conn.execute(
                "INSERT INTO dirty_debt (node, partition, reason, status,"
                " released_by_plan, consumed_by, created_at)"
                " VALUES (?,?,?, 'pending', ?, NULL, ?)",
                (r["node"], r["partition"],
                 f"carry-over from plan {plan_id}: item {r['node']}/{r['partition']}"
                 f" ended {r['status']}", plan_id, _now()))

    # ------------------------------------------------------------------
    # attribution snapshots (upstream row registration)
    # ------------------------------------------------------------------
    def register_snapshot(self, node, partition, version, rows):
        """Register the complete row set of node/partition@version.

        The version must already be published. Rows are validated and stored
        atomically: invalid rows or duplicate ids leave nothing behind.
        Re-registering identical content is a no-op; different content for the
        same version is rejected (snapshots are immutable). An empty snapshot
        must be registered explicitly with rows=[].
        """
        _, gdef = self._current_graph()
        kind = upstream_roles(gdef).get(node)
        if kind is None:
            raise SnapshotError(
                f"node {node!r} is not an orders/touches upstream of any "
                "attribution node in the current graph")
        if not is_day(partition):
            raise SnapshotError(f"invalid partition {partition!r}; expected YYYY-MM-DD")
        if not _VERSION_RE.match(version):
            raise SnapshotError(f"version must look like 'vN', got {version!r}")
        pub = self.conn.execute(
            "SELECT 1 FROM published WHERE node=? AND partition=? AND version=?",
            (node, partition, version)).fetchone()
        if pub is None:
            raise SnapshotError(
                f"{node}/{partition}@{version} is not published; register snapshots"
                " only after the upstream version is published")
        norm_rows = validate_rows(kind, rows, partition)
        digest = rows_digest(norm_rows)
        existing = self.conn.execute(
            "SELECT digest, row_count FROM node_snapshots"
            " WHERE node=? AND partition=? AND version=?",
            (node, partition, version)).fetchone()
        if existing is not None:
            if existing["digest"] == digest:
                return {"result": "duplicate", "node": node, "partition": partition,
                        "version": version, "rows": existing["row_count"]}
            raise SnapshotError(
                f"{node}/{partition}@{version} already registered with different "
                "content; snapshots are immutable")
        with self.conn:
            self.conn.execute(
                "INSERT INTO node_snapshots (node, partition, version, kind, digest,"
                " row_count, registered_at) VALUES (?,?,?,?,?,?,?)",
                (node, partition, version, kind, digest, len(norm_rows), _now()))
            self.conn.executemany(
                "INSERT INTO snapshot_rows (node, partition, version, row_id,"
                " customer_id, ts, channel) VALUES (?,?,?,?,?,?,?)",
                [(node, partition, version, r["id"], r["customer_id"], r["ts"],
                  r["channel"]) for r in norm_rows])
        return {"result": "registered", "node": node, "partition": partition,
                "version": version, "kind": kind, "rows": len(norm_rows)}

    def _snapshot_registered(self, node, partition, version):
        return self.conn.execute(
            "SELECT 1 FROM node_snapshots WHERE node=? AND partition=? AND version=?",
            (node, partition, version)).fetchone() is not None

    # ------------------------------------------------------------------
    # attribution materialization
    # ------------------------------------------------------------------
    @staticmethod
    def _read_touches(reader, touches_node, day_versions, customers=None):
        touches = []
        for d, v in sorted(day_versions.items()):
            for r in reader.rows(touches_node, d, v, customers=customers):
                touches.append({**r, "date": d})
        return touches

    def run_attribution_item(self, plan_id, node, partition):
        """Materialize one attribution plan item and publish it atomically.

        Reads exactly the upstream versions pinned by the plan item (never the
        runtime-latest), computes per-order results for the item's new
        version, and commits results + used input versions + the item's
        success publication in a single transaction. With a materialized
        baseline (the attribution version locked at plan creation) only
        customers directly affected by changed upstream partitions are
        recomputed; every other row is copied from the baseline.
        """
        plan = self._require_plan(plan_id)
        if plan["status"] != "active":
            raise PlanError(f"plan {plan_id} is {plan['status']}; cannot run items")
        gdef = self._plan_graph(plan)
        spec = gdef["nodes"].get(node)
        if spec is None or spec.get("materialize") != "attribution":
            raise PlanError(
                f"node {node!r} is not an attribution-materialized node in plan {plan_id}")
        item = self.conn.execute(
            "SELECT * FROM plan_items WHERE plan_id=? AND node=? AND partition=?",
            (plan_id, node, partition)).fetchone()
        if item is None:
            raise PlanError(f"no item {node}/{partition} in plan {plan_id}")
        if item["status"] == "success":
            raise PlanError(
                f"item {node}/{partition} already succeeded; re-publishing is "
                "refused while the plan is retained")
        if item["status"] not in ("pending", "running"):
            raise PlanError(f"cannot run item in status {item['status']}")
        waits = json.loads(item["waits_on_json"])
        prereq = self._prereq_statuses(plan_id, waits)
        bad = [w for w, s in prereq.items() if s in ("failed", "blocked", "abandoned")]
        if bad:
            with self.conn:
                self.conn.execute(
                    "UPDATE plan_items SET status='blocked'"
                    " WHERE plan_id=? AND node=? AND partition=?",
                    (plan_id, node, partition))
                blocked = self._block_dependents(plan_id, [(node, partition)])
            self._finalize_if_done(plan_id)
            return {"status": "blocked", "reason": f"prerequisite failed/blocked: {bad}",
                    "blocked": [f"{n}/{p}" for n, p in blocked]}
        incomplete = [w for w, s in prereq.items() if s != "success"]
        if incomplete:
            raise PlanError(
                f"prerequisites not complete for {node}/{partition}: {incomplete}")

        orders_node, touches_node = attribution_roles(spec)
        input_versions = json.loads(item["input_versions_json"])
        pinned = ([(orders_node, d, v)
                   for d, v in sorted(input_versions[orders_node].items())]
                  + [(touches_node, d, v)
                     for d, v in sorted(input_versions[touches_node].items())])
        missing = []
        for n, d, v in pinned:
            if v is None:
                missing.append(f"{n}/{d} (unpublished)")
            elif not self._snapshot_registered(n, d, v):
                missing.append(f"{n}/{d}@{v}")
        if missing:
            raise PlanError(
                "attribution inputs not registered: " + ", ".join(missing)
                + "; register the complete snapshot of every input partition "
                "(empty ones explicitly) before running the item")

        # baseline = the attribution version locked when the plan was created,
        # but only if its results were actually materialized
        snapshot = json.loads(plan["snapshot_json"])
        base_ver = (snapshot.get(f"{node}|{partition}") or {}).get("v")
        base_run = None
        if base_ver is not None:
            base_run = self.conn.execute(
                "SELECT * FROM attribution_runs"
                " WHERE node=? AND partition=? AND version=?",
                (node, partition, base_ver)).fetchone()

        reader = SnapshotReader(self.conn)
        orders_ver = input_versions[orders_node][partition]
        if base_run is None:
            mode = "full"
            orders = reader.rows(orders_node, partition, orders_ver)
            touches = self._read_touches(reader, touches_node,
                                         input_versions[touches_node])
            rows = attribute_all(orders, touches)
            changed, affected, copied = None, None, 0
        else:
            mode = "incremental"
            base_inputs = json.loads(base_run["input_versions_json"])
            changed = []
            for n, d, v in pinned:
                bv = (base_inputs.get(n) or {}).get(d)
                if bv != v:
                    changed.append({"node": n, "partition": d, "from": bv, "to": v})
            affected = set()
            for c in changed:
                old_rows = reader.rows(c["node"], c["partition"], c["from"])
                new_rows = reader.rows(c["node"], c["partition"], c["to"])
                affected |= diff_customers(old_rows, new_rows)
            base_rows = self.conn.execute(
                "SELECT * FROM attribution_results"
                " WHERE node=? AND partition=? AND version=?",
                (node, partition, base_ver)).fetchall()
            kept = [{"order_id": r["order_id"], "customer_id": r["customer_id"],
                     "order_ts": r["order_ts"], "touch_date": r["touch_date"],
                     "touch_id": r["touch_id"], "channel": r["channel"]}
                    for r in base_rows if r["customer_id"] not in affected]
            recomputed = []
            for cust in sorted(affected):
                c_orders = reader.rows(orders_node, partition, orders_ver,
                                       customers={cust})
                c_touches = self._read_touches(reader, touches_node,
                                               input_versions[touches_node],
                                               customers={cust})
                recomputed += attribute_customer(c_orders, c_touches)
            rows = kept + recomputed
            copied = len(kept)
        rows.sort(key=lambda r: r["order_id"])

        new_ver = item["new_version"]
        report = {
            "plan_id": plan_id, "node": node, "partition": partition,
            "version": new_ver, "mode": mode,
            "baseline_version": base_ver if base_run else None,
            "input_versions": input_versions,
            "changed_inputs": changed,
            "affected_customers": sorted(affected) if affected is not None else None,
            "rows_total": len(rows), "rows_recomputed": len(rows) - copied,
            "rows_copied": copied, "reads": reader.reads,
        }
        evidence = {"inputs": input_versions, "reasons": json.loads(item["reason_json"]),
                    "plan_id": plan_id, "materialized": mode}
        with self.conn:
            self.conn.executemany(
                "INSERT INTO attribution_results (node, partition, version, order_id,"
                " customer_id, order_ts, touch_date, touch_id, channel)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                [(node, partition, new_ver, r["order_id"], r["customer_id"],
                  r["order_ts"], r["touch_date"], r["touch_id"], r["channel"])
                 for r in rows])
            self.conn.execute(
                "INSERT INTO attribution_runs (node, partition, version, plan_id, mode,"
                " baseline_version, input_versions_json, report_json, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (node, partition, new_ver, plan_id, mode,
                 base_ver if base_run else None, _canon(input_versions),
                 _canon(report), _now()))
            self.conn.execute(
                "INSERT INTO published (node, partition, version, evidence_json, source,"
                " created_at) VALUES (?,?,?,?,?,?)",
                (node, partition, new_ver, _canon(evidence), plan_id, _now()))
            self.conn.execute(
                "UPDATE plan_items SET status='success'"
                " WHERE plan_id=? AND node=? AND partition=?",
                (plan_id, node, partition))
        self._finalize_if_done(plan_id)
        return report

    def read_attribution(self, node, partition, version):
        """Per-order results of one materialized attribution version.

        Raises StateError when that version was never materialized; an empty
        list means the version was materialized with no orders.
        """
        run = self.conn.execute(
            "SELECT 1 FROM attribution_runs WHERE node=? AND partition=? AND version=?",
            (node, partition, version)).fetchone()
        if run is None:
            raise StateError(
                f"no materialized attribution results for {node}/{partition}@{version}")
        rows = self.conn.execute(
            "SELECT * FROM attribution_results WHERE node=? AND partition=? AND version=?"
            " ORDER BY order_id", (node, partition, version)).fetchall()
        return [{"order_id": r["order_id"], "customer_id": r["customer_id"],
                 "order_ts": r["order_ts"], "touch_date": r["touch_date"],
                 "touch_id": r["touch_id"], "channel": r["channel"]} for r in rows]

    def attribution_run_info(self, node, partition, version):
        """Materialization metadata: mode, baseline, input versions actually
        read, and the run report (changed inputs, affected customers, reads)."""
        r = self.conn.execute(
            "SELECT * FROM attribution_runs WHERE node=? AND partition=? AND version=?",
            (node, partition, version)).fetchone()
        if r is None:
            raise StateError(
                f"no materialized attribution run for {node}/{partition}@{version}")
        return {"node": r["node"], "partition": r["partition"], "version": r["version"],
                "plan_id": r["plan_id"], "mode": r["mode"],
                "baseline_version": r["baseline_version"],
                "input_versions": json.loads(r["input_versions_json"]),
                "report": json.loads(r["report_json"]), "created_at": r["created_at"]}

    # ------------------------------------------------------------------
    # bounded retention
    # ------------------------------------------------------------------
    def _prune(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM events WHERE status='consumed' AND rowid NOT IN"
                " (SELECT rowid FROM events WHERE status='consumed'"
                "  ORDER BY received_at DESC, event_id DESC LIMIT ?)", (self.max_events,))
            self.conn.execute(
                "DELETE FROM dirty_debt WHERE status='consumed' AND id NOT IN"
                " (SELECT id FROM dirty_debt WHERE status='consumed'"
                "  ORDER BY id DESC LIMIT ?)", (self.max_events,))
            self.conn.execute(
                "DELETE FROM plans WHERE status != 'active' AND plan_id NOT IN"
                " (SELECT plan_id FROM plans WHERE status != 'active'"
                "  ORDER BY created_at DESC, plan_id DESC LIMIT ?)", (self.max_plans,))

    def prune(self):
        self._prune()

    def stats(self):
        def c(sql):
            return self.conn.execute(sql).fetchone()[0]
        return {
            "events_pending": c("SELECT COUNT(*) FROM events WHERE status='pending'"),
            "events_total": c("SELECT COUNT(*) FROM events"),
            "debt_pending": c("SELECT COUNT(*) FROM dirty_debt WHERE status='pending'"),
            "plans_total": c("SELECT COUNT(*) FROM plans"),
            "plans_active": c("SELECT COUNT(*) FROM plans WHERE status='active'"),
            "published_rows": c("SELECT COUNT(*) FROM published"),
            "snapshots_registered": c("SELECT COUNT(*) FROM node_snapshots"),
            "attribution_runs": c("SELECT COUNT(*) FROM attribution_runs"),
            "attribution_rows": c("SELECT COUNT(*) FROM attribution_results"),
        }
