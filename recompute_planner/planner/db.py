"""SQLite persistence: schema and connection helper."""

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS graphs (
    graph_id   TEXT PRIMARY KEY,
    def_json   TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    source_node   TEXT NOT NULL,
    business_date TEXT NOT NULL,
    arrival_seq   INTEGER NOT NULL,
    digest        TEXT NOT NULL,
    status        TEXT NOT NULL,          -- pending | superseded | consumed
    consumed_by   TEXT,
    received_at   TEXT NOT NULL,
    PRIMARY KEY (event_id, revision)
);

CREATE TABLE IF NOT EXISTS published (
    node          TEXT NOT NULL,
    partition     TEXT NOT NULL,
    version       TEXT NOT NULL,
    evidence_json TEXT,                   -- NULL => invalid state, plans are refused
    source        TEXT NOT NULL,          -- 'bootstrap' or plan id
    created_at    TEXT NOT NULL,
    PRIMARY KEY (node, partition, version)
);

CREATE TABLE IF NOT EXISTS plans (
    plan_id         TEXT PRIMARY KEY,
    graph_id        TEXT NOT NULL,
    event_refs_json TEXT NOT NULL,        -- bound source-event set
    debt_refs_json  TEXT NOT NULL,        -- bound carry-over dirty debt
    snapshot_json   TEXT NOT NULL,        -- bound published-state snapshot
    status          TEXT NOT NULL,        -- active | completed | completed_with_failures | abandoned
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_items (
    plan_id             TEXT NOT NULL,
    node                TEXT NOT NULL,
    partition           TEXT NOT NULL,
    publish_order       INTEGER NOT NULL,
    new_version         TEXT NOT NULL,
    reason_json         TEXT NOT NULL,
    input_versions_json TEXT NOT NULL,
    waits_on_json       TEXT NOT NULL,
    status              TEXT NOT NULL,    -- pending | running | success | failed | blocked | abandoned
    PRIMARY KEY (plan_id, node, partition),
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dirty_debt (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    node             TEXT NOT NULL,
    partition        TEXT NOT NULL,
    reason           TEXT NOT NULL,
    status           TEXT NOT NULL,       -- pending | consumed
    released_by_plan TEXT,
    consumed_by      TEXT,
    created_at       TEXT NOT NULL
);

-- Complete row snapshots registered per upstream node/day/version.
-- Registration is atomic; re-registering the same version with identical
-- content is a no-op, with different content is rejected.
CREATE TABLE IF NOT EXISTS node_snapshots (
    node          TEXT NOT NULL,
    partition     TEXT NOT NULL,          -- day key
    version       TEXT NOT NULL,          -- 'vN'
    kind          TEXT NOT NULL,          -- orders | touches
    digest        TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY (node, partition, version)
);

CREATE TABLE IF NOT EXISTS snapshot_rows (
    node        TEXT NOT NULL,
    partition   TEXT NOT NULL,
    version     TEXT NOT NULL,
    row_id      TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    ts          TEXT NOT NULL,            -- canonical UTC, fixed width
    channel     TEXT,                     -- touches only
    PRIMARY KEY (node, partition, version, row_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_rows_customer
    ON snapshot_rows (node, partition, version, customer_id);

-- One row per materialized attribution version: which input versions were
-- actually read, which baseline (if any) was reused, and the run report.
CREATE TABLE IF NOT EXISTS attribution_runs (
    node                TEXT NOT NULL,
    partition           TEXT NOT NULL,
    version             TEXT NOT NULL,
    plan_id             TEXT NOT NULL,
    mode                TEXT NOT NULL,    -- full | incremental
    baseline_version    TEXT,
    input_versions_json TEXT NOT NULL,
    report_json         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    PRIMARY KEY (node, partition, version)
);

-- Per-order attribution results, immutable per version.
CREATE TABLE IF NOT EXISTS attribution_results (
    node        TEXT NOT NULL,
    partition   TEXT NOT NULL,
    version     TEXT NOT NULL,
    order_id    TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    order_ts    TEXT NOT NULL,
    touch_date  TEXT,                     -- NULL when unattributed
    touch_id    TEXT,
    channel     TEXT,
    PRIMARY KEY (node, partition, version, order_id)
);
"""


def connect(db_path):
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL lets a delivery keep a stable read snapshot while another publisher
    # commits a new version concurrently.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    return conn
