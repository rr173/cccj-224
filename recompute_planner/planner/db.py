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
"""


def connect(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    return conn
