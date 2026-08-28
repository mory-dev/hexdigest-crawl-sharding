"""A small lease-aware SQLite work store for sharded crawlers."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path

from .sharding import ShardSpec, stable_owner

SCHEMA = """
CREATE TABLE IF NOT EXISTS crawl_work (
    work_key TEXT PRIMARY KEY,
    shard INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'done', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    lease_until REAL,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_crawl_work_claim
    ON crawl_work (shard, state, updated_at);
"""


class SQLiteWorkStore:
    """Persist work items and claim one fixed shard at a time.

    Work is assigned when enqueued using a stable SHA-256 key hash. A worker
    can be restarted safely: expired leases return to the claimable set, and
    completed items are never claimed again.
    """

    def __init__(self, path: str | Path, spec: ShardSpec):
        self.path = Path(path)
        self.spec = spec
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def enqueue(self, keys: Iterable[str], *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else now
        rows = [
            (key, stable_owner(key, self.spec.count), "pending", 0, timestamp)
            for key in dict.fromkeys(str(key) for key in keys)
        ]
        with self._connect() as conn:
            before = conn.total_changes
            conn.executemany(
                "INSERT OR IGNORE INTO crawl_work "
                "(work_key, shard, state, attempts, updated_at) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            return conn.total_changes - before

    def claim(
        self,
        limit: int = 100,
        *,
        lease_seconds: float = 1800,
        now: float | None = None,
    ) -> list[str]:
        """Atomically claim up to ``limit`` uncompleted items for this shard."""

        if limit < 1:
            return []
        timestamp = time.time() if now is None else now
        lease_until = timestamp + max(1.0, lease_seconds)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT work_key FROM crawl_work "
                "WHERE shard = ? AND ("
                "state = 'pending' OR "
                "(state = 'running' AND lease_until IS NOT NULL AND lease_until <= ?) OR "
                "(state = 'failed' AND updated_at <= ?)"
                ") ORDER BY updated_at, work_key LIMIT ?",
                (self.spec.index, timestamp, timestamp, limit),
            ).fetchall()
            keys = [row["work_key"] for row in rows]
            if keys:
                marks = ",".join("?" for _ in keys)
                conn.execute(
                    f"UPDATE crawl_work SET state = 'running', attempts = attempts + 1, "
                    f"updated_at = ?, lease_until = ?, last_error = NULL "
                    f"WHERE work_key IN ({marks})",
                    (timestamp, lease_until, *keys),
                )
            conn.commit()
        return keys

    def complete(self, key: str, *, now: float | None = None) -> None:
        timestamp = time.time() if now is None else now
        with self._connect() as conn:
            conn.execute(
                "UPDATE crawl_work SET state = 'done', updated_at = ?, lease_until = NULL "
                "WHERE work_key = ? AND shard = ?",
                (timestamp, key, self.spec.index),
            )

    def fail(self, key: str, error: str, *, now: float | None = None) -> None:
        timestamp = time.time() if now is None else now
        with self._connect() as conn:
            conn.execute(
                "UPDATE crawl_work SET state = 'failed', updated_at = ?, lease_until = NULL, "
                "last_error = ? WHERE work_key = ? AND shard = ?",
                (timestamp, error[:2000], key, self.spec.index),
            )

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS n FROM crawl_work WHERE shard = ? GROUP BY state",
                (self.spec.index,),
            ).fetchall()
        counts = {state: 0 for state in ("pending", "running", "done", "failed")}
        counts.update({row["state"]: row["n"] for row in rows})
        return counts
