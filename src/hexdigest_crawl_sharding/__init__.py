"""Stable work sharding and safe multi-worker launches for crawlers."""

from .runner import launch_workers
from .sharding import (
    ShardSpec,
    assigned_indices,
    canonical_key,
    legacy_rowid_owner,
    stable_owner,
)
from .store import SQLiteWorkStore

__all__ = [
    "SQLiteWorkStore",
    "ShardSpec",
    "assigned_indices",
    "canonical_key",
    "launch_workers",
    "legacy_rowid_owner",
    "stable_owner",
]
__version__ = "0.1.0"
