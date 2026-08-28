"""Deterministic shard assignment independent of a crawler framework."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit


@dataclass(frozen=True, slots=True)
class ShardSpec:
    """The zero-based worker index and total worker count for one crawl."""

    index: int
    count: int

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError("shard count must be at least 1")
        if not 0 <= self.index < self.count:
            raise ValueError("shard index must satisfy 0 <= index < count")

    def owns(self, key: str) -> bool:
        return stable_owner(key, self.count) == self.index

    def legacy_owns_rowid(self, rowid: int) -> bool:
        return legacy_rowid_owner(rowid, self.count) == self.index


def canonical_key(value: str) -> str:
    """Normalize a URL enough for stable assignment while preserving its query."""

    parsed = urlsplit(value.strip())
    if not parsed.scheme or not parsed.netloc:
        return value.strip()
    host = (parsed.hostname or "").lower()
    if parsed.port:
        default = (parsed.scheme.lower() == "http" and parsed.port == 80) or (
            parsed.scheme.lower() == "https" and parsed.port == 443
        )
        authority = host if default else f"{host}:{parsed.port}"
    else:
        authority = host
    return urlunsplit(
        (parsed.scheme.lower(), authority, parsed.path or "/", parsed.query, "")
    )


def stable_owner(key: str, count: int) -> int:
    """Return a stable owner in ``range(count)`` using SHA-256."""

    if count < 1:
        raise ValueError("shard count must be at least 1")
    digest = hashlib.sha256(canonical_key(key).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % count


def legacy_rowid_owner(rowid: int, count: int) -> int:
    """Return the owner used by HexDigest's existing SQLite ``rowid`` mode."""

    if count < 1:
        raise ValueError("shard count must be at least 1")
    if rowid < 0:
        raise ValueError("rowid must be non-negative")
    return rowid % count


def assigned_indices(total: int, spec: ShardSpec):
    """Yield integer positions assigned to a shard in an enumerated list."""

    if total < 0:
        raise ValueError("total must be non-negative")
    yield from range(spec.index, total, spec.count)
