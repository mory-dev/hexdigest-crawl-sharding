from __future__ import annotations

import sys
from pathlib import Path

import pytest

from hexdigest_crawl_sharding import (
    ShardSpec,
    SQLiteWorkStore,
    assigned_indices,
    canonical_key,
    launch_workers,
    legacy_rowid_owner,
    stable_owner,
)


def test_shard_spec_validates_index_and_count():
    assert ShardSpec(0, 1).owns("key")
    with pytest.raises(ValueError):
        ShardSpec(2, 2)
    with pytest.raises(ValueError):
        ShardSpec(0, 0)


def test_canonical_url_is_stable_and_drops_fragment():
    assert canonical_key(" HTTPS://Example.COM:443/products/1#reviews ") == (
        "https://example.com/products/1"
    )
    assert canonical_key("https://example.com/products/1?variant=2#reviews") == (
        "https://example.com/products/1?variant=2"
    )


def test_stable_assignment_is_exhaustive_and_non_overlapping():
    keys = [f"https://example.com/products/{i}" for i in range(200)]
    owners = [[key for key in keys if ShardSpec(index, 4).owns(key)] for index in range(4)]
    assert sorted(key for group in owners for key in group) == sorted(keys)
    assert sum(len(group) for group in owners) == len(keys)
    assert [stable_owner(key, 4) for key in keys] == [stable_owner(key, 4) for key in keys]


def test_legacy_rowid_mode_matches_existing_contract():
    assert [legacy_rowid_owner(rowid, 3) for rowid in range(7)] == [0, 1, 2, 0, 1, 2, 0]
    assert list(assigned_indices(7, ShardSpec(1, 3))) == [1, 4]


def test_store_assigns_only_this_shard_and_tracks_leases(tmp_path: Path):
    keys = [f"key-{i}" for i in range(50)]
    store = SQLiteWorkStore(tmp_path / "crawl.sqlite3", ShardSpec(0, 2))
    other = SQLiteWorkStore(tmp_path / "crawl.sqlite3", ShardSpec(1, 2))
    assert store.enqueue(keys, now=100) == 50
    assert store.enqueue(keys, now=100) == 0
    mine = store.claim(limit=100, now=100, lease_seconds=10)
    theirs = other.claim(limit=100, now=100, lease_seconds=10)
    assert set(mine).isdisjoint(theirs)
    assert set(mine + theirs) == set(keys)
    assert store.claim(limit=100, now=105) == []
    for key in mine[:2]:
        store.complete(key, now=110)
    assert store.counts()["done"] == 2
    assert store.claim(limit=100, now=109) == []
    assert set(store.claim(limit=100, now=2000)) == set(mine[2:])


def test_expired_lease_can_be_reclaimed(tmp_path: Path):
    key = next(key for key in (f"key-{i}" for i in range(20)) if stable_owner(key, 1) == 0)
    store = SQLiteWorkStore(tmp_path / "crawl.sqlite3", ShardSpec(0, 1))
    store.enqueue([key], now=100)
    assert store.claim(now=100, lease_seconds=10) == [key]
    assert store.claim(now=109) == []
    assert store.claim(now=111) == [key]


def test_failed_work_is_retryable(tmp_path: Path):
    store = SQLiteWorkStore(tmp_path / "crawl.sqlite3", ShardSpec(0, 1))
    store.enqueue(["key"], now=100)
    assert store.claim(now=100) == ["key"]
    store.fail("key", "temporary failure", now=110)
    assert store.claim(now=110) == ["key"]


def test_launcher_sets_worker_environment_and_returns_failures(tmp_path: Path, monkeypatch):
    output = tmp_path / "workers.txt"
    command = [
        sys.executable,
        "-c",
        f"from pathlib import Path; Path(r'{output}').open('a').write("
        + "f'{__import__(\"os\").environ[\"CRAWL_SHARD\"]}/'"
        + " f'{__import__(\"os\").environ[\"CRAWL_SHARDS\"]}\\n')",
    ]
    monkeypatch.setenv("PROXY_URL_0", "http://secret.example")
    assert launch_workers(command, 2, log_dir=tmp_path / "logs") == 0
    assert sorted(output.read_text(encoding="utf-8").splitlines()) == ["0/2", "1/2"]


def test_launcher_dry_run_does_not_start_process(capsys):
    assert launch_workers(["echo", "{shard}", "{shards}"], 2, dry_run=True) == 0
    assert "shard 0: echo 0 2" in capsys.readouterr().out
