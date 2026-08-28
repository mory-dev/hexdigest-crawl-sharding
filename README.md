# HexDigest Crawl Sharding

[![CI](https://github.com/mory-dev/hexdigest-crawl-sharding/actions/workflows/ci.yml/badge.svg)](https://github.com/mory-dev/hexdigest-crawl-sharding/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%2B-3776ab)](https://www.python.org/)
[![License](https://img.shields.io/github/license/mory-dev/hexdigest-crawl-sharding)](LICENSE)

Stable work sharding, lease-aware SQLite state, and safe multi-worker launches
for web crawlers. Split a large URL or product crawl across parallel workers,
resume interrupted work, and give each worker its own proxy without putting
shell interpolation or credentials in the launcher.

This is the reusable crawl-coordination layer extracted from the production
infrastructure behind [HexDigest](https://hexdigest.com), a marketplace
intelligence service built from public listings.

## The model

```text
work key → stable SHA-256 owner → one worker → lease → done or retry
                              └─ optional PROXY_URL_<shard>
```

Every key belongs to exactly one shard for a fixed shard count. Changing the
count intentionally creates a new assignment plan; keep the same count while
resuming a crawl.

## Install

```bash
pip install git+https://github.com/mory-dev/hexdigest-crawl-sharding.git
```

## Stable shard assignment

```python
from hexdigest_crawl_sharding import ShardSpec

spec = ShardSpec(index=2, count=4)
if spec.owns("https://example.com/products/123"):
    fetch_product("https://example.com/products/123")
```

URLs are canonicalized by scheme, host, path, and query before hashing. A
fragment is not part of the work key. Use a stable product ID instead when the
same listing can have multiple URLs.

For an existing SQLite deployment that already uses `rowid % N`, the package
also exposes `ShardSpec.legacy_owns_rowid()` and `legacy_rowid_owner()`. Treat
that mode as a migration aid: row IDs are local to one database and are not a
portable public work identity.

## Lease-aware SQLite work store

```python
from hexdigest_crawl_sharding import SQLiteWorkStore, ShardSpec

store = SQLiteWorkStore("crawl.sqlite3", ShardSpec(index=0, count=4))
store.enqueue(product_urls)

for key in store.claim(limit=100, lease_seconds=1800):
    try:
        fetch_product(key)
    except Exception as exc:
        store.fail(key, str(exc))
    else:
        store.complete(key)
```

Claims are atomic through SQLite’s write lock. A crashed worker’s lease becomes
claimable after it expires. Failed items are retryable, completed items are not
claimed again, and `counts()` provides a small operational summary.

## Launch N crawler workers

The launcher takes an argument list, never a shell string. `{shard}` and
`{shards}` placeholders are replaced in child arguments, and every child gets
`CRAWL_SHARD` and `CRAWL_SHARDS`:

```bash
export PROXY_URL_0="http://user-a:password@gateway-a.example:8080"
export PROXY_URL_1="http://user-b:password@gateway-b.example:8080"

hexdigest-shard run --shards 2 --log-dir .logs -- \
  python -m scrapy crawl products \
  -a shard={shard} -a shards={shards}
```

Each child receives `PROXY_URL` from its matching `PROXY_URL_<index>` value.
Missing entries run without a proxy. Proxy values are never printed. Use
`--inherit-proxy` only when deliberately sharing a parent `PROXY_URL`.

Use `--dry-run` to print the expanded child commands without starting workers.
The process exits non-zero when any worker exits non-zero and returns 130 after
cleanly terminating all workers on Ctrl-C.

For an enumerated sitemap index, use `assigned_indices(total, ShardSpec(...))`
to give each worker disjoint child positions. The marketplace-specific URL
shape and sitemap rules remain in the crawler that calls this library.

## Responsible use

Parallelism is not permission to overload an origin. Respect robots.txt, terms,
rate limits, privacy requirements, and applicable law. Configure concurrency,
delays, and worker count for the origin and your authorization.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[test]"
pytest
ruff check .
python -m build
twine check dist/*
```

Tests use local SQLite files and local child processes only; no marketplace or
proxy is contacted.

## About HexDigest

HexDigest compiles daily market intelligence from public marketplace listings.
If you need maintained datasets rather than crawler infrastructure, visit
[hexdigest.com](https://hexdigest.com/?utm_source=github&utm_medium=referral&utm_campaign=hexdigest-crawl-sharding).

## License

MIT. See [LICENSE](LICENSE).
