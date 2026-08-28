# Contributing

Focused bug fixes, tests, and documentation improvements are welcome.

```bash
python -m pip install -e ".[test]"
pytest
ruff check .
```

Tests must use local SQLite files and local child processes. Do not contact
marketplaces, external proxies, or third-party APIs from the test suite.
Changes to shard assignment must prove both no overlap and complete coverage.
