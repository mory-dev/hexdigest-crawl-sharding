"""Command-line interface for sharded crawl workers."""

from __future__ import annotations

import argparse

from .runner import launch_workers
from .sharding import ShardSpec, stable_owner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hexdigest-shard",
        description="Launch safe, independently observable crawler shards.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    run = subparsers.add_parser("run", help="launch one child process per shard")
    run.add_argument("--shards", type=int, required=True)
    run.add_argument("--cwd")
    run.add_argument("--log-dir")
    run.add_argument("--proxy-env-prefix", default="PROXY_URL_")
    run.add_argument("--inherit-proxy", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("worker_command", nargs=argparse.REMAINDER)

    own = subparsers.add_parser("owner", help="show the stable owner for a key")
    own.add_argument("key")
    own.add_argument("--shards", type=int, required=True)

    args = parser.parse_args(argv)
    if args.subcommand == "run":
        command = args.worker_command
        if command[:1] == ["--"]:
            command = command[1:]
        if not command:
            parser.error("run requires a command after --")
        return launch_workers(
            command,
            args.shards,
            cwd=args.cwd,
            log_dir=args.log_dir,
            proxy_env_prefix=args.proxy_env_prefix,
            inherit_proxy=args.inherit_proxy,
            dry_run=args.dry_run,
        )
    if args.subcommand == "owner":
        ShardSpec(0, args.shards)
        print(stable_owner(args.key, args.shards))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
