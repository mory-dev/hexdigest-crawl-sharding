"""Safe list-based launcher for one process per crawl shard."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Sequence
from pathlib import Path


def _replace_placeholders(command: Sequence[str], index: int, count: int) -> list[str]:
    return [
        argument.replace("{shard}", str(index)).replace("{shards}", str(count))
        for argument in command
    ]


def launch_workers(
    command: Sequence[str],
    shards: int,
    *,
    cwd: str | Path | None = None,
    log_dir: str | Path | None = None,
    proxy_env_prefix: str | None = "PROXY_URL_",
    inherit_proxy: bool = False,
    dry_run: bool = False,
) -> int:
    """Launch all workers and return non-zero when any worker fails.

    ``command`` is never passed through a shell. Use literal ``{shard}`` and
    ``{shards}`` placeholders in arguments when the child crawler needs them.
    Each child receives ``CRAWL_SHARD`` and ``CRAWL_SHARDS``. Proxy values are
    read from the parent environment and never printed.
    """

    if not command:
        raise ValueError("command must contain an executable")
    if shards < 1:
        raise ValueError("shards must be at least 1")
    log_path = Path(log_dir) if log_dir else None
    if log_path:
        log_path.mkdir(parents=True, exist_ok=True)

    processes: list[tuple[subprocess.Popen, object | None]] = []
    try:
        for index in range(shards):
            child_env = os.environ.copy()
            child_env["CRAWL_SHARD"] = str(index)
            child_env["CRAWL_SHARDS"] = str(shards)
            if proxy_env_prefix:
                proxy = os.environ.get(f"{proxy_env_prefix}{index}")
                if proxy:
                    child_env["PROXY_URL"] = proxy
                elif not inherit_proxy:
                    child_env.pop("PROXY_URL", None)
            elif not inherit_proxy:
                child_env.pop("PROXY_URL", None)
            child_command = _replace_placeholders(command, index, shards)
            if dry_run:
                print(f"shard {index}: {' '.join(child_command)}")
                continue
            output = None
            if log_path:
                output = (log_path / f"shard-{index}.log").open("ab")
            process = subprocess.Popen(
                child_command,
                cwd=str(cwd) if cwd else None,
                env=child_env,
                stdout=output,
                stderr=subprocess.STDOUT if output else None,
            )
            processes.append((process, output))
        if dry_run:
            return 0
        codes = [process.wait() for process, _ in processes]
        return next((code for code in codes if code != 0), 0)
    except KeyboardInterrupt:
        for process, _ in processes:
            if process.poll() is None:
                process.terminate()
        for process, _ in processes:
            process.wait()
        return 130
    finally:
        for _, output in processes:
            if output:
                output.close()
