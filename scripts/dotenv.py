#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


def _iter_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise SystemExit(f"dotenv file not found: {path}")


def _parse_key(raw_line: str) -> str | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[len("export ") :].lstrip()
    if "=" not in line:
        return None
    key, _ = line.split("=", 1)
    key = key.strip()
    if not key:
        return None
    if any(ch.isspace() for ch in key):
        return None
    return key


def _parse_value(raw_line: str) -> str | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[len("export ") :].lstrip()
    if "=" not in line:
        return None
    _, value = line.split("=", 1)
    value = value.strip()
    if len(value) >= 2 and value[0] in ("'", '"') and value[-1] == value[0]:
        value = value[1:-1]
    return value


def cmd_get(args: argparse.Namespace) -> int:
    env_path = Path(args.file)
    key = args.key
    for raw in _iter_lines(env_path):
        if _parse_key(raw) != key:
            continue
        value = _parse_value(raw)
        if value is None:
            return 1
        sys.stdout.write(value)
        return 0
    return 1


def cmd_filter(args: argparse.Namespace) -> int:
    env_path = Path(args.file)
    exclude_keys = set(args.exclude_key or [])
    exclude_prefixes = tuple(args.exclude_prefix or [])

    out_lines: list[str] = []
    for raw in _iter_lines(env_path):
        key = _parse_key(raw)
        if key is None:
            # Preserve non key/value lines (blank lines + comments).
            out_lines.append(raw)
            continue
        if key in exclude_keys:
            continue
        if any(key.startswith(p) for p in exclude_prefixes):
            continue
        out_lines.append(raw)

    sys.stdout.write("\n".join(out_lines).rstrip("\n") + "\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Small .env helper (no evaluation).")
    parser.add_argument("--file", default=".env", help="Path to .env file (default: ./.env)")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get", help="Get a single key's value.")
    p_get.add_argument("key")
    p_get.set_defaults(fn=cmd_get)

    p_filter = sub.add_parser("filter", help="Filter .env file lines by key.")
    p_filter.add_argument("--exclude-key", action="append", default=[])
    p_filter.add_argument("--exclude-prefix", action="append", default=[])
    p_filter.set_defaults(fn=cmd_filter)

    args = parser.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
