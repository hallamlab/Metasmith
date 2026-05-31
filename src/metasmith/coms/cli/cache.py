"""`metasmith cache ...` and `metasmith status` subcommands (S8)."""
from __future__ import annotations

from ...ops import cache as _ops


def register(subs):
    p = subs.add_parser("cache", help="lineage-addressed task-cache operations")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _list = sp.add_parser("list", help="list cache entries")
    _list.add_argument("--cache-root", default=None)
    _list.add_argument("--include-tombstoned", action="store_true")
    _list.set_defaults(func=lambda a: _ops.list_cache(
        a.cache_root, include_tombstoned=a.include_tombstoned,
    ))

    _gc = sp.add_parser("gc", help="tombstone + delayed-delete cache entries")
    _gc.add_argument("--cache-root", default=None)
    _gc.add_argument(
        "--older-than", type=int, default=None,
        help="seconds since last_hit_at; entries older than this get tombstoned",
    )
    _gc.add_argument(
        "--max-size", type=int, default=None,
        help="total cache size cap in bytes; LRU tombstone until under cap",
    )
    _gc.add_argument(
        "--grace", type=int, default=24 * 60 * 60,
        help="seconds after tombstone before physical delete (default 24h)",
    )
    _gc.add_argument(
        "--delete", action="store_true",
        help="also unlink entries whose tombstone is past the grace period",
    )
    _gc.set_defaults(func=lambda a: _ops.gc_cache(
        a.cache_root,
        older_than_seconds=a.older_than,
        max_size_bytes=a.max_size,
        grace_seconds=a.grace,
        delete=a.delete,
    ))

    _ex = sp.add_parser("explain", help="show a cache entry's manifest + lineage")
    _ex.add_argument("key")
    _ex.add_argument("--cache-root", default=None)
    _ex.set_defaults(func=lambda a: _ops.explain_cache_entry(a.key, a.cache_root))


def register_status(subs):
    p = subs.add_parser(
        "status",
        help="render per-task hit/run status from <run_dir>/_metasmith/trace.jsonl",
    )
    p.add_argument("run_dir")
    p.set_defaults(func=lambda a: _ops.status_for_run(a.run_dir))
