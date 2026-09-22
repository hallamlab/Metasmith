from __future__ import annotations

from ...ops import cache as _ops


def register(subs):
    p = subs.add_parser("cache", help="lineage-addressed task-cache operations")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _list = sp.add_parser(
        "list", help="list what the store holds, filtered and grouped",
    )
    _list.add_argument("--cache-root", default=None)
    _list.add_argument("--agent-home", default=None,
                       help="the agent whose store to read, instead of one here")
    _list.add_argument("--include-tombstoned", action="store_true")
    _list.add_argument("--origin", choices=["lineage", "imported"], default=None,
                       help="products, or data you imported")
    _list.add_argument("--run", default=None, help="only what this run produced")
    _list.add_argument("--tag", default=None, help="only entries carrying this tag")
    _list.add_argument("--dtype", default=None, metavar="NS::TYPE")
    _list.add_argument("--name", default=None,
                       help="only entries an import recorded under this name")
    _list.add_argument("--group-by", choices=list(_ops.GROUPINGS), default=None)
    _list.add_argument("--sort-by", choices=list(_ops.SORTS), default="created_at")
    _list.add_argument("--ascending", action="store_true")
    _list.set_defaults(func=lambda a: _ops.list_cache(
        a.cache_root, agent_home=a.agent_home,
        include_tombstoned=a.include_tombstoned,
        origin=a.origin, run=a.run, tag=a.tag, dtype=a.dtype, name=a.name,
        group_by=a.group_by, sort_by=a.sort_by, descending=not a.ascending,
    ))

    _tag = sp.add_parser("tag", help="label an entry, so you can group by it later")
    _tag.add_argument("key")
    _tag.add_argument("tags", nargs="+")
    _tag.add_argument("--cache-root", default=None)
    _tag.add_argument("--agent-home", default=None)
    _tag.add_argument("--replace", action="store_true",
                      help="these tags instead of, not beside, the existing ones")
    _tag.add_argument("--remove", action="store_true", help="drop these tags")
    _tag.set_defaults(func=lambda a: _ops.set_entry_tags(
        a.key, a.tags, cache_root=a.cache_root, agent_home=a.agent_home,
        replace=a.replace, remove=a.remove,
    ))

    _gc = sp.add_parser("gc", help="tombstone + delayed-delete cache entries")
    _gc.add_argument("--cache-root", default=None)
    _gc.add_argument("--agent-home", default=None)
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
        agent_home=a.agent_home,
    ))

    _ex = sp.add_parser("explain", help="show a cache entry's manifest + lineage")
    _ex.add_argument("key")
    _ex.add_argument("--cache-root", default=None)
    _ex.add_argument("--agent-home", default=None)
    _ex.set_defaults(func=lambda a: _ops.explain_cache_entry(
        a.key, a.cache_root, a.agent_home,
    ))


def register_status(subs):
    p = subs.add_parser(
        "status",
        help="render per-task hit/run status from <run_dir>/_metasmith/trace.jsonl",
    )
    p.add_argument("run_dir")
    p.set_defaults(func=lambda a: _ops.status_for_run(a.run_dir))
