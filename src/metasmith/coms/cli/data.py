from __future__ import annotations

import json

from ...ops import data as _ops


def register(subs):
    p = subs.add_parser("data", help="data instance library operations")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _inspect = sp.add_parser("inspect", help="show schema, namespaces, item count")
    _inspect.add_argument("library")
    _inspect.set_defaults(func=lambda a: _ops.inspect_library(a.library))

    _list = sp.add_parser("list", help="list items in a library")
    _list.add_argument("library")
    _list.add_argument("--type", dest="type_filter")
    _list.set_defaults(func=lambda a: _ops.list_items(a.library, a.type_filter))

    _create = sp.add_parser("create", help="create a new data instance library")
    _create.add_argument("path")
    _create.add_argument("--type-lib", action="append", default=[])
    _create.add_argument("--purge", action="store_true")
    _create.set_defaults(func=lambda a: _ops.create_library(a.path, a.type_lib, a.purge))

    _fork = sp.add_parser("fork", help="copy a library under a new fork id (new task key, no data copied)")
    _fork.add_argument("library")
    _fork.add_argument("dest")
    _fork.add_argument("--fork-id", help="explicit fork id; a random one is generated otherwise")
    _fork.set_defaults(func=lambda a: _ops.fork_library(a.library, a.dest, a.fork_id))

    _attach = sp.add_parser("attach-types", help="attach a type library to a data library")
    _attach.add_argument("library")
    _attach.add_argument("type_library")
    _attach.add_argument("--namespace")
    _attach.add_argument("--on-exist", default="skip")
    _attach.set_defaults(func=lambda a: _ops.attach_type_library(
        a.library, a.type_library, a.namespace, a.on_exist,
    ))

    _addi = sp.add_parser("add-item", help="register an existing file as a typed item")
    _addi.add_argument("library")
    _addi.add_argument("--path", required=True, dest="host_path")
    _addi.add_argument("--dtype", required=True)
    _addi.add_argument("--parent", action="append", default=[], dest="parents")
    _addi.add_argument("--no-save", action="store_true")
    _addi.set_defaults(func=lambda a: _ops.add_item(
        a.library, a.host_path, a.dtype, a.parents or None, not a.no_save,
    ))

    _addv = sp.add_parser("add-value", help="register a scalar/dict value as a typed item")
    _addv.add_argument("library")
    _addv.add_argument("--name", required=True)
    _addv.add_argument("--value", required=True, help="string, or JSON if --json-value")
    _addv.add_argument("--json-value", action="store_true",
                       help="parse --value as JSON instead of treating as a string")
    _addv.add_argument("--dtype", required=True)
    _addv.add_argument("--parent", action="append", default=[], dest="parents")
    _addv.add_argument("--no-save", action="store_true")
    _addv.set_defaults(func=_cmd_add_value)

    _setp = sp.add_parser("set-parents", help="attach parents to an item")
    _setp.add_argument("library")
    _setp.add_argument("item_path")
    _setp.add_argument("--parent", action="append", required=True, dest="parents")
    _setp.add_argument("--no-save", action="store_true")
    _setp.set_defaults(func=lambda a: _ops.set_item_parents(
        a.library, a.item_path, a.parents, not a.no_save,
    ))

    _rm = sp.add_parser("remove", help="unregister an item (filesystem unchanged)")
    _rm.add_argument("library")
    _rm.add_argument("item_path")
    _rm.add_argument("--no-save", action="store_true")
    _rm.set_defaults(func=lambda a: _ops.remove_item(a.library, a.item_path, not a.no_save))

    _ren = sp.add_parser("rename", help="rename an item (manifest + filesystem)")
    _ren.add_argument("library")
    _ren.add_argument("old")
    _ren.add_argument("new")
    _ren.set_defaults(func=lambda a: _ops.rename_item(a.library, a.old, a.new))

    _renp = sp.add_parser("rename-by-parent", help="rename items by their parent's stem")
    _renp.add_argument("library")
    _renp.add_argument("parent_type")
    _renp.set_defaults(func=lambda a: _ops.rename_by_parent(a.library, a.parent_type))

    _prune = sp.add_parser("prune-types", help="drop unused type definitions")
    _prune.add_argument("library")
    _prune.add_argument("--whitelist", action="append", default=[])
    _prune.add_argument("--no-save", action="store_true")
    _prune.set_defaults(func=lambda a: _ops.prune_types(
        a.library, a.whitelist or None, not a.no_save,
    ))

    _con = sp.add_parser("consolidate", help="replace absolute-path items with local symlinks")
    _con.add_argument("library")
    _con.set_defaults(func=lambda a: _ops.consolidate(a.library))

    _save = sp.add_parser("save", help="persist library manifest")
    _save.add_argument("library")
    _save.add_argument("--no-update-types", action="store_true")
    _save.set_defaults(func=lambda a: _ops.save_library(a.library, not a.no_update_types))

    _trace = sp.add_parser("trace", help="yield (from, to) lineage pairs by type")
    _trace.add_argument("library")
    _trace.add_argument("from_type")
    _trace.add_argument("to_type")
    _trace.set_defaults(func=lambda a: _ops.trace_lineage(a.library, a.from_type, a.to_type))

    _lr = sp.add_parser("load-remote", help="fetch a library image from a Source URI")
    _lr.add_argument("src_uri")
    _lr.add_argument("dest")
    _lr.add_argument("--on-exist", default="skip")
    _lr.add_argument("--no-image", action="store_true",
                     help="treat src as a directory, not a packed image")
    _lr.set_defaults(func=lambda a: _ops.load_remote_library(
        a.src_uri, a.dest, a.on_exist, not a.no_image,
    ))

    _il = sp.add_parser(
        "import-library",
        help="fetch a library + upsert lineage/imported entries into task_cache",
    )
    _il.add_argument("src_uri")
    _il.add_argument("dest")
    _il.add_argument("--cache-root", default=None,
                     help="override cache_root (default: <dest>/../task_cache)")
    _il.add_argument("--on-exist", default="skip")
    _il.add_argument("--no-image", action="store_true",
                     help="treat src as a directory, not a packed image")
    _il.set_defaults(func=lambda a: _ops.import_library(
        a.src_uri, a.dest, a.cache_root, a.on_exist, not a.no_image,
    ))

    _pn = sp.add_parser("pin", help="trust this library's recorded ids; refuse mutation")
    _pn.add_argument("library")
    _pn.add_argument("--deep", action="store_true",
                     help="also record content digests -- a full pass over the data,"
                          " and the only thing `verify --deep` can compare against")
    _pn.set_defaults(func=lambda a: _ops.pin_library(a.library, a.deep))

    _up = sp.add_parser("unpin", help="lift a pin so the library can be rebuilt")
    _up.add_argument("library")
    _up.set_defaults(func=lambda a: _ops.unpin_library(a.library))

    _rs = sp.add_parser(
        "restamp",
        help="re-record a pinned library's stat stamps, moving no identity",
    )
    _rs.add_argument("library")
    _rs.add_argument("--entry", default=None, help="one entry, instead of all")
    _rs.set_defaults(func=lambda a: _ops.restamp_library(a.library, a.entry))

    _iv = sp.add_parser(
        "invalidate",
        help="say an item's data changed, so runs that used it re-run",
    )
    _iv.add_argument("library")
    _iv.add_argument("entries", nargs="*",
                     help="entry paths within the library")
    _iv.add_argument("--all", action="store_true",
                     help="every leaf entry in the library")
    _iv.set_defaults(func=lambda a: _ops.invalidate_items(
        a.library, a.entries, a.all,
    ))

    _vf = sp.add_parser("verify", help="report drift in a pinned library")
    _vf.add_argument("library")
    _vf.add_argument("--deep", action="store_true",
                     help="re-derive content digests; expensive, and the only check"
                          " without the holes the cheap ones have")
    _vf.set_defaults(func=lambda a: _ops.verify_library(a.library, a.deep))

    _lin = sp.add_parser("lineage", help="show an item's type + ancestors")
    _lin.add_argument("library")
    _lin.add_argument("item_path")
    _lin.add_argument("--of", default=None, metavar="PATH",
                      help="write rendered output to PATH (default: stdout)")
    _lin.add_argument("--format", choices=["json", "mermaid"], default="json",
                      help="output format (default: json)")
    _lin.add_argument("--depth", type=int, default=None,
                      help="cap ancestor walk depth (default: no cap)")
    _lin.add_argument("--logs", action="store_true",
                      help="include per-invocation log paths in output")
    _lin.set_defaults(func=lambda a: _ops.show_item_lineage(
        a.library, a.item_path,
        of=a.of, fmt=a.format, depth=a.depth, include_logs=a.logs,
    ))


def _cmd_add_value(args):
    value = json.loads(args.value) if args.json_value else args.value
    return _ops.add_value(
        args.library, args.name, value, args.dtype,
        args.parents or None, not args.no_save,
    )
