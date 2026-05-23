"""`metasmith transform ...` subcommands."""
from __future__ import annotations

from ...ops import transforms as _ops


def register(subs):
    p = subs.add_parser("transform", help="transform library inspection and authoring")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _list = sp.add_parser("list", help="list transforms across libraries")
    _list.add_argument("--library", "-r", action="append", required=True,
                       help="transform library dir (repeatable)")
    _list.set_defaults(func=lambda a: _ops.list_transforms(a.library))

    _libs = sp.add_parser("libraries", help="summarize transform libraries")
    _libs.add_argument("--library", "-r", action="append", required=True)
    _libs.set_defaults(func=lambda a: _ops.list_libraries(a.library))

    _show = sp.add_parser("show", help="show a transform's contract")
    _show.add_argument("library")
    _show.add_argument("transform")
    _show.set_defaults(func=lambda a: _ops.show_contract(a.library, a.transform))

    _read = sp.add_parser("read", help="dump the .py source of a transform")
    _read.add_argument("library")
    _read.add_argument("transform")
    _read.set_defaults(func=lambda a: _ops.read_source(a.library, a.transform))

    _write = sp.add_parser("write", help="write/overwrite a transform .py")
    _write.add_argument("library")
    _write.add_argument("transform")
    _write.add_argument("--source", required=True, help="path to source file (use - for stdin)")
    _write.add_argument("--no-register", action="store_true")
    _write.set_defaults(func=_cmd_write)

    _scaf = sp.add_parser("scaffold", help="generate a transform .py skeleton")
    _scaf.add_argument("library")
    _scaf.add_argument("name")
    _scaf.add_argument("--in", action="append", required=True, dest="inputs",
                       help="input type (repeatable)")
    _scaf.add_argument("--out", action="append", required=True, dest="outputs",
                       help="output type (repeatable)")
    _scaf.add_argument("--group-by", help="input type to group by (defaults to first --in)")
    _scaf.add_argument("--container", help="container type, e.g. containers::myimage.oci")
    _scaf.add_argument("--cpus", type=int)
    _scaf.add_argument("--memory-gb", type=float)
    _scaf.add_argument("--duration-h", type=float)
    _scaf.set_defaults(func=_cmd_scaffold)

    _val = sp.add_parser("validate", help="reload + validate the contract resolves")
    _val.add_argument("library")
    _val.add_argument("transform")
    _val.set_defaults(func=lambda a: _ops.validate_contract(a.library, a.transform))

    _prop = sp.add_parser("propagate-types", help="copy type libs into the transform lib")
    _prop.add_argument("library")
    _prop.add_argument("--type-lib", "-t", action="append", required=True)
    _prop.set_defaults(func=lambda a: _ops.propagate_types(a.library, a.type_lib))


def _cmd_write(args):
    import sys
    if args.source == "-":
        source = sys.stdin.read()
    else:
        with open(args.source) as f:
            source = f.read()
    return _ops.write_transform(args.library, args.transform, source, not args.no_register)


def _cmd_scaffold(args):
    resources: dict = {}
    if args.cpus is not None: resources["cpus"] = args.cpus
    if args.memory_gb is not None: resources["memory_gb"] = args.memory_gb
    if args.duration_h is not None: resources["duration_h"] = args.duration_h
    return _ops.scaffold_transform(
        args.library, args.name, args.inputs, args.outputs,
        args.group_by, args.container, resources or None,
    )
