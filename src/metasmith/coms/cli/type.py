"""`metasmith type ...` subcommands."""
from __future__ import annotations

import json

from ...ops import types as _ops


def register(subs):
    p = subs.add_parser("type", help="data type inspection and authoring")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _list = sp.add_parser("list", help="list data types in the given libraries")
    _list.add_argument("--type-lib", "-t", action="append", default=[],
                       help="type library YAML (repeatable)")
    _list.add_argument("--transform-lib", "-r", action="append", default=[],
                       help="transform library dir (repeatable)")
    _list.add_argument("--namespace", "-n", help="filter to one namespace")
    _list.set_defaults(func=_cmd_list)

    _show = sp.add_parser("show", help="show details for a 'ns::name' type")
    _show.add_argument("type_name")
    _show.add_argument("--type-lib", "-t", action="append", default=[])
    _show.add_argument("--transform-lib", "-r", action="append", default=[])
    _show.set_defaults(func=_cmd_show)

    _compat = sp.add_parser("compat", help="check type compatibility (src satisfies tgt)")
    _compat.add_argument("source_type")
    _compat.add_argument("target_type")
    _compat.add_argument("--type-lib", "-t", action="append", default=[])
    _compat.add_argument("--transform-lib", "-r", action="append", default=[])
    _compat.set_defaults(func=_cmd_compat)

    _create = sp.add_parser("create", help="create a new type library YAML")
    _create.add_argument("path")
    _create.add_argument("--ontology", help="JSON dict (defaults to EDAM)")
    _create.add_argument("--types", help="JSON dict of {name: {properties, extends?}}")
    _create.set_defaults(func=_cmd_create)

    _add = sp.add_parser("add", help="append a type to an existing library YAML")
    _add.add_argument("library_path")
    _add.add_argument("name")
    _add.add_argument("--properties", required=True, help="JSON dict")
    _add.add_argument("--extends", action="append", default=[])
    _add.add_argument("--overwrite", action="store_true")
    _add.set_defaults(func=_cmd_add)


def _cmd_list(args):
    return _ops.list_types(args.type_lib, args.transform_lib, args.namespace)


def _cmd_show(args):
    return _ops.get_type(args.type_name, args.type_lib, args.transform_lib)


def _cmd_compat(args):
    return _ops.check_compatibility(
        args.source_type, args.target_type, args.type_lib, args.transform_lib,
    )


def _cmd_create(args):
    ont = json.loads(args.ontology) if args.ontology else None
    types = json.loads(args.types) if args.types else None
    return _ops.create_type_library(args.path, ont, types)


def _cmd_add(args):
    return _ops.add_type(
        args.library_path,
        args.name,
        json.loads(args.properties),
        args.extends or None,
        args.overwrite,
    )
