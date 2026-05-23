"""`metasmith build ...` — decomposed library compilation.

Bare `metasmith build -t ... -r ...` is an alias for `metasmith build all` for
backwards compatibility with `dev.sh` and existing user habits.
"""
from __future__ import annotations

import argparse

from ...ops import build as _ops


def _add_flags(parser: argparse.ArgumentParser, suppress_defaults: bool = False) -> None:
    """Attach -t/-r/-u to a parser. Sub-step parsers use SUPPRESS so they don't
    clobber values the top-level parser already collected."""
    default = argparse.SUPPRESS if suppress_defaults else []
    parser.add_argument("-t", "--types", action="append", default=default, dest="type_dirs",
                        help="data type definition directory (repeatable)")
    parser.add_argument("-r", "--transforms", action="append", default=default, dest="transform_dirs",
                        help="transform library directory (repeatable)")
    parser.add_argument("-u", "--uniques", action="append", default=default, dest="unique_dirs",
                        help="unique resource directory (repeatable)")


def register(subs):
    p = subs.add_parser(
        "build",
        help="compile data type and transform libraries",
        description="Compile data types, transform libraries, and (optionally) "
                    "'unique' resource libraries. Run a single step or all of them. "
                    "Bare `metasmith build` runs all steps.",
    )
    _add_flags(p, suppress_defaults=False)

    sub_parent = argparse.ArgumentParser(add_help=False)
    _add_flags(sub_parent, suppress_defaults=True)

    sp = p.add_subparsers(dest="sub", metavar="STEP")

    _all = sp.add_parser("all", help="run types → uniques → transforms (default)",
                         parents=[sub_parent])
    _all.set_defaults(func=_cmd_all)

    _types = sp.add_parser("types", help="just load + report type libraries",
                           parents=[sub_parent])
    _types.set_defaults(func=_cmd_types)

    _u = sp.add_parser("uniques", help="compile unique resource libraries",
                       parents=[sub_parent])
    _u.set_defaults(func=_cmd_uniques)

    _tr = sp.add_parser("transforms", help="compile transform libraries",
                        parents=[sub_parent])
    _tr.set_defaults(func=_cmd_transforms)

    p.set_defaults(func=_cmd_all)


def _arg(args, name: str) -> list:
    return getattr(args, name, None) or []


def _cmd_all(args):
    return _ops.build_all(_arg(args, "type_dirs"), _arg(args, "transform_dirs"), _arg(args, "unique_dirs"))


def _cmd_types(args):
    return _ops.load_types(_arg(args, "type_dirs"))


def _cmd_uniques(args):
    return _ops.compile_uniques(_arg(args, "unique_dirs"), _arg(args, "type_dirs"))


def _cmd_transforms(args):
    return _ops.compile_transforms(_arg(args, "transform_dirs"), _arg(args, "type_dirs"))
