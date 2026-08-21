from __future__ import annotations

import argparse

from ...ops import build as _ops


def _add_flags(parser: argparse.ArgumentParser, suppress_defaults: bool = False) -> None:
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

    _vl = sp.add_parser(
        "vendor-library",
        help="copy a metasmith library's shippable pieces into a vendored destination",
        description="Copy each --src NAME=PATH into --dst/NAME, replacing --dst "
                    "wholesale, and stamp a content hash for later drift checks. "
                    "--check verifies an existing bundle against live source "
                    "without copying, e.g. --src data_types=src/metasmith_libraries/"
                    "data_types --src resources=... --src envs=envs/metasmith_libraries.",
    )
    _vl.add_argument("--src", action="append", required=True, dest="vendor_srcs",
                     help="NAME=PATH to vendor into --dst/NAME (repeatable)")
    _vl.add_argument("--dst", required=True, dest="vendor_dst",
                     help="destination directory (replaced wholesale)")
    _vl.add_argument("--check", action="store_true",
                     help="verify the existing bundle matches live source; do not copy")
    _vl.add_argument("--no-metadata", action="store_true", dest="vendor_no_metadata",
                     help="ship content only: skip _metadata/ and do not require it. "
                          "For a consumer that compiles its own copy.")
    _vl.set_defaults(func=_cmd_vendor_library)

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


def _cmd_vendor_library(args):
    expect_metadata = not getattr(args, "vendor_no_metadata", False)
    if args.check:
        return _ops.check_vendor_library(args.vendor_srcs, args.vendor_dst, expect_metadata)
    return _ops.vendor_library(args.vendor_srcs, args.vendor_dst, expect_metadata)
