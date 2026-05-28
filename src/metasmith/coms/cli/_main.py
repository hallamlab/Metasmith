"""Top-level CLI dispatch."""
from __future__ import annotations

import argparse
import os
import sys
import traceback

from ...constants import NAME, VERSION, GIT_URL
from ...logging import _handler as _log_handler
from ._format import emit, die
from . import (
    type as _type,
    data as _data,
    transform as _transform,
    workflow as _workflow,
    agent as _agent,
    source as _source,
    task as _task,
    build as _build,
    run as _run,
    e2e as _e2e,
    legacy as _legacy,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metasmith",
        description=f"{NAME} v{VERSION} — {GIT_URL}",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--quiet", action="store_true", help="suppress output")
    parser.add_argument(
        "--workspace", default=os.environ.get("METASMITH_WORKSPACE"),
        help="workspace dir for cached tasks (default: ~/.metasmith/workspace; "
             "env: METASMITH_WORKSPACE)",
    )
    parser.add_argument("-V", "--version", action="version", version=f"{NAME} {VERSION}")

    subs = parser.add_subparsers(dest="cmd", metavar="COMMAND")

    _type.register(subs)
    _data.register(subs)
    _transform.register(subs)
    _workflow.register(subs)
    _agent.register(subs)
    _source.register(subs)
    _task.register(subs)
    _build.register(subs)
    _run.register(subs)
    _e2e.register(subs)
    _legacy.register(subs)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.json:
        # Keep stdout clean for structured output; route progress logs to stderr.
        _log_handler.stream = sys.stderr
    if not getattr(args, "func", None):
        parser.print_help()
        return 0
    try:
        result = args.func(args)
    except SystemExit:
        raise
    except Exception as exc:
        if os.environ.get("METASMITH_TRACEBACK"):
            traceback.print_exc()
        die(f"{type(exc).__name__}: {exc}")
        return 1
    if result is not None:
        emit(args, result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
