"""`metasmith e2e ...` subcommands.

These commands exist to support end-to-end agentic test harnesses where
a coding agent drives the CLI in a Ralph-style outer loop. Each loop
iteration ends when the agent writes ``CONTROL.json`` in the sandbox
cwd; this command provides a small, well-named verb that the agent can
call instead of hand-crafting JSON.

The schema is the contract consumed by
``tests/e2e_agentic/harness/control.py``.
"""
from __future__ import annotations

import json
from pathlib import Path


CONTROL_FILENAME = "CONTROL.json"


def _write_control(cwd: Path, payload: dict) -> dict:
    cwd = cwd.resolve()
    if not cwd.exists():
        raise FileNotFoundError(f"control cwd does not exist: {cwd}")
    out = cwd / CONTROL_FILENAME
    out.write_text(json.dumps(payload, indent=2))
    return {"written": str(out), "payload": payload}


def _checkpoint(args) -> dict:
    cwd = Path(getattr(args, "cwd", None) or Path.cwd())
    action = args.action
    payload: dict = {"action": action}
    if args.notes is not None:
        payload["notes"] = args.notes
    if action == "done":
        if not args.key:
            raise SystemExit("`done` requires --key <task_key>")
        payload["task_key"] = args.key
    elif action == "give_up":
        if not args.reason:
            raise SystemExit("`give_up` requires --reason <text>")
        payload["reason"] = args.reason
    elif action == "continue":
        pass
    else:
        raise SystemExit(f"unknown action {action!r}; expected done|give_up|continue")
    return _write_control(cwd, payload)


def register(subs):
    p = subs.add_parser(
        "e2e",
        help="End-to-end test harness helpers (CONTROL.json sentinel)",
    )
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    cp = sp.add_parser(
        "checkpoint",
        help="Write CONTROL.json to declare iteration outcome (done|give_up|continue)",
    )
    cp.add_argument(
        "action", choices=("done", "give_up", "continue"),
        help="terminal state the loop should observe",
    )
    cp.add_argument("--key", help="task_key (required when action=done)")
    cp.add_argument("--reason", help="free text (required when action=give_up)")
    cp.add_argument("--notes", help="free text notes carried in the payload")
    cp.add_argument(
        "--cwd", default=None,
        help="directory to write CONTROL.json into (default: current working dir)",
    )
    cp.set_defaults(func=_checkpoint)
