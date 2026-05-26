"""CONTROL.json sentinel contract.

The agent declares iteration outcome by writing ``CONTROL.json`` in the
sandbox. The ralph loop reads it between iterations and decides whether
to keep looping.

Schemas accepted:

    {"action": "done",     "task_key": "<key>", "notes": "..."}
    {"action": "give_up",  "reason":  "...",    "notes": "..."}
    {"action": "continue", "notes":   "..."}

Anything else (missing file, malformed JSON, unknown action) is treated
as implicit ``continue`` so the loop runs again next iteration.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

CONTROL_FILENAME = "CONTROL.json"

VALID_ACTIONS = frozenset({"done", "give_up", "continue"})


@dataclass(frozen=True)
class Control:
    action: str
    payload: dict

    @property
    def task_key(self) -> str | None:
        return self.payload.get("task_key")

    @property
    def reason(self) -> str | None:
        return self.payload.get("reason")

    @property
    def notes(self) -> str | None:
        return self.payload.get("notes")


def control_path(sandbox: Path) -> Path:
    return sandbox / CONTROL_FILENAME


def read_control(sandbox: Path) -> Control | None:
    """Read CONTROL.json from the sandbox.

    Returns ``None`` if the file is missing, unreadable, malformed, or
    declares an unknown action. The caller treats ``None`` as implicit
    continue.
    """
    p = control_path(sandbox)
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    action = payload.get("action")
    if action not in VALID_ACTIONS:
        return None
    return Control(action=action, payload=payload)


def clear_control(sandbox: Path) -> None:
    p = control_path(sandbox)
    if p.exists():
        p.unlink()


def write_control(sandbox: Path, action: str, **fields) -> Path:
    """Test helper — write a CONTROL.json from harness code.

    Production code path: the agent writes this file itself (typically via
    ``metasmith e2e checkpoint``).
    """
    if action not in VALID_ACTIONS:
        raise ValueError(f"invalid action {action!r}; expected one of {sorted(VALID_ACTIONS)}")
    payload = {"action": action, **fields}
    p = control_path(sandbox)
    p.write_text(json.dumps(payload, indent=2))
    return p
