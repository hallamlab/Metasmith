from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

CONTROL_FILENAME = "CONTROL.json"

VALID_ACTIONS = frozenset(
    {"submit", "done", "give_up", "continue", "report_issue"}
)

SUBMIT_ACTIONS = frozenset({"submit", "done"})


@dataclass(frozen=True)
class Control:
    action: str
    payload: dict

    @property
    def task_key(self) -> str | None:
        return self.payload.get("task_key")

    @property
    def agent(self) -> str | None:
        return self.payload.get("agent")

    @property
    def entrypoint(self) -> str | None:
        return self.payload.get("entrypoint")

    @property
    def reason(self) -> str | None:
        return self.payload.get("reason")

    @property
    def notes(self) -> str | None:
        return self.payload.get("notes")

    @property
    def is_submission(self) -> bool:
        return self.action in SUBMIT_ACTIONS


def control_path(sandbox: Path) -> Path:
    return sandbox / CONTROL_FILENAME


def read_control(sandbox: Path) -> Control | None:
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
    if action not in VALID_ACTIONS:
        raise ValueError(f"invalid action {action!r}; expected one of {sorted(VALID_ACTIONS)}")
    payload = {"action": action, **fields}
    p = control_path(sandbox)
    p.write_text(json.dumps(payload, indent=2))
    return p
