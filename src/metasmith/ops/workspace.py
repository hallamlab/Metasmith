"""Workspace = on-disk task storage. Each task lives at <workspace>/<task_key>/."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from ..models.workflow import WorkflowTask
from ..models.remote import Source


DEFAULT_WORKSPACE = Path.home() / ".metasmith" / "workspace"


def resolve_workspace(workspace: str | Path | None) -> Path:
    """Pick a workspace dir: explicit arg > $METASMITH_WORKSPACE > default."""
    if workspace is not None:
        p = Path(workspace)
    elif os.environ.get("METASMITH_WORKSPACE"):
        p = Path(os.environ["METASMITH_WORKSPACE"])
    else:
        p = DEFAULT_WORKSPACE
    p.mkdir(parents=True, exist_ok=True)
    return p


def task_path(workspace: str | Path | None, task_key: str) -> Path:
    return resolve_workspace(workspace) / task_key


def save_task(workspace: str | Path | None, task: WorkflowTask) -> str:
    """Persist a WorkflowTask under workspace/<key>/; return the key."""
    ws = resolve_workspace(workspace)
    key = task.GetKey()
    dest = ws / key
    dest.mkdir(parents=True, exist_ok=True)
    task.SaveAs(Source.FromLocal(dest))
    return key


def load_task(workspace: str | Path | None, task_key: str) -> WorkflowTask:
    p = task_path(workspace, task_key)
    assert p.exists(), f"task [{task_key}] not found in workspace [{resolve_workspace(workspace)}]"
    return WorkflowTask.Load(p)


def list_tasks(workspace: str | Path | None) -> list[dict]:
    ws = resolve_workspace(workspace)
    results: list[dict] = []
    for entry in sorted(ws.iterdir()):
        if not entry.is_dir():
            continue
        info: dict = {"task_key": entry.name, "path": str(entry)}
        try:
            task = WorkflowTask.Load(entry)
            info["ok"] = task.ok
            info["step_count"] = len(task.plan.steps)
        except Exception as exc:
            info["error"] = str(exc)
        results.append(info)
    return results


def delete_task(workspace: str | Path | None, task_key: str) -> dict:
    p = task_path(workspace, task_key)
    assert p.exists(), f"task [{task_key}] not in workspace"
    shutil.rmtree(p)
    return {"task_key": task_key, "deleted": True}
