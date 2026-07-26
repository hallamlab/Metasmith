"""The project store: the on-disk layout the GUI reads and writes.

The working directory *is* the project. Everything here is either a native
metasmith artefact (an agent yaml, a data instance library, a task bundle) or a
small GUI-owned record beside it, so nothing the GUI creates is opaque to the
CLI or to a person with a text editor.

    <project>/
      agents/<agent>.yml              native, written by ops.agent.save_agent
      workflows/<workflow>/
        request.yml                   GUI: the filled form
        result.yml                    GUI: task key on success, hints on failure
        input.xgdb/                   the live, editable input library
        task.yml, data/, transforms/  native task bundle (WorkflowTask.SaveAs)
        runs/<run>/
          run.yml                     GUI: agent, task key, state, timestamps
          outputs/                    collected results
      MetasmithLibraries/             the standard library clone
      .metasmith_gui.yml              GUI: archive state

Two decisions are load-bearing. The task bundle sits at the *root* of the
workflow directory, which means the directory is itself a valid task reference --
that is what `metasmith workflow stage AGENT workflows/blazing-ape` addresses.
And `input.xgdb` is not the bundle's copy of itself: the bundle carries a frozen
snapshot under `data/`, while `input.xgdb` stays live and editable, which is why
the two exist side by side and why re-planning is always explicit.

Archive state lives in one project-level file rather than in each object, so
archiving is a single code path and a single write regardless of what is being
archived. It is a timestamp and a list filter, never a directory move: recorded
paths stay valid.
"""
from __future__ import annotations

import os
import shutil
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .names import assert_valid_name, generate_run_name, generate_workflow_name

SCHEMA = "v1"

AGENTS_DIRNAME = "agents"
WORKFLOWS_DIRNAME = "workflows"
RUNS_DIRNAME = "runs"
OUTPUTS_DIRNAME = "outputs"
INPUT_LIBRARY_DIRNAME = "input.xgdb"
STDLIB_DIRNAME = "MetasmithLibraries"

REQUEST_FILE = "request.yml"
RESULT_FILE = "result.yml"
RUN_FILE = "run.yml"
GUI_STATE_FILE = ".metasmith_gui.yml"

# runs that have not reached a terminal state; deletion is refused for these
LIVE_RUN_STATES = {"staging", "staged", "launching", "running"}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _write_yaml(path: Path, data: dict):
    """Write atomically -- a half-written record is worse than a stale one.

    The temp name carries the writer's pid and thread: one fixed `.tmp` beside
    the record makes two concurrent writes fight over one path, and the loser
    fails on the rename with a file-not-found that says nothing about the
    actual cause. The recipe form writes as it is edited, so overlapping
    requests are ordinary rather than exotic.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident():x}.tmp")
    try:
        with open(tmp, "w") as f:
            yaml.dump(data, f, sort_keys=False)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


class ProjectError(Exception):
    """A refusal the GUI should show to the user rather than a bug."""


@dataclass
class WorkflowRecord:
    name: str
    path: Path
    request: dict = field(default_factory=dict)
    result: dict = field(default_factory=dict)
    archived_at: str | None = None

    @property
    def planned(self) -> bool:
        return bool(self.result)

    @property
    def ok(self) -> bool:
        return bool(self.result.get("success"))

    @property
    def task_key(self) -> str | None:
        return self.result.get("task_key")


@dataclass
class RunRecord:
    name: str
    workflow: str
    path: Path
    record: dict = field(default_factory=dict)
    archived_at: str | None = None

    @property
    def state(self) -> str:
        return self.record.get("state", "unknown")

    @property
    def live(self) -> bool:
        return self.state in LIVE_RUN_STATES


class Project:
    """A project directory. Stateless beyond its root: every read hits disk."""

    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()
        self._state_lock = threading.RLock()

    # -- layout ------------------------------------------------------------

    @property
    def agents_dir(self) -> Path:
        return self.root / AGENTS_DIRNAME

    @property
    def workflows_dir(self) -> Path:
        return self.root / WORKFLOWS_DIRNAME

    @property
    def stdlib_dir(self) -> Path:
        return self.root / STDLIB_DIRNAME

    def agent_path(self, name: str) -> Path:
        assert_valid_name(name, "agent name")
        return self.agents_dir / f"{name}.yml"

    def workflow_path(self, name: str) -> Path:
        assert_valid_name(name, "workflow name")
        return self.workflows_dir / name

    def run_path(self, workflow: str, run: str) -> Path:
        assert_valid_name(run, "run name")
        return self.workflow_path(workflow) / RUNS_DIRNAME / run

    def initialize(self):
        self.agents_dir.mkdir(parents=True, exist_ok=True)
        self.workflows_dir.mkdir(parents=True, exist_ok=True)

    # -- archive state -----------------------------------------------------
    #
    # One file, one lock, one code path for every kind of object.

    def _state_path(self) -> Path:
        return self.root / GUI_STATE_FILE

    def _read_state(self) -> dict:
        state = _read_yaml(self._state_path())
        state.setdefault("schema", SCHEMA)
        archived = state.setdefault("archived", {})
        for kind in ("agents", "workflows", "runs"):
            archived.setdefault(kind, {})
        return state

    def archived_at(self, kind: str, key: str) -> str | None:
        with self._state_lock:
            return self._read_state()["archived"][kind].get(key)

    def set_archived(self, kind: str, key: str, archived: bool) -> str | None:
        with self._state_lock:
            state = self._read_state()
            table = state["archived"][kind]
            if archived:
                table.setdefault(key, utcnow())
            else:
                table.pop(key, None)
            _write_yaml(self._state_path(), state)
            return table.get(key)

    def _forget_archive(self, kind: str, key: str):
        with self._state_lock:
            state = self._read_state()
            if state["archived"][kind].pop(key, None) is not None:
                _write_yaml(self._state_path(), state)

    # -- agents ------------------------------------------------------------

    def agent_names(self, include_archived: bool = False) -> list[str]:
        if not self.agents_dir.is_dir():
            return []
        names = sorted(p.stem for p in self.agents_dir.glob("*.yml"))
        if include_archived:
            return names
        return [n for n in names if self.archived_at("agents", n) is None]

    def agent_exists(self, name: str) -> bool:
        return self.agent_path(name).is_file()

    def rename_agent(self, name: str, new_name: str) -> dict:
        """Move an agent's file, which is its name, and take its runs with it.

        Unlike a workflow -- whose directory *is* the task bundle a run stages
        from, and so cannot move once a plan is keyed to it -- an agent is one
        yaml that nothing points into. What does point *at* it is a run record,
        by name, and three things read that back: an agent's run list, the tail,
        and the cancel. So the rename re-points them rather than refusing, which
        is what makes correcting a made-up name an ordinary edit.

        `Path.rename` overwrites silently on posix, so the destination is
        reserved with an exclusive create first: two requests racing here would
        otherwise both pass the existence check and one agent would vanish.
        """
        src = self.agent_path(name)
        if not src.is_file():
            raise ProjectError(f"no agent named [{name}]")
        assert_valid_name(new_name, "agent name")
        if new_name == name:
            return {"name": name, "renamed": False, "runs_repointed": []}
        dest = self.agent_path(new_name)
        try:
            with open(dest, "x"):
                pass
        except FileExistsError:
            raise ProjectError(f"agent [{new_name}] already exists") from None
        src.rename(dest)
        if self.archived_at("agents", name) is not None:
            self._forget_archive("agents", name)
            self.set_archived("agents", new_name, True)
        repointed = []
        for rec in self.list_runs(include_archived=True):
            if rec.record.get("agent") != name:
                continue
            self.update_run(rec.workflow, rec.name, {"agent": new_name})
            repointed.append(rec.name)
        return {"name": new_name, "renamed": True, "runs_repointed": repointed}

    def delete_agent(self, name: str) -> dict:
        """Delete an agent, or archive it if any run still points at it.

        A run record names its agent, and a run whose agent has vanished cannot
        be tailed, cancelled, or collected -- so the agent outlives its runs.
        """
        path = self.agent_path(name)
        if not path.is_file():
            raise ProjectError(f"no agent named [{name}]")
        dependents = [r.name for r in self.list_runs(include_archived=True)
                      if r.record.get("agent") == name]
        if dependents:
            self.set_archived("agents", name, True)
            return {
                "name": name, "action": "archived",
                "reason": f"{len(dependents)} run(s) still reference this agent",
                "dependents": dependents,
            }
        path.unlink()
        self._forget_archive("agents", name)
        return {"name": name, "action": "deleted"}

    # -- workflows ---------------------------------------------------------

    def workflow_names(self, include_archived: bool = False) -> list[str]:
        if not self.workflows_dir.is_dir():
            return []
        names = sorted(
            p.name for p in self.workflows_dir.iterdir()
            if p.is_dir() and (p / REQUEST_FILE).is_file()
        )
        if include_archived:
            return names
        return [n for n in names if self.archived_at("workflows", n) is None]

    def read_workflow(self, name: str) -> WorkflowRecord:
        path = self.workflow_path(name)
        if not (path / REQUEST_FILE).is_file():
            raise ProjectError(f"no workflow named [{name}]")
        return WorkflowRecord(
            name=name,
            path=path,
            request=_read_yaml(path / REQUEST_FILE),
            result=_read_yaml(path / RESULT_FILE),
            archived_at=self.archived_at("workflows", name),
        )

    def list_workflows(self, include_archived: bool = False) -> list[WorkflowRecord]:
        # deliberately reads only the two small records: listing must never
        # deserialise a plan.
        return [self.read_workflow(n) for n in self.workflow_names(include_archived)]

    def create_workflow(self, name: str | None = None, request: dict | None = None) -> WorkflowRecord:
        self.initialize()
        if name is None:
            name = generate_workflow_name(taken=self.workflow_names(include_archived=True))
        assert_valid_name(name, "workflow name")
        path = self.workflow_path(name)
        if path.exists():
            raise ProjectError(f"workflow [{name}] already exists")
        path.mkdir(parents=True)
        record = {
            "schema": SCHEMA,
            "name": name,
            "created_at": utcnow(),
            "sample_type": None,
            "target_types": [],
            "transform_libraries": [],
            "resource_libraries": [],
            "input_library": INPUT_LIBRARY_DIRNAME,
            "forked_from": None,
        } | (request or {})
        record["name"] = name
        _write_yaml(path / REQUEST_FILE, record)
        return self.read_workflow(name)

    def write_request(self, name: str, request: dict) -> WorkflowRecord:
        wf = self.read_workflow(name)
        merged = wf.request | request
        # the directory is the name; the recipe form cannot move it -- that is
        # `rename_workflow`, which has conditions this path does not check
        merged["name"] = name
        _write_yaml(wf.path / REQUEST_FILE, merged)
        return self.read_workflow(name)

    def rename_workflow(self, name: str, new_name: str) -> WorkflowRecord:
        """Move a workflow's directory, which is its name.

        Only before it has generated. The directory *is* the task bundle once a
        plan lands in it, and staging a run resolves the workflow by that path --
        so renaming afterwards would strand the bundle under a name nothing
        points at any more. `fork` is the deliberate way to get a copy under a
        new name later, and it says out loud that it discards cache reuse.

        Names are made up for you at create time, so this exists to correct one
        while the workflow is still empty. The archive mark is keyed by name and
        moves with the directory.
        """
        wf = self.read_workflow(name)
        assert_valid_name(new_name, "workflow name")
        if new_name == name:
            return wf
        if wf.planned:
            raise ProjectError(
                f"[{name}] has already generated; its plan is keyed to this "
                f"directory. Fork it to get a copy under a new name."
            )
        runs = self.list_runs(workflow=name, include_archived=True)
        if runs:
            raise ProjectError(
                f"[{name}] has {len(runs)} run(s) belonging to it and cannot be renamed"
            )
        dest = self.workflow_path(new_name)
        if dest.exists():
            raise ProjectError(f"workflow [{new_name}] already exists")
        try:
            wf.path.rename(dest)
        except OSError as exc:
            # the check above is not a lock -- two requests can both pass it, and
            # the loser must be refused rather than raise out of the route
            raise ProjectError(f"could not rename [{name}] to [{new_name}]: {exc}") from exc
        if self.archived_at("workflows", name) is not None:
            self._forget_archive("workflows", name)
            self.set_archived("workflows", new_name, True)
        record = _read_yaml(dest / REQUEST_FILE)
        record["name"] = new_name
        _write_yaml(dest / REQUEST_FILE, record)
        return self.read_workflow(new_name)

    def write_result(self, name: str, result: dict) -> WorkflowRecord:
        wf = self.read_workflow(name)
        _write_yaml(wf.path / RESULT_FILE, {"schema": SCHEMA, "generated_at": utcnow()} | result)
        return self.read_workflow(name)

    def input_library_path(self, name: str) -> Path:
        wf = self.read_workflow(name)
        return wf.path / wf.request.get("input_library", INPUT_LIBRARY_DIRNAME)

    def delete_workflow(self, name: str) -> dict:
        wf = self.read_workflow(name)
        runs = self.list_runs(workflow=name, include_archived=True)
        if runs:
            self.set_archived("workflows", name, True)
            return {
                "name": name, "action": "archived",
                "reason": f"{len(runs)} run(s) belong to this workflow",
                "dependents": [r.name for r in runs],
            }
        shutil.rmtree(wf.path)
        self._forget_archive("workflows", name)
        return {"name": name, "action": "deleted"}

    # -- runs --------------------------------------------------------------

    def runs_dir(self, workflow: str) -> Path:
        return self.workflow_path(workflow) / RUNS_DIRNAME

    def run_names(self, workflow: str) -> list[str]:
        d = self.runs_dir(workflow)
        if not d.is_dir():
            return []
        return sorted(p.name for p in d.iterdir() if p.is_dir() and (p / RUN_FILE).is_file())

    def read_run(self, workflow: str, run: str) -> RunRecord:
        path = self.run_path(workflow, run)
        if not (path / RUN_FILE).is_file():
            raise ProjectError(f"no run named [{run}] in workflow [{workflow}]")
        return RunRecord(
            name=run,
            workflow=workflow,
            path=path,
            record=_read_yaml(path / RUN_FILE),
            archived_at=self.archived_at("runs", f"{workflow}/{run}"),
        )

    def list_runs(self, workflow: str | None = None, include_archived: bool = False) -> list[RunRecord]:
        workflows = [workflow] if workflow else self.workflow_names(include_archived=True)
        out: list[RunRecord] = []
        for wf in workflows:
            for run in self.run_names(wf):
                rec = self.read_run(wf, run)
                if not include_archived and rec.archived_at is not None:
                    continue
                out.append(rec)
        # newest first: the Runs view is a log, not a directory
        out.sort(key=lambda r: r.record.get("created_at", ""), reverse=True)
        return out

    def create_run(self, workflow: str, record: dict) -> RunRecord:
        wf = self.read_workflow(workflow)
        name = generate_run_name(workflow, taken=self.run_names(workflow))
        path = self.runs_dir(workflow) / name
        (path / OUTPUTS_DIRNAME).mkdir(parents=True)
        body = {
            "schema": SCHEMA,
            "name": name,
            "workflow": workflow,
            "created_at": utcnow(),
            "state": "staging",
            "task_key": wf.task_key,
        } | record
        body["name"] = name
        body["workflow"] = workflow
        _write_yaml(path / RUN_FILE, body)
        return self.read_run(workflow, name)

    def update_run(self, workflow: str, run: str, patch: dict) -> RunRecord:
        rec = self.read_run(workflow, run)
        _write_yaml(rec.path / RUN_FILE, rec.record | patch)
        return self.read_run(workflow, run)

    def outputs_path(self, workflow: str, run: str) -> Path:
        return self.run_path(workflow, run) / OUTPUTS_DIRNAME

    def delete_run(self, workflow: str, run: str) -> dict:
        """Runs delete outright -- but never while live.

        The GUI is not the run's parent process; Nextflow is detached on the
        agent. Deleting the record while it runs orphans that process with
        nothing left pointing at it, so cancel first.
        """
        rec = self.read_run(workflow, run)
        if rec.live:
            raise ProjectError(
                f"run [{run}] is {rec.state}; cancel it before deleting, "
                f"otherwise the workflow keeps running on the agent with nothing tracking it"
            )
        shutil.rmtree(rec.path)
        self._forget_archive("runs", f"{workflow}/{run}")
        return {"name": run, "workflow": workflow, "action": "deleted"}

    # -- live runs ---------------------------------------------------------

    def live_runs(self) -> list[RunRecord]:
        """Runs the server should re-attach to on startup.

        Launch is fire-and-forget, so a restarted server has no child process to
        wait on -- these records are the only thing that knows a run exists.
        """
        return [r for r in self.list_runs(include_archived=True) if r.live]
