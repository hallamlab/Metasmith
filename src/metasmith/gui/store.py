from __future__ import annotations

import os
import shutil
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .names import assert_valid_name, generate_run_name, generate_workflow_name

try:
    _YamlLoader = yaml.CSafeLoader
    _YamlDumper = yaml.CSafeDumper
except AttributeError:
    _YamlLoader = yaml.SafeLoader
    _YamlDumper = yaml.SafeDumper

SCHEMA = "v1"

AGENTS_DIRNAME = "agents"
WORKFLOWS_DIRNAME = "workflows"
RUNS_DIRNAME = "runs"
OUTPUTS_DIRNAME = "outputs"
INPUT_LIBRARY_DIRNAME = "input.xgdb"
STDLIB_DIRNAME = "MetasmithLibraries"
CACHE_DIRNAME = ".cache"

REQUEST_FILE = "request.yml"
RESULT_FILE = "result.yml"
OVERRIDES_FILE = "overrides.yml"
PRESET_FILE = "preset.nf"
RUN_FILE = "run.yml"
GUI_STATE_FILE = ".metasmith_gui.yml"

# `cancelling` is live: the cancel returned survivors, so work is still on the
# agent and the run must not be deletable.
LIVE_RUN_STATES = {"staging", "staged", "launching", "running", "cancelling"}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        return {}
    with open(path) as f:
        return yaml.load(f, Loader=_YamlLoader) or {}


def _write_yaml(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident():x}.tmp")
    try:
        with open(tmp, "w") as f:
            yaml.dump(data, f, Dumper=_YamlDumper, sort_keys=False)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


class ProjectError(Exception):
    pass
@dataclass
class WorkflowRecord:
    name: str
    path: Path
    request: dict = field(default_factory=dict)
    result: dict = field(default_factory=dict)
    overrides: dict = field(default_factory=dict)
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
    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()
        self._state_lock = threading.RLock()


    @property
    def agents_dir(self) -> Path:
        return self.root / AGENTS_DIRNAME

    @property
    def workflows_dir(self) -> Path:
        return self.root / WORKFLOWS_DIRNAME

    @property
    def stdlib_dir(self) -> Path:
        return self.root / STDLIB_DIRNAME

    @property
    def cache_dir(self) -> Path:
        return self.root / CACHE_DIRNAME

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


    def _state_path(self) -> Path:
        return self.root / GUI_STATE_FILE

    def _read_state(self) -> dict:
        state = _read_yaml(self._state_path())
        state.setdefault("schema", SCHEMA)
        archived = state.setdefault("archived", {})
        for kind in ("agents", "workflows", "runs"):
            archived.setdefault(kind, {})
        state.setdefault("agent_names", {})
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


    def agent_naming(self, name: str) -> dict | None:
        with self._state_lock:
            return self._read_state()["agent_names"].get(name)

    def set_agent_naming(self, name: str, prefix: str, sort_name: str) -> dict:
        with self._state_lock:
            state = self._read_state()
            record = {"prefix": prefix, "sort_name": sort_name}
            state["agent_names"][name] = record
            _write_yaml(self._state_path(), state)
            return record

    def forget_agent_naming(self, name: str):
        with self._state_lock:
            state = self._read_state()
            if state["agent_names"].pop(name, None) is not None:
                _write_yaml(self._state_path(), state)


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
        naming = self.agent_naming(name)
        if naming is not None:
            self.forget_agent_naming(name)
            self.set_agent_naming(new_name, naming["prefix"], naming["sort_name"])
        repointed = []
        for rec in self.list_runs(include_archived=True):
            if rec.record.get("agent") != name:
                continue
            self.update_run(rec.workflow, rec.name, {"agent": new_name})
            repointed.append(rec.name)
        return {"name": new_name, "renamed": True, "runs_repointed": repointed}

    def delete_agent(self, name: str) -> dict:
        path = self.agent_path(name)
        if not path.is_file():
            raise ProjectError(f"no agent named [{name}]")
        dependents = [r.name for r in self.list_runs(include_archived=True)
                      if r.record.get("agent") == name]
        if self.archived_at("agents", name) is None:
            self.set_archived("agents", name, True)
            return {
                "name": name, "action": "archived",
                "reason": (
                    f"{len(dependents)} run(s) still reference this agent"
                    if dependents else
                    "archived rather than deleted; delete it again to remove it for good"
                ),
                "dependents": dependents,
            }
        if dependents:
            raise ProjectError(
                f"agent [{name}] is named by {len(dependents)} run(s) "
                f"({', '.join(dependents[:3])}{'…' if len(dependents) > 3 else ''}); "
                f"a run whose agent is gone cannot be tailed, cancelled or collected"
            )
        path.unlink()
        self._forget_archive("agents", name)
        self.forget_agent_naming(name)
        return {"name": name, "action": "deleted"}


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
            overrides=_read_yaml(path / OVERRIDES_FILE),
            archived_at=self.archived_at("workflows", name),
        )

    def list_workflows(self, include_archived: bool = False) -> list[WorkflowRecord]:
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
        merged["name"] = name
        _write_yaml(wf.path / REQUEST_FILE, merged)
        return self.read_workflow(name)

    def rename_workflow(self, name: str, new_name: str) -> WorkflowRecord:
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

    def write_overrides(self, name: str, overrides: dict) -> WorkflowRecord:
        wf = self.read_workflow(name)
        _write_yaml(wf.path / OVERRIDES_FILE, overrides)
        return self.read_workflow(name)

    def preset_path(self, name: str) -> Path:
        return self.workflow_path(name) / PRESET_FILE

    def read_preset(self, name: str) -> str | None:
        p = self.preset_path(name)
        return p.read_text() if p.is_file() else None

    def write_preset(self, name: str, content: str):
        p = self.preset_path(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{p.name}.{os.getpid()}.{threading.get_ident():x}.tmp")
        try:
            tmp.write_text(content)
            os.replace(tmp, p)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def input_library_path(self, name: str) -> Path:
        wf = self.read_workflow(name)
        return wf.path / wf.request.get("input_library", INPUT_LIBRARY_DIRNAME)

    def delete_workflow(self, name: str) -> dict:
        wf = self.read_workflow(name)
        runs = self.list_runs(workflow=name, include_archived=True)
        if self.archived_at("workflows", name) is None:
            self.set_archived("workflows", name, True)
            return {
                "name": name, "action": "archived",
                "reason": (
                    f"{len(runs)} run(s) belong to this workflow"
                    if runs else
                    "archived rather than deleted; delete it again to remove it for good"
                ),
                "dependents": [r.name for r in runs],
            }
        live = [r.name for r in runs if r.live]
        if live:
            raise ProjectError(
                f"workflow [{name}] has {len(live)} live run(s) ({', '.join(live[:3])}); "
                f"cancel them before deleting it"
            )
        shutil.rmtree(wf.path)
        self._forget_archive("workflows", name)
        for r in runs:
            self._forget_archive("runs", f"{name}/{r.name}")
        return {"name": name, "action": "deleted", "dependents": [r.name for r in runs]}


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
        out.sort(key=lambda r: r.record.get("created_at", ""), reverse=True)
        return out

    def create_run(self, workflow: str, record: dict) -> RunRecord:
        wf = self.read_workflow(workflow)
        # Every run of a workflow stages into the same task_key workspace on the
        # agent -- one PID.lock, one process group. A second run launched while
        # the first is still live doesn't run alongside it, it silently takes
        # over that workspace, so cancelling either run record afterwards kills
        # whatever the agent is actually running, not necessarily the one whose
        # button was clicked.
        live = [r.name for r in self.list_runs(workflow) if r.live]
        if live:
            raise ProjectError(
                f"workflow [{workflow}] already has a live run ({', '.join(live[:3])}); "
                f"cancel it before starting another"
            )
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
        rec = self.read_run(workflow, run)
        key = f"{workflow}/{run}"
        if rec.live:
            raise ProjectError(
                f"run [{run}] is {rec.state}; cancel it before deleting, "
                f"otherwise the workflow keeps running on the agent with nothing tracking it"
            )
        if self.archived_at("runs", key) is None:
            self.set_archived("runs", key, True)
            return {
                "name": run, "workflow": workflow, "action": "archived",
                "reason": "archived rather than deleted; delete it again to remove it for good",
            }
        shutil.rmtree(rec.path)
        self._forget_archive("runs", key)
        return {"name": run, "workflow": workflow, "action": "deleted"}


    def live_runs(self) -> list[RunRecord]:
        return [r for r in self.list_runs(include_archived=True) if r.live]
