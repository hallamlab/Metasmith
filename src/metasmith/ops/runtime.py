from __future__ import annotations

import csv
import io
import os
from pathlib import Path

from ..constants import AgentPaths
from ..logging import Log
from ..models.libraries import Resources, Size, Duration
from ..models.lineage import InvocationEvent
from ..models.remote import Source, SourceType, Logistics
from ..models.workflow import NextflowProcessName
from ..coms.terminals import IDLE_TIMEOUT
from . import workspace as _ws
from .agent import load_agent


def stage(
    agent_path: str, task_ref: str, on_exist: str = "skip",
    workspace: str | None = None, idle_timeout: float | None = None,
    rootfs: str | None = None,
) -> dict:
    agent = load_agent(agent_path)
    task = _ws.load_task(workspace, task_ref)
    task.RefuseIfDeferred()
    agent.StageWorkflow(
        task, on_exist,
        idle_timeout=IDLE_TIMEOUT if idle_timeout is None else idle_timeout,
        rootfs=rootfs,
    )
    return {"status": "staged", "task_key": task.GetKey(), "agent": Path(agent_path).stem}


def materialise(agent_path: str, task_key: str, force: bool = False) -> dict:
    # Fetch every tool image a staged task needs, onto the agent's host.
    #
    # For a cluster whose compute nodes cannot reach a registry: run this from a
    # host that can (the login node), and the images are in the store before any
    # task looks for them. Idempotent -- a second run does nothing -- and it
    # raises if any image could not be fetched, so a caller does not proceed to
    # submit believing the store is complete.
    agent = load_agent(agent_path)
    return agent.MaterialiseImages(task_key, force=force)


def setup_environment(
    agent_path: str, task_key: str, force: bool = False, library: str | None = None,
) -> dict:
    # Prepare the agent's host for a staged task: tool images for a container
    # agent, tool conda envs for a mamba or native one.
    #
    # `library` is where a conda env's recipe is read from -- the library the
    # workflow was planned against -- and falls back to the installed package.
    agent = load_agent(agent_path)
    return agent.SetupEnvironment(task_key, force=force, library=library)


def staged_path(agent_path: str, task_key: str) -> str:
    agent = load_agent(agent_path)
    return str(agent._task_workspace(task_key))


def run(
    agent_path: str,
    task_key: str,
    config_preset: str | None = None,
    config_file: str | None = None,
    params: dict | None = None,
    resource_overrides: dict | None = None,
    stub_delay: float = 0,
    is_local_preset: bool | None = None,
) -> dict:
    agent = load_agent(agent_path)
    if config_file is None and config_preset:
        presets = agent.GetNxfConfigPresets()
        assert config_preset in presets, (
            f"preset [{config_preset}] not found, available: {list(presets.keys())}"
        )
        config_file = presets[config_preset]
    if is_local_preset is None and config_preset:
        is_local_preset = config_preset == "local"

    ro = None
    if resource_overrides:
        ro = {}
        for k, v in resource_overrides.items():
            kw = {}
            if "cpus" in v: kw["cpus"] = v["cpus"]
            if "memory_gb" in v: kw["memory"] = Size.GB(v["memory_gb"])
            if "duration_h" in v:
                d = v["duration_h"]
                kw["duration"] = (
                    Duration.Unlimited()
                    if isinstance(d, str) and d.strip().lower() == "unlimited"
                    else Duration(hours=float(d))
                )
            if isinstance(k, str) and k.lstrip("-").isdigit(): k = int(k)
            ro[k] = Resources(**kw)

    agent.RunWorkflow(
        task_key,
        config_file=Path(config_file) if config_file else None,
        params=params,
        resource_overrides=ro,
        stub_delay=stub_delay,
        is_local_preset=is_local_preset,
    )
    return {"status": "running", "task_key": task_key, "agent": Path(agent_path).stem}


def wait(
    agent_path: str,
    task_key: str,
    timeout_s: float = 3600.0,
    poll_s: float = 5.0,
    run: int | None = None,
    since_mtime: float | None = None,
    grace_s: float = 5.0,
) -> dict:
    agent = load_agent(agent_path)
    return agent.WaitForWorkflow(
        task_key, timeout_s, poll_s, run, "run completed at", since_mtime, grace_s,
    )


def tail(
    agent_path: str,
    task_key: str,
    source: str = "agent",
    lines: int = 50,
    run: int | None = None,
) -> dict:
    agent = load_agent(agent_path)
    return agent.TailWorkflowLog(task_key, source, lines, run)


_TRACE_STATES = {
    "COMPLETED": "done",
    "CACHED":    "done",
    "FAILED":    "failed",
    "ABORTED":   "failed",
    "RUNNING":   "running",
    "SUBMITTED": "running",
    "NEW":       "running",
}


def parse_trace(lines) -> list[dict]:
    if isinstance(lines, str):
        lines = lines.splitlines()
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="\t")
    rows = []
    for raw in reader:
        if not raw.get("name"):
            continue
        row = {k: v for k, v in raw.items() if k is not None}
        status = (row.get("status") or "").strip().upper()
        try:
            code = int(str(row.get("exit", "")).strip())
        except (TypeError, ValueError):
            code = None
        row["exit"] = code
        state = _TRACE_STATES.get(status, "other")
        if state == "done" and code not in (0, None):
            state = "failed"
        row["state"] = state
        rows.append(row)
    return rows


def _trace_envelope(rows: list[dict], source: str, path: str | None) -> dict:
    return {
        "source": source,
        "file": path,
        "tasks": rows,
        "failed": sum(1 for r in rows if r["state"] == "failed"),
        "running": sum(1 for r in rows if r["state"] == "running"),
        "done": sum(1 for r in rows if r["state"] == "done"),
    }


def parse_cache_hits(lines) -> list[dict]:
    # A cache-hit step never becomes a Nextflow process -- codegen splices its
    # cached files straight into the channel instead of invoking it -- so it
    # has no row in Nextflow's own trace file. Reconstruct one from the
    # lineage trace so a cached step reads as done rather than never-ran.
    if isinstance(lines, str):
        lines = lines.splitlines()
    rows = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = InvocationEvent.from_jsonl(line)
        except Exception:
            continue
        if ev is None or ev.status != "hit" or ev.step_order is None:
            continue
        process = NextflowProcessName(ev.step_order, ev.step_name or ev.transform_key)
        rows.append({
            "name": f"{process} (cached)",
            "hash": "",
            "status": "CACHED-METASMITH",
            "exit": 0,
            "state": "done",
        })
    return rows


def _merge_cache_hits(rows: list[dict], hit_lines) -> list[dict]:
    hits = parse_cache_hits(hit_lines)
    if not hits:
        return rows
    known = {r["name"].split(" ")[0] for r in rows}
    return rows + [h for h in hits if h["name"].split(" ")[0] not in known]


def read_trace(log_dir: str | Path) -> dict:
    p = Path(log_dir)
    d = p if p.is_dir() else p.parent
    trace_file = p/AgentPaths.NXF_TRACE_FILE if p.is_dir() else p
    if not trace_file.is_file():
        return _trace_envelope([], "local", str(trace_file))
    rows = parse_trace(trace_file.read_text(encoding="utf-8", errors="replace"))
    hits_file = d/"trace.jsonl"
    if hits_file.is_file():
        rows = _merge_cache_hits(rows, hits_file.read_text(encoding="utf-8", errors="replace"))
    return _trace_envelope(rows, "local", str(trace_file))


def trace(agent_path: str, task_key: str, run: int | None = None) -> dict:
    agent = load_agent(agent_path)
    res = agent.ReadWorkflowTrace(task_key, run)
    rows = parse_trace(res["lines"]) if res["exists"] else []
    try:
        hits = agent.ReadCacheHits(task_key, run)
        if hits["exists"]:
            rows = _merge_cache_hits(rows, hits["lines"])
    except Exception as e:
        Log.Warn(f"failed to read cache-hit trace for [{task_key}]: {e}")
    return _trace_envelope(rows, "agent", res["file"]) | {"run_dir": res["run_dir"]}


def cancel(agent_path: str, task_key: str, timeout_s: float = 30.0) -> dict:
    agent = load_agent(agent_path)
    return agent.CancelWorkflow(task_key, timeout_s)


def ps(agent_path: str, task_key: str, scope: str = "run") -> dict:
    # What this run still has running on the agent host: process group members,
    # anything carrying its METASMITH_RUN token, labelled containers, relay jobs.
    # `scope="workload"` drops the run group and the token scan, leaving what the
    # driver put to work rather than the driver itself.
    agent = load_agent(agent_path)
    return agent.InspectWorkflowProcesses(task_key, scope)


def reap(agent_path: str, task_key: str, passes: int = 3, scope: str = "run") -> dict:
    # Kill what `ps` finds, sweeping until the set is empty or the passes are
    # spent -- a group kill is not atomic against a fan-out still spawning.
    agent = load_agent(agent_path)
    return agent.ReapWorkflow(task_key, passes=passes, scope=scope)


def list_runs(agent_path: str, task_key: str) -> list[dict]:
    agent = load_agent(agent_path)
    return agent.ListWorkflowRuns(task_key)


def check(task_key: str, run_num: int | None = None) -> dict:
    from ..agents import CheckWorkflow as _CheckWorkflow
    return _CheckWorkflow(task_key, run_num, quiet=True)


_META_DIR = "_metadata"
_LOG_ALIAS = "logs.latest"
_NO_REFERENT = "symlink has no referent"


def _dangling_links(root: Path) -> list[Path]:
    out = []
    for here, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            p = Path(here)/name
            if p.is_symlink() and not p.exists():
                out.append(p)
    return sorted(out)


def collect(
    agent_path: str,
    task_key: str,
    dest_uri: str,
    allow_globus: bool = True,
) -> dict:
    agent = load_agent(agent_path)
    src = agent.GetResultSource(task_key, allow_globus=allow_globus, check_exists=False)
    dest = Source.Parse(dest_uri)
    mover = Logistics()
    mover.QueueTransfer(src=src, dest=dest)
    res = mover.ExecuteTransfers(
        f"collect.{task_key}", True,
        resolve_symlinks=True, exclude=[f"/{_META_DIR}/{_LOG_ALIAS}"],
    )
    dangling = [e for e in res.errors if _NO_REFERENT in e]
    out = {
        "src": src.address,
        "dest": dest.address,
        "completed": [(s.address, d.address) for s, d in res.completed],
        "errors": list(res.errors),
        "dangling": dangling,
    }

    dest_path = Path(dest.address) if dest.type == SourceType.DIRECT else None
    if dest_path is not None and dest_path.is_dir():
        meta = dest_path/_META_DIR
        logs = sorted(
            p for p in meta.glob("logs.*")
            if p.is_dir() and p.name != _LOG_ALIAS
        ) if meta.is_dir() else []
        if logs:
            alias = meta/_LOG_ALIAS
            if alias.is_symlink() or alias.is_file(): alias.unlink()
            if not alias.exists(): alias.symlink_to(logs[-1].name)
        out["dangling"] += [
            str(p.relative_to(dest_path)) for p in _dangling_links(dest_path)
        ]
    return out


def result_source(agent_path: str, task_key: str) -> dict:
    agent = load_agent(agent_path)
    src = agent.GetResultSource(task_key)
    return {"address": src.address, "type": src.type.name}


def list_presets(agent_path: str) -> dict:
    agent = load_agent(agent_path)
    presets = agent.GetNxfConfigPresets()
    return {name: str(p) for name, p in presets.items()}
