"""Workflow runtime: stage / run / wait / tail / cancel / collect on an agent."""
from __future__ import annotations

import csv
import io
import os
from pathlib import Path

from ..constants import AgentPaths
from ..models.libraries import Resources, Size, Duration
from ..models.remote import Source, SourceType, Logistics
from ..coms.terminals import IDLE_TIMEOUT
from . import workspace as _ws
from .agent import load_agent


def stage(
    agent_path: str, task_ref: str, on_exist: str = "skip",
    workspace: str | None = None, idle_timeout: float | None = None,
    rootfs: str | None = None,
) -> dict:
    """Stage a task on an agent. `task_ref` is a task key or a task bundle directory.

    The returned task_key is the task's own key, never the reference that was passed
    in -- every later verb (run/wait/tail/cancel) uses it to address the agent-side
    directory, which is named by the key.

    `idle_timeout` is how long an agent-side step may say nothing before staging
    gives up; None takes the default (see coms.terminals.IDLE_TIMEOUT).

    `rootfs` forces how this task's step images are materialised (`auto`, `sif`
    or `sandbox`), overriding the agent's own tendency for these steps. It is
    compiled into the workspace, so changing it means re-staging.
    """
    agent = load_agent(agent_path)
    task = _ws.load_task(workspace, task_ref)
    # Asked here as well as inside StageWorkflow so the refusal costs no ssh
    # connection: this is the CLI's and the web page's door, and the message is
    # about the recipe, not about the agent.
    task.RefuseIfDeferred()
    agent.StageWorkflow(
        task, on_exist,
        idle_timeout=IDLE_TIMEOUT if idle_timeout is None else idle_timeout,
        rootfs=rootfs,
    )
    return {"status": "staged", "task_key": task.GetKey(), "agent": Path(agent_path).stem}


def run(
    agent_path: str,
    task_key: str,
    config_preset: str | None = None,
    params: dict | None = None,
    resource_overrides: dict | None = None,
    stub_delay: float = 0,
) -> dict:
    agent = load_agent(agent_path)
    config_file = None
    if config_preset:
        presets = agent.GetNxfConfigPresets()
        assert config_preset in presets, (
            f"preset [{config_preset}] not found, available: {list(presets.keys())}"
        )
        config_file = presets[config_preset]

    ro = None
    if resource_overrides:
        ro = {}
        for k, v in resource_overrides.items():
            kw = {}
            if "cpus" in v: kw["cpus"] = v["cpus"]
            if "memory_gb" in v: kw["memory"] = Size.GB(v["memory_gb"])
            if "duration_h" in v:
                d = v["duration_h"]
                # the sentinel the GUI's infinity button sends: "remove the
                # limit", which is not the same as sending no duration at all
                kw["duration"] = (
                    Duration.Unlimited()
                    if isinstance(d, str) and d.strip().lower() == "unlimited"
                    else Duration(hours=float(d))
                )
            # An int key is the only form that selects *one* step; a str is read
            # as a transform name and matches every step running it. Keys arrive
            # from JSON as strings, so a page addressing step 3 was silently
            # asking for "every step of the transform named 3" -- i.e. nothing.
            if isinstance(k, str) and k.lstrip("-").isdigit(): k = int(k)
            ro[k] = Resources(**kw)

    # By keyword: passed positionally this was one argument short, so stub_delay
    # landed in `gpus` and dry run was dead everywhere below the agent API.
    agent.RunWorkflow(
        task_key,
        config_file=config_file,
        params=params,
        resource_overrides=ro,
        stub_delay=stub_delay,
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


# Nextflow's own task vocabulary, folded onto the four states anything
# displaying a run cares about. `other` is deliberate rather than a fallthrough
# to "running": a status this does not know is not evidence of progress.
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
    """Nextflow's `-with-trace` TSV as rows, with a normalised state.

    Every column is kept as written -- the widths, the `%cpu`, the human sizes
    are all nextflow's formatting and re-deriving them here would only be a
    second opinion. What is added is `state` and an integer `exit`, because a
    caller asking which steps died should not have to know that a task can be
    COMPLETED and still exit non-zero under an ignoring error strategy.
    """
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


def read_trace(log_dir: str | Path) -> dict:
    """The trace of a run whose logs are already on this machine.

    A collected run carries its whole log directory, so the common case needs
    no agent and no ssh at all.
    """
    p = Path(log_dir)
    if p.is_dir():
        p = p/AgentPaths.NXF_TRACE_FILE
    if not p.is_file():
        return _trace_envelope([], "local", str(p))
    return _trace_envelope(
        parse_trace(p.read_text(encoding="utf-8", errors="replace")), "local", str(p),
    )


def trace(agent_path: str, task_key: str, run: int | None = None) -> dict:
    """The trace of a run that is still only on the agent."""
    agent = load_agent(agent_path)
    res = agent.ReadWorkflowTrace(task_key, run)
    rows = parse_trace(res["lines"]) if res["exists"] else []
    return _trace_envelope(rows, "agent", res["file"]) | {"run_dir": res["run_dir"]}


def cancel(agent_path: str, task_key: str, timeout_s: float = 30.0) -> dict:
    agent = load_agent(agent_path)
    return agent.CancelWorkflow(task_key, timeout_s)


def list_runs(agent_path: str, task_key: str) -> list[dict]:
    agent = load_agent(agent_path)
    return agent.ListWorkflowRuns(task_key)


def check(task_key: str, run_num: int | None = None) -> dict:
    """Same-machine status + logs check (no agent needed)."""
    from ..agents import CheckWorkflow as _CheckWorkflow
    return _CheckWorkflow(task_key, run_num, quiet=True)


# The results library carries exactly one link that is bookkeeping rather than
# data: the alias naming the newest log directory. It and the timestamped log
# link name the same directory (agents.py, end of RunWorkflow), so following
# both would carry the log tree across twice.
_META_DIR = "_metadata"
_LOG_ALIAS = "logs.latest"
# rsync's wording when `-L` is pointed at a link whose target is gone; it names
# the path and skips the entry rather than landing a broken link.
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
    # A results library publishes its outputs as links back into nextflow's work
    # directory, so a verbatim copy lands as a folder of pointers at a disk the
    # caller does not have. Follow them -- including the per-step logs, which is
    # what makes a collected folder auditable on its own -- and exclude only the
    # alias, recreated below so that it resolves inside the copy.
    res = mover.ExecuteTransfers(
        f"collect.{task_key}", True,
        resolve_symlinks=True, exclude=[f"/{_META_DIR}/{_LOG_ALIAS}"],
    )
    # Asked to follow a link with no target, rsync names it on stderr and skips
    # the entry -- so the output simply is not there, and this line is the only
    # evidence of it. Collected here rather than left in the error list so the
    # one thing a caller has to check is the one thing that means data is
    # missing; a stray stderr line from an ssh banner is not that.
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
            # a stale one from a previous collect names the wrong run
            if alias.is_symlink() or alias.is_file(): alias.unlink()
            if not alias.exists(): alias.symlink_to(logs[-1].name)
        # And belt-and-braces for the links rsync did copy verbatim: anything
        # still a link and still broken is an output that is not there either.
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
