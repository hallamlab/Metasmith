"""Workflow runtime: stage / run / wait / tail / cancel / collect on an agent."""
from __future__ import annotations

from pathlib import Path

from ..models.libraries import Resources, Size, Duration
from ..models.remote import Source, Logistics
from . import workspace as _ws
from .agent import load_agent


def stage(agent_path: str, task_key: str, on_exist: str = "skip", workspace: str | None = None) -> dict:
    agent = load_agent(agent_path)
    task = _ws.load_task(workspace, task_key)
    agent.StageWorkflow(task, on_exist)
    return {"status": "staged", "task_key": task_key, "agent": Path(agent_path).stem}


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
            if "duration_h" in v: kw["duration"] = Duration(hours=v["duration_h"])
            ro[k] = Resources(**kw)

    agent.RunWorkflow(task_key, config_file, params, ro, stub_delay)
    return {"status": "running", "task_key": task_key, "agent": Path(agent_path).stem}


def wait(
    agent_path: str,
    task_key: str,
    timeout_s: float = 3600.0,
    poll_s: float = 5.0,
    run: int | None = None,
    since_mtime: float | None = None,
) -> dict:
    agent = load_agent(agent_path)
    return agent.WaitForWorkflow(
        task_key, timeout_s, poll_s, run, "run completed at", since_mtime,
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
    res = mover.ExecuteTransfers(f"collect.{task_key}", True)
    return {
        "src": src.address,
        "dest": dest.address,
        "completed": [(s.address, d.address) for s, d in res.completed],
        "errors": list(res.errors),
    }


def result_source(agent_path: str, task_key: str) -> dict:
    agent = load_agent(agent_path)
    src = agent.GetResultSource(task_key)
    return {"address": src.address, "type": src.type.name}


def list_presets(agent_path: str) -> dict:
    agent = load_agent(agent_path)
    presets = agent.GetNxfConfigPresets()
    return {name: str(p) for name, p in presets.items()}
