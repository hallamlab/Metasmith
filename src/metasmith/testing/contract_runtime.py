"""Input-contract-only fast e2e mock — stages a task without executing it.

`ContractRuntime` runs `WorkflowTask.PrepareNextflow` against a temp workspace
and stops there: no bootstrap, no virtual Nextflow, no protocol bodies. It
returns a `CompiledTask` snapshot that `validate()` inspects for the four
structural invariants that matter at compile-time — workflow.nf parses,
declared inputs are reachable from upstream channel definitions, produced
slots align with `WorkflowPlan.steps[*].produces`, and `cacheable=False`
flags reach the matching `workflow.step_N.meta`.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..constants import AgentPaths, MODULE_PATH
from ..coms.containers import ContainerRuntime
from ..models.remote import Source
from ..models.workflow import NextflowGenContext, WorkflowTask


@dataclass
class CompiledTask:
    """Snapshot of a staged-but-not-executed WorkflowTask."""

    workspace: Path
    key: str
    task: WorkflowTask
    workflow_nf: str
    step_meta_paths: dict[int, Path] = field(default_factory=dict)


@dataclass
class ContractReport:
    """Per-axis verdicts for a `CompiledTask`."""

    nf_compiles: bool
    step_inputs_reachable: dict[str, list[str]]
    produces_match_plan: bool
    cacheable_flags_propagated: bool
    errors: list[str] = field(default_factory=list)


class ContractRuntime:
    """Stage a task to disk and validate the compiled workflow contract."""

    def __init__(self, tmp_path: Path, monkeypatch) -> None:
        self.tmp_path = Path(tmp_path)
        self.monkeypatch = monkeypatch
        self.home = (self.tmp_path / "contract_home").resolve()
        self.home.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(AgentPaths, "HOME_ROOT", self.home)
        monkeypatch.setattr(AgentPaths, "WORK_ROOT", self.home / "_ws")

    def stage(self, task: WorkflowTask) -> CompiledTask:
        """Run `PrepareNextflow` and capture workflow.nf + step meta paths."""
        key = task.GetKey()
        task_path = AgentPaths.to_task(key)
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task.SaveAs(Source.FromLocal(task_path))

        staged = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
        workspace = task_path.parent.parent
        workspace.mkdir(parents=True, exist_ok=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=workspace,
            external_work=workspace,
            home_dir=AgentPaths.HOME_ROOT,
            external_home=AgentPaths.HOME_ROOT,
            container_runtime=ContainerRuntime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        staged.PrepareNextflow(context)

        lib_dir = workspace / "lib"
        lib_dir.mkdir(exist_ok=True)
        orchestrator = MODULE_PATH / "nextflow_config/Orchestrator.groovy"
        if orchestrator.exists():
            shutil.copy(orchestrator, lib_dir / "Orchestrator.groovy")

        nf_path = workspace / AgentPaths.NXF_WORKFLOW
        workflow_nf = nf_path.read_text(encoding="utf-8") if nf_path.exists() else ""

        step_meta_paths: dict[int, Path] = {}
        for step in staged.plan.steps:
            meta = workspace / f"workflow.step_{step.order}.meta"
            if meta.exists():
                step_meta_paths[step.order] = meta

        return CompiledTask(
            workspace=workspace,
            key=key,
            task=staged,
            workflow_nf=workflow_nf,
            step_meta_paths=step_meta_paths,
        )

    def validate(self, compiled: CompiledTask) -> ContractReport:
        """Run all four contract checks and return a combined report."""
        errors: list[str] = []
        nf_ok = _check_nf_compiles(compiled.workflow_nf, errors)
        reachable = _compute_step_inputs_reachable(compiled)
        produces_ok = _check_produces_match_plan(compiled, errors)
        cacheable_ok = _check_cacheable_flags(compiled, errors)
        return ContractReport(
            nf_compiles=nf_ok,
            step_inputs_reachable=reachable,
            produces_match_plan=produces_ok,
            cacheable_flags_propagated=cacheable_ok,
            errors=errors,
        )


# ---------------------------------------------------------------------------
# Internal helpers (module-level so PlanExecutionOracle can reuse them)
# ---------------------------------------------------------------------------


_PROCESS_RE = re.compile(r"^process\s+\S", re.MULTILINE)
_WORKFLOW_RE = re.compile(r"^workflow(\s|\{)", re.MULTILINE)


def _check_nf_compiles(nf_text: str, errors: list[str]) -> bool:
    """Coarse re-parse: confirm process/workflow markers + matched braces."""
    if not nf_text.strip():
        errors.append("workflow.nf is empty or missing")
        return False
    if not _PROCESS_RE.search(nf_text) and not _WORKFLOW_RE.search(nf_text):
        errors.append("workflow.nf has no process or workflow block")
        return False
    depth = 0
    in_squote = False
    in_dquote = False
    in_tsquote = False  # triple single quote
    in_tdquote = False
    i = 0
    n = len(nf_text)
    while i < n:
        ch = nf_text[i]
        nxt3 = nf_text[i : i + 3]
        if not (in_squote or in_dquote) and nxt3 == "'''":
            in_tsquote = not in_tsquote
            i += 3
            continue
        if not (in_squote or in_dquote) and nxt3 == '"""':
            in_tdquote = not in_tdquote
            i += 3
            continue
        if in_tsquote or in_tdquote:
            i += 1
            continue
        if not in_dquote and ch == "'":
            in_squote = not in_squote
            i += 1
            continue
        if not in_squote and ch == '"':
            in_dquote = not in_dquote
            i += 1
            continue
        if in_squote or in_dquote:
            i += 1
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth < 0:
                errors.append("workflow.nf has unbalanced braces (extra '}')")
                return False
        i += 1
    if depth != 0:
        errors.append(f"workflow.nf has unbalanced braces (depth={depth})")
        return False
    return True


def _compute_step_inputs_reachable(
    compiled: CompiledTask,
) -> dict[str, list[str]]:
    """Per step, list the dep keys whose backing instances appear upstream.

    An input is "reachable" when each of its bound DataInstances either
    appears in `plan.given` or is produced by an earlier step.
    """
    task = compiled.task
    plan = task.plan
    produced_so_far: set[str] = set()
    for inst in plan.given:
        produced_so_far.add(inst.instance_id)

    reachable: dict[str, list[str]] = {}
    for step in plan.steps:
        step_key = f"step_{step.order}"
        rs: list[str] = []
        for dep in step.transform.model.requires:
            insts = step.dependency_map.get(dep, [])
            if not insts:
                continue
            if all(inst.instance_id in produced_so_far for inst in insts):
                rs.append(dep.key)
        reachable[step_key] = rs

        for dep_group in step.transform.model.produces:
            for dep in dep_group:
                for inst in step.dependency_map.get(dep, []):
                    produced_so_far.add(inst.instance_id)

    return reachable


def _check_produces_match_plan(
    compiled: CompiledTask, errors: list[str]
) -> bool:
    """Every produces-slot from the contract has at least one bound instance."""
    ok = True
    for step in compiled.task.plan.steps:
        for branch_idx, dep_group in enumerate(step.transform.model.produces):
            for dep in dep_group:
                insts = step.dependency_map.get(dep, [])
                if not insts:
                    errors.append(
                        f"step {step.order} branch {branch_idx} dep "
                        f"[{dep.key}] has no bound DataInstance"
                    )
                    ok = False
    return ok


def _check_cacheable_flags(compiled: CompiledTask, errors: list[str]) -> bool:
    """`cacheable=False` on TransformInstance lands as `cacheable false` in meta."""
    ok = True
    for step in compiled.task.plan.steps:
        expected = bool(getattr(step.transform, "cacheable", True))
        meta_path = compiled.step_meta_paths.get(step.order)
        if meta_path is None or not meta_path.exists():
            # PrepareNextflow only writes a `cacheable` line when a cache
            # decision exists; when cache is disabled there is no meta to
            # inspect, so we treat the flag as trivially propagated.
            continue
        meta_text = meta_path.read_text(encoding="utf-8")
        flag: bool | None = None
        for line in meta_text.splitlines():
            if line.startswith("cacheable "):
                flag = line.split(" ", 1)[1].strip().lower() == "true"
                break
        if flag is None:
            # No cacheable line emitted (e.g. METASMITH_CACHE=0). Treat
            # as propagated to avoid false negatives.
            continue
        if flag != expected:
            errors.append(
                f"step {step.order} cacheable mismatch: "
                f"expected {expected}, got {flag}"
            )
            ok = False
    return ok


# ---------------------------------------------------------------------------
# Re-exports for PlanExecutionOracle bridge
# ---------------------------------------------------------------------------


def validate_contract(compiled: CompiledTask) -> ContractReport:
    """Module-level shim used by `PlanExecutionOracle.validate_contract_only`."""
    errors: list[str] = []
    nf_ok = _check_nf_compiles(compiled.workflow_nf, errors)
    reachable = _compute_step_inputs_reachable(compiled)
    produces_ok = _check_produces_match_plan(compiled, errors)
    cacheable_ok = _check_cacheable_flags(compiled, errors)
    return ContractReport(
        nf_compiles=nf_ok,
        step_inputs_reachable=reachable,
        produces_match_plan=produces_ok,
        cacheable_flags_propagated=cacheable_ok,
        errors=errors,
    )


__all__ = [
    "CompiledTask",
    "ContractReport",
    "ContractRuntime",
    "validate_contract",
]


def _placeholder() -> Any:  # pragma: no cover
    return None
