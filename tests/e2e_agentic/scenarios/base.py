"""Scenario protocol — one per scripted agent task.

A scenario knows:
    * which tutorial doc (if any) to point the agent at
    * what artifact(s) and lineage trace count as success
    * how to assemble the per-iteration prompt
    * how to verify the loop result
"""
from __future__ import annotations

import glob
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from ..harness.loop import LoopResult, LoopOutcome
from .arms import Arm, DEFAULT_ARM


@dataclass(frozen=True)
class PromptContext:
    sandbox: Path
    version: str
    image_tag: str
    runtime: str
    docs_dir: Path           # in-sandbox copy of docs
    tutorial_rel: str        # path relative to sandbox/docs/
    # Which env × orchestrator arm this render targets. Defaults to full
    # metasmith (A10) so existing scenarios render exactly as before.
    arm: Arm = DEFAULT_ARM


@dataclass(frozen=True)
class VerifyContext:
    """What scenario verifiers need beyond the sandbox path itself."""
    sandbox: Path
    agent_env: dict[str, str]   # the env the agent's shells ran with
    metasmith_env_name: str     # the conda env name the agent should have created
    installed_env_path: Path    # absolute path to <sandbox>/envs/<metasmith_env_name>
    # The arm being verified. Defaults to metasmith (A10) so the metasmith-only
    # lineage-trace check keeps running for existing scenarios.
    arm: Arm = DEFAULT_ARM


def compose_prompt(shared_block: str, arm: Arm) -> str:
    """Assemble an arm's prompt: shared goal/data/done block + arm preamble.

    The ``shared_block`` is byte-identical across arms (the goal, the data
    layout, and the done/report protocol). The arm's additive ``preamble``
    ("your environment / available tools / reference material") is prepended
    when non-empty. The metasmith arm (A10) has an empty preamble, so its
    prompt is the shared block verbatim — preserving current behavior.

    This is the seam P4 builds the 7 benchmark scenarios on: author one
    shared block per scenario, then ``compose_prompt(shared, ctx.arm)``.
    """
    preamble = arm.preamble.strip()
    if not preamble:
        return shared_block
    return f"{preamble}\n\n{shared_block}"


@runtime_checkable
class Scenario(Protocol):
    name: str
    tutorial_path: str           # relative to docs/source/
    expected_artifact_globs: list[str]
    expected_trace: tuple[str, str] | None
    timeout_s: float

    def build_prompt(self, ctx: PromptContext) -> str:
        ...

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        ...


# ---------------------------------------------------------------------------
# Shared verifier helpers
# ---------------------------------------------------------------------------


def _self_report_failures(result: LoopResult) -> list[str]:
    if result.outcome is LoopOutcome.DONE:
        return []
    if result.outcome is LoopOutcome.GAVE_UP:
        reason = result.terminal_control.reason if result.terminal_control else "(no reason)"
        return [f"agent gave up: {reason}"]
    if result.outcome is LoopOutcome.REPORTED_ISSUE:
        reason = result.terminal_control.reason if result.terminal_control else "(no reason)"
        return [f"agent reported issue: {reason}"]
    if result.outcome is LoopOutcome.OVER_BUDGET:
        return [f"loop exceeded token budget after {result.iterations} iterations "
                f"({result.tokens_used} tokens used)"]
    if result.outcome is LoopOutcome.MAX_ITERS:
        return [f"loop hit max_iters ({result.iterations}) without a checkpoint"]
    return [f"unexpected outcome: {result.outcome}"]


def _artifact_failures(sandbox: Path, globs: list[str]) -> list[str]:
    failures = []
    for pat in globs:
        matches = glob.glob(str(sandbox / pat), recursive=True)
        if not matches:
            failures.append(f"no file matched {pat!r} in sandbox")
    return failures


def _trace_failures(
    vctx: VerifyContext,
    pair: tuple[str, str] | None,
    result: LoopResult,
) -> list[str]:
    if pair is None:
        return []
    from_type, to_type = pair
    if result.terminal_control is None:
        return ["trace check skipped: no terminal control / task_key"]
    results_lib = vctx.sandbox / "workspace" / "results.xgdb"
    if not results_lib.exists():
        return [f"results library not found at {results_lib}; "
                f"agent did not run `metasmith data load-remote`"]
    metasmith_bin = vctx.installed_env_path / "bin" / "metasmith"
    if not metasmith_bin.exists():
        return [f"metasmith not installed at {metasmith_bin}; "
                f"cannot run trace check"]
    cmd = [str(metasmith_bin), "--json", "data", "trace",
           str(results_lib), from_type, to_type]
    r = subprocess.run(cmd, capture_output=True, text=True, env=vctx.agent_env)
    if r.returncode != 0:
        return [f"`metasmith data trace {from_type} {to_type}` failed: {r.stderr.strip()}"]
    if not r.stdout.strip() or r.stdout.strip() in ("[]", "{}", "null"):
        return [f"trace from {from_type!r} to {to_type!r} is empty"]
    return []


def standard_verify(
    vctx: VerifyContext,
    result: LoopResult,
    *,
    artifact_globs: list[str],
    expected_trace: tuple[str, str] | None,
) -> list[str]:
    fails: list[str] = []
    # self-report + artifact-glob checks apply to EVERY arm.
    fails.extend(_self_report_failures(result))
    fails.extend(_artifact_failures(vctx.sandbox, artifact_globs))
    # the lineage-trace check is metasmith-specific (`metasmith data trace`
    # against a results.xgdb): only the metasmith arm produces one. Non-metasmith
    # arms (ad-hoc/mamba/container × ad-hoc/snakemake/nextflow) have no such
    # provenance store, so the trace check is skipped for them.
    if vctx.arm.is_metasmith:
        fails.extend(_trace_failures(vctx, expected_trace, result))
    return fails
