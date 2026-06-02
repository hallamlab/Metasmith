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


@dataclass(frozen=True)
class PromptContext:
    sandbox: Path
    version: str
    image_tag: str
    runtime: str
    docs_dir: Path           # in-sandbox copy of docs
    tutorial_rel: str        # path relative to sandbox/docs/


@dataclass(frozen=True)
class VerifyContext:
    """What scenario verifiers need beyond the sandbox path itself."""
    sandbox: Path
    agent_env: dict[str, str]   # the env the agent's shells ran with
    metasmith_env_name: str     # the conda env name the agent should have created
    installed_env_path: Path    # absolute path to <sandbox>/envs/<metasmith_env_name>


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
    fails.extend(_self_report_failures(result))
    fails.extend(_artifact_failures(vctx.sandbox, artifact_globs))
    fails.extend(_trace_failures(vctx, expected_trace, result))
    return fails
