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
    docs_dir: Path
    tutorial_rel: str
    arm: Arm = DEFAULT_ARM


@dataclass(frozen=True)
class VerifyContext:
    sandbox: Path
    agent_env: dict[str, str]
    metasmith_env_name: str
    installed_env_path: Path
    arm: Arm = DEFAULT_ARM


@dataclass(frozen=True)
class GoldenCheck:
    png_glob: str
    table_glob: str
    min_png_bytes: int
    required_columns: tuple[str, ...]
    min_rows: int


def compose_prompt(shared_block: str, arm: Arm) -> str:
    preamble = arm.preamble.strip()
    if not preamble:
        return shared_block
    return f"{preamble}\n\n{shared_block}"


@runtime_checkable
class Scenario(Protocol):
    name: str
    tutorial_path: str
    expected_artifact_globs: list[str]
    expected_trace: tuple[str, str] | None
    timeout_s: float

    def build_prompt(self, ctx: PromptContext) -> str:
        ...

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        ...


def _self_report_failures(result: LoopResult) -> list[str]:
    if result.outcome in (LoopOutcome.DONE, LoopOutcome.SUBMITTED):
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


def _golden_content_failures(sandbox: Path, check: GoldenCheck) -> list[str]:
    failures: list[str] = []

    pngs = glob.glob(str(sandbox / check.png_glob), recursive=True)
    if pngs:
        biggest = max(pngs, key=lambda p: Path(p).stat().st_size)
        size = Path(biggest).stat().st_size
        if size < check.min_png_bytes:
            failures.append(
                f"final PNG {Path(biggest).name} is {size} B "
                f"(< {check.min_png_bytes} B min — likely an empty/stub plot)"
            )

    tsvs = glob.glob(str(sandbox / check.table_glob), recursive=True)
    if not tsvs:
        failures.append(f"no results table matched {check.table_glob!r} in sandbox")
        return failures
    enrichment: Path | None = None
    closest: tuple[list[str], Path] | None = None
    for cand in sorted(tsvs):
        try:
            lines = Path(cand).read_text().splitlines()
        except OSError:
            continue
        header = lines[0].split("\t") if lines else []
        missing = [c for c in check.required_columns if c not in header]
        if not missing:
            enrichment = Path(cand)
            break
        if closest is None or len(missing) < len(closest[0]):
            closest = (missing, Path(cand))
    if enrichment is None:
        miss, path = closest if closest else ([], None)
        failures.append(
            f"no results table has the enrichment schema; closest "
            f"({path.name if path else '?'}) missing {miss}"
        )
        return failures
    lines = enrichment.read_text().splitlines()
    n_rows = sum(1 for ln in lines[1:] if ln.strip())
    if n_rows < check.min_rows:
        failures.append(
            f"results table {enrichment.name} has {n_rows} data rows "
            f"(< {check.min_rows} min — enrichment produced nothing)"
        )
    return failures


def standard_verify(
    vctx: VerifyContext,
    result: LoopResult,
    *,
    artifact_globs: list[str],
    expected_trace: tuple[str, str] | None,
    golden_check: GoldenCheck | None = None,
) -> list[str]:
    fails: list[str] = []
    fails.extend(_self_report_failures(result))
    fails.extend(_artifact_failures(vctx.sandbox, artifact_globs))
    if golden_check is not None:
        fails.extend(_golden_content_failures(vctx.sandbox, golden_check))
    if vctx.arm.is_metasmith:
        fails.extend(_trace_failures(vctx, expected_trace, result))
    return fails
