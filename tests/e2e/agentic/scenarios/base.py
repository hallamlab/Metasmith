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


@dataclass(frozen=True)
class GoldenCheck:
    """Tolerant content oracle for the final pipeline artifacts (T3).

    Checks the produced PNG is a non-empty real image and the results TSV has the
    expected clusterProfiler columns + at least ``min_rows`` data rows — SHAPE +
    a content bound, not bit-identity to golden. Pipeline scenarios pass one;
    non-pipeline scenarios (t1 install) leave it ``None`` and the check is skipped.
    """
    png_glob: str
    table_glob: str
    min_png_bytes: int
    required_columns: tuple[str, ...]
    min_rows: int


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
    # DONE (legacy, artifact produced in-loop) and SUBMITTED (checker executes
    # the submission) are both non-failures at the self-report stage — the
    # artifact-glob + trace checks decide the cell.
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
    """Tolerant golden content oracle: the produced PNG is a real non-empty image
    and the results TSV has the expected columns + >=min_rows data rows.

    Absence of the PNG is already reported by ``_artifact_failures`` on the same
    glob, so here a present-but-tiny PNG (stub / 0-byte) is the interesting case.
    The TSV is checked here in full (it is not a hard artifact-glob).
    """
    failures: list[str] = []

    pngs = glob.glob(str(sandbox / check.png_glob), recursive=True)
    if pngs:  # absence handled by _artifact_failures
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
    # A scenario may emit MORE than one TSV (t6 adds abricate.tsv alongside the
    # enrichment table). Identify the enrichment table by its schema — the TSV
    # whose header carries the required clusterProfiler columns — rather than by
    # size/name, so an extra tool report never shadows the real check.
    enrichment: Path | None = None
    closest: tuple[list[str], Path] | None = None   # (missing, path) best partial
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
        # remember the closest miss for a useful message if none fully match
        if closest is None or len(missing) < len(closest[0]):
            closest = (missing, Path(cand))
    if enrichment is None:  # no TSV had the full enrichment schema
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
    # self-report + artifact-glob checks apply to EVERY arm.
    fails.extend(_self_report_failures(result))
    fails.extend(_artifact_failures(vctx.sandbox, artifact_globs))
    # golden content sanity check (pipeline scenarios only): the produced final
    # artifacts are real + correctly-shaped, not just present. Arm-independent.
    if golden_check is not None:
        fails.extend(_golden_content_failures(vctx.sandbox, golden_check))
    # the lineage-trace check is metasmith-specific (`metasmith data trace`
    # against a results.xgdb): only the metasmith arm produces one. Non-metasmith
    # arms (ad-hoc/mamba/container × ad-hoc/snakemake/nextflow) have no such
    # provenance store, so the trace check is skipped for them.
    if vctx.arm.is_metasmith:
        fails.extend(_trace_failures(vctx, expected_trace, result))
    return fails
