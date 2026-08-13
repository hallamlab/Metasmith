"""Non-agentic checker — executes an agent's *submission* deterministically.

Under the submit/checker model (plan T2) the agent's job ends when it declares
its implementation ready via ``metasmith e2e submit`` (CONTROL.json ``submit``).
The token cost we measure is then *authoring* cost only: the pipeline is run by
this checker, which spends **zero agent tokens** and is fully scripted.

Two submission contracts, keyed by arm:

  * **metasmith arm (A10)** — the submission is a *staged workflow task*. The
    agent ran ``metasmith plan`` + ``metasmith workflow stage AGENT TASK`` and
    reported ``task_key`` + ``agent`` in the submit payload. The checker drives
    the standard lifecycle: ``workflow run`` → ``workflow wait`` (blocks on the
    ``run completed at`` sentinel) → ``workflow collect --dest workspace/results``
    → best-effort ``data load-remote`` of the result library into
    ``workspace/results.xgdb`` so the lineage-trace oracle can run.

  * **baseline arms** — the submission is a *runnable entrypoint* at the path the
    start-state seeded (``workspace/run.sh`` / ``workspace/Snakefile`` /
    ``workspace/main.nf``). The checker runs the fixed command for that file type
    with the sandbox env, publishing to ``workspace/results/``.

``action="validate"`` (test t2) skips execution — the agent's own dry-validate
already wrote the marker the oracle checks; the checker is a no-op success.

The checker shells out to the *same* ``metasmith`` CLI the docs/agent use, with
the *same* spoofed sandbox env (and, on micb0, inside the same bwrap jail for
apptainer). A ``runner`` seam lets unit tests fake the subprocess.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ..scenarios.base import VerifyContext
from .loop import LoopResult

# Where the checker publishes the final artifact (sandbox-relative), matching
# _pipeline.FINAL_RESULTS_REL / the success glob.
_RESULTS_REL = "workspace/results"
#: Lineage library the metasmith trace oracle looks for (see base._trace_failures).
_RESULTS_XGDB_REL = "workspace/results.xgdb"


@dataclass
class StepRecord:
    label: str
    argv: list[str]
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass
class CheckerOutcome:
    ok: bool
    kind: str                       # metasmith_run | baseline_run | validate | skipped
    steps: list[StepRecord] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# subprocess seam
# ---------------------------------------------------------------------------


def _default_runner(argv: list[str], *, cwd: Path, env: dict[str, str],
                    timeout_s: float | None) -> subprocess.CompletedProcess:
    return subprocess.run(
        argv, cwd=str(cwd), env=env, capture_output=True, text=True,
        timeout=timeout_s, check=False,
    )


def _metasmith_bin(vctx: VerifyContext) -> str:
    """Resolve the sandbox metasmith launcher (installed env, else PATH name)."""
    cand = vctx.installed_env_path / "bin" / "metasmith"
    return str(cand) if cand.exists() else "metasmith"


def _format_overrides(resource_overrides: dict[str, dict] | None) -> list[str]:
    """Turn ``{"spades": {"cpus": 8}}`` into ``--override spades=cpus:8`` args."""
    args: list[str] = []
    for step, kv in (resource_overrides or {}).items():
        body = ",".join(f"{k}:{v}" for k, v in kv.items())
        args += ["--override", f"{step}={body}"]
    return args


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def run_checker(
    vctx: VerifyContext,
    result: LoopResult,
    *,
    action: str = "run",
    resource_overrides: dict[str, dict] | None = None,
    timeout_s: float = 3600.0,
    poll_s: float = 10.0,
    runner=None,
) -> CheckerOutcome:
    """Execute the agent's submission. Returns a :class:`CheckerOutcome`.

    ``action`` ∈ {run, validate}. A submission the checker fails to execute is a
    legitimate ``ok=false`` (→ the cell's ``artifact_ok=false``), NOT a harness
    error; the caller keeps that distinction.
    """
    runner = runner or _default_runner
    control = result.terminal_control
    if control is None or not control.is_submission:
        return CheckerOutcome(ok=True, kind="skipped",
                              failures=[] )  # nothing to execute

    if action == "validate":
        # t2: the agent's dry-validate already wrote the marker; nothing to run.
        return CheckerOutcome(ok=True, kind="validate")

    if vctx.arm.is_metasmith:
        return _check_metasmith(vctx, control, resource_overrides, timeout_s,
                                poll_s, runner)
    return _check_baseline(vctx, control, timeout_s, runner)


# ---------------------------------------------------------------------------
# metasmith arm
# ---------------------------------------------------------------------------


def _check_metasmith(vctx, control, resource_overrides, timeout_s, poll_s,
                     runner) -> CheckerOutcome:
    steps: list[StepRecord] = []
    failures: list[str] = []
    msm = _metasmith_bin(vctx)
    sandbox = vctx.sandbox
    env = vctx.agent_env
    task_key = control.task_key
    agent = control.agent

    if not task_key:
        return CheckerOutcome(
            ok=False, kind="metasmith_run",
            failures=["submission missing task_key (metasmith arm expects "
                      "`metasmith e2e submit --key <task_key> --agent <agent>`)"],
        )
    if not agent:
        return CheckerOutcome(
            ok=False, kind="metasmith_run",
            failures=["submission missing agent reference (pass `--agent <path>` "
                      "to `metasmith e2e submit` for the metasmith arm)"],
        )

    def _run(label, argv, tmo) -> StepRecord:
        cp = runner(argv, cwd=sandbox, env=env, timeout_s=tmo)
        rec = StepRecord(label, argv, cp.returncode,
                         (cp.stdout or "")[-4000:], (cp.stderr or "")[-4000:])
        steps.append(rec)
        return rec

    # 1. launch (detached) — fast; overrides give SPAdes its threads (T3).
    run_argv = [msm, "workflow", "run", agent, task_key] + \
        _format_overrides(resource_overrides)
    if not _run("run", run_argv, 300.0).ok:
        failures.append(f"`workflow run` failed: {steps[-1].stderr.strip()[:300]}")
        return CheckerOutcome(ok=False, kind="metasmith_run", steps=steps,
                              failures=failures)

    # 2. block until the completion sentinel.
    wait_argv = [msm, "workflow", "wait", agent, task_key,
                 "--timeout", str(int(timeout_s)), "--poll", str(poll_s)]
    if not _run("wait", wait_argv, timeout_s + 120.0).ok:
        failures.append(f"`workflow wait` failed / timed out: "
                        f"{steps[-1].stderr.strip()[:300]}")
        return CheckerOutcome(ok=False, kind="metasmith_run", steps=steps,
                              failures=failures)

    # 3. collect results into the canonical artifact dir.
    dest = str((sandbox / _RESULTS_REL).resolve())
    (sandbox / _RESULTS_REL).mkdir(parents=True, exist_ok=True)
    collect_argv = [msm, "workflow", "collect", agent, task_key, "--dest", dest]
    if not _run("collect", collect_argv, 900.0).ok:
        failures.append(f"`workflow collect` failed: "
                        f"{steps[-1].stderr.strip()[:300]}")
        return CheckerOutcome(ok=False, kind="metasmith_run", steps=steps,
                              failures=failures)

    # 4. best-effort: materialize the lineage library so the trace oracle passes.
    #    Non-fatal — the PNG glob is the load-bearing check; a missing results.xgdb
    #    surfaces as its own trace failure in standard_verify.
    _load_result_library(vctx, agent, task_key, msm, runner, steps)

    return CheckerOutcome(ok=True, kind="metasmith_run", steps=steps,
                          failures=failures)


def _load_result_library(vctx, agent, task_key, msm, runner, steps) -> None:
    sandbox = vctx.sandbox
    env = vctx.agent_env
    src = runner([msm, "--json", "workflow", "result-source", agent, task_key],
                 cwd=sandbox, env=env, timeout_s=120.0)
    steps.append(StepRecord("result-source", ["workflow", "result-source"],
                            src.returncode, (src.stdout or "")[-2000:],
                            (src.stderr or "")[-2000:]))
    if src.returncode != 0:
        return
    import json as _json
    try:
        addr = _json.loads(src.stdout).get("address")
    except (ValueError, AttributeError):
        addr = None
    if not addr:
        return
    dest = str((sandbox / _RESULTS_XGDB_REL).resolve())
    lr = runner([msm, "data", "load-remote", addr, dest, "--on-exist", "clear"],
                cwd=sandbox, env=env, timeout_s=600.0)
    steps.append(StepRecord("load-remote", ["data", "load-remote"],
                            lr.returncode, (lr.stdout or "")[-2000:],
                            (lr.stderr or "")[-2000:]))


# ---------------------------------------------------------------------------
# baseline arms
# ---------------------------------------------------------------------------


#: How to run each entrypoint kind. The command is fixed per file type so the
#: agent cannot smuggle authoring work into the checker phase.
def _baseline_command(entrypoint: Path) -> list[str] | None:
    name = entrypoint.name
    if name == "Snakefile" or entrypoint.suffix == ".smk":
        return ["snakemake", "-s", str(entrypoint), "--cores", "8",
                "--directory", str(entrypoint.parent)]
    if entrypoint.suffix == ".nf":
        return ["nextflow", "run", str(entrypoint)]
    if entrypoint.suffix == ".sh" or name.endswith(".bash"):
        return ["bash", str(entrypoint)]
    return None


def _check_baseline(vctx, control, timeout_s, runner) -> CheckerOutcome:
    sandbox = vctx.sandbox
    env = dict(vctx.agent_env)
    ep = control.entrypoint
    if not ep:
        return CheckerOutcome(
            ok=False, kind="baseline_run",
            failures=["submission missing entrypoint (baseline arm expects "
                      "`metasmith e2e submit --entrypoint <path>`)"],
        )
    entry = Path(ep)
    if not entry.is_absolute():
        entry = (sandbox / ep).resolve()
    if not entry.exists():
        return CheckerOutcome(
            ok=False, kind="baseline_run",
            failures=[f"submitted entrypoint does not exist: {entry}"],
        )
    cmd = _baseline_command(entry)
    if cmd is None:
        return CheckerOutcome(
            ok=False, kind="baseline_run",
            failures=[f"unrecognized entrypoint kind: {entry.name} "
                      "(expected run.sh | Snakefile | main.nf)"],
        )

    # Export the canonical pipeline env the shell scaffold references, so a
    # run.sh that guards on ${READS_R1:?}/${RESULTS:?} runs unattended.
    results = (sandbox / _RESULTS_REL).resolve()
    results.mkdir(parents=True, exist_ok=True)
    reads = sandbox / "workspace" / "reads"
    env.setdefault("READS_R1", str(reads / "ecoli_R1.fastq.gz"))
    env.setdefault("READS_R2", str(reads / "ecoli_R2.fastq.gz"))
    env.setdefault("RESULTS", str(results))

    cp = runner(cmd, cwd=entry.parent, env=env, timeout_s=timeout_s)
    rec = StepRecord("entrypoint", cmd, cp.returncode,
                     (cp.stdout or "")[-4000:], (cp.stderr or "")[-4000:])
    if cp.returncode != 0:
        return CheckerOutcome(
            ok=False, kind="baseline_run", steps=[rec],
            failures=[f"submitted entrypoint exited {cp.returncode}: "
                      f"{rec.stderr.strip()[:300]}"],
        )
    return CheckerOutcome(ok=True, kind="baseline_run", steps=[rec])
