from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ..scenarios.base import VerifyContext
from .loop import LoopResult

_RESULTS_REL = "workspace/results"
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
    kind: str
    steps: list[StepRecord] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


def _default_runner(argv: list[str], *, cwd: Path, env: dict[str, str],
                    timeout_s: float | None) -> subprocess.CompletedProcess:
    return subprocess.run(
        argv, cwd=str(cwd), env=env, capture_output=True, text=True,
        timeout=timeout_s, check=False,
    )


def _metasmith_bin(vctx: VerifyContext) -> str:
    cand = vctx.installed_env_path / "bin" / "metasmith"
    return str(cand) if cand.exists() else "metasmith"


def _format_overrides(resource_overrides: dict[str, dict] | None) -> list[str]:
    args: list[str] = []
    for step, kv in (resource_overrides or {}).items():
        body = ",".join(f"{k}:{v}" for k, v in kv.items())
        args += ["--override", f"{step}={body}"]
    return args


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
    runner = runner or _default_runner
    control = result.terminal_control
    if control is None or not control.is_submission:
        return CheckerOutcome(ok=True, kind="skipped",
                              failures=[] )

    if action == "validate":
        return CheckerOutcome(ok=True, kind="validate")

    if vctx.arm.is_metasmith:
        return _check_metasmith(vctx, control, resource_overrides, timeout_s,
                                poll_s, runner)
    return _check_baseline(vctx, control, timeout_s, runner)


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

    run_argv = [msm, "workflow", "run", agent, task_key] + \
        _format_overrides(resource_overrides)
    if not _run("run", run_argv, 300.0).ok:
        failures.append(f"`workflow run` failed: {steps[-1].stderr.strip()[:300]}")
        return CheckerOutcome(ok=False, kind="metasmith_run", steps=steps,
                              failures=failures)

    wait_argv = [msm, "workflow", "wait", agent, task_key,
                 "--timeout", str(int(timeout_s)), "--poll", str(poll_s)]
    if not _run("wait", wait_argv, timeout_s + 120.0).ok:
        failures.append(f"`workflow wait` failed / timed out: "
                        f"{steps[-1].stderr.strip()[:300]}")
        return CheckerOutcome(ok=False, kind="metasmith_run", steps=steps,
                              failures=failures)

    dest = str((sandbox / _RESULTS_REL).resolve())
    (sandbox / _RESULTS_REL).mkdir(parents=True, exist_ok=True)
    collect_argv = [msm, "workflow", "collect", agent, task_key, "--dest", dest]
    if not _run("collect", collect_argv, 900.0).ok:
        failures.append(f"`workflow collect` failed: "
                        f"{steps[-1].stderr.strip()[:300]}")
        return CheckerOutcome(ok=False, kind="metasmith_run", steps=steps,
                              failures=failures)

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
