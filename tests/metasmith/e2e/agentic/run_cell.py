from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import subprocess
import sys
import time
import traceback
from pathlib import Path

from tests.metasmith.e2e.agentic.drivers.factory import make_driver
from tests.metasmith.e2e.agentic.harness.cell import (
    DEFAULT_METASMITH_ENV_NAME,
    prepare_sandbox,
)
from tests.metasmith.e2e.agentic.harness.checker import run_checker
from tests.metasmith.e2e.agentic.harness.loop import (
    LoopBudgets,
    LoopOutcome,
    LoopResult,
    ralph_loop,
)
from tests.metasmith.e2e.agentic.install_mock.verify_local_artifacts import (
    InstallContext,
    PreflightError,
    _read_version,
    verify as verify_install,
)
from tests.metasmith.e2e.agentic.scenarios.arms import ARM_BY_ID, ARMS, DEFAULT_ARM
from tests.metasmith.e2e.agentic.scenarios.base import PromptContext, VerifyContext
from tests.metasmith.e2e.agentic.scenarios.benchmark import BENCHMARK_SCENARIOS


_STAMP_COLS = ("model", "effort", "commit")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _test_id(test_name: str) -> int:
    return int(test_name.split("_", 1)[0][1:])


def _test_label(test_name: str) -> str:
    return test_name.split("_", 1)[1]


def _git_commit(project_root: Path) -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=project_root, capture_output=True, text=True,
        )
        if r.returncode == 0:
            return r.stdout.strip()
    except OSError:
        pass
    return ""


def _resolve_install_context(
    project_root: Path, runtime: str, agent: str, *, allow_missing: bool,
) -> InstallContext:
    try:
        return verify_install(project_root, runtime=runtime, agent=agent)
    except PreflightError:
        if not allow_missing:
            raise
        full = _read_version(project_root)
        semver = full.split("+", 1)[0]
        image_tag = f"quay.io/hallamlab/metasmith:{full.replace('+', '-')}"
        sif_path: Path | None = None
        if runtime.upper() == "APPTAINER":
            cand = project_root / "metasmith.sif"
            sif_path = cand if cand.exists() else None
        return InstallContext(
            version=semver,
            project_root=project_root,
            image_tag=image_tag,
            sif_path=sif_path,
            channel_dir=project_root / "conda_build",
            docs_dir=project_root / "docs" / "source",
        )


def _make_scenario(test_name: str, arm, tool: str | None):
    cls = BENCHMARK_SCENARIOS[test_name]
    if test_name == "t1_install":
        if not tool:
            raise SystemExit(
                "t1_install requires --tool (fastp|spades|bakta|"
                "eggnog-mapper|clusterprofiler|abricate)"
            )
        return cls(env_channel=arm.env, tool=tool)
    return cls()


def _read_header(csv_path: Path) -> list[str]:
    with csv_path.open(newline="") as fh:
        return next(csv.reader(fh))


def lookup_experiment_row(
    experiments_csv: Path, *, test_id: int, arm_id: str, rep: int,
    host: str | None = None,
) -> dict[str, str] | None:
    if not experiments_csv.exists():
        return None
    with experiments_csv.open(newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                if int(row["test_id"]) != test_id:
                    continue
                if int(row["replicate"]) != rep:
                    continue
            except (KeyError, ValueError):
                continue
            if row.get("arm") != arm_id:
                continue
            if host is not None and row.get("host") != host:
                continue
            return dict(row)
    return None


def append_result_row(
    results_csv: Path, fieldnames: list[str], row: dict[str, str],
) -> None:
    results_csv.parent.mkdir(parents=True, exist_ok=True)
    new_file = not results_csv.exists() or results_csv.stat().st_size == 0
    with results_csv.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})


def _bool_str(v: bool | None) -> str:
    if v is None:
        return ""
    return "true" if v else "false"


def build_result_row(
    *,
    experiment_fields: list[str],
    matched: dict[str, str] | None,
    test_name: str,
    test_id: int,
    arm,
    rep: int,
    host: str | None,
    model: str | None,
    effort: str | None,
    commit: str,
    status: str,
    result: LoopResult | None,
    artifact_ok: bool | None,
) -> dict[str, str]:
    row: dict[str, str] = {k: "" for k in experiment_fields}
    if matched is not None:
        row.update({k: matched.get(k, "") for k in experiment_fields})
    else:
        row["run_id"] = ""
        row["condition_id"] = f"T{test_id}-{arm.id}"
        row["test_id"] = str(test_id)
        row["test"] = _test_label(test_name)
        row["arm"] = arm.id
        row["env"] = arm.env
        row["orchestrator"] = arm.orchestrator

    row["replicate"] = str(rep)
    if host is not None:
        row["host"] = host

    row["status"] = status
    row["loc_authored"] = ""
    if result is not None:
        row["tokens_in"] = str(result.tokens_in)
        row["tokens_cached"] = str(result.tokens_cached)
        row["tokens_out"] = str(result.tokens_out)
        row["tokens_cache_creation"] = str(result.tokens_cache_creation)
        row["iterations"] = str(result.iterations)
        row["outcome"] = result.outcome.value
    else:
        for c in ("tokens_in", "tokens_cached", "tokens_out",
                  "tokens_cache_creation", "iterations", "outcome", "wall_s"):
            row[c] = ""
    row["artifact_ok"] = _bool_str(artifact_ok)

    row["model"] = model or ""
    row["effort"] = effort or ""
    row["commit"] = commit
    return row


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="run_cell",
        description="Run one token-benchmark cell (arm × test × replicate).",
    )
    ap.add_argument("--arm", required=True, choices=tuple(a.id for a in ARMS),
                    help="env × orchestrator arm (A1..A10).")
    ap.add_argument("--test", required=True, choices=tuple(BENCHMARK_SCENARIOS),
                    help="benchmark scenario id (t1_install..t7_adapt_from_middle).")
    ap.add_argument("--rep", type=int, default=1, help="replicate number (1-based).")
    ap.add_argument("--tool", default=None,
                    help="tool for t1_install (fastp|spades|bakta|"
                         "eggnog-mapper|clusterprofiler|abricate).")

    ap.add_argument("--agent", default="claude", choices=("opencode", "claude"))
    ap.add_argument("--agent-model", default=None)
    ap.add_argument("--agent-effort", default=None)
    ap.add_argument("--host", default=None,
                    help="host label stamped on the result row (and used to "
                         "disambiguate the experiments.csv lookup).")
    ap.add_argument("--runtime", default="DOCKER", choices=("DOCKER", "APPTAINER"))
    ap.add_argument("--dry-run", action="store_true",
                    help="render prompt + build/provision sandbox WITHOUT a "
                         "model call; writes a status=dry result row.")

    ap.add_argument("--max-iters", type=int, default=20)
    ap.add_argument("--max-tokens", type=int, default=None)
    ap.add_argument("--max-tokens-per-iter", type=int, default=200_000)
    ap.add_argument("--max-usd-per-iter", type=float, default=None)
    ap.add_argument("--iter-timeout-s", type=float, default=300.0)

    ap.add_argument("--project-root", type=Path, default=None)
    ap.add_argument("--experiments-csv", type=Path, default=None,
                    help="master run list (read-only). Default: "
                         "<data>/token-benchmark/experiments.csv.")
    ap.add_argument("--results-csv", type=Path, default=None,
                    help="append target (never the master). Default: results.csv "
                         "beside experiments.csv.")
    ap.add_argument("--runs-dir", type=Path, default=None,
                    help="root for transcripts + result.json. Default: "
                         "tests/e2e/agentic/.runs/<ts>.")
    ap.add_argument("--metasmith-env-name", default=DEFAULT_METASMITH_ENV_NAME)
    return ap


_DEFAULT_EXPERIMENTS = Path(
    "/home/tony/agentic_workspace/data/metasmith/token-benchmark/experiments.csv"
)

_GLOBAL_MAX_TOKENS_FALLBACK = 2_000_000


def _effective_max_tokens(cli_value: int | None, scenario, fallback: int) -> int:
    if cli_value is not None:
        return cli_value
    scenario_quota = getattr(scenario, "max_tokens", None)
    return scenario_quota if scenario_quota else fallback


def run(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)

    project_root = (args.project_root or _project_root()).resolve()
    arm = ARM_BY_ID[args.arm]
    test_name = args.test
    test_id = _test_id(test_name)
    rep = args.rep

    experiments_csv = (args.experiments_csv or _DEFAULT_EXPERIMENTS).resolve()
    results_csv = (args.results_csv
                   or experiments_csv.with_name("results.csv")).resolve()

    if experiments_csv.exists():
        experiment_fields = _read_header(experiments_csv)
    else:
        experiment_fields = [
            "run_id", "condition_id", "test_id", "test", "kind", "host", "arm",
            "env", "orchestrator", "provenance", "tool", "detail", "replicate",
            "status", "tokens_in", "tokens_cached", "tokens_out",
            "tokens_cache_creation", "iterations", "wall_s", "outcome",
            "artifact_ok", "loc_authored",
        ]
    fieldnames = experiment_fields + [
        c for c in _STAMP_COLS if c not in experiment_fields
    ]

    matched = lookup_experiment_row(
        experiments_csv, test_id=test_id, arm_id=arm.id, rep=rep, host=args.host,
    )
    host = args.host if args.host is not None else (
        matched.get("host") if matched else None
    )
    commit = _git_commit(project_root)

    ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    runs_root = (args.runs_dir
                 or project_root / "tests" / "metasmith" / "e2e" / "agentic" / ".runs" / ts)
    log_dir = (runs_root / test_name / arm.id / f"rep{rep}").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    scenario = _make_scenario(test_name, arm, args.tool)

    agent = args.agent
    model = args.agent_model or ("haiku" if agent == "claude" else None)
    effort = args.agent_effort

    status = "dry" if args.dry_run else "complete"
    result: LoopResult | None = None
    failures: list[str] = []
    artifact_ok: bool | None = None
    prompt: str = ""
    err: str | None = None

    try:
        ctx = _resolve_install_context(
            project_root, args.runtime, agent, allow_missing=args.dry_run,
        )
        sb_root = log_dir / "sandbox"
        layout, agent_env = prepare_sandbox(
            sb_root, ctx,
            runtime=args.runtime, scenario=scenario, arm=arm,
            metasmith_env_name=args.metasmith_env_name,
            do_pre_install=not args.dry_run,
        )

        prompt = scenario.build_prompt(PromptContext(
            sandbox=sb_root,
            version=ctx.version,
            image_tag=ctx.image_tag,
            runtime=args.runtime,
            docs_dir=layout.docs,
            tutorial_rel=scenario.tutorial_path,
            arm=arm,
        ))
        (log_dir / "PROMPT.md").write_text(prompt)

        if args.dry_run:
            print(f"[dry-run] prompt + sandbox ready for {test_name}/{arm.id} "
                  f"rep{rep} at {log_dir}")
        else:
            driver = make_driver(
                agent, model=model,
                **({"effort": effort} if (effort and agent == "claude") else {}),
                timeout_s=args.iter_timeout_s,
            )
            budgets = LoopBudgets(
                max_iters=args.max_iters,
                max_tokens=_effective_max_tokens(
                    args.max_tokens, scenario, _GLOBAL_MAX_TOKENS_FALLBACK,
                ),
                max_tokens_per_iter=args.max_tokens_per_iter,
                max_usd_per_iter=args.max_usd_per_iter,
            )
            t0 = time.monotonic()
            result = ralph_loop(
                driver=driver, prompt=prompt, sandbox=sb_root,
                budgets=budgets, log_dir=log_dir, env=agent_env,
            )
            wall_s = time.monotonic() - t0

            vctx = VerifyContext(
                sandbox=sb_root,
                agent_env=agent_env,
                metasmith_env_name=args.metasmith_env_name,
                installed_env_path=sb_root / "envs" / args.metasmith_env_name,
                arm=arm,
            )

            checker_failures: list[str] = []
            if result.outcome is LoopOutcome.SUBMITTED:
                co = run_checker(
                    vctx, result,
                    action=getattr(scenario, "checker_action", "run"),
                    resource_overrides=getattr(
                        scenario, "checker_resource_overrides", None),
                    timeout_s=scenario.timeout_s,
                )
                (log_dir / "checker.json").write_text(json.dumps(
                    {"ok": co.ok, "kind": co.kind, "failures": co.failures,
                     "steps": [vars(s) for s in co.steps]}, indent=2))
                if not co.ok:
                    checker_failures = [f"checker: {f}" for f in co.failures]

            failures = checker_failures + scenario.verify(vctx, result)
            artifact_ok = not failures
    except Exception as exc:  # noqa: BLE001 — a harness failure is a censored
        err = f"{type(exc).__name__}: {exc}"
        status = "error"
        artifact_ok = False
        failures = [err]
        traceback.print_exc()

    row = build_result_row(
        experiment_fields=experiment_fields,
        matched=matched,
        test_name=test_name, test_id=test_id, arm=arm, rep=rep, host=host,
        model=model, effort=effort, commit=commit,
        status=status, result=result, artifact_ok=artifact_ok,
    )
    if not args.dry_run and result is not None:
        row["wall_s"] = f"{wall_s:.1f}"  # type: ignore[possibly-undefined]

    append_result_row(results_csv, fieldnames, row)

    result_json = {
        "row": row,
        "failures": failures,
        "error": err,
        "test": test_name,
        "arm": arm.id,
        "replicate": rep,
        "dry_run": bool(args.dry_run),
        "prompt_path": str(log_dir / "PROMPT.md"),
        "results_csv": str(results_csv),
        "outcome": (result.outcome.value if result is not None else status),
    }
    (log_dir / "result.json").write_text(json.dumps(result_json, indent=2))

    print(f"wrote result row -> {results_csv}")
    print(f"  run_id={row.get('run_id') or '(synth)'} "
          f"condition_id={row.get('condition_id')} "
          f"status={row.get('status')} outcome={row.get('outcome')} "
          f"artifact_ok={row.get('artifact_ok')}")
    print(f"  result.json -> {log_dir / 'result.json'}")

    if args.dry_run:
        return 0
    return 0 if (result is not None and not failures and err is None) else 1


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
