"""Unit tests for the run_cell CLI (dry path) + aggregate.

These run in the default suite (NOT marked ``e2e_agentic``): the dry path never
spawns the ``claude`` CLI and never calls the ralph loop — it renders the prompt,
builds + provisions the sandbox, and writes a ``status=dry`` result row. We
monkeypatch the driver factory + loop to hard-fail if the dry path ever tries to
drive a model, proving the short-circuit.

Sandbox construction is real (copies docs/data_types/transforms, hardlinks the
conda channel, provisions the arm start-state), so these tests are skipped when
the sandbox sources are unavailable (fresh CI without the repo data dirs).
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from tests.e2e.agentic import run_cell as rc


_EXPERIMENTS_HEADER = (
    "run_id,condition_id,test_id,test,kind,host,arm,env,orchestrator,provenance,"
    "tool,detail,replicate,status,tokens_in,tokens_cached,tokens_out,"
    "tokens_cache_creation,iterations,wall_s,outcome,artifact_ok,loc_authored"
)

# Minimal enumerated run list covering the two reference cells at rep 1.
_EXPERIMENTS_ROWS = [
    "R0130,T3-A10,3,run,exec,micb0,A10,metasmith,metasmith,toy,,,1,pending,,,,,,,,,",
    "R0121,T3-A7,3,run,exec,micb0,A7,container,ad-hoc,toy,,,1,pending,,,,,,,,,",
]


def _write_experiments(tmp_path: Path) -> Path:
    p = tmp_path / "experiments.csv"
    p.write_text(_EXPERIMENTS_HEADER + "\n" + "\n".join(_EXPERIMENTS_ROWS) + "\n")
    return p


def _sources_available() -> bool:
    root = rc._project_root()
    docs = root / "docs" / "source"
    dt = (root / "lib" / "data_types").exists() or (root / "examples" / "data_types").exists()
    tr = root / "main" / "transforms" / "std" / "transforms"
    ch = root / "conda_build"
    return docs.exists() and dt and tr.exists() and ch.exists()


def _read_single_row(results_csv: Path) -> dict:
    with results_csv.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1, f"expected exactly one result row, got {len(rows)}"
    return rows[0]


@pytest.mark.parametrize(
    "arm_id,expect_run_id,expect_condition,provision_probe",
    [
        ("A10", "R0130", "T3-A10", "workspace/std"),
        ("A7", "R0121", "T3-A7", "workspace/env/images.txt"),
    ],
)
def test_run_cell_dry_run(
    tmp_path: Path, monkeypatch, arm_id, expect_run_id, expect_condition,
    provision_probe,
) -> None:
    if not _sources_available():
        pytest.skip("sandbox sources unavailable (docs/data_types/transforms/channel)")

    # Hard-fail if the dry path ever tries to drive a live model.
    def _boom(*a, **k):
        raise AssertionError("dry-run must not build a driver / drive the loop")

    monkeypatch.setattr(rc, "make_driver", _boom)
    monkeypatch.setattr(rc, "ralph_loop", _boom)

    experiments = _write_experiments(tmp_path)
    results_csv = tmp_path / "results.csv"
    runs_dir = tmp_path / "runs"

    code = rc.run([
        "--arm", arm_id, "--test", "t3_run", "--rep", "1",
        "--agent", "claude", "--agent-model", "haiku", "--agent-effort", "medium",
        "--host", "micb0", "--dry-run",
        "--experiments-csv", str(experiments),
        "--results-csv", str(results_csv),
        "--runs-dir", str(runs_dir),
    ])
    assert code == 0

    # --- prompt rendered
    log_dir = runs_dir / "t3_run" / arm_id / "rep1"
    prompt_md = log_dir / "PROMPT.md"
    assert prompt_md.is_file()
    prompt = prompt_md.read_text()
    assert prompt.strip()
    assert "clusterProfiler" in prompt

    # --- sandbox built + provisioned
    sandbox = log_dir / "sandbox"
    for sub in ("docs", "data_types", "transforms", "workspace", "home", "envs"):
        assert (sandbox / sub).exists(), f"sandbox missing {sub}"
    assert (sandbox / provision_probe).exists(), (
        f"arm {arm_id} start-state not provisioned: missing {provision_probe}"
    )

    # --- exactly one well-formed results row
    row = _read_single_row(results_csv)
    assert row["run_id"] == expect_run_id
    assert row["condition_id"] == expect_condition
    assert row["test"] == "run"
    assert row["test_id"] == "3"
    assert row["arm"] == arm_id
    assert row["replicate"] == "1"
    assert row["host"] == "micb0"
    assert row["status"] == "dry"
    # stamps
    assert row["model"] == "haiku"
    assert row["effort"] == "medium"
    assert row["commit"]                      # non-empty git short hash
    # dry rows carry no token/outcome numbers
    assert row["tokens_in"] == ""
    assert row["outcome"] == ""
    assert row["artifact_ok"] == ""

    # --- result.json dropped next to the transcript
    rj = log_dir / "result.json"
    assert rj.is_file()
    payload = json.loads(rj.read_text())
    assert payload["dry_run"] is True
    assert payload["arm"] == arm_id
    assert payload["row"]["run_id"] == expect_run_id


def test_run_cell_synthesizes_keys_without_experiments(tmp_path: Path) -> None:
    """When the cell is not enumerated, run_cell synthesizes condition_id."""
    if not _sources_available():
        pytest.skip("sandbox sources unavailable")
    results_csv = tmp_path / "results.csv"
    code = rc.run([
        "--arm", "A7", "--test", "t3_run", "--rep", "2", "--dry-run",
        "--experiments-csv", str(tmp_path / "does_not_exist.csv"),
        "--results-csv", str(results_csv),
        "--runs-dir", str(tmp_path / "runs"),
    ])
    assert code == 0
    row = _read_single_row(results_csv)
    assert row["run_id"] == ""                # not enumerated
    assert row["condition_id"] == "T3-A7"     # synthesized
    assert row["env"] == "container"          # from the arm
    assert row["orchestrator"] == "ad-hoc"
    assert row["replicate"] == "2"


# ---------------------------------------------------------------------------
# _effective_max_tokens: explicit CLI > scenario.max_tokens > global fallback
# ---------------------------------------------------------------------------


class _FakeScenario:
    def __init__(self, max_tokens):
        self.max_tokens = max_tokens


def test_effective_max_tokens_cli_wins() -> None:
    sc = _FakeScenario(max_tokens=5_000_000)
    assert rc._effective_max_tokens(999, sc, 2_000_000) == 999


def test_effective_max_tokens_scenario_quota() -> None:
    sc = _FakeScenario(max_tokens=5_000_000)
    assert rc._effective_max_tokens(None, sc, 2_000_000) == 5_000_000


def test_effective_max_tokens_falls_back() -> None:
    sc = _FakeScenario(max_tokens=None)
    assert rc._effective_max_tokens(None, sc, 2_000_000) == 2_000_000


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


def test_aggregate_summary_and_plot_degradation(tmp_path: Path) -> None:
    from tests.e2e.agentic import aggregate as agg

    results = tmp_path / "results.csv"
    results.write_text(
        _EXPERIMENTS_HEADER + ",model,effort,commit\n"
        # two successes + one censored failure for T3-A10
        "R1,T3-A10,3,run,exec,h,A10,metasmith,metasmith,toy,,,1,complete,"
        "1000,200,300,50,3,120,done,true,,haiku,,abc\n"
        "R2,T3-A10,3,run,exec,h,A10,metasmith,metasmith,toy,,,2,complete,"
        "1200,250,350,60,4,140,done,true,,haiku,,abc\n"
        "R3,T3-A10,3,run,exec,h,A10,metasmith,metasmith,toy,,,3,complete,"
        "9999,0,0,0,20,600,over_budget,false,,haiku,,abc\n"
    )
    out_dir = tmp_path / "agg"
    report = agg.aggregate([results], out_dir)

    assert report["n_rows"] == 3
    assert report["n_cells"] == 1
    summary_csv = Path(report["summary_path"])
    assert summary_csv.is_file()
    row = _read_single_row(summary_csv)
    assert row["arm"] == "A10"
    assert row["test"] == "run"
    assert row["n_executed"] == "3"
    assert row["n_success"] == "2"
    assert float(row["success_rate"]) == pytest.approx(2 / 3, abs=1e-3)
    # DNF = the one over_budget (quota-reached) row
    assert row["n_dnf"] == "1"
    assert float(row["dnf_rate"]) == pytest.approx(1 / 3, abs=1e-3)
    assert row["n_over_budget"] == "1"
    # token medians are over successes only (the over_budget row is excluded)
    assert float(row["median_tokens_in"]) == pytest.approx(1100.0)
    assert json.loads(row["points_tokens_in"]) == [1000, 1200]

    # Plot either renders (matplotlib present) or degrades with a note + file.
    if report["plot_ok"]:
        assert Path(report["plot_path"]).is_file()
    else:
        assert "matplotlib" in report["plot_note"] or "no rows" in report["plot_note"]
        assert (out_dir / "PLOT_NOTE.txt").is_file()
