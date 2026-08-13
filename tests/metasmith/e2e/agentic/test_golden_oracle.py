"""Unit tests for the golden content oracle (plan T3).

The oracle checks the produced final artifacts are REAL + correctly-shaped
(non-empty PNG, enrichment TSV with the expected columns + >=1 data row) — a
tolerant content bound, not bit-identity to golden. No agent, no sandbox run.
"""
from __future__ import annotations

from pathlib import Path

from tests.metasmith.e2e.agentic.scenarios.arms import ARM_BY_ID
from tests.metasmith.e2e.agentic.scenarios.base import (
    GoldenCheck,
    VerifyContext,
    _golden_content_failures,
    standard_verify,
)
from tests.metasmith.e2e.agentic.scenarios.benchmark._pipeline import (
    FINAL_ARTIFACT_GLOB,
    FINAL_TABLE_GLOB,
    GOLDEN_MIN_PNG_BYTES,
    GOLDEN_MIN_TSV_ROWS,
    GOLDEN_TSV_REQUIRED_COLUMNS,
)
from tests.metasmith.e2e.agentic.harness.loop import LoopOutcome, LoopResult


_CHECK = GoldenCheck(
    png_glob=FINAL_ARTIFACT_GLOB,
    table_glob=FINAL_TABLE_GLOB,
    min_png_bytes=GOLDEN_MIN_PNG_BYTES,
    required_columns=GOLDEN_TSV_REQUIRED_COLUMNS,
    min_rows=GOLDEN_MIN_TSV_ROWS,
)

_HEADER = "\t".join(GOLDEN_TSV_REQUIRED_COLUMNS)
_ROW = "\t".join(["GO:0000001", "desc", "1/10", "1/10", "1", "1", "1", "geneA", "1"])


def _results(tmp_path: Path) -> Path:
    d = tmp_path / "workspace" / "results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_png(path: Path, nbytes: int) -> None:
    # PNG magic + padding so it is a plausible non-empty image of a given size.
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * max(0, nbytes - 8))


def _write_enrichment_tsv(path: Path, rows: int = 3) -> None:
    path.write_text(_HEADER + "\n" + "\n".join([_ROW] * rows) + "\n")


# ---------------------------------------------------------------------------
# _golden_content_failures — direct
# ---------------------------------------------------------------------------


def test_golden_passes_on_valid_artifacts(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 50_000)
    _write_enrichment_tsv(res / "enrichment.tsv", rows=42)
    assert _golden_content_failures(tmp_path, _CHECK) == []


def test_golden_fails_on_stub_png(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 100)          # below the 1 KB floor
    _write_enrichment_tsv(res / "enrichment.tsv")
    fails = _golden_content_failures(tmp_path, _CHECK)
    assert any("PNG" in f and "min" in f for f in fails)


def test_golden_fails_on_missing_tsv(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 50_000)       # PNG present, no TSV
    fails = _golden_content_failures(tmp_path, _CHECK)
    assert any("no results table matched" in f for f in fails)


def test_golden_fails_on_empty_tsv(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 50_000)
    (res / "enrichment.tsv").write_text(_HEADER + "\n")   # header only, 0 rows
    fails = _golden_content_failures(tmp_path, _CHECK)
    assert any("data rows" in f for f in fails)


def test_golden_fails_on_wrong_columns(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 50_000)
    (res / "enrichment.tsv").write_text("foo\tbar\nx\ty\n")
    fails = _golden_content_failures(tmp_path, _CHECK)
    assert any("enrichment schema" in f for f in fails)


def test_golden_ignores_extra_tool_tsv(tmp_path: Path) -> None:
    """t6 emits abricate.tsv alongside the enrichment table — the check must find
    the enrichment TSV by schema, not be shadowed by the extra report."""
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 50_000)
    # abricate.tsv: different schema, and larger than the enrichment table.
    (res / "abricate.tsv").write_text(
        "FILE\tSEQUENCE\tGENE\tPRODUCT\n" + ("c\ts\tg\tp\n" * 1000)
    )
    _write_enrichment_tsv(res / "enrichment.tsv", rows=5)
    assert _golden_content_failures(tmp_path, _CHECK) == []


# ---------------------------------------------------------------------------
# standard_verify — integration (golden check gated by golden_check arg)
# ---------------------------------------------------------------------------


def _vctx(tmp_path: Path, arm_id: str = "A7") -> VerifyContext:
    return VerifyContext(
        sandbox=tmp_path,
        agent_env={"HOME": str(tmp_path / "home")},
        metasmith_env_name="msm_env",
        installed_env_path=tmp_path / "envs" / "msm_env",
        arm=ARM_BY_ID[arm_id],
    )


def _done(tmp_path: Path) -> LoopResult:
    return LoopResult(outcome=LoopOutcome.DONE, iterations=1, tokens_used=0,
                      last_iter=None, terminal_control=None)


def test_standard_verify_runs_golden_when_configured(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 50_000)
    _write_enrichment_tsv(res / "enrichment.tsv")
    fails = standard_verify(
        _vctx(tmp_path), _done(tmp_path),
        artifact_globs=[FINAL_ARTIFACT_GLOB],
        expected_trace=None,
        golden_check=_CHECK,
    )
    assert fails == []


def test_standard_verify_skips_golden_when_none(tmp_path: Path) -> None:
    """t2 (dry-validate) passes golden_check=None: only the marker glob is checked,
    a missing TSV is NOT a failure."""
    (tmp_path / "workspace" / "results").mkdir(parents=True, exist_ok=True)
    (tmp_path / "workspace" / "results" / "DRYRUN_OK.txt").write_text("ok\n")
    fails = standard_verify(
        _vctx(tmp_path), _done(tmp_path),
        artifact_globs=["workspace/results/DRYRUN_OK.txt"],
        expected_trace=None,
        golden_check=None,
    )
    assert fails == []


def test_standard_verify_golden_catches_stub(tmp_path: Path) -> None:
    res = _results(tmp_path)
    _write_png(res / "enrichment.png", 10)           # stub
    _write_enrichment_tsv(res / "enrichment.tsv")
    fails = standard_verify(
        _vctx(tmp_path), _done(tmp_path),
        artifact_globs=[FINAL_ARTIFACT_GLOB],
        expected_trace=None,
        golden_check=_CHECK,
    )
    assert fails  # stub PNG is caught
