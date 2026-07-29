"""Regression tests for inbox #162.

`RunWorkflow`'s post-processing step crashed in production because it
parsed `nxf_report.html` as strict JSON, but Nextflow embeds the trace
table as a JavaScript object literal that contains JS-only escapes
(notably `\\'` from any embedded `.command.sh` with a single quote).

The fix:
  - Prefer the dedicated `nxf_trace.tsv` (already produced via
    `-with-trace`); it's a clean TSV with no escape ambiguity.
  - Fall back to the HTML report only if the TSV is missing, and
    sanitize JS-only escapes before parsing.
  - Never raise: a parse failure here must degrade to a warning, not
    abort the surrounding workflow.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metasmith.agents import _extract_nxf_task_metadata


# A line that comes straight out of the offending Nextflow report:
# `\'` is a valid JS escape (single quote can be escaped) but strict
# JSON only allows `\"`, so json.loads raises Invalid \escape on this.
TRACE_LINE_WITH_JS_ESCAPES = (
    '{"task_id":1,"name":"shardFasta","status":"COMPLETED",'
    '"script":"echo \\\'usr {}\\\' >>.command.metadata\\ncat \\/scratch/x"} ]};\n'
)


def _write_report(path: Path, trace_line: str) -> None:
    path.write_text(
        "<html><body><script>\n"
        'window.data = { "trace":[\n'
        f"{trace_line}"
        "</script></body></html>\n"
    )


def _write_trace_tsv(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df.to_csv(path, sep="\t", index=False)


def test_prefers_trace_tsv_when_present(tmp_path: Path):
    """The TSV is canonical Nextflow output; use it and skip the HTML."""
    _write_trace_tsv(
        tmp_path / "nxf_trace.tsv",
        [
            {"task_id": 1, "name": "shardFasta", "status": "COMPLETED"},
            {"task_id": 2, "name": "embed",      "status": "COMPLETED"},
        ],
    )
    # Provide a deliberately corrupt HTML report to prove the TSV path wins.
    _write_report(tmp_path / "nxf_report.html", "<<not json at all>>\n")

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is not None
    assert list(df["task_id"]) == [1, 2]
    assert list(df["name"]) == ["shardFasta", "embed"]


def test_falls_back_to_html_when_tsv_missing(tmp_path: Path):
    """If only the HTML report exists, parse it (with sanitization)."""
    _write_report(tmp_path / "nxf_report.html", TRACE_LINE_WITH_JS_ESCAPES)

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is not None
    assert list(df["task_id"]) == [1]
    assert df.loc[0, "name"] == "shardFasta"
    # The embedded script field is preserved (unescaped to a valid form).
    assert "echo" in df.loc[0, "script"]


def test_html_fallback_handles_js_only_escape(tmp_path: Path):
    """The historic crash: \\' inside the script field must not raise.

    Reproduces inbox #162: Nextflow renders single quotes from
    `.command.sh` as `\\'` in the HTML trace's `script` field, which
    strict JSON rejects with `Invalid \\escape`.
    """
    _write_report(tmp_path / "nxf_report.html", TRACE_LINE_WITH_JS_ESCAPES)

    # Must not raise.
    df = _extract_nxf_task_metadata(tmp_path)

    assert df is not None
    assert len(df) == 1


def test_returns_none_on_irrecoverably_bad_html(tmp_path: Path):
    """Malformed report should degrade to a warning (None), not crash."""
    (tmp_path / "nxf_report.html").write_text(
        "<html><body><script>\n"
        'window.data = { "trace":[\n'
        "this is not json or javascript ]};\n"
        "</script></body></html>\n"
    )

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is None


def test_returns_none_when_no_inputs(tmp_path: Path):
    """Neither file present -> None, no exception."""
    assert _extract_nxf_task_metadata(tmp_path) is None
