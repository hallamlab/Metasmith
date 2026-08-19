from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metasmith.agents import _extract_nxf_task_metadata


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
    _write_trace_tsv(
        tmp_path / "nxf_trace.tsv",
        [
            {"task_id": 1, "name": "shardFasta", "status": "COMPLETED"},
            {"task_id": 2, "name": "embed",      "status": "COMPLETED"},
        ],
    )
    _write_report(tmp_path / "nxf_report.html", "<<not json at all>>\n")

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is not None
    assert list(df["task_id"]) == [1, 2]
    assert list(df["name"]) == ["shardFasta", "embed"]


def test_falls_back_to_html_when_tsv_missing(tmp_path: Path):
    _write_report(tmp_path / "nxf_report.html", TRACE_LINE_WITH_JS_ESCAPES)

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is not None
    assert list(df["task_id"]) == [1]
    assert df.loc[0, "name"] == "shardFasta"
    assert "echo" in df.loc[0, "script"]


def test_html_fallback_handles_js_only_escape(tmp_path: Path):
    _write_report(tmp_path / "nxf_report.html", TRACE_LINE_WITH_JS_ESCAPES)

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is not None
    assert len(df) == 1


def test_returns_none_on_irrecoverably_bad_html(tmp_path: Path):
    (tmp_path / "nxf_report.html").write_text(
        "<html><body><script>\n"
        'window.data = { "trace":[\n'
        "this is not json or javascript ]};\n"
        "</script></body></html>\n"
    )

    df = _extract_nxf_task_metadata(tmp_path)

    assert df is None


def test_returns_none_when_no_inputs(tmp_path: Path):
    assert _extract_nxf_task_metadata(tmp_path) is None
