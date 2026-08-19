from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
AGENTIC = REPO_ROOT / "tests" / "metasmith" / "e2e" / "agentic"

_SEGMENTS = re.compile(r'project_root\s*/\s*((?:"[^"]+"\s*/\s*)+)"\.runs"')


def _declared_runs_roots() -> dict[Path, Path]:
    found: dict[Path, Path] = {}
    for name in ("conftest.py", "run_cell.py"):
        src = AGENTIC / name
        text = src.read_text(encoding="utf-8")
        for m in _SEGMENTS.finditer(text):
            parts = re.findall(r'"([^"]+)"', m.group(1))
            found[src] = REPO_ROOT.joinpath(*parts, ".runs")
    return found


def test_every_writer_agrees_on_the_runs_root():
    roots = _declared_runs_roots()
    assert roots, "found no runs-root construction -- has the harness changed shape?"
    distinct = set(roots.values())
    assert len(distinct) == 1, "the harness writes transcripts to more than one root:\n  " + "\n  ".join(
        f"{p.relative_to(REPO_ROOT)} -> {r}" for p, r in sorted(roots.items())
    )
    assert distinct.pop() == AGENTIC / ".runs"


def test_git_ignores_the_runs_root():
    runs = AGENTIC / ".runs"
    probe = runs / "20260101-000000" / "transcript.jsonl"
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "check-ignore", "-q", str(probe)],
    )
    if proc.returncode != 0:
        pytest.fail(
            f"{probe.relative_to(REPO_ROOT)} is NOT git-ignored -- an agentic sweep "
            "would commit its transcripts. Add the directory to .gitignore."
        )
