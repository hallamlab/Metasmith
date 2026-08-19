from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LIB = REPO_ROOT / "src" / "metasmith_libraries" / "resources" / "lib"
BREF = REPO_ROOT / "src" / "fabfos" / "build_references"
sys.path.insert(0, str(LIB))

import fabfos_evidence as fe                                          # noqa: E402

HOST = "e_coli_test"
ACC = "NC_TEST.1"
ORFS = [f"{ACC}_prot_{i}" for i in range(6)]


def _genomes(tmp: Path) -> Path:
    d = tmp / "genomes" / HOST / "genome"
    d.mkdir(parents=True)
    (d / f"{ACC}.faa").write_text(
        "".join(f">{o}\nMKRIS\n" for o in ORFS))
    return tmp / "genomes"


def _score(channel: str, i: int) -> float:
    lo, hi = fe.SCORE_KINDS[fe.CHANNEL_SCORE_KIND[channel]]
    if hi is None:
        return float(lo + 40.0 + i)
    return float(lo + (hi - lo) * (0.4 + 0.05 * (i % 8)))


def _mapper_table(path: Path, channels) -> pd.DataFrame:
    rows = []
    for i, orf in enumerate(ORFS):
        for ch in channels:
            rows.append({
                "source": ACC, "orf": orf, "channel": ch,
                "mnxr": f"MNXR{100000 + i}", "intermediate_id": f"X{i}",
                "intermediate_name": f"thing {i}", "raw_score": _score(ch, i),
                "score_kind": fe.CHANNEL_SCORE_KIND[ch], "projection_via": "ec",
                "evidence_quality": "reviewed", "lane_set": "chosen_4",
            })
    df = pd.DataFrame(rows)[fe.SCHEMA_COLS]
    df.to_parquet(path, index=False)
    return df


DRIVER = BREF / "resources" / "buildlib" / "benchmark" / "host_gpr_denovo.py"


def _run_driver(tmp: Path, channels) -> subprocess.CompletedProcess:
    genomes = _genomes(tmp)
    _mapper_table(tmp / "mapper.parquet", channels)
    out = tmp / "out"
    out.mkdir()
    argv = [
        sys.executable, str(DRIVER),
        "--genomes", str(genomes),
        "--out", str(out),
        "--gpr-paths", repr([str(tmp / "mapper.parquet")]),
        "--ev-lib", str(LIB / "fabfos_evidence.py"),
        "--lane-set", "chosen_4",
        "--extensions", repr(["attribution", "feature", "universe"]),
    ]
    return subprocess.run(argv, cwd=tmp, capture_output=True, text=True)


@pytest.mark.parametrize("missing", ["pbert", "kofam", "clean", "uniref50"])
def test_the_collector_refuses_a_short_lane_set(tmp_path, missing):
    channels = [c for c in fe.LANE_SETS["chosen_4"] if c != missing]
    r = _run_driver(tmp_path, channels)
    assert r.returncode != 0, f"a table missing {missing} was collected"
    assert missing in r.stderr, r.stderr
    assert not list((tmp_path / "out").rglob("gpr_denovo.parquet"))


def test_the_collector_accepts_the_declared_lane_set(tmp_path):
    r = _run_driver(tmp_path, list(fe.LANE_SETS["chosen_4"]))
    assert r.returncode == 0, r.stderr
    made = tmp_path / "out" / "hosts" / HOST / "gpr_denovo.parquet"
    assert made.exists(), r.stdout + r.stderr
    df = pd.read_parquet(made)
    assert set(df["channel"]) == set(fe.LANE_SETS["chosen_4"])
    assert fe.is_unified(df) and fe.extensions_of(df) == (
        "attribution", "feature", "universe")


def _run_by_hand(tmp: Path, channels) -> subprocess.CompletedProcess:
    genomes = tmp / "data" / "fabfos" / "originals" / "genomes" / HOST / "genome"
    genomes.mkdir(parents=True)
    (genomes / f"{ACC}.faa").write_text("".join(f">{o}\nMKRIS\n" for o in ORFS))
    mapper = tmp / "results"
    (mapper / "annotation-gpr_table").mkdir(parents=True)
    _mapper_table(mapper / "annotation-gpr_table" / "1-1-1.test.parquet", channels)

    script = tmp / "host_denovo_from_mapper.py"
    src = (BREF / "host_denovo_from_mapper.py").read_text()
    script.write_text(src.replace(
        'sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))',
        f'sys.path.insert(0, {str(LIB)!r})'))
    env = dict(os.environ, PYTHONPATH=str(LIB))
    return subprocess.run(
        [sys.executable, str(script), "--host", HOST, "--mapper", str(mapper)],
        cwd=tmp, capture_output=True, text=True, env=env)


def test_the_by_hand_collector_refuses_a_short_lane_set(tmp_path):
    channels = [c for c in fe.LANE_SETS["chosen_4"] if c != "pbert"]
    r = _run_by_hand(tmp_path, channels)
    assert r.returncode != 0, "the by-hand route collected a three-lane table"
    assert "pbert" in r.stderr, r.stderr


def test_the_by_hand_collector_accepts_the_declared_lane_set(tmp_path):
    r = _run_by_hand(tmp_path, list(fe.LANE_SETS["chosen_4"]))
    assert r.returncode == 0, r.stderr
