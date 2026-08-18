"""The host de-novo collectors refuse a table short of the declared lane set.

Two routes write the benchmark's de-novo GPR from a mapper table -- the in-graph
`benchmark/host_gpr_denovo.py` and the by-hand `host_denovo_from_mapper.py` -- and they
are two copies of one claim. Both are covered here, because a gate on one is not a gate.

The lane set matters downstream rather than cosmetically: `nomination_contributions`
divides each ORF's belief by its own distinct-channel count, so a table one lane short
carries a different denominator under the same `lane_set` label.
"""
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


def _mapper_table(path: Path, channels) -> pd.DataFrame:
    rows = []
    for i, orf in enumerate(ORFS):
        for ch in channels:
            rows.append({
                "source": ACC, "orf": orf, "channel": ch,
                "mnxr": f"MNXR{100000 + i}", "intermediate_id": f"X{i}",
                "intermediate_name": f"thing {i}", "raw_score": 1.0 + i,
                "score_kind": fe.CHANNEL_SCORE_KIND[ch], "projection_via": "ec",
                "evidence_quality": "reviewed", "lane_set": "chosen_4",
            })
    df = pd.DataFrame(rows)[fe.SCHEMA_COLS]
    df.to_parquet(path, index=False)
    return df


def _render_driver(**kw) -> str:
    """Substitute the collector's DRIVER exactly as its protocol() does."""
    src = (BREF / "transforms" / "benchmark" / "host_gpr_denovo.py").read_text()
    start = src.index("DRIVER = r'''")
    end = src.index("'''", start + len("DRIVER = r'''"))
    template = src[start + len("DRIVER = r'''"):end]
    ns: dict = {}
    exec(f"DRIVER = {template!r}", ns)
    return ns["DRIVER"].format(**kw)


def _run_driver(tmp: Path, channels) -> subprocess.CompletedProcess:
    genomes = _genomes(tmp)
    _mapper_table(tmp / "mapper.parquet", channels)
    out = tmp / "out"
    out.mkdir()
    driver = _render_driver(
        genomes=str(genomes), out=str(out),
        gpr_paths=repr([str(tmp / "mapper.parquet")]),
        gpr_cols=repr((
            "build_id", "host", "unit_id", "feature_id", "feature_kind",
            "feature_name", "mnxr", "channel", "evidence_id", "evidence_name",
            "raw_score", "projection_via", "in_atom_universe", "gpr_rule")),
        prefix="denovo", ev_lib=str(LIB / "fabfos_evidence.py"),
        lane_set="chosen_4")
    script = tmp / "_driver.py"
    script.write_text(driver)
    return subprocess.run([sys.executable, str(script)], cwd=tmp,
                          capture_output=True, text=True)


@pytest.mark.parametrize("missing", ["pbert", "kofam", "clean", "uniref50"])
def test_the_collector_refuses_a_short_lane_set(tmp_path, missing):
    """Whichever lane is absent, the refusal says which one."""
    channels = [c for c in fe.LANE_SETS["chosen_4"] if c != missing]
    r = _run_driver(tmp_path, channels)
    assert r.returncode != 0, f"a table missing {missing} was collected"
    assert missing in r.stderr, r.stderr
    assert not list((tmp_path / "out").rglob("gpr_denovo.parquet"))


def test_the_collector_accepts_the_declared_lane_set(tmp_path):
    """The gate must pass the case it exists to protect -- otherwise the refusal
    above is indistinguishable from a collector that never works."""
    r = _run_driver(tmp_path, list(fe.LANE_SETS["chosen_4"]))
    assert r.returncode == 0, r.stderr
    made = tmp_path / "out" / "hosts" / HOST / "gpr_denovo.parquet"
    assert made.exists(), r.stdout + r.stderr
    df = pd.read_parquet(made)
    assert set(df["channel"]) == {f"denovo_{c}" for c in fe.LANE_SETS["chosen_4"]}


def _run_by_hand(tmp: Path, channels) -> subprocess.CompletedProcess:
    """The by-hand collector, pointed at a fixture repo through its own walk-up."""
    genomes = tmp / "data" / "fabfos" / "originals" / "genomes" / HOST / "genome"
    genomes.mkdir(parents=True)
    (genomes / f"{ACC}.faa").write_text("".join(f">{o}\nMKRIS\n" for o in ORFS))
    # The results tree the retrieve step writes: one product directory per type.
    mapper = tmp / "results"
    (mapper / "annotation-gpr_table").mkdir(parents=True)
    _mapper_table(mapper / "annotation-gpr_table" / "1-1-1.test.parquet", channels)

    script = tmp / "host_denovo_from_mapper.py"
    src = (BREF / "host_denovo_from_mapper.py").read_text()
    # The walk-up looks for a `data/fabfos` ancestor; running the copy from the
    # fixture root is what points it at the fixture rather than the real tree.
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
