"""The score/p-value cut in deepvirfinder_pratama, which nothing else checks.

Like merge_candidate_calls_pratama (test_viromics_intervals.py), this transform's lineage-dependent
behaviour cannot be exercised by `metasmith run` in isolation. What is testable is the part most
likely to be silently wrong: the paper's cut (score >= 0.9, p-value <= 0.05) and the DVF table
parse that feeds it.
"""
import importlib.util
from pathlib import Path

import pytest

from metasmith.python_api import TransformInstanceLibrary

from conftest import WORKSPACE

RESEARCH_LIBRARY = WORKSPACE.parent.parent / "research" / "metasmith_benchmark" / "library"


@pytest.fixture(scope="module")
def dvf_module():
    # The module calls lib.GetType at import, so the compiled library has to be loaded first -- the
    # same order `metasmith run` uses. build.sh compiles this dir's _metadata from BOTH
    # src/metasmith_libraries/data_types and this library's own data_types together, so bench::,
    # e3:: and viromics:: types all resolve from this one load.
    TransformInstanceLibrary.Load(RESEARCH_LIBRARY / "transforms" / "e3")
    path = RESEARCH_LIBRARY / "transforms" / "e3" / "deepvirfinder_pratama.py"
    spec = importlib.util.spec_from_file_location("deepvirfinder_pratama", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_dvfpred(path: Path, rows):
    with open(path, "w") as f:
        f.write("name\tlen\tscore\tpvalue\n")
        for name, length, score, pvalue in rows:
            f.write(f"{name}\t{length}\t{score}\t{pvalue}\n")


def test_score_exactly_at_cut_is_kept(dvf_module):
    assert dvf_module._passes_cut(0.9, 0.05) is True


def test_pvalue_exactly_at_cut_is_kept(dvf_module):
    assert dvf_module._passes_cut(0.95, 0.05) is True


def test_score_just_below_cut_is_rejected(dvf_module):
    assert dvf_module._passes_cut(0.899999, 0.01) is False


def test_pvalue_just_above_cut_is_rejected(dvf_module):
    assert dvf_module._passes_cut(0.95, 0.050001) is False


def test_low_score_and_high_pvalue_is_rejected(dvf_module):
    assert dvf_module._passes_cut(0.1, 0.9) is False


def test_write_calls_keeps_only_rows_passing_both_cuts_and_takes_the_header_token(dvf_module, tmp_path):
    dvfpred = tmp_path / "contigs.fa_gt1000bp_dvfpred.txt"
    _write_dvfpred(dvfpred, [
        ("k141_1 flag=1 multi=3", 5000, 0.9, 0.05),   # exactly at both cuts: kept
        ("k141_2", 3000, 0.89, 0.01),                 # score just under the cut: rejected
        ("k141_3", 4000, 0.95, 0.06),                 # pvalue just over the cut: rejected
        ("k141_4", 2500, 0.99, 0.001),                # comfortably past both cuts: kept
    ])
    out = tmp_path / "calls.tsv"
    dvf_module._write_calls(dvfpred, out)

    lines = out.read_text().splitlines()
    assert lines[0] == "contig_id\tstart\tend\tcaller\tscore"
    rows = [line.split("\t") for line in lines[1:]]
    assert rows == [
        ["k141_1", "1", "5000", "deepvirfinder", "0.9"],
        ["k141_4", "1", "2500", "deepvirfinder", "0.99"],
    ]


def test_dvfpred_missing_a_required_column_raises(dvf_module, tmp_path):
    bad = tmp_path / "bad.txt"
    bad.write_text("name\tlen\tscore\nk141_1\t100\t0.9\n")
    with pytest.raises(AssertionError):
        dvf_module._read_dvfpred(bad)
