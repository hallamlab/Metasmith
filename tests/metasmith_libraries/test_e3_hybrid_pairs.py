"""The hybrid pairing logic in e3_pratama.py, and its agreement with stage_hybrid_pairs.sh.

Nothing here touches fir: hybrid_pairings()/hybrid_partners() are pure functions over the
repo-local research/pratama2026/runs.tsv, and the staging script is read as text, never run.
"""
import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DRIVERS = REPO / "research" / "metasmith_benchmark" / "drivers"


@pytest.fixture(scope="module")
def e3_pratama():
    # The module imports `_common` by bare name at its own top level, the same way its
    # `if __name__ == "__main__"` invocation relies on its own directory being on sys.path.
    sys.path.insert(0, str(DRIVERS))
    spec = importlib.util.spec_from_file_location("e3_pratama", DRIVERS / "e3_pratama.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _staging_pairs():
    """illumina_run -> minion_run, parsed out of stage_hybrid_pairs.sh's PAIRS array literal."""
    text = (DRIVERS / "stage_hybrid_pairs.sh").read_text()
    return dict(re.findall(r"\[(\S+)\]=(\S+)", text))


def test_seventeen_pairs(e3_pratama):
    assert len(e3_pratama.hybrid_pairings()) == e3_pratama.EXPECTED_HYBRIDS == 17


def test_pairs_are_one_illumina_run_per_pairing(e3_pratama):
    illumina_runs = [illumina for illumina, _ in e3_pratama.hybrid_pairings()]
    assert len(illumina_runs) == len(set(illumina_runs)) == 17


def test_pairs_span_six_minion_runs(e3_pratama):
    minion_runs = {minion for _, minion in e3_pratama.hybrid_pairings()}
    assert len(minion_runs) == 6


def test_partners_are_seventeen_distinct_paths_under_hybrid_pairs(e3_pratama):
    partners = e3_pratama.hybrid_partners()
    assert len(partners) == 17
    paths = list(partners.values())
    assert len(set(paths)) == 17, "distinct paths are the whole fix -- a repeated path re-collapses the given"
    for illumina_run, path in partners.items():
        assert path.parent == e3_pratama.HYBRID_PAIRS
        assert path.name.startswith(f"{illumina_run}__")
        assert path.suffix == ".gz"


def test_staging_script_agrees_with_the_driver(e3_pratama):
    assert _staging_pairs() == dict(e3_pratama.hybrid_pairings())
