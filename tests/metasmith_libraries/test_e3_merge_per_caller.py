# Pratama pooled one record per caller: two callers on one contig stay two records with their own
# boundaries, and only MMseqs2 decides whether they are one vOTU.
import importlib.util
from pathlib import Path

import pytest

from metasmith.python_api import TransformInstanceLibrary

BENCH_LIB = Path(__file__).resolve().parents[2] / "research" / "metasmith_benchmark" / "library"


@pytest.fixture(scope="module")
def merge_module():
    TransformInstanceLibrary.Load(BENCH_LIB / "transforms" / "e3")
    path = BENCH_LIB / "transforms" / "e3" / "merge_candidate_calls_pratama.py"
    spec = importlib.util.spec_from_file_location("merge_candidate_calls_pratama", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_overlapping_calls_from_two_callers_stay_two_records(merge_module):
    regions, records = merge_module._plan_extraction({
        "k141_9": [(1, 100_000, "virsorter2"), (40_001, 70_000, "genomad")],
    })
    assert records == [("k141_9", 1, 100_000, "virsorter2"), ("k141_9", 40_001, 70_000, "genomad")]
    assert regions == [("k141_9", 1, 100_000), ("k141_9", 40_001, 70_000)]


def test_two_callers_on_the_same_span_share_one_extraction(merge_module):
    regions, records = merge_module._plan_extraction({
        "NODE_3": [(1, 8_000, "genomad"), (1, 8_000, "vibrant")],
    })
    assert regions == [("NODE_3", 1, 8_000)]
    assert records == [("NODE_3", 1, 8_000, "genomad"), ("NODE_3", 1, 8_000, "vibrant")]


def test_a_repeated_call_is_one_record(merge_module):
    _, records = merge_module._plan_extraction({"c1": [(5, 9, "vibrant"), (5, 9, "vibrant")]})
    assert records == [("c1", 5, 9, "vibrant")]
