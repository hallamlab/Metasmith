# The row->interval decision in e3/virsorter2_pratama.py, which nothing else checks.
# That transform cannot be exercised by `metasmith run` in isolation (it needs a staged
# VirSorter2 db and image); what is testable is the part most likely to be silently
# wrong -- picking start/end off a boundary-table row, including the --keep-original-seq
# special case where the table's own columns are not the answer.
import importlib.util
from pathlib import Path

import pytest

from metasmith.python_api import TransformInstanceLibrary

from conftest import WORKSPACE

BLIB = WORKSPACE.parent.parent / "research" / "metasmith_benchmark" / "library"


@pytest.fixture(scope="module")
def vs2_module():
    # The module calls lib.GetType at import, so the library has to be loaded first --
    # the same order `metasmith run` uses.
    TransformInstanceLibrary.Load(BLIB / "transforms/e3")
    path = BLIB / "transforms/e3/virsorter2_pratama.py"
    spec = importlib.util.spec_from_file_location("virsorter2_pratama", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_full_row_is_the_whole_contig(vs2_module):
    row = {"seqname": "NODE_463_length_12000_cov_10.1||full",
           "full_bp_start": "1", "full_bp_end": "1"}
    contig_lengths = {"NODE_463_length_12000_cov_10.1": 12000}
    assert vs2_module._row_interval(row, contig_lengths) == (
        "NODE_463_length_12000_cov_10.1", 1, 12000)


def test_lt2gene_row_is_also_the_whole_contig(vs2_module):
    row = {"seqname": "k141_202869||lt2gene", "full_bp_start": "1", "full_bp_end": "1"}
    contig_lengths = {"k141_202869": 5432}
    assert vs2_module._row_interval(row, contig_lengths) == ("k141_202869", 1, 5432)


def test_partial_row_uses_full_bp_not_trim_bp(vs2_module):
    # The row carries both columns, as a real boundary-table row does; trim_bp_* must
    # never be read, and its values here are deliberately not the expected answer.
    row = {"seqname": "NODE_9_length_20000_cov_3.4||1_partial",
           "full_bp_start": "100", "full_bp_end": "8500",
           "trim_bp_start": "150", "trim_bp_end": "8000"}
    contig_lengths = {"NODE_9_length_20000_cov_3.4": 20000}
    assert vs2_module._row_interval(row, contig_lengths) == (
        "NODE_9_length_20000_cov_3.4", 100, 8500)


def test_contig_id_is_stripped_of_the_vs2_suffix(vs2_module):
    row = {"seqname": "H32R301um_2022-megahit_k141_202869||1_partial",
           "full_bp_start": "40", "full_bp_end": "9000"}
    contig_lengths = {"H32R301um_2022-megahit_k141_202869": 9500}
    contig_id, start, end = vs2_module._row_interval(row, contig_lengths)
    assert contig_id == "H32R301um_2022-megahit_k141_202869"
    assert (start, end) == (40, 9000)
