# Filtering 2's keep rule, the CheckV provirus id decomposition, and the island filter's gene
# id decomposition and pattern match -- the pure logic behind curate_trim_batch_pratama.py and
# island_filter_pratama.py, which `metasmith run` cannot exercise the same way merge_candidate_
# calls's interval arithmetic cannot (see test_viromics_intervals.py): both read lineage that a
# direct run has no slots to supply.
import importlib.util
from pathlib import Path

import pytest

from metasmith.python_api import TransformInstanceLibrary

BENCH_LIB = Path(__file__).resolve().parents[2] / "research" / "metasmith_benchmark" / "library"


def _load(name):
    TransformInstanceLibrary.Load(BENCH_LIB / "transforms" / "e3")
    path = BENCH_LIB / "transforms" / "e3" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def curate_module():
    return _load("curate_trim_batch_pratama")


@pytest.fixture(scope="module")
def island_module():
    return _load("island_filter_pratama")


# ---------------------------------------------------------------------------
# Filtering 2's keep rule (curate_trim_batch_pratama._keep)

def test_kept_for_having_viral_genes(curate_module):
    assert curate_module._keep(viral_genes=3, host_genes=5, gene_count=10) is True


def test_kept_for_no_viral_and_no_host_genes(curate_module):
    assert curate_module._keep(viral_genes=0, host_genes=0, gene_count=4) is True


def test_kept_for_mostly_unknown_genes(curate_module):
    # 3 of 4 genes are neither viral nor host: unknown fraction is exactly 0.75.
    assert curate_module._keep(viral_genes=0, host_genes=1, gene_count=4) is True


def test_rejected_below_every_rule(curate_module):
    # Some host genes, no viral genes, and under 75% unknown (1 of 4).
    assert curate_module._keep(viral_genes=0, host_genes=3, gene_count=4) is False


def test_gene_count_zero_falls_to_the_no_host_no_viral_rule(curate_module):
    assert curate_module._keep(viral_genes=0, host_genes=0, gene_count=0) is True


def test_gene_count_zero_with_host_genes_does_not_divide_by_zero(curate_module):
    # gene_count == 0 can't itself carry host genes in real CheckV output, but the guard must
    # still short-circuit before the unknown-fraction division rather than raising.
    assert curate_module._keep(viral_genes=0, host_genes=1, gene_count=0) is False


# ---------------------------------------------------------------------------
# The trimmed-vs-whole choice (curate_trim_batch_pratama._classify)

def test_classify_splits_whole_and_provirus_by_the_keep_rule(curate_module):
    rows = [
        ("kept_whole", False, 4, 0, 0),
        ("kept_provirus", True, 4, 1, 0),
        ("discarded_whole", False, 4, 0, 3),
        ("discarded_provirus", True, 4, 0, 3),
    ]
    keep_whole, keep_provirus, all_provirus_ids = curate_module._classify(rows)
    assert keep_whole == {"kept_whole"}
    assert keep_provirus == {"kept_provirus"}
    # A discarded contig's provirus flag still has to be known, so its own fragments in
    # proviruses.fna decompose rather than looking like a corrupt id.
    assert all_provirus_ids == {"kept_provirus", "discarded_provirus"}


# ---------------------------------------------------------------------------
# Provirus id decomposition (curate_trim_batch_pratama._split_provirus_id)

def test_split_provirus_id_simple(curate_module):
    known = {"S1|megahit|k141_5|100_200"}
    assert curate_module._split_provirus_id("S1|megahit|k141_5|100_200_1", known) == "S1|megahit|k141_5|100_200"


def test_split_provirus_id_frozen_id_ending_in_a_start_end_segment(curate_module):
    # The frozen_id's own last segment ("1_5000") already ends in digits; a naive strip of the
    # trailing "_<n>" could stop one underscore too early if it didn't rely on the known set.
    frozen_id = "S1|megahit|k141_9|1_5000"
    known = {frozen_id}
    assert curate_module._split_provirus_id(f"{frozen_id}_2", known) == frozen_id


def test_split_provirus_id_frozen_id_ending_in_an_assembler_style_number(curate_module):
    frozen_id = "S1|spades|NODE_12_length_5000_cov_3.1"
    known = {frozen_id}
    assert curate_module._split_provirus_id(f"{frozen_id}_1", known) == frozen_id


def test_split_provirus_id_does_not_collide_with_a_shorter_known_id(curate_module):
    # Both "S1|ctg" and "S1|ctg_5" are real, distinct provirus-flagged contigs. A region of the
    # second must resolve to the second, not be swallowed by the first as a false match.
    known = {"S1|ctg", "S1|ctg_5"}
    assert curate_module._split_provirus_id("S1|ctg_5_1", known) == "S1|ctg_5"


def test_split_provirus_id_rejects_an_unknown_base(curate_module):
    with pytest.raises(AssertionError):
        curate_module._split_provirus_id("S1|ctg_1", set())


# ---------------------------------------------------------------------------
# The island filter's pattern list (the paper's categories), over every column Antonio's script reads

@pytest.mark.parametrize("column,value", [
    ("marker", "integrase"),
    ("annotation_description", "putative transposase"),
    ("annotation_accessions", "PF01527_transposase"),
    ("annotation_conjscan", "glycosyltransferase family 2"),
    ("annotation_amr", "plasmid stability protein StbB"),
    ("taxname", "Integrase family"),
])
def test_bad_pattern_matches_every_read_column(island_module, tmp_path, column, value):
    columns = list(island_module.ANNOTATION_COLUMNS)
    header = ["gene"] + columns
    row = ["ctg_100kb_1"] + ["" for _ in columns]
    row[1 + columns.index(column)] = value
    path = tmp_path / "genes.tsv"
    path.write_text("\t".join(header) + "\n" + "\t".join(row) + "\n")

    bad = island_module._bad_contigs(path, known_ids={"ctg_100kb"})
    assert bad == {"ctg_100kb"}


def test_bad_pattern_does_not_match_ordinary_annotation(island_module, tmp_path):
    header = ["gene"] + list(island_module.ANNOTATION_COLUMNS)
    row = ["ctg_100kb_1", "VOG0099.V", "hypothetical protein", "-", "-", "-", "Caudoviricetes"]
    path = tmp_path / "genes.tsv"
    path.write_text("\t".join(header) + "\n" + "\t".join(row) + "\n")

    assert island_module._bad_contigs(path, known_ids={"ctg_100kb"}) == set()


@pytest.mark.parametrize("value", ["chromosome partitioning protein ParA", "type II toxin-antitoxin system RelE",
                                   "DNA separation protein"])
def test_antonios_extra_patterns_no_longer_match(island_module, tmp_path, value):
    header = ["gene"] + list(island_module.ANNOTATION_COLUMNS)
    row = ["ctg_100kb_1", "-", value, "-", "-", "-", "-"]
    path = tmp_path / "genes.tsv"
    path.write_text("\t".join(header) + "\n" + "\t".join(row) + "\n")
    assert island_module._bad_contigs(path, known_ids={"ctg_100kb"}) == set()


def test_bad_contigs_on_an_empty_table_is_the_empty_set(island_module, tmp_path):
    path = tmp_path / "genes.tsv"
    path.write_text("")
    assert island_module._bad_contigs(path, known_ids=set()) == set()


def test_contig_of_recovers_the_annotated_id(island_module):
    assert island_module._contig_of("ctg_100kb_3", {"ctg_100kb"}) == "ctg_100kb"


def test_contig_of_rejects_a_gene_outside_the_100kb_gate(island_module):
    # genomad_island_annotate_pratama only ever annotates representatives >=100 kb, so a gene
    # naming anything else means the length gate was not honoured upstream.
    with pytest.raises(AssertionError):
        island_module._contig_of("ctg_under_100kb_1", {"ctg_100kb"})


def test_island_filter_removes_only_long_matched_votus(island_module, tmp_path):
    records = [
        ("short_bad", "A" * 50_000),   # matches the pattern list, but under the 100 kb gate
        ("long_bad", "A" * 150_000),   # matches and is long: removed
        ("long_clean", "A" * 150_000),  # long, but annotated with nothing suspicious
    ]
    annotated = {rid for rid, seq in records if len(seq) >= island_module.ISLAND_LENGTH_BP}
    assert annotated == {"long_bad", "long_clean"}

    header = ["gene"] + list(island_module.ANNOTATION_COLUMNS)
    rows = [
        ["long_bad_1", "-", "integrase", "-", "-", "-", "-"],
        ["long_clean_1", "-", "hypothetical protein", "-", "-", "-", "-"],
    ]
    path = tmp_path / "genes.tsv"
    path.write_text("\t".join(header) + "\n" + "\n".join("\t".join(r) for r in rows) + "\n")

    bad = island_module._bad_contigs(path, annotated)
    assert bad == {"long_bad"}
