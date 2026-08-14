"""The per-assembly split must not lose, duplicate, or misattribute a row.

The splitter is the last thing between 994 shard tables and the 2,844 tables
that get delivered, and every one of its failure modes is quiet: a row routed to
the wrong assembly still validates, a dropped shard part still writes a
plausible table, and a `source` column left naming the shard still opens fine in
pandas. So the tests here are about attribution and conservation, not about
whether it runs.

Fixtures are synthetic but schema-real: they go through the same
`fabfos_evidence.validate_gpr` the transform calls, so a table this splitter
writes is checked by the same contract that guarded the one it read.

Run: python tests/test_split_by_assembly.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "src" / "metasmith_libraries" / "resources" / "lib"
os.environ["FABFOS_LIB"] = str(LIB)
sys.path.insert(0, str(LIB))
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples" / "fir"))

import pandas as pd                                                  # noqa: E402

import fabfos_evidence as fe                                         # noqa: E402
import split_by_assembly as sp                                       # noqa: E402

# (channel, intermediate_id, score, score_kind) tuples that satisfy the contract.
LANES = [
    ("kofam", "K00001", 120.0, "hmm_bitscore"),
    ("clean", "1.1.1.1", 0.25, "clean_maxsep_inv"),
    ("uniref50", "P00001", 0.90, "blast_bsr"),
    ("pbert", "REF00001", 0.75, "knn_vote"),
]


def _rows(sample: str, orf: str, shard: str, channels=None):
    out = []
    for i, (ch, iid, score, kind) in enumerate(LANES):
        if channels is not None and ch not in channels:
            continue
        out.append({
            "source": shard, "orf": f"{sample}::{orf}", "channel": ch,
            "mnxr": f"MNXR{1000 + i}", "intermediate_id": iid,
            "intermediate_name": iid, "raw_score": score, "score_kind": kind,
            "projection_via": "test", "evidence_quality": "reviewed",
            "lane_set": "chosen_4",
        })
    return out


def _shard_table(path: Path, shard: str, members: dict[str, list[str]],
                 channels=None):
    rows = []
    for sample, orfs in members.items():
        for orf in orfs:
            rows.extend(_rows(sample, orf, shard, channels))
    df = pd.DataFrame(rows)[fe.SCHEMA_COLS]
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def _corpus(tmp: Path, layout: dict[str, dict[str, list[str]]], channels=None):
    """Build shard tables, the parts.tsv the resharder would have written, and
    the per-sample ORF fastas the compaction validates against."""
    gpr = tmp / "gpr"
    results = tmp / "results"
    orfs = tmp / "orfs"
    orfs.mkdir(parents=True, exist_ok=True)
    sp.ORFS_DIR = orfs

    per_sample: dict[str, list[str]] = {}
    lines = ["shard\torder\tsample\tstart\tn"]
    for order, (shard, members) in enumerate(sorted(layout.items())):
        _shard_table(results / f"{shard}.parquet", shard, members, channels)
        for sample, ids in members.items():
            lines.append(f"{shard}\t{order}\t{sample}\t0\t{len(ids)}")
            per_sample.setdefault(sample, []).extend(ids)
    gpr.mkdir(parents=True, exist_ok=True)
    (gpr / "parts.tsv").write_text("\n".join(lines) + "\n")

    for sample, ids in per_sample.items():
        (orfs / f"{sample}.faa").write_text(
            "".join(f">{i} desc\nMAAA\n" for i in ids))
    return gpr, results, per_sample


def _run(gpr: Path, results: Path):
    assert sp.partition(gpr, [results]) == 0
    assert sp.compact(gpr) == 0
    assert sp.finish(gpr) == 0


def _expect_refusal(fn, needle: str, what: str):
    try:
        fn()
    except SystemExit as e:
        assert needle in str(e), f"wrong refusal: {e}"
    else:
        raise AssertionError(what)


def test_split_conserves_rows_and_reattributes_them(tmp_path):
    """The load-bearing case: an assembly split across two shards.

    Sequential fill means 993 of 994 shards end mid-assembly, so this is the
    common case, not the corner one. Both slices must land in one table, the
    `{sample}::` prefix must come off, and `source` must stop naming the shard.
    """
    layout = {
        "shard_0000": {"SAMP_A": ["o1", "o2"], "SAMP_B": ["o1"]},
        "shard_0001": {"SAMP_B": ["o2", "o3"], "SAMP_C": ["o1"]},
    }
    gpr, results, per_sample = _corpus(tmp_path, layout)
    _run(gpr, results)

    tables = sorted((gpr / "split" / "tables").glob("*.gpr.parquet"))
    assert [p.name for p in tables] == [
        "SAMP_A.gpr.parquet", "SAMP_B.gpr.parquet", "SAMP_C.gpr.parquet"]

    total = 0
    for p in tables:
        sample = p.name[: -len(".gpr.parquet")]
        df = pd.read_parquet(p)
        total += len(df)
        assert set(df["source"]) == {sample}, (
            f"{sample}: source still names {set(df['source'])}, so the "
            f"delivered table describes an ORF set nobody asked for")
        assert set(df["orf"]) == set(per_sample[sample]), (
            f"{sample}: ids are {sorted(set(df['orf']))[:3]}, not the fasta's")
        assert not df["orf"].str.contains("::").any()

    # 6 ORFs x 4 lanes, and SAMP_B's two shard slices arrived as 3 ORFs in one
    # table rather than as two tables or one truncated one.
    assert total == 6 * len(LANES)
    assert len(pd.read_parquet(gpr / "split" / "tables" / "SAMP_B.gpr.parquet")) \
        == 3 * len(LANES)


def test_a_lost_shard_part_is_refused(tmp_path):
    """Delete one slice of a split assembly AFTER its marker was written.

    Without this the table for a split sample silently ships the ORFs of
    whichever shards happened to finish -- a table that is complete-looking,
    validates cleanly, and is missing a third of its assembly. The marker lists
    the samples the shard emitted, so a part absent from a shard that CLAIMS to
    have emitted it is a loss, not an assembly with nothing to contribute.
    """
    layout = {
        "shard_0000": {"SAMP_A": ["o1", "o2"]},
        "shard_0001": {"SAMP_A": ["o3"]},
    }
    gpr, results, _ = _corpus(tmp_path, layout)
    assert sp.partition(gpr, [results]) == 0
    (gpr / "split" / "parts" / "SAMP_A" / "shard_0001.parquet").unlink()
    _expect_refusal(lambda: sp.compact(gpr), "was lost after the marker",
                    "a sample missing one of its shard slices compacted")


def test_an_unpartitioned_shard_is_refused(tmp_path):
    """A part absent with NO marker means the shard was never partitioned."""
    layout = {
        "shard_0000": {"SAMP_A": ["o1", "o2"]},
        "shard_0001": {"SAMP_A": ["o3"]},
    }
    gpr, results, _ = _corpus(tmp_path, layout)
    assert sp.partition(gpr, [results]) == 0
    (gpr / "split" / "parts" / "SAMP_A" / "shard_0001.parquet").unlink()
    (gpr / "split" / "parts" / "_shard_0001.done").unlink()
    _expect_refusal(lambda: sp.compact(gpr), "carries no completion marker",
                    "an unpartitioned shard compacted")


def test_an_assembly_with_no_evidence_still_delivers(tmp_path):
    """The population the first version could not deliver at all.

    An assembly whose ORFs produce no row in any lane has no group in the shard
    table, so no part file -- indistinguishable on disk from a lost part unless
    the marker says what the shard emitted. The corpus minimum is 1 ORF, so
    this is near-certain, not a corner case, and refusing it would block the
    whole delivery on the smallest assemblies.
    """
    layout = {"shard_0000": {"QUIET": ["o1"], "LOUD": ["o1", "o2"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    # Rewrite the shard table with QUIET contributing nothing at all.
    _shard_table(results / "shard_0000.parquet", "shard_0000", {"LOUD": ["o1", "o2"]})
    _run(gpr, results)

    out = gpr / "split" / "tables" / "QUIET.gpr.parquet"
    assert out.exists(), "an assembly with no evidence was not delivered"
    df = pd.read_parquet(out)
    assert len(df) == 0
    assert list(df.columns) == fe.SCHEMA_COLS, "the empty table is off-schema"
    assert "QUIET" in (gpr / "split" / "no_evidence.tsv").read_text()


def test_a_large_assembly_missing_a_channel_is_refused(tmp_path):
    """The relaxation must NOT extend to an assembly big enough to know better.

    A header-only legacy kofam file for one sample is invisible to every
    shard-level check -- the shard's other ~34 members keep its own kofam count
    non-zero -- so without a size bound the delivered table claims four lanes in
    its `lane_set` column and carries three.
    """
    layout = {"shard_0000": {"BIGSAMP": [f"o{i}" for i in range(12)]}}
    gpr, results, _ = _corpus(tmp_path, layout,
                              channels={"clean", "uniref50", "pbert"})
    assert sp.partition(gpr, [results]) == 0
    sp.RELAX_MAX_ORFS = 5          # 12 ORFs is "large" for this fixture
    try:
        _expect_refusal(lambda: sp.compact(gpr), "lost evidence, not",
                        "a large assembly missing a whole channel was delivered")
    finally:
        sp.RELAX_MAX_ORFS = 1000


def test_the_row_ledger_catches_a_dropped_table(tmp_path):
    """Rows in must equal rows out. Delete a delivered table's contents and the
    sample-set check still passes -- only the ledger notices."""
    layout = {"shard_0000": {"SAMP_A": ["o1"], "SAMP_B": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    assert sp.partition(gpr, [results]) == 0
    assert sp.compact(gpr) == 0
    # Same filename, same sample set, fewer rows: everything but the ledger passes.
    p = gpr / "split" / "tables" / "SAMP_B.gpr.parquet"
    pd.read_parquet(p).iloc[:1].to_parquet(p, index=False)
    _expect_refusal(lambda: sp.finish(gpr), "across the delivered tables",
                    "a table that lost rows passed the delivery gate")


def test_a_tiny_assembly_missing_a_channel_still_delivers(tmp_path):
    """One ORF with no KOfam hit is biology, not a broken join.

    `validate_gpr` refuses a table missing any of the four channels, which is
    right for a 100,000-ORF shard and would block delivery for the smallest
    assemblies in the corpus (the per-sample minimum is 1 ORF). The relaxation
    must be scoped to the sample -- and `finish` must still refuse a channel
    that is empty corpus-wide.
    """
    layout = {
        "shard_0000": {"TINY": ["o1"]},
        "shard_0001": {"BIG": ["o1", "o2"]},
    }
    gpr, results, _ = _corpus(tmp_path, layout)
    # TINY gets three lanes; BIG keeps all four, so no channel is empty overall.
    _shard_table(results / "shard_0000.parquet", "shard_0000",
                 {"TINY": ["o1"]}, channels={"clean", "uniref50", "pbert"})
    _run(gpr, results)

    df = pd.read_parquet(gpr / "split" / "tables" / "TINY.gpr.parquet")
    assert set(df["channel"]) == {"clean", "uniref50", "pbert"}
    assert fe.LANE_SETS["chosen_4"] == ("kofam", "clean", "uniref50", "pbert"), \
        "the per-sample relaxation leaked into the module constant"


def test_a_channel_empty_corpus_wide_is_refused(tmp_path):
    """The check the per-sample relaxation is standing in for."""
    layout = {"shard_0000": {"SAMP_A": ["o1"]}, "shard_0001": {"SAMP_B": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout,
                              channels={"clean", "uniref50", "pbert"})
    assert sp.partition(gpr, [results]) == 0
    assert sp.compact(gpr) == 0
    try:
        sp.finish(gpr)
    except SystemExit as e:
        assert "zero rows across all" in str(e), str(e)
    else:
        raise AssertionError("a corpus-wide empty channel was delivered")


def test_a_shard_collected_twice_is_refused(tmp_path):
    """Two batches both collecting one shard would double every row it holds."""
    layout = {"shard_0000": {"SAMP_A": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    other = tmp_path / "results_b"
    _shard_table(other / "shard_0000.parquet", "shard_0000", layout["shard_0000"])
    _expect_refusal(lambda: sp.partition(gpr, [results, other]), "claim source=",
                    "one shard collected into two batches was accepted")


def test_an_empty_results_root_is_refused(tmp_path):
    """An unmatched glob must not make every array task exit 0 doing nothing."""
    layout = {"shard_0000": {"SAMP_A": ["o1"]}}
    gpr, _results, _ = _corpus(tmp_path, layout)
    _expect_refusal(
        lambda: sp.partition(gpr, [tmp_path / "no" / "such" / "*" / "results"]),
        "no gpr_table parquet found",
        "an empty results root partitioned silently")


def test_a_non_gpr_parquet_in_the_results_root_is_ignored(tmp_path):
    """The results directory holds the other lanes' products too.

    `partition` is pointed at whole run-results directories, which carry the
    ProteinBERT embeddings parquet alongside the gpr tables. Identifying a
    gpr_table by "reading its `source` column didn't raise" does NOT work:
    pyarrow's `iter_batches(columns=["source"])` returns a batch that merely
    lacks the column, so the failure surfaces on the `.column()` call after the
    guard. This is the real-data case that a fixture of only-gpr-tables misses.
    """
    layout = {"shard_0000": {"SAMP_A": ["o1", "o2"]}}
    gpr, results, orfs = _corpus(tmp_path, layout)
    pd.DataFrame({"id": ["SAMP_A::o1"], "dim_0": [0.5]}).to_parquet(
        results / "proteinbert_embeddings.parquet", index=False)
    _run(gpr, results)
    got = pd.read_parquet(gpr / "split" / "tables" / "SAMP_A.gpr.parquet")
    assert len(got) == len(_rows("SAMP_A", "o1", "shard_0000")) * 2, (
        "the embeddings parquet perturbed the delivered table")


def test_a_drifted_contract_copy_is_refused(tmp_path):
    """The split re-validates against a COPY of the contract on the cluster.

    A drift that reorders SCHEMA_COLS refuses loudly anyway; one that WIDENS a
    score range or adds a lane set would pass in silence and make the
    re-validation a decoration. The digest pin is what closes that.
    """
    layout = {"shard_0000": {"SAMP_A": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    saved = sp.EXPECT_LIB_SHA
    sp.EXPECT_LIB_SHA = "0" * 64
    try:
        _expect_refusal(lambda: sp.partition(gpr, [results]), "digests",
                        "a drifted contract copy was accepted")
    finally:
        sp.EXPECT_LIB_SHA = saved


if __name__ == "__main__":
    import tempfile

    # Collected from the module, NOT from a hand-kept list: a list silently
    # drops any test added below it, so the one test written for the bug you
    # just found is the one that never runs. Definition order is preserved.
    tests = [v for k, v in list(globals().items())
             if k.startswith("test_") and callable(v)]
    for fn in tests:
        with tempfile.TemporaryDirectory() as td:
            fn(Path(td))
        print(f"  OK  {fn.__name__}")
    print(f"OK  ({len(tests)} tests)")
