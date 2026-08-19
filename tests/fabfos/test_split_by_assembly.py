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
import pytest                                                        # noqa: E402

import fabfos_evidence as fe                                         # noqa: E402
import split_by_assembly as sp                                       # noqa: E402

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

    assert total == 6 * len(LANES)
    assert len(pd.read_parquet(gpr / "split" / "tables" / "SAMP_B.gpr.parquet")) \
        == 3 * len(LANES)


def test_a_lost_shard_part_is_refused(tmp_path):
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
    layout = {"shard_0000": {"QUIET": ["o1"], "LOUD": ["o1", "o2"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    _shard_table(results / "shard_0000.parquet", "shard_0000", {"LOUD": ["o1", "o2"]})
    _run(gpr, results)

    out = gpr / "split" / "tables" / "QUIET.gpr.parquet"
    assert out.exists(), "an assembly with no evidence was not delivered"
    df = pd.read_parquet(out)
    assert len(df) == 0
    assert list(df.columns) == fe.SCHEMA_COLS, "the empty table is off-schema"
    assert "QUIET" in (gpr / "split" / "no_evidence.tsv").read_text()


@pytest.mark.parametrize("n_orfs", [1, 12])
def test_an_assembly_missing_a_channel_is_refused(tmp_path, n_orfs):
    layout = {"shard_0000": {"SAMP": [f"o{i}" for i in range(n_orfs)]}}
    gpr, results, _ = _corpus(tmp_path, layout,
                              channels={"clean", "uniref50", "pbert"})
    assert sp.partition(gpr, [results]) == 0
    _expect_refusal(lambda: sp.compact(gpr), "contributed 0 rows",
                    "an assembly missing a whole channel was delivered")


def test_the_row_ledger_catches_a_dropped_table(tmp_path):
    layout = {"shard_0000": {"SAMP_A": ["o1"], "SAMP_B": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    assert sp.partition(gpr, [results]) == 0
    assert sp.compact(gpr) == 0
    p = gpr / "split" / "tables" / "SAMP_B.gpr.parquet"
    pd.read_parquet(p).iloc[:1].to_parquet(p, index=False)
    _expect_refusal(lambda: sp.finish(gpr), "across the delivered tables",
                    "a table that lost rows passed the delivery gate")


def test_one_short_assembly_refuses_the_whole_delivery(tmp_path):
    layout = {
        "shard_0000": {"TINY": ["o1"]},
        "shard_0001": {"BIG": ["o1", "o2"]},
    }
    gpr, results, _ = _corpus(tmp_path, layout)
    _shard_table(results / "shard_0000.parquet", "shard_0000",
                 {"TINY": ["o1"]}, channels={"clean", "uniref50", "pbert"})
    assert sp.partition(gpr, [results]) == 0
    _expect_refusal(lambda: sp.compact(gpr), "contributed 0 rows",
                    "a short table rode along beside a well-formed one")
    assert fe.LANE_SETS["chosen_4"] == ("kofam", "clean", "uniref50", "pbert"), \
        "the declared lane set is not the module's to rewrite"


def test_a_corpus_with_no_evidence_at_all_is_refused(tmp_path):
    layout = {"shard_0000": {"SAMP_A": ["o1"]}, "shard_0001": {"SAMP_B": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    for shard in ("shard_0000", "shard_0001"):
        pd.DataFrame(columns=fe.SCHEMA_COLS).to_parquet(
            results / f"{shard}.parquet", index=False)
    _expect_refusal(lambda: sp.partition(gpr, [results]),
                    "no gpr_table parquet found",
                    "a corpus with no evidence at all was partitioned")


def test_a_shard_collected_twice_is_refused(tmp_path):
    layout = {"shard_0000": {"SAMP_A": ["o1"]}}
    gpr, results, _ = _corpus(tmp_path, layout)
    other = tmp_path / "results_b"
    _shard_table(other / "shard_0000.parquet", "shard_0000", layout["shard_0000"])
    _expect_refusal(lambda: sp.partition(gpr, [results, other]), "claim source=",
                    "one shard collected into two batches was accepted")


def test_an_empty_results_root_is_refused(tmp_path):
    layout = {"shard_0000": {"SAMP_A": ["o1"]}}
    gpr, _results, _ = _corpus(tmp_path, layout)
    _expect_refusal(
        lambda: sp.partition(gpr, [tmp_path / "no" / "such" / "*" / "results"]),
        "no gpr_table parquet found",
        "an empty results root partitioned silently")


def test_a_non_gpr_parquet_in_the_results_root_is_ignored(tmp_path):
    layout = {"shard_0000": {"SAMP_A": ["o1", "o2"]}}
    gpr, results, orfs = _corpus(tmp_path, layout)
    pd.DataFrame({"id": ["SAMP_A::o1"], "dim_0": [0.5]}).to_parquet(
        results / "proteinbert_embeddings.parquet", index=False)
    _run(gpr, results)
    got = pd.read_parquet(gpr / "split" / "tables" / "SAMP_A.gpr.parquet")
    assert len(got) == len(_rows("SAMP_A", "o1", "shard_0000")) * 2, (
        "the embeddings parquet perturbed the delivered table")


def test_a_drifted_contract_copy_is_refused(tmp_path):
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

    tests = [v for k, v in list(globals().items())
             if k.startswith("test_") and callable(v)]
    for fn in tests:
        with tempfile.TemporaryDirectory() as td:
            fn(Path(td))
        print(f"  OK  {fn.__name__}")
    print(f"OK  ({len(tests)} tests)")
