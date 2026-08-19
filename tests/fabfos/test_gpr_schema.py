"""Execute both GPR mappers' drivers against synthetic lane outputs.

`test_gpr_workflow.py` proves the stage *plans*; this proves the code inside it
*runs* and that what it writes satisfies the schema contract. Neither needs a
container, a reference, or a GPU: the drivers are plain pandas/numpy, and the
inputs here are a dozen hand-written rows in each lane's real on-disk format.

That distinction is the point. The first real GPR table has never been written,
so every defect in these drivers -- a `.format()` placeholder that no longer
exists, a header the CLEAN parser no longer recognises, an ORF-id space that does
not match between two lanes -- would otherwise surface hours into a run that
staged an 8 GB DIAMOND database first.

The negative cases matter as much as the positive one: each asserts the mapper
*refuses* rather than writing a zero-row parquet and reporting success, which is
what it used to do for all of them.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/test_gpr_schema.py -v
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MLIB = REPO_ROOT / "src" / "metasmith_libraries"
EV_LIB = MLIB / "resources" / "lib" / "fabfos_evidence.py"

sys.path.insert(0, str(EV_LIB.parent))
import fabfos_evidence as fe  # noqa: E402

# Two ORFs, two KOs, two ECs, two UniProt accessions -- enough that every lane has
# rows and every dedup/merge has something to do.
ORFS = ["pool1:megahit:k141_1:0-1200_1", "pool1:megahit:k141_1:1300-2400_2"]
KOS = ["K00001", "K00002"]
ECS = ["1.1.1.1", "2.7.1.1"]
ACCS = ["P00001", "Q00002"]
MNXRS = ["MNXR100001", "MNXR100002", "MNXR100003"]
DIM = 16


def _write_bridge(p: Path):
    rows = [
        (KOS[0], "ko", MNXRS[0], "reviewed"),
        (KOS[1], "ko", MNXRS[1], "reviewed"),
        (ECS[0], "ec", MNXRS[0], "reviewed"),
        (ECS[1], "ec", MNXRS[2], "reviewed"),
        (ACCS[0], "uniprot", MNXRS[1], "reviewed"),
        (ACCS[1], "uniprot", MNXRS[2], "unreviewed"),
    ]
    pd.DataFrame(rows, columns=["id", "id_source", "mnxr", "evidence_quality"]).to_parquet(p, index=False)


def _write_orfs(p: Path):
    with open(p, "w") as fh:
        for o in ORFS:
            fh.write(f">{o} # some prodigal trailer\nMKV\n")


def _write_kofam(p: Path):
    pd.DataFrame({
        "gene_name": ORFS,
        "KO": KOS,
        "thrshld": [100.0, 100.0],
        "score": [250.5, 130.0],       # both above threshold
        "E-value": [1e-70, 1e-40],
        "best": ["*", "*"],
    }).to_csv(p, index=False)


def _write_clean(p: Path, header=("Query ID", "Predicted EC number", "clean_score")):
    with open(p, "w") as fh:
        fh.write("\t".join(header) + "\n")
        # maxsep DISTANCES, in CLEAN's own scale -- its parser's worked example is 8.06
        fh.write(f"{ORFS[0]}\t{ECS[0]}\t8.06\n")
        fh.write(f"{ORFS[1]}\t{ECS[1]}\t3.41\n")


def _write_uniref(p: Path):
    rows = []
    for orf, acc in zip(ORFS, ACCS):
        rows.append([orf, f"UniRef50_{acc}", "88.1", "300", "10", "1", "1", "300",
                     "1", "300", "1e-90", "410.0",
                     f"UniRef50_{acc} Some enzyme n=5 Tax=Bacteria RepID={acc}_BACSU", "0.93"])
    pd.DataFrame(rows).to_csv(p, sep="\t", header=False, index=False)


def _write_deepec(p: Path):
    with open(p, "w") as fh:
        fh.write("Query ID\tPredicted EC number\n")
        # PREFIXED, as DeepEC actually writes them -- `EC:1.1.1.1`, not `1.1.1.1`.
        # The fixture used to write them bare, which is why the gate passed while the
        # real lane matched nothing and contributed zero rows.
        for orf, ec in zip(ORFS, ECS):
            fh.write(f"{orf}\tEC:{ec}\n")


def _write_ezpred(p: Path):
    pd.DataFrame({
        "sequence_id": ORFS,
        "ec_number": ECS,
        "score": [0.91, 0.62],
        "head_kind": ["enzyme", "enzyme"],
    }).to_csv(p, index=False)


def _dims(a: np.ndarray) -> pd.DataFrame:
    return pd.DataFrame(a, columns=[f"dim_{i}" for i in range(a.shape[1])])


def _write_query_embeddings(emb: Path, rng, dim=DIM, idx: Path = None):
    """The ProteinBERT type names its own rows; ESM-C's still uses a sibling index."""
    vecs = _dims(rng.normal(size=(len(ORFS), dim)).astype(np.float32))
    if idx is None:
        pd.concat([pd.DataFrame({"sequence_id": ORFS}), vecs], axis=1).to_parquet(
            emb, index=False)
    else:
        vecs.to_parquet(emb, index=False)
        pd.DataFrame({"sequence_id": ORFS}).to_csv(idx, index=False)


def _write_landmarks(lm_dir: Path, rng, dim=DIM, table="landmarks.parquet"):
    """40 labelled landmarks. K=30 in the mappers, so there must be at least that many
    or the top-K partition indexes past the end."""
    n = 40
    lm_dir.mkdir(parents=True, exist_ok=True)
    pd.concat([
        pd.DataFrame({
            "accession": [f"REF{i:04d}" for i in range(n)],
            # every member carries the same two labels, so the kNN vote is 1.0 for
            # both and clears any floor -- this test is about plumbing, not recall
            "mnxr_list": [";".join(MNXRS[:2])] * n,
        }),
        _dims(rng.normal(size=(n, dim)).astype(np.float32)),
    ], axis=1).to_parquet(lm_dir / table, index=False)


def _lanes(work: Path, rng, seven: bool, lm_table="landmarks.parquet",
           esmc_lm_dim=DIM):
    """Write every input both mappers read; return the format kwargs."""
    _write_orfs(work / "orfs.faa")
    _write_kofam(work / "kofam.csv")
    _write_clean(work / "clean.tsv")
    _write_uniref(work / "uniref.tsv")
    _write_bridge(work / "bridge.parquet")
    _write_query_embeddings(work / "pbert.parquet", rng)
    _write_landmarks(work / "landmarks", rng, table=lm_table)
    kw = dict(
        ev_lib=str(EV_LIB), orfs=str(work / "orfs.faa"),
        kofam=str(work / "kofam.csv"), clean=str(work / "clean.tsv"),
        uniref=str(work / "uniref.tsv"), bridge=str(work / "bridge.parquet"),
        pbert_emb=str(work / "pbert.parquet"),
        landmarks=str(work / "landmarks"), out=str(work / "gpr.parquet"),
        # The BLAS thread floor the 4-lane driver bakes in; only that mapper has
        # the slot, and `format` ignores a key the 7-lane template does not use.
        # One, because these fixtures are a few rows and the driver would
        # otherwise oversubscribe every core in the suite.
        threads=1,
    )
    if seven:
        _write_deepec(work / "deepec.tsv")
        _write_ezpred(work / "ezpred.csv")
        _write_query_embeddings(work / "esmc.parquet", rng, dim=esmc_lm_dim,
                                idx=work / "esmc_index.csv")
        # A SECOND landmark set, in its own directory. The ESM-C lane votes against
        # ESM-C embeddings -- cosine distance between two embedding spaces is a number
        # with no referent -- and the leaf name differs because nextflow stages a
        # process's inputs by basename and the mapper takes both.
        _write_landmarks(work / "landmarks_esmc", rng, dim=esmc_lm_dim)
        kw.update(
            lane_set="full_7", source="orfs",
            deepec=str(work / "deepec.tsv"), ezpred=str(work / "ezpred.csv"),
            esmc_emb=str(work / "esmc.parquet"), esmc_idx=str(work / "esmc_index.csv"),
            lm_esmc=str(work / "landmarks_esmc"),
        )
    else:
        kw.update(lane_set="chosen_4", source="orfs")
    return kw


def _render(mapper: str, kw: dict) -> str:
    """Substitute the mapper's DRIVER exactly as its protocol() does.

    Imported by exec rather than by `import` because the transform module calls
    metasmith's python_api at import time.
    """
    src = (MLIB / "transforms" / "fabfos" / f"{mapper}.py").read_text()
    ns: dict = {}
    start = src.index("DRIVER = r'''")
    end = src.index("'''", start + len("DRIVER = r'''"))
    template = src[start + len("DRIVER = r'''"):end]
    exec(f"DRIVER = {template!r}", ns)
    return ns["DRIVER"].format(**kw)


def _run(driver: str, work: Path) -> subprocess.CompletedProcess:
    script = work / "_driver.py"
    script.write_text(driver)
    return subprocess.run([sys.executable, str(script)], cwd=work,
                          capture_output=True, text=True)


def _check_table(out: Path, lane_set: str):
    df = pd.read_parquet(out)
    assert list(df.columns) == fe.SCHEMA_COLS
    assert len(df) > 0
    assert not df.isna().any().any(), "the schema is non-null in every column"
    expected = set(fe.LANE_SETS[lane_set])
    assert set(df["channel"]) == expected, f"missing channels: {expected - set(df['channel'])}"
    assert set(df["lane_set"]) == {lane_set}
    assert set(df["orf"]) <= set(ORFS)
    assert set(df["evidence_quality"]) <= set(fe.EVIDENCE_QUALITY)
    for kind, grp in df.groupby("score_kind"):
        lo, hi = fe.SCORE_KINDS[kind]
        v = grp["raw_score"].astype(float)
        assert np.isfinite(v).all(), f"{kind} carries a non-finite score"
        assert lo is None or v.min() >= lo - 1e-6, f"{kind} min {v.min()} < {lo}"
        assert hi is None or v.max() <= hi + 1e-6, f"{kind} max {v.max()} > {hi}"
    key = ["source", "orf", "channel", "intermediate_id", "mnxr"]
    assert not df.duplicated(subset=key).any(), "the grain key is not unique"
    return df


# =====================================================================
# the happy paths
# =====================================================================

def test_gpr_4lane_driver_writes_a_valid_table(tmp_path):
    rng = np.random.default_rng(0)
    kw = _lanes(tmp_path, rng, seven=False)
    r = _run(_render("gpr_4lane", kw), tmp_path)
    assert r.returncode == 0, f"driver failed:\n{r.stdout}\n{r.stderr}"
    df = _check_table(tmp_path / "gpr.parquet", "chosen_4")

    # CLEAN's distance survives losslessly through the monotone re-expression:
    # the 8.06 the parser's own example shows comes back out of 1/s - 1.
    clean = df[df["channel"] == "clean"]
    assert np.isclose(sorted(1.0 / clean["raw_score"] - 1.0), [3.41, 8.06]).all()
    # and the LARGER distance is now the WEAKER score, which is the whole point
    assert clean["raw_score"].min() < clean["raw_score"].max()

    # evidence_quality is carried from the bridge, not defaulted: one of the two
    # UniProt accessions is unreviewed there.
    assert set(df[df["channel"] == "uniref50"]["evidence_quality"]) == {"reviewed", "unreviewed"}
    # ... while the embedding lane inherits the landmarks' reviewed cut
    assert set(df[df["channel"] == "pbert"]["evidence_quality"]) == {"reviewed"}


def test_gpr_7lane_driver_writes_a_valid_table(tmp_path):
    rng = np.random.default_rng(1)
    kw = _lanes(tmp_path, rng, seven=True)
    r = _run(_render("gpr_7lane", kw), tmp_path)
    assert r.returncode == 0, f"driver failed:\n{r.stdout}\n{r.stderr}"
    df = _check_table(tmp_path / "gpr.parquet", "full_7")

    # DeepEC is score-less: presence is 1.0, never NaN. A NaN here makes the
    # downstream share-of-sum read the lane's total as zero and fall back to a
    # uniform split with nothing raised.
    deepec = df[df["channel"] == "deepec"]
    assert (deepec["raw_score"] == 1.0).all()
    assert set(deepec["score_kind"]) == {"presence"}


# =====================================================================
# the refusals
# =====================================================================

def test_embedding_lane_refuses_a_landmark_dir_without_its_table(tmp_path):
    """No landmarks.parquet means no landmarks, and there is no degraded mode."""
    rng = np.random.default_rng(2)
    kw = _lanes(tmp_path, rng, seven=False, lm_table="something_else.parquet")
    r = _run(_render("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "landmarks.parquet" in r.stderr and "pbert" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_embedding_lane_refuses_a_query_of_a_different_width(tmp_path):
    """Voting a query against landmarks embedded by a different model is not a weaker
    answer, it is a meaningless one. Differing width is the half of that a mapper can
    see, and it is what the ESM-C lane pointed at the ProteinBERT set would hit."""
    rng = np.random.default_rng(2)
    kw = _lanes(tmp_path, rng, seven=True, esmc_lm_dim=DIM)
    kw["lm_esmc"] = kw["landmarks"]          # the pbert set, at the pbert width
    _write_query_embeddings(tmp_path / "esmc.parquet", rng, dim=DIM + 4,
                            idx=tmp_path / "esmc_index.csv")
    r = _run(_render("gpr_7lane", kw), tmp_path)
    assert r.returncode != 0
    assert "esmc" in r.stderr and "dims" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_mapper_refuses_when_a_lane_contributes_no_rows(tmp_path):
    """An unstaged reference or a broken join empties one lane. Name which."""
    rng = np.random.default_rng(3)
    kw = _lanes(tmp_path, rng, seven=False)
    # a bridge with no `ko` rows: the kofam lane joins to nothing
    b = pd.read_parquet(tmp_path / "bridge.parquet")
    b[b["id_source"] != "ko"].to_parquet(tmp_path / "bridge.parquet", index=False)
    r = _run(_render("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "kofam" in r.stderr and "0 rows" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_mapper_refuses_an_orf_id_mismatch(tmp_path):
    """CLEAN splits headers on whitespace and DIAMOND does not; if the lanes ever
    disagree about the id space, every cross-lane join is meaningless."""
    rng = np.random.default_rng(4)
    kw = _lanes(tmp_path, rng, seven=False)
    # the ORF FASTA no longer contains what the lanes annotated
    with open(tmp_path / "orfs.faa", "w") as fh:
        fh.write(">something_else_1\nMKV\n")
    r = _run(_render("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    # The mapper catches this at the lane rather than downstream at the empty
    # table, and says which lane and which id, because the id space is the thing
    # actually wrong and "the table is empty" named only the symptom.
    assert "ORF ids that are not in this shard's FASTA" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_clean_lane_refuses_a_header_drift(tmp_path):
    """The old parser renamed columns positionally, so a header change silently
    emptied the lane instead of raising."""
    rng = np.random.default_rng(5)
    kw = _lanes(tmp_path, rng, seven=False)
    _write_clean(tmp_path / "clean.tsv", header=("query", "ec", "score"))
    r = _run(_render("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "clean_predictions header" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_validator_rejects_an_out_of_range_score():
    """The direction/range contract is enforced, not merely documented."""
    df = pd.DataFrame([{
        "source": "orfs", "orf": ORFS[0], "channel": "pbert", "mnxr": MNXRS[0],
        "intermediate_id": "REF0001", "intermediate_name": "",
        "raw_score": 1.5, "score_kind": "knn_vote",
        "projection_via": "embedding_knn", "evidence_quality": "reviewed",
        "lane_set": "chosen_4",
    }])
    with pytest.raises(SystemExit, match="above the declared ceiling"):
        fe.validate_gpr(df, "chosen_4", ORFS, "orfs")


def test_validator_rejects_a_nan_score():
    df = pd.DataFrame([{
        "source": "orfs", "orf": ORFS[0], "channel": "deepec", "mnxr": MNXRS[0],
        "intermediate_id": ECS[0], "intermediate_name": "",
        "raw_score": np.nan, "score_kind": "presence",
        "projection_via": "ec", "evidence_quality": "reviewed",
        "lane_set": "full_7",
    }])
    with pytest.raises(SystemExit, match="null values"):
        fe.validate_gpr(df, "full_7", ORFS, "orfs")


def test_the_channel_vocabulary_has_exactly_one_spelling():
    """Two vocabularies existed in this tree at once. One now, declared once."""
    assert set(fe.CHANNEL_SCORE_KIND) == set(fe.CHANNELS)
    assert set(fe.CHANNEL_SCORE_KIND.values()) <= set(fe.SCORE_KINDS)
    assert set(fe.LANE_SETS["chosen_4"]) < set(fe.LANE_SETS["full_7"])
    for mapper in ("gpr_4lane", "gpr_7lane"):
        src = (MLIB / "transforms" / "fabfos" / f"{mapper}.py").read_text()
        for dead in ("dl_ec", "uniref50_dr", "pbert_transfer"):
            assert f'"{dead}"' not in src, f"{mapper} still spells a channel {dead!r}"

    # Every re-emitter of a mapper table, too. These carry the lane forward into the
    # benchmark schema, so a retired spelling here is a join that silently returns
    # nothing against a table the mapper wrote. `pbert_transfer` is NOT checked for
    # them: `lib::fabfos_embed_transfer.py` owns that name for a different
    # measurement, and `fabfos_evidence.read_embed_transfer` reads it on purpose.
    reemitters = {
        "host_gpr_denovo": REPO_ROOT / "src" / "fabfos" / "build_references" / "transforms"
                           / "benchmark" / "host_gpr_denovo.py",
        "host_denovo_from_mapper": REPO_ROOT / "src" / "fabfos" / "build_references"
                                   / "host_denovo_from_mapper.py",
    }
    for name, path in reemitters.items():
        src = path.read_text()
        for dead in ("dl_ec", "uniref50_dr", "clean_ec"):
            assert f'"{dead}"' not in src, f"{name} still spells a channel {dead!r}"


def test_the_schema_covers_every_gpr_table_in_the_tree():
    """One schema, one validator, for hosts, clones, cohorts, runs and communities.

    Four incompatible layouts existed here, and `validate_gpr` could check only one of
    them -- so the tables that most needed a contract were the ones outside it. This
    walks the actual tree and asserts the standing property: every GPR table carries the
    core plus whole declared blocks, and one validator passes on it.

    A table that needs converting on the way through is reported by name and fails.
    `to_unified` exists for reading an old file, not for letting a PRODUCER keep writing
    one: the layouts drifted apart in the first place because nothing said, at the point
    a table was written, which schema it was supposed to be on.
    """
    import glob
    import io
    import contextlib

    tables = sorted(set(
        glob.glob(str(REPO_ROOT / "data/fabfos/runs/*/gpr/*.parquet"))
        + glob.glob(str(REPO_ROOT / "data/fabfos/nostoc/annotation/*/gpr_4lane.parquet"))
        + glob.glob(str(REPO_ROOT / "data/fabfos/benchmarks/hosts/*/gpr_gem.parquet"))
        + glob.glob(str(REPO_ROOT / "data/fabfos/benchmarks/*/gpr_manual.parquet"))))
    if not tables:
        pytest.skip("the DVC-tracked GPR tables are not materialised here")

    refused, legacy_layout, checked = [], [], 0
    for f in tables:
        df = pd.read_parquet(f)
        # A GPR table is (nominator -> reaction, with a strength). The two epi300 union
        # files are derived aggregates over one -- reaction sets with an origin, no
        # score -- so the schema is not theirs to carry.
        if not {"channel", "mnxr", "raw_score"} <= set(df.columns):
            continue
        ext = fe.extensions_of(df)
        try:
            if not fe.is_unified(df):
                ext = ["attribution", "feature", "universe"]
                if "condition_id" in df.columns:
                    ext.append("cohort")
                df = fe.to_unified(df, tuple(ext))
                ext = tuple(ext)
                legacy_layout.append(str(Path(f).relative_to(REPO_ROOT)))
            with contextlib.redirect_stdout(io.StringIO()):
                fe.validate_gpr(df, df["lane_set"].iat[0], None, df["source"].iat[0],
                                ext)
            checked += 1
        except SystemExit as e:
            refused.append(f"{Path(f).relative_to(REPO_ROOT)}: {e}")

    assert checked, "the walk found no GPR table at all"
    assert not refused, "tables outside the schema:\n" + "\n".join(refused)
    assert not legacy_layout, (
        "these tables are on a pre-schema layout -- whatever wrote them is still "
        "building the old columns by hand:\n" + "\n".join(legacy_layout))
