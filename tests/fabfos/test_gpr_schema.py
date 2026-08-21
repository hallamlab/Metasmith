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
        "score": [250.5, 130.0],
        "E-value": [1e-70, 1e-40],
        "best": ["*", "*"],
    }).to_csv(p, index=False)


def _write_clean(p: Path, header=("Query ID", "Predicted EC number", "clean_score")):
    with open(p, "w") as fh:
        fh.write("\t".join(header) + "\n")
        # GMM-calibrated CONFIDENCES, in CLEAN's own scale -- higher is better, and
        # bounded by 1. The third call is below fe.CLEAN_MIN_SCORE and must not reach
        # the table: it names an EC the bridge does carry, so its absence is the
        # lane abstaining rather than a join that found nothing.
        fh.write(f"{ORFS[0]}\t{ECS[0]}\t0.9974\n")
        fh.write(f"{ORFS[1]}\t{ECS[1]}\t0.1500\n")
        fh.write(f"{ORFS[0]}\t{ECS[1]}\t0.0008\n")


def _write_uniref(p: Path, descriptions: Path):
    # The hit table carries the key; the subject title lives once per key beside
    # it, which is the pair the lane joins.
    rows, titles = [], []
    for orf, acc in zip(ORFS, ACCS):
        rows.append([orf, f"UniRef50_{acc}", "88.1", "300", "10", "1", "1", "300",
                     "1", "300", "1e-90", "410.0", "0.93"])
        titles.append([f"UniRef50_{acc}",
                       f"UniRef50_{acc} Some enzyme n=5 Tax=Bacteria RepID={acc}_BACSU"])
    pd.DataFrame(rows, columns=fe._BLAST6_BSR_COLS).to_csv(p, sep="\t", index=False)
    pd.DataFrame(titles, columns=["sseqid", "description"]).to_csv(
        descriptions, sep="\t", index=False)


def _write_deepec(p: Path):
    with open(p, "w") as fh:
        fh.write("Query ID\tPredicted EC number\n")
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
    # The ProteinBERT type names its own rows; ESM-C's still uses a sibling index.
    #
    # Returns the vectors, because the landmark set has to be built AROUND them: the
    # pbert lane refuses an ORF whose nearest landmark is below fe.PBERT_NN_MIN, and
    # two independent normal draws are orthogonal in expectation, so a landmark set
    # drawn on its own leaves every query abstaining and every lane empty.
    a = rng.normal(size=(len(ORFS), dim)).astype(np.float32)
    vecs = _dims(a)
    if idx is None:
        pd.concat([pd.DataFrame({"sequence_id": ORFS}), vecs], axis=1).to_parquet(
            emb, index=False)
    else:
        vecs.to_parquet(emb, index=False)
        pd.DataFrame({"sequence_id": ORFS}).to_csv(idx, index=False)
    return a


def _write_landmarks(lm_dir: Path, rng, dim=DIM, table="landmarks.parquet",
                     near: np.ndarray = None):
    # 40 labelled landmarks. PBERT_K_MAX is 30, so there must be at least that many
    # or the top-K partition indexes past the end.
    #
    # `near` puts a landmark on top of each query so the queries clear the lane's
    # proximity abstain; without it the fixture tests an empty table.
    n = 40
    a = rng.normal(size=(n, dim)).astype(np.float32)
    if near is not None:
        a[:len(near)] = near + 0.001 * rng.normal(size=near.shape).astype(np.float32)
    lm_dir.mkdir(parents=True, exist_ok=True)
    pd.concat([
        pd.DataFrame({
            "accession": [f"REF{i:04d}" for i in range(n)],
            # THE LABELS DIFFER BY POSITION. Giving every landmark the same list made
            # the emitted rows the same two MNXR whatever the quota admitted, so this
            # test passed with the quota deleted. The first `len(ORFS)` landmarks are
            # the ones placed on top of the queries by `near=`, and only they carry
            # MNXRS[:2]; everything else carries a third id that must NOT appear
            # unless the admission rule is broken.
            "mnxr_list": [";".join(MNXRS[:2])] * len(ORFS)
                         + [MNXRS[2]] * (n - len(ORFS)),
        }),
        _dims(a),
    ], axis=1).to_parquet(lm_dir / table, index=False)


def _lanes(work: Path, rng, seven: bool, lm_table="landmarks.parquet",
           esmc_lm_dim=DIM):
    # Write every input both mappers read; return the argv kwargs.
    _write_orfs(work / "orfs.faa")
    _write_kofam(work / "kofam.csv")
    _write_clean(work / "clean.tsv")
    _write_uniref(work / "uniref.tsv", work / "uniref_descriptions.tsv")
    _write_bridge(work / "bridge.parquet")
    q = _write_query_embeddings(work / "pbert.parquet", rng)
    _write_landmarks(work / "landmarks", rng, table=lm_table, near=q)
    kw = dict(
        ev_lib=str(EV_LIB), orfs=str(work / "orfs.faa"),
        kofam=str(work / "kofam.csv"), clean=str(work / "clean.tsv"),
        uniref=str(work / "uniref.tsv"),
        uniref_descriptions=str(work / "uniref_descriptions.tsv"),
        bridge=str(work / "bridge.parquet"),
        pbert_emb=str(work / "pbert.parquet"),
        landmarks=str(work / "landmarks"), out=str(work / "gpr.parquet"),
        # The BLAS thread floor the 4-lane mapper takes; only it has the flag. One,
        # because these fixtures are a few rows and it would otherwise oversubscribe
        # every core in the suite.
        threads=1,
    )
    if seven:
        _write_deepec(work / "deepec.tsv")
        _write_ezpred(work / "ezpred.csv")
        qe = _write_query_embeddings(work / "esmc.parquet", rng, dim=esmc_lm_dim,
                                     idx=work / "esmc_index.csv")
        # A SECOND landmark set, in its own directory. The ESM-C lane votes against
        # ESM-C embeddings -- cosine distance between two embedding spaces is a number
        # with no referent -- and the leaf name differs because nextflow stages a
        # process's inputs by basename and the mapper takes both.
        _write_landmarks(work / "landmarks_esmc", rng, dim=esmc_lm_dim, near=qe)
        kw.pop("threads")
        kw.update(
            lane_set="full_7", source="orfs",
            deepec=str(work / "deepec.tsv"), ezpred=str(work / "ezpred.csv"),
            esmc_emb=str(work / "esmc.parquet"), esmc_idx=str(work / "esmc_index.csv"),
            lm_esmc=str(work / "landmarks_esmc"),
        )
    else:
        kw.update(lane_set="chosen_4", source="orfs")
    return kw


def _argv(mapper: str, kw: dict) -> list[str]:
    driver = MLIB / "resources" / "lib" / "fabfos_gpr" / f"{mapper}.py"
    argv = [sys.executable, str(driver)]
    for flag, value in kw.items():
        argv += [f"--{flag.replace('_', '-')}", str(value)]
    return argv


def _run(argv: list[str], work: Path) -> subprocess.CompletedProcess:
    return subprocess.run(argv, cwd=work, capture_output=True, text=True)


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


def test_gpr_4lane_driver_writes_a_valid_table(tmp_path):
    rng = np.random.default_rng(0)
    kw = _lanes(tmp_path, rng, seven=False)
    r = _run(_argv("gpr_4lane", kw), tmp_path)
    assert r.returncode == 0, f"driver failed:\n{r.stdout}\n{r.stderr}"
    df = _check_table(tmp_path / "gpr.parquet", "chosen_4")

    # CLEAN's confidence reaches the table AS ITSELF. It used to be pushed through
    # a 1/(1+d) inversion on the reading that it was a distance, which ranked the
    # lane backwards -- the least confident call carried the largest weight.
    clean = df[df["channel"] == "clean"]
    assert sorted(np.round(clean["raw_score"].unique(), 4)) == [0.15, 0.9974]
    # THE ABSTAIN. The 0.0008 call names an EC the bridge carries, so its absence is
    # the lane declining rather than a join that found nothing.
    assert len(clean[(clean["orf"] == ORFS[0]) & (clean["intermediate_id"] == ECS[1])]) == 0

    # THE QUOTA ACTUALLY BIT. Only the landmarks sitting on top of the queries carry
    # MNXRS[:2]; every other landmark carries MNXRS[2]. A lane that admitted the
    # distant ones would emit it, so its absence is the admission rule working rather
    # than a fixture that cannot tell the difference.
    pb = df[df["channel"] == "pbert"]
    assert set(pb["mnxr"]) == set(MNXRS[:2]), sorted(set(pb["mnxr"]))

    assert set(df[df["channel"] == "uniref50"]["evidence_quality"]) == {"reviewed", "unreviewed"}
    assert set(df[df["channel"] == "pbert"]["evidence_quality"]) == {"reviewed"}


def test_gpr_7lane_driver_writes_a_valid_table(tmp_path):
    rng = np.random.default_rng(1)
    kw = _lanes(tmp_path, rng, seven=True)
    r = _run(_argv("gpr_7lane", kw), tmp_path)
    assert r.returncode == 0, f"driver failed:\n{r.stdout}\n{r.stderr}"
    df = _check_table(tmp_path / "gpr.parquet", "full_7")

    deepec = df[df["channel"] == "deepec"]
    assert (deepec["raw_score"] == 1.0).all()
    assert set(deepec["score_kind"]) == {"presence"}


def test_embedding_lane_refuses_a_landmark_dir_without_its_table(tmp_path):
    # No landmarks.parquet means no landmarks, and there is no degraded mode.
    rng = np.random.default_rng(2)
    kw = _lanes(tmp_path, rng, seven=False, lm_table="something_else.parquet")
    r = _run(_argv("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "landmarks.parquet" in r.stderr and "pbert" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_embedding_lane_refuses_a_query_of_a_different_width(tmp_path):
    # Voting a query against landmarks embedded by a different model is not a weaker
    # answer, it is a meaningless one. Differing width is the half of that a mapper can
    # see, and it is what the ESM-C lane pointed at the ProteinBERT set would hit.
    rng = np.random.default_rng(2)
    kw = _lanes(tmp_path, rng, seven=True, esmc_lm_dim=DIM)
    kw["lm_esmc"] = kw["landmarks"]          # the pbert set, at the pbert width
    _write_query_embeddings(tmp_path / "esmc.parquet", rng, dim=DIM + 4,
                            idx=tmp_path / "esmc_index.csv")
    r = _run(_argv("gpr_7lane", kw), tmp_path)
    assert r.returncode != 0
    assert "esmc" in r.stderr and "dims" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_embedding_lane_abstains_when_no_landmark_is_near(tmp_path):
    # The refusal the pbert lane could not make.
    #
    # `PBERT_FLOOR` gates the VOTE, which is normalised within the admitted
    # neighbours, so it reports agreement and not proximity -- thirty neighbours at
    # cosine 0.15 that agree score 1.0. Query vectors drawn independently of the
    # landmark set are orthogonal in expectation, which is the geometry of an ORF with
    # no relative in the reference: before the quota every one of them got a confident
    # call, which is the mechanism behind the spurious glycogen annotations.
    rng = np.random.default_rng(11)
    kw = _lanes(tmp_path, rng, seven=False)
    # the landmarks no longer sit on top of the queries
    _write_landmarks(tmp_path / "landmarks", np.random.default_rng(12))
    r = _run(_argv("gpr_4lane", kw), tmp_path)
    assert "2 with no landmark at cosine" in r.stdout, r.stdout
    # and the empty lane is then refused rather than written as a table with a
    # silently missing channel
    assert r.returncode != 0
    assert "pbert" in r.stderr and "0 rows" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_embedding_lane_admits_by_neighbourhood_not_by_a_fixed_count(tmp_path):
    # A dense neighbourhood votes with many neighbours, a thin one with exactly one.
    #
    # The whole reason for the quota: a fixed top-K gives every ORF K votes whether or
    # not it has K worth having, so the distant ones vote at full weight precisely when
    # the near ones are few. Both runs here retrieve the same PBERT_K_MAX candidates;
    # what differs is how many of them clear the band.
    def admitted(work: Path, seed: int, dense: bool) -> float:
        work.mkdir(parents=True, exist_ok=True)
        rng = np.random.default_rng(seed)
        kw = _lanes(work, rng, seven=False)
        q = pd.read_parquet(work / "pbert.parquet")
        qv = q[[c for c in q.columns if c.startswith("dim_")]].to_numpy(np.float32)
        n = 40
        lm = np.random.default_rng(seed + 1).normal(size=(n, DIM)).astype(np.float32)
        if dense:
            # every landmark is a near-copy of the first query
            lm[:] = qv[0] + 0.001 * rng.normal(size=(n, DIM)).astype(np.float32)
        else:
            # exactly one landmark per query, everything else far away
            lm[:len(qv)] = qv + 0.001 * rng.normal(size=qv.shape).astype(np.float32)
        (work / "landmarks").mkdir(exist_ok=True)
        pd.concat([pd.DataFrame({"accession": [f"REF{i:04d}" for i in range(n)],
                                 "mnxr_list": [";".join(MNXRS[:2])] * n}),
                   _dims(lm)], axis=1).to_parquet(
            work / "landmarks" / "landmarks.parquet", index=False)
        r = _run(_argv("gpr_4lane", kw), work)
        line = next(ln for ln in r.stdout.splitlines() if ln.startswith("[gpr] pbert:"))
        return float(line.split(" voting on ")[1].split(" admitted")[0])

    thin = admitted(tmp_path / "thin", 21, dense=False)
    packed = admitted(tmp_path / "packed", 31, dense=True)
    assert thin == 1.0, f"a thin neighbourhood admitted {thin} neighbours"
    assert packed >= 30.0, f"a dense neighbourhood admitted only {packed} neighbours"


def test_mapper_refuses_when_a_lane_contributes_no_rows(tmp_path):
    rng = np.random.default_rng(3)
    kw = _lanes(tmp_path, rng, seven=False)
    b = pd.read_parquet(tmp_path / "bridge.parquet")
    b[b["id_source"] != "ko"].to_parquet(tmp_path / "bridge.parquet", index=False)
    r = _run(_argv("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "kofam" in r.stderr and "0 rows" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_mapper_refuses_an_orf_id_mismatch(tmp_path):
    rng = np.random.default_rng(4)
    kw = _lanes(tmp_path, rng, seven=False)
    with open(tmp_path / "orfs.faa", "w") as fh:
        fh.write(">something_else_1\nMKV\n")
    r = _run(_argv("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "ORF ids that are not in this shard's FASTA" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_clean_lane_refuses_a_header_drift(tmp_path):
    rng = np.random.default_rng(5)
    kw = _lanes(tmp_path, rng, seven=False)
    _write_clean(tmp_path / "clean.tsv", header=("query", "ec", "score"))
    r = _run(_argv("gpr_4lane", kw), tmp_path)
    assert r.returncode != 0
    assert "clean_predictions header" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_validator_rejects_an_out_of_range_score():
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
    assert set(fe.CHANNEL_SCORE_KIND) == set(fe.CHANNELS)
    assert set(fe.CHANNEL_SCORE_KIND.values()) <= set(fe.SCORE_KINDS)
    assert set(fe.LANE_SETS["chosen_4"]) < set(fe.LANE_SETS["full_7"])
    for mapper in ("gpr_4lane", "gpr_7lane"):
        # The transform declares the lane; the module under `lib::fabfos_gpr` is where
        # the channel strings are actually written, so both are read.
        src = ((MLIB / "transforms" / "fabfos" / f"{mapper}.py").read_text()
               + (MLIB / "resources" / "lib" / "fabfos_gpr" / f"{mapper}.py").read_text())
        for dead in ("dl_ec", "uniref50_dr", "pbert_transfer"):
            assert f'"{dead}"' not in src, f"{mapper} still spells a channel {dead!r}"

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
