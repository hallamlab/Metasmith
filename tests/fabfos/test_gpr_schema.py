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

# The retired spellings of the nominator column. A table on a pre-schema layout
# names it in one of these, and `to_unified` is what renames it.
_ORF_ALIASES = ("feature_id", "feature_name", "evidence_id")

ORFS = ["pool1:megahit:k141_1:0-1200_1", "pool1:megahit:k141_1:1300-2400_2"]
KOS = ["K00001", "K00002"]
ECS = ["1.1.1.1", "2.7.1.1"]
ACCS = ["P00001", "Q00002"]
MNXRS = ["MNXR100001", "MNXR100002", "MNXR100003"]
DIM = 16


# `ref::lane_calibration`, cut down to the four fitted lanes and their real parameters.
# REAL PARAMETERS ON PURPOSE: the fixture has to exercise the clamp, both transforms and
# the constant-model branch, and a made-up curve would test `calibrate`'s arithmetic
# against itself. `deepec`, `ezpred` and `esmc` are absent here because they are absent
# from the artifact -- that is what makes a `full_7` table weighted on four lanes and
# flat on three.
_CALIBRATION = {
    "schema": "lane_calibration/v1",
    "prior": {"reference_lam0": fe.POOL_LAM0},
    "lanes": {
        "kofam": dict(transform="log", eps=1e-4, A=0.0, B=3.14485,
                      score_min=1.00008, score_max=39.17484),
        "clean": dict(transform="logit", eps=1e-4, A=0.45028, B=1.00176,
                      score_min=0.0251, score_max=0.9989),
        "uniref50": dict(transform="identity", eps=1e-4, A=0.0, B=5.86945,
                         score_min=0.5872, score_max=1.0119),
        "pbert": dict(transform="logit", eps=1e-4, A=0.25361, B=0.82117,
                      score_min=0.03267, score_max=1.0),
    },
}


def _write_calibration(d: Path) -> Path:
    import json
    d.mkdir(parents=True, exist_ok=True)
    (d / "lane_calibration.json").write_text(json.dumps(_CALIBRATION))
    return d


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


# ONE TAGGED RECORD AND ONE UNTAGGED ONE, in one FASTA. A RefSeq `_protein.faa` tags
# every record; a prodigal contig tags none; and the mapper has to describe both without
# a mode. `gene` is the tag or it is null, per record.
GENE_OF = {ORFS[0]: "thrA", ORFS[1]: None}


def _write_orfs(p: Path):
    with open(p, "w") as fh:
        for o in ORFS:
            tag = f" [gene={GENE_OF[o]}]" if GENE_OF[o] else ""
            fh.write(f">{o}{tag} # some prodigal trailer\nMKV\n")


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
    _write_calibration(work / "calibration")
    kw = dict(
        ev_lib=str(EV_LIB), calibration=str(work / "calibration"),
        orfs=str(work / "orfs.faa"),
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
    core = [c for c in fe.SCHEMA_COLS if c not in fe.NULLABLE_CORE]
    assert not df[core].isna().any().any(), "the core is non-null in every column"
    # The symbol is the FASTA header's, per record -- not a lane's, and not the table's.
    assert dict(df.groupby("orf")["gene"].agg(
        lambda s: s.dropna().unique()[0] if s.notna().any() else None)) == {
        o: GENE_OF[o] for o in set(df["orf"])}
    expected = set(fe.LANE_SETS[lane_set])
    assert set(df["channel"]) == expected, f"missing channels: {expected - set(df['channel'])}"
    assert set(df["lane_set"]) == {lane_set}
    assert set(df["orf"]) <= set(ORFS)
    assert set(df["evidence_quality"]) <= set(fe.EVIDENCE_QUALITY)
    # ONE COLUMN, ONE UNIT, ONE DIRECTION. The lane's own scale is gone by here --
    # `calibrate` is the last place it exists -- so what survives to be checked is that
    # every call is worth a non-negative, finite number of nats below the ceiling.
    v = df["llr"].astype(float)
    assert np.isfinite(v).all(), "a non-finite llr takes a whole reaction's belief"
    assert v.min() >= -1e-6, "these lanes emit no disconfirming calls"
    assert v.max() <= fe.LLR_CEILING, f"llr reaches {v.max()}, past the ceiling"
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
    # The fixture's two admitted calls are 0.15 and 0.9974, and `clean` is the one lane
    # here with a fitted SLOPE, so a reversal would show as an inverted order rather
    # than as a flat column. Both land inside the fitted band [2.354, 7.069] nats.
    lo, hi = sorted(np.round(clean["llr"].astype(float).unique(), 4))
    assert 2.3 < lo < hi < 7.1, (lo, hi)
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

    # THREE OF THESE SEVEN LANES HAVE NO FITTED CURVE. `deepec` is score-less, and
    # `ezpred` and `esmc` were never benchmarked, so all three carry the declared
    # placeholder and a `full_7` table is weighted on four lanes and flat on three.
    for ch in ("deepec", "ezpred", "esmc"):
        v = df.loc[df["channel"] == ch, "llr"].astype(float)
        assert len(v) and (v == fe.UNCALIBRATED_LLR).all(), f"{ch}: {sorted(set(v))}"
    assert df.loc[df["channel"] == "clean", "llr"].nunique() > 1


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


def _one_row(channel, lane_set, **over):
    row = {
        "source": "orfs", "orf": ORFS[0], "gene": None, "channel": channel,
        "mnxr": MNXRS[0], "intermediate_id": ECS[0], "intermediate_name": "",
        "llr": 4.0, "llr_route": fe.llr_route_for(_CALIBRATION, channel),
        "projection_via": "ec", "evidence_quality": "reviewed",
        "lane_set": lane_set,
    }
    row.update(over)
    return pd.DataFrame([row])[fe.SCHEMA_COLS]


def test_validator_rejects_an_llr_past_the_ceiling():
    # What a raw score written straight into the column looks like. e^20 is odds no
    # single annotation call earns, so past the ceiling is a scale error, not a strong
    # call -- and it would otherwise pool as one.
    df = _one_row("pbert", "chosen_4", llr=fe.LLR_CEILING + 1.0)
    with pytest.raises(SystemExit, match="ceiling"):
        fe.validate_gpr(df, "chosen_4", ORFS, "orfs")


def test_validator_rejects_a_negative_llr():
    # These lanes emit no disconfirming calls and `calibrate` floors at zero, so a
    # negative here means something bypassed it.
    df = _one_row("pbert", "chosen_4", llr=-0.5)
    with pytest.raises(SystemExit, match="negative"):
        fe.validate_gpr(df, "chosen_4", ORFS, "orfs")


def test_validator_rejects_a_nan_llr():
    # Pooling SUMS this column, so one NaN takes a whole reaction's belief with it.
    # The null check reaches it first, which is the same refusal by a shorter route --
    # `llr` is core and not in NULLABLE_CORE. The finite check is what catches an inf.
    df = _one_row("deepec", "full_7", llr=np.nan)
    with pytest.raises(SystemExit, match="null values"):
        fe.validate_gpr(df, "full_7", ORFS, "orfs")
    with pytest.raises(SystemExit, match="NaN or infinite"):
        fe.validate_gpr(_one_row("deepec", "full_7", llr=np.inf),
                        "full_7", ORFS, "orfs")


def test_validator_rejects_an_assertion_carrying_a_lanes_weight():
    # A curated row that picked up a fitted value would claim a measurement nothing
    # made. An assertion carries its declared constant or it is refused.
    df = _one_row("manual_gpr", fe.CURATED, llr=6.157)
    with pytest.raises(SystemExit, match="declared constant"):
        fe.validate_gpr(df, fe.CURATED, ORFS, "orfs")


def test_validator_accepts_a_null_gene():
    # `gene` is core and nullable. A de-novo ORF from an unnamed contig has no symbol,
    # and the validator must not read that as a malformed table.
    fe.validate_gpr(_one_row("manual_gpr", fe.CURATED, llr=fe.ASSERTION_LLR),
                    fe.CURATED, ORFS, "orfs")


def test_the_channel_vocabulary_has_exactly_one_spelling():
    # Every lane declares the range its own score runs in, checked where that score is
    # spent (`calibrate`). An undeclared lane turns an out-of-range score into a
    # confident number of nats rather than a refusal.
    assert set(fe.RAW_SCORE_RANGE) == set(fe.CHANNELS)
    assert set(fe.ASSERTION_CHANNELS).isdisjoint(fe.CHANNELS)
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
        + glob.glob(str(REPO_ROOT / "data/fabfos/benchmarks/*/gpr_manual.parquet"))))
    if not tables:
        pytest.skip("the DVC-tracked GPR tables are not materialised here")

    # NOT EVIDENCE TABLES, and the walk says so by name rather than by falling through a
    # column test. These two are reaction SETS -- mnxr x channel x origin, with no
    # nominator and no weight -- written by an older host-plus-insert union arm. The
    # schema describes nominations, so there is nothing here for it to describe.
    NOT_EVIDENCE = {
        "data/fabfos/runs/e_coli_epi300/gpr/gpr_epi300_clone2.parquet",
        "data/fabfos/runs/e_coli_epi300/gpr/gpr_union_epi300_183.parquet",
    }

    refused, legacy_layout, unnamed, checked = [], [], [], 0
    for f in tables:
        rel = str(Path(f).relative_to(REPO_ROOT))
        df = pd.read_parquet(f)
        if not ({"orf", *_ORF_ALIASES} & set(df.columns)):
            # A table with no nominator at all. Listed above, or it is news.
            if rel not in NOT_EVIDENCE:
                unnamed.append(rel)
            continue
        assert rel not in NOT_EVIDENCE, f"{rel} grew a nominator; drop it from the list"
        ext = fe.extensions_of(df)
        try:
            if not fe.is_unified(df):
                # THE BLOCKS ARE READ OFF THE TABLE, not assumed. A mapper writes the
                # core alone and the collector attaches attribution+feature+universe
                # later, so demanding those three of every table refuses the 13
                # perfectly good mapper outputs in this tree.
                df = fe.to_unified(df, ext)
                legacy_layout.append(rel)
            with contextlib.redirect_stdout(io.StringIO()):
                fe.validate_gpr(df, df["lane_set"].iat[0], None, df["source"].iat[0],
                                ext)
            checked += 1
        except SystemExit as e:
            refused.append(f"{Path(f).relative_to(REPO_ROOT)}: {e}")

    assert checked, "the walk found no GPR table at all"
    assert not unnamed, (
        "these tables name no nominator in any spelling, so nothing here can group "
        "belief over them:\n" + "\n".join(unnamed))
    assert not refused, "tables outside the schema:\n" + "\n".join(refused)
    assert not legacy_layout, (
        "these tables are on a pre-schema layout -- whatever wrote them is still "
        "building the old columns by hand:\n" + "\n".join(legacy_layout))
