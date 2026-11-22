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
            fh.write(f">{o}{tag} # 1 # 300 # 1 # ID=1_1;partial=00\nMKVCAACHM\n")


def _write_kofam(p: Path):
    pd.DataFrame({
        "gene_name": ORFS,
        "KO": KOS,
        "thrshld": [100.0, 100.0],
        "score": [250.5, 130.0],
        "E-value": [1e-70, 1e-40],
        "best": ["*", "*"],
    }).to_csv(p, index=False)


def _write_kofam_descriptions(p: Path):
    pd.DataFrame({"KO": KOS, "definition": ["alcohol dehydrogenase", "hexokinase"]}
                 ).to_csv(p, index=False)


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
    # diamond_uniref50 keeps up to five hits per ORF. Each ORF's weaker hit names
    # the OTHER ORF's accession and comes first, so a lane that skipped the
    # best-hit sort would swap the two ORFs' reactions.
    rows, titles = [], []
    for orf, acc, other in zip(ORFS, ACCS, ACCS[::-1]):
        rows.append([orf, f"UniRef50_{other}", "41.0", "120", "60", "4", "1", "120",
                     "1", "120", "1e-8", "60.0", "0.14"])
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
    _write_kofam_descriptions(work / "kofam_descriptions.csv")
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
        kofam_descriptions=str(work / "kofam_descriptions.csv"),
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
        kw.pop("kofam_descriptions")
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


GPR_LIB = MLIB / "resources" / "lib" / "fabfos_gpr"


def _annotation_argv(kw: dict, out: Path, extra_lanes=()) -> list[str]:
    argv = [sys.executable, str(GPR_LIB / "annotation_table.py"),
            "--ev-lib", kw["ev_lib"], "--orfs", kw["orfs"], "--source", kw["source"],
            "--landmarks", kw["landmarks"], "--threads", "1", "--out", str(out),
            "--lane", "kofam", kw["kofam"], kw["kofam_descriptions"],
            "--lane", "clean", kw["clean"],
            "--lane", "uniref50", kw["uniref"], kw["uniref_descriptions"],
            "--lane", "pbert", kw["pbert_emb"]]
    for name, *paths in extra_lanes:
        argv += ["--lane", name, *map(str, paths)]
    return argv


def _gpr_argv(kw: dict, table: Path, lane_set: str, out: Path) -> list[str]:
    return [sys.executable, str(GPR_LIB / "gpr_table.py"),
            "--ev-lib", kw["ev_lib"], "--annotation-table", str(table),
            "--orfs", kw["orfs"], "--bridge", kw["bridge"],
            "--calibration", kw["calibration"], "--lane-set", lane_set,
            "--source", kw["source"], "--out", str(out)]


def _run_chain(work: Path, *argvs) -> subprocess.CompletedProcess:
    out, err = "", ""
    for argv in argvs:
        r = _run(argv, work)
        out, err = out + r.stdout, err + r.stderr
        if r.returncode:
            break
    return subprocess.CompletedProcess(argvs, r.returncode, out, err)


def _run4(kw: dict, work: Path) -> subprocess.CompletedProcess:
    # The two programs the `gpr_4lane` transform runs, in its order.
    table = Path(work) / "annotation_chosen_4.parquet"
    return _run_chain(work, _annotation_argv(kw, table),
                      _gpr_argv(kw, table, "chosen_4", Path(kw["out"])))


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


def _check_best_uniref_hit(df):
    uniref = df[df["channel"] == "uniref50"]
    assert set(zip(uniref["orf"], uniref["intermediate_id"])) == set(zip(ORFS, ACCS))


def test_gpr_4lane_driver_writes_a_valid_table(tmp_path):
    rng = np.random.default_rng(0)
    kw = _lanes(tmp_path, rng, seven=False)
    r = _run4(kw, tmp_path)
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
    _check_best_uniref_hit(df)


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
    r = _run4(kw, tmp_path)
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
    r = _run4(kw, tmp_path)
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
        r = _run4(kw, work)
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
    r = _run4(kw, tmp_path)
    assert r.returncode != 0
    assert "kofam" in r.stderr and "0 rows" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_mapper_refuses_an_orf_id_mismatch(tmp_path):
    rng = np.random.default_rng(4)
    kw = _lanes(tmp_path, rng, seven=False)
    with open(tmp_path / "orfs.faa", "w") as fh:
        fh.write(">something_else_1\nMKV\n")
    r = _run4(kw, tmp_path)
    assert r.returncode != 0
    assert "ORF ids that are not in this shard's FASTA" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


def test_clean_lane_refuses_a_header_drift(tmp_path):
    rng = np.random.default_rng(5)
    kw = _lanes(tmp_path, rng, seven=False)
    _write_clean(tmp_path / "clean.tsv", header=("query", "ec", "score"))
    r = _run4(kw, tmp_path)
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
    assert set(fe.LANE_SETS["chosen_4"]) < set(fe.LANE_SETS["all_reaction"])
    # The transform declares the lane; the modules under `lib::fabfos_gpr` are where the
    # channel strings are actually written, so both are read.
    for src_path in [*(MLIB / "transforms" / "fabfos").glob("gpr_*.py"),
                     *GPR_LIB.glob("*.py")]:
        src = src_path.read_text()
        for dead in ("dl_ec", "uniref50_dr", "pbert_transfer"):
            assert f'"{dead}"' not in src, f"{src_path.name} still spells a channel {dead!r}"

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


# ---- the annotation table, and the GPR tables cut from it -----------------------------
#
# One fixture file per lane the aggregator reads beyond the four above, each in the layout
# its producer writes. The batch-split lanes name their query `<stem>~<orf>`, which is what
# the splitter feeding them writes and no merge removes; the table must carry the ORF as the
# FASTA names it.
CONTIGS = ["pool1:megahit:k141_1", "pool1:megahit:k141_2"]
_EGGNOG_HEADER = ("#query\tseed_ortholog\tevalue\tscore\teggNOG_OGs\tmax_annot_lvl\t"
                  "COG_category\tDescription\tPreferred_name\tGOs\tEC\tKEGG_ko\t"
                  "KEGG_Pathway\tKEGG_Module\tKEGG_Reaction\tKEGG_rclass\tBRITE\tKEGG_TC\t"
                  "CAZy\tBiGG_Reaction\tPFAMs")


DEEPARG_HEADER = ("#ARG\tquery-start\tquery-end\tread_id\tpredicted_ARG-class\tbest-hit\t"
                  "probability\tidentity\talignment-length\talignment-bitscore\t"
                  "alignment-evalue\tcounts")


def _deeparg_row(orf):
    return (f"TEM-1\t1\t280\t{orf}\tbeta-lactam\tAAA1|FEATURES|TEM-1|beta-lactam|TEM-1\t"
            "0.987\t99.0\t280\t560.0\t1e-150\t1")


def _write_extra_lanes(work: Path) -> list:
    w = work
    with open(w / "eggnog.tsv", "w") as fh:
        fh.write("## emapper-2.1.12\n" + _EGGNOG_HEADER + "\n")
        fh.write(f"{ORFS[0]}\t562.ABC\t1e-50\t200.5\tCOG1@1|root\troot\tC\t"
                 f"Alcohol dehydrogenase\tadhE\tGO:0000001,GO:0000002\t-\tko:{KOS[0]}\t"
                 f"-\t-\tR00754\t-\t-\t-\t-\tALCD2x\tADH_N,ADH_zinc_N\n")
        fh.write(f"{ORFS[1]}\t562.DEF\t1e-30\t150.0\tCOG2@1|root\troot\tG\tHexokinase\t"
                 f"glk\t-\t{ECS[1]}\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\n")
        fh.write("## 2 queries scanned\n")
    pd.DataFrame({
        "orf": [ORFS[0]], "start": ["5"], "stop": ["180"], "score": ["1.2E-40"],
        "database": ["Pfam"], "Name": ["PF00107"], "Dbxref": ["IPR013149"],
        "Ontology_term": ["GO:0016491|GO:0055114"],
    }).to_csv(w / "ips.csv", index=False)
    pd.DataFrame({"Name": ["PF00107"], "description": ["Zinc-binding dehydrogenase"]}
                 ).to_csv(w / "ips_descriptions.csv", index=False)
    blast = lambda q, s, title: (f"orfs~{q}\t{s}\t95.0\t300\t5\t0\t1\t300\t1\t300\t1e-80\t"
                                 f"410.0" + (f"\t{title}" if title else "") + "\n")
    for lane in ("mobileog", "megares", "bacmet", "vfdb"):
        (w / f"{lane}.tsv").write_text(blast(ORFS[0], f"{lane.upper()}0001",
                                             f"{lane.upper()}0001 some {lane} gene"))
    (w / "megares.tsv").write_text("")      # a lane with no hits is a lane, not a failure
    (w / "bigg.tsv").write_text(blast(ORFS[1], "b0001", None).replace("orfs~", ""))
    pd.DataFrame({
        "ORF_ID": [f"orfs~{ORFS[1]} # 1 # 300 # 1 # ID=1_2"], "Cut_Off": ["Strict"],
        "Best_Hit_Bitscore": ["512.3"], "Best_Hit_ARO": ["TEM-1"], "ARO": ["3000873"],
    }).to_csv(w / "rgi.tsv", sep="\t", index=False)
    pd.DataFrame({
        "Protein id": [f"orfs~{ORFS[0]}"], "Element symbol": ["blaTEM-1"],
        "Element name": ["class A beta-lactamase TEM-1"],
        "% Identity to reference": ["100.00"], "Closest reference accession": ["WP_000027057.1"],
    }).to_csv(w / "amrfinderplus.tsv", sep="\t", index=False)
    with open(w / "bakta.tsv", "w") as fh:
        fh.write("# Annotated with Bakta\n#Sequence Id\tType\tStart\tStop\tStrand\t"
                 "Locus Tag\tGene\tProduct\tDbXrefs\n")
        fh.write(f"{CONTIGS[0]}\ttRNA\t10\t85\t+\tX_1\ttrnA\ttRNA-Ala\tSO:0000253\n")
        fh.write(f"{CONTIGS[1]}\tncRNA\t200\t300\t-\tX_2\t\t6S ncRNA\tRFAM:RF00013\n")
    import json
    (w / "antismash.json").write_text(json.dumps({"records": [
        {"id": CONTIGS[0], "areas": [{"start": 0, "end": 5000, "products": ["NRPS", "T1PKS"]}]},
        {"id": CONTIGS[1], "areas": []},
    ]}))
    # hmmsearch --domtblout: ORF, -, len, model, -, model len, full E, score, bias, #, of,
    # c-E, i-E, dom score, bias, hmm from, hmm to, ali from, ali to, env from, env to, acc.
    dom = lambda orf, model, mlen, e, hf, ht: (
        f"{orf} - 300 {model} - {mlen} {e} 140.0 0.1 1 1 {e} {e} 139.0 0.1 {hf} {ht} "
        f"5 260 3 262 0.95 -\n")
    trailer = "# Program:         hmmsearch\n# [ok]\n"
    # GH5_4 twice, the second domain covering less: one row, at the better coverage.
    # GT2 passes hmmsearch's -E and fails DRAM's 1e-18, so it is in the table and not
    # in DRAM's render.
    (w / "dbcan.domtbl").write_text(
        "# target name\n" + dom(ORFS[0], "GH5_4.hmm", 280, "1e-40", 10, 250)
        + dom(ORFS[0], "GH5_4.hmm", 280, "1e-40", 100, 120)
        + dom(ORFS[0], "GT2.hmm", 200, "1e-10", 1, 190) + trailer)
    (w / "dbcan_descriptions.tsv").write_text(
        "term\tdescription\nGH5_4\tGH5 endo-beta-1,4-glucanase\nGT2\tGT2 cellulose synthase\n")
    (w / "vogdb.domtbl").write_text(
        "# target name\n" + dom(ORFS[0], "VOG00001", 169, "1e-30", 5, 160) + trailer)
    (w / "vogdb_descriptions.tsv").write_text(
        "term\tdescription\nVOG00001\tREFSEQ hypothetical protein; XrXs\n")
    pep = ("MER0000002 - chymotrypsin A (cattle-type) (Bos taurus) [S01.001]#S01A#"
           "{peptidase unit: 34-263}~source XP_608091~")
    (w / "merops.tsv").write_text(
        blast(ORFS[0], "MER0000002", pep).replace("orfs~", "")
        + blast(ORFS[0], "MER0000009", "MER0000009 - weak [S01.009]#S01A#").replace(
            "orfs~", "").replace("410.0", "55.0"))
    (w / "refseq_viral.tsv").write_text(
        blast(ORFS[0], "YP_009001.1", "YP_009001.1 ribonucleotide reductase [Synechococcus phage]")
        .replace("orfs~", ""))
    _write_deepec(w / "deepec.tsv")
    header = ("qseqid\tsseqid\tpident\tlength\tmismatch\tgapopen\tqstart\tqend\tsstart\tsend\t"
              "evalue\tbitscore\tbsr\n")
    bsr_lanes = {"swissprot": "sp|P07327|ADH1A_HUMAN", "uniref90": "UniRef90_P07327",
                 "metacyc": "gnl|META|MONOMER-1"}
    for lane, sid in bsr_lanes.items():
        (w / f"{lane}.tsv").write_text(header + f"orfs~{ORFS[0]}\t{sid}\t95.0\t300\t5\t0\t1\t300\t1\t300\t"
                                       "1e-80\t410.0\t0.9\n")
        (w / f"{lane}_descriptions.tsv").write_text(f"sseqid\tdescription\n{sid}\tAlcohol dehydrogenase\n")
    # hmmsearch --tblout: ORF, -, model, -, full E, score, bias, then the best domain's.
    tbl = lambda orf, model, score: (f"{orf} - {model} - 1e-10 {score} 0.1 1e-10 {score} 0.1 "
                                     "1.0 1 0 0 1 1 1 1 -\n")
    (w / "pf_tox.tbl").write_text("# target name\n" + tbl(ORFS[0], "pfam00001", "41.0")
                                  + "# [ok]\n")
    (w / "pf_vf.tbl").write_text("# target name\n" + tbl(ORFS[0], "cd00001", "45.0")
                                 + tbl(ORFS[0], "cd00002", "60.5") + "# [ok]\n")
    (w / "pf_desc.tsv").write_text("term\tdescription\ncd00001: adhesin\tadhesin\n")
    # The toxin script rounds to two decimals and the VF script does not.
    (w / "pf_tox_ml.tsv").write_text(
        f"Sequence\tPrediction\tProbability\n{ORFS[0]}\t1\t0.55\n{ORFS[1]}\t1\t0.80\n")
    (w / "pf_vf_ml.tsv").write_text(
        f"Sequence\tPrediction\tProbability\n{ORFS[0]}\t1\t0.95123456\n"
        f"{ORFS[1]}\t0\t0.2\n")
    (w / "deeparg.tsv").write_text(DEEPARG_HEADER + "\n" + _deeparg_row(ORFS[0]) + "\n")
    return [
        *[(lane, w / f"{lane}.tsv", w / f"{lane}_descriptions.tsv") for lane in bsr_lanes],
        ("pathofact_hmm", w / "pf_tox.tbl", w / "pf_vf.tbl", w / "pf_desc.tsv"),
        ("pathofact_ml", w / "pf_tox_ml.tsv", w / "pf_vf_ml.tsv"),
        ("deeparg", w / "deeparg.tsv"),
        ("dbcan", w / "dbcan.domtbl", w / "dbcan_descriptions.tsv"),
        ("merops", w / "merops.tsv"),
        ("vogdb", w / "vogdb.domtbl", w / "vogdb_descriptions.tsv"),
        ("refseq_viral", w / "refseq_viral.tsv"),
        ("deepec", w / "deepec.tsv"), ("eggnog", w / "eggnog.tsv"),
        ("interproscan", w / "ips.csv", w / "ips_descriptions.csv"),
        *[(lane, w / f"{lane}.tsv") for lane in ("mobileog", "megares", "bacmet", "vfdb")],
        ("bigg", w / "bigg.tsv"), ("rgi", w / "rgi.tsv"),
        ("amrfinderplus", w / "amrfinderplus.tsv"),
        ("bakta_nc", w / "bakta.tsv"), ("antismash", w / "antismash.json"),
    ]


# By path: the transform library's own annotation_table.py takes that module name first
# whenever a planning test runs earlier in the session.
def _gpr_module(name):
    import importlib.util
    spec = importlib.util.spec_from_file_location("fabfos_gpr_" + name, GPR_LIB / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _annotation_table(work: Path, rng):
    kw = _lanes(work, rng, seven=False)
    extra = _write_extra_lanes(work)
    table = work / "annotation.parquet"
    r = _run(_annotation_argv(kw, table, extra), work)
    assert r.returncode == 0, f"aggregator failed:\n{r.stdout}\n{r.stderr}"
    return kw, table


def test_annotation_table_carries_every_lane_under_the_fastas_ids(tmp_path):
    at = _gpr_module("annotation_table")
    kw, table = _annotation_table(tmp_path, np.random.default_rng(40))
    df = pd.read_parquet(table)
    assert list(df.columns) == at.ANNOTATION_COLS
    # every lane given is a lane of the table, the hitless one included only by name
    assert set(df["lane"]) == set(at.LANES) - {"megares"}
    orf_rows = df[df["orf"].notna()]
    assert set(orf_rows["orf"]) == set(ORFS), "the `<stem>~` prefix reached the table"
    contig_rows = df[df["lane"].isin(["bakta_nc", "antismash"])]
    assert contig_rows["orf"].isna().all() and set(contig_rows["contig"]) <= set(CONTIGS)
    assert set(df.loc[df["lane"] == "antismash", "term"]) == {"NRPS", "T1PKS"}
    assert set(df.loc[df["lane"] == "bakta_nc", "term"]) == {"trnA", "6S ncRNA"}
    # nothing abstains here: CLEAN's 0.0008 call is in the table, for a consumer to drop
    assert (df.loc[df["lane"] == "clean", "score"] < fe.CLEAN_MIN_SCORE).any()
    egg = df[df["lane"] == "eggnog"]
    assert set(egg.loc[egg["namespace"] == "ko", "term"]) == {KOS[0]}
    assert set(egg.loc[egg["namespace"] == "ec", "term"]) == {ECS[1]}
    assert {"go", "pfam", "bigg.reaction", "kegg.reaction", "eggnog.seed"} <= set(egg["namespace"])
    assert set(df.loc[df["lane"] == "rgi", "term"]) == {"3000873"}
    assert set(df.loc[df["lane"] == "interproscan", "namespace"]) == {"pfam", "interpro", "go"}
    assert set(df.loc[df["lane"] == "kofam", "description"]) == {"alcohol dehydrogenase",
                                                                 "hexokinase"}
    # one row per ORF x model at its best-covering domain, and DRAM's E-value cut not applied
    cazy = df[df["lane"] == "dbcan"].set_index("term")
    assert sorted(cazy.index) == ["GH5_4", "GT2"]
    assert cazy.loc["GH5_4", "score"] == pytest.approx(240 / 280)
    assert set(df.loc[df["lane"] == "merops", "term"]) == {"MER0000002", "MER0000009"}


def test_dram_annotations_render_one_mags_rows_the_way_dram_cuts_them(tmp_path):
    dram = _gpr_module("dram_annotations")
    kw, table = _annotation_table(tmp_path, np.random.default_rng(43))
    scaffold = ORFS[0].rpartition("_")[0]
    (tmp_path / "mag1.fna").write_text(f">{scaffold}\nACGT\n")
    out = tmp_path / "annotations.tsv"
    r = _run([sys.executable, str(GPR_LIB / "dram_annotations.py"), "--ev-lib", kw["ev_lib"],
              "--annotation-table", str(table), "--orfs", kw["orfs"],
              "--mag", str(tmp_path / "mag1.fna"), "--name", "mag1", "--out", str(out)],
             tmp_path)
    assert r.returncode == 0, f"render failed:\n{r.stdout}\n{r.stderr}"
    df = pd.read_csv(out, sep="\t", index_col=0)
    assert list(df.columns) == dram.DRAM_COLS
    # only the MAG's ORF, under the id the FASTA gives it
    assert list(df.index) == [ORFS[0]]
    row = df.loc[ORFS[0]]
    assert (row["fasta"], row["scaffold"], row["gene_position"]) == ("mag1", scaffold, 1)
    assert (row["start_position"], row["end_position"], row["strandedness"]) == (1, 300, 1)
    assert (row["ko_id"], row["kegg_hit"]) == (KOS[0], "alcohol dehydrogenase")
    assert row["uniref_id"] == f"UniRef50_{ACCS[0]}"
    assert row["pfam_hits"] == "Zinc-binding dehydrogenase [PF00107.]"
    assert (row["cazy_ids"], row["cazy_best_hit"]) == ("GH5_4", "GH5_4")
    assert row["cazy_hits"] == "GH5 endo-beta-1,4-glucanase"
    assert (row["peptidase_id"], row["peptidase_family"]) == ("MER0000002", "S01A")
    assert (row["vogdb_id"], row["vogdb_categories"]) == ("VOG00001", "Xr;Xs")
    assert row["heme_regulatory_motif_count"] == 1
    assert row["rank"] == "C"


def test_dramv_annotations_move_virsorter2_genes_into_the_representatives_frame(tmp_path):
    dram = _gpr_module("dram_annotations")
    kw, table = _annotation_table(tmp_path, np.random.default_rng(44))
    # The representative is bases 51..2000 of `asm1~k141_1`, and its one ORF (prodigal-gv on
    # the frozen set) is already in its frame. The member is not a representative.
    rep, member = "s1|asm1~k141_1|51_2000", "s1|asm1~k141_1|60_1990"
    orf = rep + "_1"
    t = pd.read_parquet(table)
    t.loc[t["orf"] == ORFS[0], "orf"] = orf
    t.to_parquet(tmp_path / "frozen_table.parquet", index=False)
    (tmp_path / "orfs.faa").write_text(f">{orf} # 51 # 350 # -1 # ID=1_1\nMKVCAACHM\n"
                                       f">{member}_1 # 1 # 300 # 1 # ID=2_1\nMKV\n")
    (tmp_path / "frozen.fna").write_text(f">{rep}\nACGT\n>{member}\nACGT\n")
    pd.DataFrame({"frozen_id": [rep, member], "sample": ["s1", "s1"],
                  "source_contig": ["asm1~k141_1"] * 2, "start": [51, 60],
                  "end": [2000, 1990], "length": [1950, 1931], "callers": ["virsorter2"] * 2}
                 ).to_csv(tmp_path / "prov.tsv", sep="\t", index=False)
    (tmp_path / "votu.tsv").write_text(f"{rep}\t{rep}\n{rep}\t{member}\n")
    # VirSorter2 calls genes on the whole batch contig and keeps those coordinates in the
    # affi table; the one at 10..40 lies outside the interval.
    gene = lambda i, s, e, cat: (f"asm1~k141_1__full__{i}|{s}|{e}|{e - s + 1}|1|"
                                 f"hmm{i}|50|1e-10|{cat}|-|-|-\n")
    (tmp_path / "affi.tab").write_text(
        ">asm1~k141_1__full|4|l\n" + gene(1, 10, 40, 1) + gene(2, 60, 99, 0)
        + gene(3, 101, 400, 2) + gene(4, 500, 900, 0))
    out = tmp_path / "rendered.tsv"
    r = _run([sys.executable, str(GPR_LIB / "dramv_annotations.py"), "--ev-lib", kw["ev_lib"],
              "--frozen", str(tmp_path / "frozen.fna"), "--provenance", str(tmp_path / "prov.tsv"),
              "--votu", str(tmp_path / "votu.tsv"), "--orfs", str(tmp_path / "orfs.faa"),
              "--annotation-table", str(tmp_path / "frozen_table.parquet"),
              "--affi", str(tmp_path / "affi.tab"), "--name", "votus",
              "--out-annotations", str(out), "--out-affi", str(tmp_path / "votu.tab"),
              "--out-fasta", str(tmp_path / "reps.fna")], tmp_path)
    assert r.returncode == 0, f"render failed:\n{r.stdout}\n{r.stderr}"
    df = pd.read_csv(out, sep="\t", index_col=0)
    assert list(df.columns) == dram.DRAMV_COLS
    assert list(df.index) == [orf]
    row = df.iloc[0]
    assert (row["fasta"], row["scaffold"]) == ("votus", rep)
    assert (row["start_position"], row["end_position"], row["strandedness"]) == (51, 350, -1)
    assert (row["viral_id"], row["viral_bitScore"]) == ("YP_009001.1", 410.0)
    assert row["ko_id"] == KOS[0] and row["pfam_hits"] == "Zinc-binding dehydrogenase [PF00107.]"
    name = rep.replace("|", "_")
    assert (tmp_path / "votu.tab").read_text().splitlines() == [
        f">{name}|3|l",
        f"{name}__1|10|49|40|1|hmm2|50|1e-10|0|-|-|-",
        f"{name}__2|51|350|300|1|hmm3|50|1e-10|2|-|-|-",
        f"{name}__3|450|850|401|1|hmm4|50|1e-10|0|-|-|-",
    ]
    assert (tmp_path / "reps.fna").read_text() == f">{rep}\nACGT\n"


def test_gpr_table_all_holds_every_reaction_lane(tmp_path):
    kw, table = _annotation_table(tmp_path, np.random.default_rng(41))
    out = tmp_path / "gpr_all.parquet"
    r = _run(_gpr_argv(kw, table, "all_reaction", out), tmp_path)
    assert r.returncode == 0, f"mapper failed:\n{r.stdout}\n{r.stderr}"
    df = _check_table(out, "all_reaction")
    # deepec and eggnog were never fitted, and the table says so per row
    for ch in ("deepec", "eggnog"):
        g = df[df["channel"] == ch]
        assert len(g) and (g["llr"] == fe.UNCALIBRATED_LLR).all()
        assert set(g["llr_route"]) == {"uncalibrated"}
    assert set(df.loc[df["channel"] == "kofam", "llr_route"]) == {"calibrated"}
    assert set(df.loc[df["channel"] == "eggnog", "intermediate_id"]) == {KOS[0], ECS[1]}


def test_chosen_4_is_the_all_reaction_table_cut_to_its_lanes(tmp_path):
    # `gpr_4lane` builds from its four lanes alone so a fosmid pool reaches it from bare
    # ORFs; it must still be exactly what a consumer gets by filtering the full table.
    kw, table = _annotation_table(tmp_path, np.random.default_rng(42))
    full = tmp_path / "gpr_all.parquet"
    assert _run(_gpr_argv(kw, table, "all_reaction", full), tmp_path).returncode == 0
    r = _run4(kw, tmp_path)
    assert r.returncode == 0, f"4-lane chain failed:\n{r.stdout}\n{r.stderr}"
    key = fe.GRAIN_KEY
    cut = pd.read_parquet(full)
    cut = cut[cut["channel"].isin(fe.LANE_SETS["chosen_4"])].drop(columns="lane_set")
    four = pd.read_parquet(kw["out"]).drop(columns="lane_set")
    pd.testing.assert_frame_equal(cut.sort_values(key).reset_index(drop=True),
                                  four.sort_values(key).reset_index(drop=True))


# Three proteins: ORFS[0] and a copy of it on another contig, and ORFS[1].
PF_COPY = "pool1:megahit:k141_9_1"


def _pathofact_inputs(w: Path):
    contig = lambda orf: orf.rpartition("_")[0]
    (w / "pf_orfs.faa").write_text(
        f">{ORFS[0]} # 1 # 300 # 1 # ID=1_1\nMKVLAAGH*\n"
        f">{ORFS[1]} # 1 # 300 # -1 # ID=2_1\nMSTNPQRW*\n"
        f">{PF_COPY} # 1 # 300 # 1 # ID=3_1\nMKVLAAGH\n")
    (w / "deeparg_argnorm.tsv").write_text(
        DEEPARG_HEADER + "\tARO\tARO_name\tCut_Off\n" + _deeparg_row(ORFS[0])
        + "\tARO:3000873\tTEM-1\tStrict\n")
    pd.DataFrame({
        "ORF_ID": [f"orfs~{ORFS[0]} # 1 # 300 # 1", f"orfs~{ORFS[1]} # 1 # 300 # -1"],
        "Cut_Off": ["Strict", "Loose"], "ARO": ["3000873", "3000001"],
        "AMR Gene Family": ["TEM beta-lactamase", "some family"],
    }).to_csv(w / "rgi.txt", sep="\t", index=False)
    pd.DataFrame({
        "Protein id": [f"orfs~{ORFS[1]}"], "Scope": ["plus"], "Element symbol": ["stxA"],
        "ARO": ["ARO:3003381"], "ARO_name": ["soxR"], "Cut_Off": ["Loose"],
    }).to_csv(w / "amrfinderplus_argnorm.tsv", sep="\t", index=False)
    # geNomad names the batch's contigs `<stem>~<contig>`. One plasmid passes --conservative
    # and one does not; the provirus gene has no marker, which geNomad writes NA.
    genes_cols = ("gene\tstart\tend\tlength\tstrand\tgc_content\tgenetic_code\trbs_motif\t"
                  "marker\tevalue\tbitscore\tuscg\tplasmid_hallmark\tvirus_hallmark\ttaxid\t"
                  "taxname\tannotation_conjscan\tannotation_amr\tannotation_accessions\t"
                  "annotation_description\n")
    gene = lambda name, start, end, strand, marker: (
        f"{name}\t{start}\t{end}\t{end - start + 1}\t{strand}\t0.5\t11\tNone\t{marker}\t"
        "1e-20\t80.0\tNA\t0\t0\t1\troot\tNA\tNA\tNA\tNA\n")
    p0, p1 = "asm~" + contig(ORFS[0]), "asm~" + contig(ORFS[1])
    (w / "plasmid_summary.tsv").write_text(
        "seq_name\tlength\ttopology\tn_genes\tgenetic_code\tplasmid_score\tfdr\t"
        "n_hallmarks\tmarker_enrichment\tconjugation_genes\tamr_genes\n"
        f"{p0}\t5000\tNo terminal repeats\t5\t11\t0.95\tNA\t1\t2.0\tNA\tNA\n"
        f"{p1}\t5000\tNo terminal repeats\t5\t11\t0.75\tNA\t1\t2.0\tNA\tNA\n")
    (w / "plasmid_genes.tsv").write_text(genes_cols + gene(p0 + "_1", 1, 300, 1, "PL_0001")
                                         + gene(p1 + "_1", 1, 300, -1, "PL_0002"))
    (w / "virus_summary.tsv").write_text(
        "seq_name\tlength\ttopology\tcoordinates\tn_genes\tgenetic_code\tvirus_score\tfdr\t"
        "n_hallmarks\tmarker_enrichment\ttaxonomy\n"
        f"{p1}|provirus_1_500\t500\tProvirus\t1-500\t1\t11\t0.9\tNA\t2\t3.0\tUnclassified\n")
    (w / "virus_genes.tsv").write_text(genes_cols + gene(p1 + "|provirus_1_500_1", 2, 300, -1, "NA"))
    import json
    cds = lambda kind, functions: {"type": "CDS", "location": "[0:300](+)", "qualifiers": {
        "gene_kind": [kind], "gene_functions": functions}}
    (w / "antismash_pf.json").write_text(json.dumps({"records": [
        {"id": contig(PF_COPY), "areas": [{"start": 0, "end": 5000, "products": ["NRPS"]}],
         "features": [cds("biosynthetic", ["biosynthetic (rule-based-clusters) NRPS: Condensation",
                                           "biosynthetic-additional (smcogs) SMCOG1"])]},
        {"id": contig(ORFS[1]), "areas": [{"start": 0, "end": 5000, "products": ["NRPS"]}],
         "features": [dict(cds("other", []), location="[0:300](-)")]},
    ]}))


def test_pathofact_report_applies_pathofacts_rules(tmp_path):
    kw, table = _annotation_table(tmp_path, np.random.default_rng(45))
    _pathofact_inputs(tmp_path)
    outs = {k: tmp_path / f"{k}.tsv"
            for k in ("vf", "vf-ambiguous", "tox", "tox-ambiguous", "amr", "combined")}
    w = tmp_path
    r = _run([sys.executable, str(GPR_LIB / "pathofact_report.py"), "--ev-lib", kw["ev_lib"],
              "--annotation-table", str(table), "--orfs", str(w / "pf_orfs.faa"),
              "--sample", "asm", "--deeparg", str(w / "deeparg_argnorm.tsv"),
              "--rgi", str(w / "rgi.txt"), "--amrfinderplus", str(w / "amrfinderplus_argnorm.tsv"),
              "--plasmid-summary", str(w / "plasmid_summary.tsv"),
              "--plasmid-genes", str(w / "plasmid_genes.tsv"),
              "--virus-summary", str(w / "virus_summary.tsv"),
              "--virus-genes", str(w / "virus_genes.tsv"),
              "--antismash-json", str(w / "antismash_pf.json"),
              *[a for k, p in outs.items() for a in (f"--out-{k}", str(p))]], w)
    assert r.returncode == 0, f"report failed:\n{r.stdout}\n{r.stderr}"
    read = lambda k: pd.read_csv(outs[k], sep="\t", dtype=str, keep_default_na=False)

    # The VF arm: hits sorted by bitscore, the copy named beside its representative.
    vf = read("vf")
    assert list(vf["ProteinID"]) == [ORFS[0]]
    row = vf.iloc[0]
    assert (row["Query_vf"], row["bit_score_vf"]) == ("cd00002,cd00001", "60.5,45.0")
    assert (row["Rfprediction_vf"], row["Rfprob_vf"]) == ("VF", "0.95123456")
    assert (row["Plasmid_marker"], row["Virus_marker"]) == ("PL_0001", "-")
    assert row["Other_proteins_in_cluster"] == PF_COPY
    assert read("vf-ambiguous").empty
    # The toxin arm cuts at 0.8 on the two-decimal probability; an HMM hit with a weak
    # call is ambiguous. ORFS[1] is on a provirus whose gene has no marker.
    tox, amb = read("tox"), read("tox-ambiguous")
    assert list(tox["ProteinID"]) == [ORFS[1]] and tox.iloc[0]["Rfprob_tox"] == "0.80"
    assert (tox.iloc[0]["Plasmid_marker"], tox.iloc[0]["Virus_marker"]) == ("-", "NA")
    assert list(amb["ProteinID"]) == [ORFS[0]] and amb.iloc[0]["Query_tox"] == "pfam00001"
    # The AMR arm: RGI's Loose hit and AMRFinderPlus's plus-scope hit are not PathoFact's.
    amr = read("amr")
    assert list(amr["ProteinID"]) == [ORFS[0]]
    assert list(amr.iloc[0][2:12]) == ["ARO:3000873", "TEM-1", "0.99", "Strict", "ARO:3000873",
                                       "Strict", "TEM beta-lactamase", "-", "-", "-"]
    # The combined report: high-probability VF and toxin rows, every AMR row, every BGC
    # gene, each column's first value; pandas reads geNomad's NA as missing, so `-`.
    comb = read("combined")
    # PathoFact orders by the digits of the last three `_` fields: k141_9 before k141_1:0-1200.
    assert list(comb["ProteinID"]) == [PF_COPY, ORFS[0], ORFS[1], "3"]
    assert comb.iloc[-1]["Sample"] == "#Total"
    c = comb.set_index("ProteinID")
    assert c.loc[ORFS[0], "Query_tox"] == "-" and c.loc[ORFS[0], "Query_vf"] == "cd00002,cd00001"
    assert c.loc[ORFS[1], "Virus_marker"] == "-"
    assert (c.loc[PF_COPY, "BGC"], c.loc[PF_COPY, "Antismash_Description"]) == (
        "biosynthetic", "NRPS(Condensation)")
    assert c.loc[ORFS[1], "BGC"] == "-"

    # SignalP is kept for the protein's first arm, and only for that arm's candidates:
    # ORFS[1] is a toxin with no toxin HMM hit.
    (w / "signalp.txt").write_text(
        "# SignalP-6.0\tOrganism: Other\n# ID\tPrediction\tOTHER\tSP(Sec/SPI)\n"
        f"{ORFS[0]} # 1 # 300\tSP\t0.01\t0.99\n{ORFS[1]} # 1 # 300\tLIPO\t0.1\t0.0\n"
        f"{PF_COPY} # 1 # 300\tOTHER\t0.99\t0.01\n")
    r = _run([sys.executable, str(GPR_LIB / "pathofact_report.py"), "--report",
              str(outs["combined"]), "--signalp", str(w / "signalp.txt"),
              "--out", str(w / "combined_signalp.tsv")], w)
    assert r.returncode == 0, f"signalp failed:\n{r.stdout}\n{r.stderr}"
    sp = pd.read_csv(w / "combined_signalp.tsv", sep="\t", dtype=str, keep_default_na=False)
    cols = list(sp.columns)
    assert cols.index("SignalP") + 1 == cols.index("Plasmid_marker")
    assert list(sp["SignalP"]) == ["-", "signal_peptide", "-", "1"]
