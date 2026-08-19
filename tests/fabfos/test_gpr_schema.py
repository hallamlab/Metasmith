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
        for orf, ec in zip(ORFS, ECS):
            fh.write(f"{orf}\tEC:{ec}\n")


def _write_ezpred(p: Path):
    pd.DataFrame({
        "sequence_id": ORFS,
        "ec_number": ECS,
        "score": [0.91, 0.62],
        "head_kind": ["enzyme", "enzyme"],
    }).to_csv(p, index=False)


def _write_query_embeddings(emb: Path, idx: Path, rng):
    pd.DataFrame(rng.normal(size=(len(ORFS), DIM)).astype(np.float32)).to_parquet(emb, index=False)
    pd.DataFrame({"sequence_id": ORFS}).to_csv(idx, index=False)


def _write_pool(pool_dir: Path, rng, stacks=("emb_pbert.npy",)):
    n = 40
    pool_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "role": ["reference"] * n,
        "row": np.arange(n, dtype=np.int64),
        "orf": [f"REF{i:04d}" for i in range(n)],
        "mnxr_list": [";".join(MNXRS[:2])] * n,
    }).to_parquet(pool_dir / "orf_index.parquet", index=False)
    for s in stacks:
        np.save(pool_dir / s, rng.normal(size=(n, DIM)).astype(np.float32))


def _lanes(work: Path, rng, seven: bool, pool_stacks=("emb_pbert.npy",),
           esmc_pool_stacks=("emb_esmc.npy",)):
    _write_orfs(work / "orfs.faa")
    _write_kofam(work / "kofam.csv")
    _write_clean(work / "clean.tsv")
    _write_uniref(work / "uniref.tsv")
    _write_bridge(work / "bridge.parquet")
    _write_query_embeddings(work / "pbert.parquet", work / "pbert_index.csv", rng)
    _write_pool(work / "pool", rng, stacks=pool_stacks)
    kw = dict(
        ev_lib=str(EV_LIB), orfs=str(work / "orfs.faa"),
        kofam=str(work / "kofam.csv"), clean=str(work / "clean.tsv"),
        uniref=str(work / "uniref.tsv"), bridge=str(work / "bridge.parquet"),
        pbert_emb=str(work / "pbert.parquet"), pbert_idx=str(work / "pbert_index.csv"),
        pool=str(work / "pool"), out=str(work / "gpr.parquet"),
        threads=1,
    )
    if seven:
        _write_deepec(work / "deepec.tsv")
        _write_ezpred(work / "ezpred.csv")
        _write_query_embeddings(work / "esmc.parquet", work / "esmc_index.csv", rng)
        _write_pool(work / "pool_esmc", rng, stacks=esmc_pool_stacks)
        kw.pop("threads")
        kw.update(
            lane_set="full_7", source="orfs",
            deepec=str(work / "deepec.tsv"), ezpred=str(work / "ezpred.csv"),
            esmc_emb=str(work / "esmc.parquet"), esmc_idx=str(work / "esmc_index.csv"),
            pool_esmc=str(work / "pool_esmc"),
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

    clean = df[df["channel"] == "clean"]
    assert np.isclose(sorted(1.0 / clean["raw_score"] - 1.0), [3.41, 8.06]).all()
    assert clean["raw_score"].min() < clean["raw_score"].max()

    assert set(df[df["channel"] == "uniref50"]["evidence_quality"]) == {"reviewed", "unreviewed"}
    assert set(df[df["channel"] == "pbert"]["evidence_quality"]) == {"reviewed"}


def test_gpr_7lane_driver_writes_a_valid_table(tmp_path):
    rng = np.random.default_rng(1)
    kw = _lanes(tmp_path, rng, seven=True, pool_stacks=("emb_pbert.npy", "emb_esmc.npy"))
    r = _run(_argv("gpr_7lane", kw), tmp_path)
    assert r.returncode == 0, f"driver failed:\n{r.stdout}\n{r.stderr}"
    df = _check_table(tmp_path / "gpr.parquet", "full_7")

    deepec = df[df["channel"] == "deepec"]
    assert (deepec["raw_score"] == 1.0).all()
    assert set(deepec["score_kind"]) == {"presence"}


def test_esmc_lane_refuses_a_pool_without_its_stack(tmp_path):
    rng = np.random.default_rng(2)
    kw = _lanes(tmp_path, rng, seven=True, esmc_pool_stacks=("emb_pbert.npy",))
    r = _run(_argv("gpr_7lane", kw), tmp_path)
    assert r.returncode != 0
    assert "emb_esmc.npy" in r.stderr and "esmc" in r.stderr
    assert not (tmp_path / "gpr.parquet").exists()


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
        src = (MLIB / "transforms" / "fabfos" / f"{mapper}.py").read_text()
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
