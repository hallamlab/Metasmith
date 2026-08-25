# Shared cohort, lane readers and EC projection for the per-lane calibration.
#
# WHY THIS READS THE RUN AND NOT THE BENCHMARK BUNDLE. `data/fabfos/benchmarks/lane_dh10b`
# holds a replication of the source study's ORF calling under its own ids
# (`ECDH10B_*`), and its DIAMOND output predates the `bsr` column the uniref50 lane
# scores on. `data/fabfos/runs/e_coli_dh10b/annotations/lanes` holds the SAME organism
# in the format the mapper actually consumes, beside the genome FASTA the run was
# called from. A calibration fitted on anything other than the production score is a
# calibration of a number the pipeline does not emit, so the cohort is rebuilt here by
# the bundle's own method against the run's ORFs.
#
# THE TRUTH SET is the bundle's, unchanged: every Swiss-Prot K-12 entry carrying a
# level-4 EC, keyed by the md5 of its sequence. A run ORF is adjudicable when its own
# sequence md5 hits that key. See `../pbert/_common.py`, which does this against the
# bundle's FASTA.
#
# THE CALL is the unit each lane is scored on, and it is that lane's own assertion
# rather than a common denominator:
#
#   kofam      (orf, KO)          projected to EC through the KO definition
#   clean      (orf, EC)          the EC itself
#   uniref50   (orf, accession)   projected to EC through the bridge, then reac_prop
#   pbert      (orf, MNXR)        projected to EC through reac_prop
#
# A call is correct when its EC set meets the ORF's curated EC set at all. EC rather
# than reaction, because reaction-level would measure the same fan-out dilution the
# mapper's own `1/F_n` divisor already applies, and removing that divisor is a
# separate change.
from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
RUN = REPO / "data/fabfos/runs/e_coli_dh10b"
LANES = RUN / "annotations/lanes"
GENOME = REPO / "data/fabfos/originals/genomes/e_coli_dh10b/genome/NC_010473.1.faa"
SPROT = REPO / "data/fabfos/benchmarks/lane_dh10b/generators/sprot_k12.tsv"
REAC_PROP = REPO / "data/fabfos/originals/metanetx/4.5/reac_prop.tsv"
BRIDGE = REPO / "data/fabfos/processed/mnxr_lookup/mnxr_lookup.parquet"
LANDMARKS = REPO / "data/fabfos/processed/label_transfer_landmarks/landmarks/landmarks.parquet"
OUT = HERE / "out"

L4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")
LANE_ORDER = ("kofam", "clean", "uniref50", "pbert")

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe  # noqa: E402


def md5(s: str) -> str:
    return hashlib.md5(s.strip("*").upper().encode()).hexdigest()


def iter_fasta(path):
    name, chunks = None, []
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                if name is not None:
                    yield name, "".join(chunks)
                name, chunks = line[1:].split(None, 1)[0], []
            else:
                chunks.append(line.strip())
    if name is not None:
        yield name, "".join(chunks)


# ---- bridges --------------------------------------------------------------

def mnxr_to_ec() -> dict[str, set[str]]:
    """reac_prop level-4 classifs, inverted. The projection both label-transfer lanes
    and the homology lane are adjudicated through."""
    out: dict[str, set[str]] = {}
    with open(REAC_PROP) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 5 or not p[0].startswith("MNXR") or not p[3]:
                continue
            for ec in p[3].split(";"):
                ec = ec.strip()
                if L4.match(ec):
                    out.setdefault(p[0], set()).add(ec)
    return out


def bridge_slice(src: str, wanted) -> pd.DataFrame:
    """One id space of `ref::mnxr_lookup`, cut to the ids this cohort hit.

    Both cuts go into the parquet reader. The uniprot space alone is 23.5 million
    rows and reading it whole costs gigabytes of object-dtype strings to keep a
    fraction of a percent."""
    wanted = sorted({str(w) for w in wanted})
    if not wanted:
        return pd.DataFrame(columns=["id", "mnxr", "evidence_quality"])
    df = pd.read_parquet(
        BRIDGE, columns=["id", "mnxr", "evidence_quality"],
        filters=[("id_source", "==", src), ("id", "in", wanted)])
    return df.drop_duplicates()


# ---- cohort ---------------------------------------------------------------

def cohort() -> pd.DataFrame:
    """[orf, ec (frozenset)] for every run ORF whose sequence md5 hits a curated
    K-12 entry carrying a level-4 EC."""
    sp = pd.read_csv(SPROT, sep="\t").fillna("")
    prot_ec: dict[str, set[str]] = {}
    for _, r in sp.iterrows():
        ecs = {e.strip() for e in str(r["EC number"]).split(";") if L4.match(e.strip())}
        if r["Sequence"] and ecs:
            prot_ec.setdefault(md5(r["Sequence"]), set()).update(ecs)
    rows = []
    for name, seq in iter_fasta(GENOME):
        ecs = prot_ec.get(md5(seq))
        if ecs:
            rows.append((name, frozenset(ecs)))
    return pd.DataFrame(rows, columns=["orf", "truth_ec"])


# ---- lanes ----------------------------------------------------------------
# Each returns [orf, intermediate_id, score, ec] with one row per (call, EC). The
# parse half of each is `gpr_4lane.py`'s, line for line, because a calibration fitted
# on calls the mapper would have dropped describes a lane that does not ship.

def lane_kofam() -> pd.DataFrame:
    df = pd.read_csv(LANES / "kofamscan.csv")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["thrshld"] = pd.to_numeric(df["thrshld"], errors="coerce")
    df = df[df["score"].notna() & df["thrshld"].notna() & (df["score"] >= df["thrshld"])]
    df = df.rename(columns={"gene_name": "orf", "KO": "intermediate_id"})
    # score/threshold, not the bitscore. A bitscore is a length-and-family quantity and
    # KOfam's own per-family threshold is the only thing that makes two of them
    # comparable. The threshold is already a column of this lane's output.
    df["score"] = df["score"] / df["thrshld"]
    defs = pd.read_csv(LANES / "kofamscan_descriptions.csv")
    ko_ec: dict[str, set[str]] = {}
    for ko, d in zip(defs["KO"], defs["definition"].astype(str)):
        for blk in re.findall(r"\[EC:([^\]]+)\]", d):
            for tok in blk.split():
                if L4.match(tok):
                    ko_ec.setdefault(ko, set()).add(tok)
    df["ec"] = df["intermediate_id"].map(ko_ec)
    return _explode(df, "kofam")


def lane_clean() -> pd.DataFrame:
    df = pd.read_csv(LANES / "clean.tsv", sep="\t")
    df.columns = ["orf", "intermediate_id", "score"]
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df = df[df["score"].notna() & (df["score"] >= fe.CLEAN_MIN_SCORE)]
    df = df[df["intermediate_id"].astype(str).str.match(L4, na=False)].copy()
    df["ec"] = df["intermediate_id"].map(lambda e: {e})
    return _explode(df, "clean")


def lane_uniref50(m2e) -> pd.DataFrame:
    df = pd.read_csv(LANES / "diamond_uniref50.tsv", sep="\t", dtype=str)
    if list(df.columns) != fe._BLAST6_BSR_COLS:
        raise SystemExit(f"[cal] diamond_uniref50.tsv header drift: {list(df.columns)}")
    for c in ("evalue", "bitscore", "bsr"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[df["bsr"].notna()]
    df = (df.sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
            .drop_duplicates(subset=["qseqid"], keep="first"))
    df["intermediate_id"] = df["sseqid"].str.replace(r"^UniRef50_", "", regex=True)
    df = df.rename(columns={"qseqid": "orf", "bsr": "score"})
    br = bridge_slice("uniprot", df["intermediate_id"].unique())
    acc_ec = (br.assign(ec=br["mnxr"].map(m2e))
                .dropna(subset=["ec"]).groupby("id")["ec"]
                .agg(lambda ss: set().union(*ss)))
    df["ec"] = df["intermediate_id"].map(acc_ec)
    return _explode(df, "uniref50")


def lane_pbert(m2e, threads=None) -> pd.DataFrame:
    """The shipped kNN vote, re-run on this ORF set.

    NO LEAKAGE CONTROL. The landmark pool is Swiss-Prot and the truth set is
    Swiss-Prot K-12, so most of these ORFs retrieve themselves. That is deliberate:
    the fitted curve answers "how often is a call at THIS cosine-vote wrong", and a
    self-hit lands at the top of that range rather than biasing it. What leakage
    changes is which part of the range the cohort populates, which is recorded as a
    coverage limit beside the fit."""
    if threads:
        for v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
            os.environ.setdefault(v, str(threads))
    ref = pd.read_parquet(LANDMARKS)
    dims = ["dim_" + str(i) for i in range(sum(c.startswith("dim_") for c in ref.columns))]
    ref_emb = _norm(ref[dims].to_numpy(dtype=np.float32))
    label_lists = [sorted({m for m in (s.split(";") if s else []) if m})
                   for s in ref["mnxr_list"].fillna("")]
    q = pd.read_parquet(LANES / "proteinbert_embeddings.parquet")
    q_orf = q["sequence_id"].to_numpy()
    q_emb = _norm(q[dims].to_numpy(dtype=np.float32))

    kk = min(fe.PBERT_K_MAX, len(ref_emb))
    rows = []
    for s in range(0, len(q_emb), 256):
        sim = q_emb[s:s + 256] @ ref_emb.T
        top = np.argpartition(-sim, min(kk, sim.shape[1] - 1), axis=1)[:, :kk]
        for bi in range(sim.shape[0]):
            cand = top[bi]
            cs = np.clip(sim[bi, cand], 0, None)
            order = np.argsort(-cs, kind="stable")
            cand, cs = cand[order], cs[order]
            if cs[0] <= 0 or cs[0] < fe.PBERT_NN_MIN:
                continue
            keep = cs >= max(fe.PBERT_NN_MIN, fe.PBERT_TAU * cs[0])
            nn, vals = cand[keep], cs[keep]
            tot = vals.sum()
            if tot <= 0:
                continue
            w = vals / tot
            acc: dict[str, float] = {}
            for wi, ri in zip(w, nn):
                for lab in label_lists[ri]:
                    acc[lab] = acc.get(lab, 0.0) + wi
            for lab, v in acc.items():
                if v >= fe.PBERT_FLOOR:
                    rows.append((q_orf[s + bi], lab, float(min(v, 1.0))))
    df = pd.DataFrame(rows, columns=["orf", "intermediate_id", "score"])
    df["ec"] = df["intermediate_id"].map(m2e)
    return _explode(df, "pbert")


def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)


def _explode(df, lane) -> pd.DataFrame:
    """Drop calls that project to no EC, then one row per (call, EC).

    A call with no EC is NOT a wrong call. It is a call this cohort cannot adjudicate,
    and counting it as an error would fit the curve to the projection's coverage
    instead of to the lane's accuracy. The count is reported rather than absorbed."""
    n_in = len(df)
    df = df[df["ec"].notna() & df["ec"].map(lambda s: bool(s) if s is not None else False)]
    out = df[["orf", "intermediate_id", "score", "ec"]].explode("ec")
    out.insert(0, "lane", lane)
    out.attrs["n_calls_in"] = n_in
    out.attrs["n_calls_kept"] = int(df.shape[0])
    return out.reset_index(drop=True)
