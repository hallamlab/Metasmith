"""Shared cohort, bridges and scoring for the DH10B annotation-lane sweeps.

THE COHORT is the scadc lane-ablation cohort, rebuilt by that study's own method
(`generators/build_upset8.py`): every Swiss-Prot K-12 entry carrying a level-4 EC is
keyed by the md5 of its sequence, and a DH10B ORF is adjudicable when its own
sequence md5 hits that key. So the truth here is the original's truth, not a
lookalike -- the only change is the query FASTA (dh10b.faa, the replication of
journal 14cafb1c) in place of epi300.faa.

TWO SCORING SPACES, because the two things we want to be comparable to disagree:

- ORF-level in EC space is the axis the seven-lane table is quoted on (KofamScan
  0.96, UniRef50 1.00, CLEAN 0.95, DeepEC 0.88). An ORF counts as correct when the
  lane's EC set intersects the curated EC set at all; precision divides by the ORFs
  the lane spoke about, recall by every adjudicable ORF. `pbert` emits MNXR, so it
  is projected to EC through reac_prop's level-4 classifs -- an MNXR with no classif
  cannot be scored here and is counted separately rather than silently dropped.
- Label-level (macro set-overlap) is the axis the embed-transfer report is quoted
  on (P = R = 0.839 on DH10B). Per-ORF precision and recall over the label sets,
  averaged; precision over ORFs with a prediction, recall over all of them.

Neither is more correct. Quoting one against a figure measured on the other is the
trap this module exists to make hard.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
CACHE = HERE / "cache"
BUNDLE = REPO / "data/fabfos/benchmarks/lane_dh10b"
ANN = BUNDLE / "annotations"
GEN = BUNDLE / "generators"
POOL = REPO / "data/fabfos/processed/reference_label_pool/pool"
REAC_PROP = REPO / "data/fabfos/originals/metanetx/4.5/reac_prop.tsv"

L4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")

# The lane's shipped configuration, from gpr_4lane.py's driver.
K = 30
PBERT_FLOOR = 0.20


def md5(s: str) -> str:
    """build_upset8.py's key: strip trailing stops, upper-case, md5."""
    return hashlib.md5(s.strip("*").upper().encode()).hexdigest()


def iter_fasta(path: Path):
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


# ---- bridges -----------------------------------------------------------

def ec_to_mnxr() -> dict[str, set[str]]:
    """reac_prop classifs (col 4, ';'-separated EC tokens) -> ec -> {mnxr}."""
    out: dict[str, set[str]] = {}
    with open(REAC_PROP) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5 or not parts[0].startswith("MNXR") or not parts[3]:
                continue
            for ec in parts[3].split(";"):
                ec = ec.strip()
                if ec:
                    out.setdefault(ec, set()).add(parts[0])
    return out


def mnxr_to_ec() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for ec, mnxrs in ec_to_mnxr().items():
        if not L4.match(ec):
            continue
        for m in mnxrs:
            out.setdefault(m, set()).add(ec)
    return out


# ---- cohort ------------------------------------------------------------

def load_cohort() -> pd.DataFrame:
    """cols: orf, ec (frozenset of level-4 EC), mnxr (frozenset, EC fanned out)."""
    sp = pd.read_csv(GEN / "sprot_k12.tsv", sep="\t").fillna("")
    prot_ec: dict[str, set[str]] = {}
    for _, r in sp.iterrows():
        ecs = {e.strip() for e in str(r["EC number"]).split(";") if L4.match(e.strip())}
        if r["Sequence"] and ecs:
            prot_ec.setdefault(md5(r["Sequence"]), set()).update(ecs)
    e2m = ec_to_mnxr()
    rows = []
    for name, seq in iter_fasta(ANN / "dh10b.faa"):
        ecs = prot_ec.get(md5(seq))
        if not ecs:
            continue
        mnxr = set().union(*(e2m.get(e, set()) for e in ecs)) if ecs else set()
        rows.append((name, frozenset(ecs), frozenset(mnxr)))
    return pd.DataFrame(rows, columns=["orf", "ec", "mnxr"])


# ---- scoring -----------------------------------------------------------

def score_orf_level(pred: dict[str, set], truth: dict[str, frozenset]) -> dict:
    """An ORF is correct when pred and truth intersect. The seven-lane table's axis."""
    fired = {o for o, p in pred.items() if p and o in truth}
    correct = {o for o in fired if pred[o] & truth[o]}
    n = len(truth)
    P = len(correct) / len(fired) if fired else 0.0
    R = len(correct) / n if n else 0.0
    F = 2 * P * R / (P + R) if (P + R) > 0 else 0.0
    return dict(n_adjudicable=n, n_fired=len(fired), n_correct=len(correct),
                precision=round(P, 4), recall=round(R, 4), f1=round(F, 4),
                coverage=round(len(fired) / n, 4) if n else 0.0)


def score_label_level(pred: dict[str, set], truth: dict[str, frozenset]) -> dict:
    """Macro set-overlap: the embed-transfer report's axis."""
    ps, rs, sizes = [], [], []
    for o, t in truth.items():
        p = pred.get(o, set())
        inter = len(p & t)
        rs.append(inter / len(t) if t else 0.0)
        if p:
            ps.append(inter / len(p))
            sizes.append(len(p))
    P = float(np.mean(ps)) if ps else 0.0
    R = float(np.mean(rs)) if rs else 0.0
    F = 2 * P * R / (P + R) if (P + R) > 0 else 0.0
    return dict(macro_precision=round(P, 4), macro_recall=round(R, 4),
                macro_f1=round(F, 4), mean_pred_size=round(float(np.mean(sizes)), 3) if sizes else 0.0)


def orf_to_accession() -> dict[str, str]:
    """DH10B ORF -> its own Swiss-Prot K-12 accession, by sequence md5.

    The pool is Swiss-Prot, so this is the row a self-retrieval control must hide.
    """
    sp = pd.read_csv(GEN / "sprot_k12.tsv", sep="\t").fillna("")
    by_md5 = {}
    for _, r in sp.iterrows():
        if r["Sequence"]:
            by_md5.setdefault(md5(r["Sequence"]), r["Entry"])
    return {name: by_md5[md5(seq)] for name, seq in iter_fasta(ANN / "dh10b.faa")
            if md5(seq) in by_md5}


def load_pool(repaired: bool = True):
    """(embeddings (N,512) float32, accessions ndarray, label_lists list[list[str]]).

    `repaired` reads the index rebuilt by repair_pool_index.py, which pairs each
    accession with the stack row that actually holds its embedding. The shipped
    index does not -- see that script's header -- so `repaired=False` is what the
    deployed lane sees and is only useful as the "before" row.
    """
    src = (CACHE / "pool_index_repaired.parquet") if repaired else (POOL / "orf_index.parquet")
    if repaired and not src.exists():
        raise SystemExit(f"no {src} -- run repair_pool_index.py first")
    idx = pd.read_parquet(src)
    idx = idx[idx["role"] == "reference"].reset_index(drop=True)
    emb = np.load(POOL / "emb_pbert.npy", mmap_mode="r")
    emb = np.asarray(emb[idx["row"].to_numpy()], dtype=np.float32)
    labels = [s.split(";") if s else [] for s in idx["mnxr_list"]]
    return emb, idx["orf"].to_numpy(), labels


def load_query():
    """(embeddings (n,512) float32, orf ids ndarray) for DH10B, in file order."""
    ids = pd.read_csv(ANN / "dh10b.pbert.index.csv")
    emb = pd.read_parquet(ANN / "dh10b.pbert.parquet").to_numpy(dtype=np.float32)
    if len(ids) != len(emb):
        raise SystemExit(f"index {len(ids)} != stack {len(emb)}")
    return emb, ids["sequence_id"].to_numpy()
