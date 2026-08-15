"""ECSPr embed-transfer lane (lane 4): protein-LM embedding label transfer.

Faithful, method-preserving port of the scadc `05_embed_transfer/` pipeline into
a single path-parameterized CLI so a metasmith transform can call it (NO hardcoded
scadc paths). Ported verbatim from:
  - _shared.py                  (id canonicalization, index + embedding loaders)
  - _eval.py                    (Context label-matrix build, K / BATCH, device)
  - 10_build_reference_pool.py  (reference pool + dark-query split + emb stacks)
  - 30_train_projector.py       (CLEAN-style SupCon projection head, frozen backbone)
  - 50_apply_fosmid.py          (the lane-4 producer: distance-weighted kNN vote)

The lane transfers MetaNetX reaction (MNXR) labels to *dark* fosmid ORFs -- fosmid
ORFs carrying ZERO reaction evidence from the other 3 lanes (kofam / dl_ec /
uniref50_dr) -- through protein-LM embedding space, using a distance-weighted kNN
vote over the full labeled reference pool (metag + epi300 + fosmid). Per-backbone
we apply the variant that won its own dark-regime (c30) validation:
  - pbert_transfer : RAW ProteinBERT embeddings   (floor 0.20)
  - esmc_transfer  : PROJECTED ESM-C embeddings   (floor 0.10)

Every emitted row uses the unified 8-column lane schema
    source, orf, channel, mnxr, intermediate_id, intermediate_name,
    raw_score, projection_via
plus nearest-neighbor audit columns (nn_similarity, k_support). This lane is
exploratory: it is NOT folded into the canonical evidence table by this module.

=====================================================================
GATES / EXTERNAL INPUTS  (what must be provided to run this fresh)
=====================================================================
This lane cannot be computed from sequence alone -- it consumes precomputed
protein-LM embeddings and the compiled evidence table. To run end to end you must
stage, per source (fosmid / epi300 / metag):
  - ProteinBERT embeddings : <src>.pbert.npy   (row i = ORF i, 512-d)
                             + <src>.pbert.index.csv  (fosmid/epi300: container+orf
                               columns; metag: single `orf_id` column)
  - ESM-C 600M embeddings  : <src>.parquet  (cols dim_0..dim_1151, 1152-d)
                             + <src>.index.csv  (sequence_id, index; metag ids use
                               `k141_<c>-<n>` dashes, canonicalized to underscores)
  - ORF amino-acid FASTA   : <src>.faa  (first header token == ORF id; only used to
                               emit reference.faa / gold.faa for a downstream
                               DIAMOND identity step -- optional for `apply`)
and, once:
  - the reference-pool LABEL SOURCE: the compiled evidence table parquet
    (evidence_table_dlec.parquet) with cols source, orf, channel, mnxr,
    intermediate_id -- this is the pool we transfer labels FROM.
  - the TRAINED PROJECTOR weights: emb_esmc_proj.npy (produced by `train-projector`
    from the reference pool) -- required by `apply` for the esmc_transfer channel.

Practical: kNN is an all-pairs cosine over a ~270k-row reference pool per query
batch -- GPU is required to be practical (local RTX 3060 in scadc). Pass
`--device cuda`; it falls back to CPU if CUDA is unavailable. Needs the `ml` conda
env (torch + CUDA) plus numpy / pandas / pyarrow. This lane is FOSMID-DARK-ONLY:
it only annotates fosmid ORFs with zero reaction evidence from the other 3 lanes.

Per-source input paths are supplied via a JSON manifest (--sources), NOT hardcoded:
  {
    "fosmid": {"esmc_parquet": "...", "esmc_index": "...",
               "pbert_npy": "...", "pbert_index": "...", "fasta": "..."},
    "epi300": {...},
    "metag":  {...}
  }
Manifest key order defines the source order; `--query-source` (default "fosmid")
names the dark source whose evidence-free ORFs become the query set.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
import torch.nn as nn

# ---- unified lane schema (shared with the other 3 evidence lanes) ----
SCHEMA_COLS = ["source", "orf", "channel", "mnxr",
               "intermediate_id", "intermediate_name", "raw_score", "projection_via"]

# ---- embedding dimensions (defaults; overridable via CLI) ----
ESMC_DIM = 1152
PBERT_DIM = 512

# ---- kNN transfer hyper-parameters (from _eval.py; method-frozen) ----
K = 30           # neighbors per query
BATCH = 256      # query rows per GPU tile

# ---- projector (30_) hyper-parameters (from 30_train_projector.py) ----
PROJ_DIM = 256
HID = 512
K_PER = 8        # samples per class per batch; also the >=K trainable floor
P_CLASSES = 32   # classes per batch  -> batch 256
STEPS = 2500
LR = 1e-3
TEMP = 0.1

# Per-backbone apply variants: (embedding npy, channel, vote floor = c30 best-F1 tau).
VARIANTS = [
    ("emb_pbert.npy",     "pbert_transfer", 0.20),   # RAW ProteinBERT
    ("emb_esmc_proj.npy", "esmc_transfer",  0.10),   # PROJECTED ESM-C
]


def resolve_device(requested: str) -> str:
    """CLI device with graceful CUDA fallback."""
    if requested == "cuda" and not torch.cuda.is_available():
        print("[warn] cuda requested but unavailable -> falling back to cpu", flush=True)
        return "cpu"
    return requested


# =====================================================================
# ID canonicalization + FASTA (port of _shared.py)
# =====================================================================

_DASH_RE = re.compile(r"-(\d+)$")


def canon_orf(s: str) -> str:
    """`k141_<contig>-<n>` -> `k141_<contig>_<n>`; no-op for fosmid/epi300 ids."""
    return _DASH_RE.sub(r"_\1", s)


def iter_fasta(path: Path):
    """Yield (orf_id, seq) -- orf_id is the first whitespace token after '>'."""
    name, chunks = None, []
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                if name is not None:
                    yield name, "".join(chunks)
                name = line[1:].split(None, 1)[0]
                chunks = []
            else:
                chunks.append(line.strip())
    if name is not None:
        yield name, "".join(chunks)


# =====================================================================
# Index loaders (port of _shared.py, parameterized by explicit paths)
# =====================================================================

def load_esmc_index(index_csv: Path) -> pd.DataFrame:
    """cols: orf (canonical), index (row in the ESM-C parquet)."""
    df = pd.read_csv(index_csv)
    df["orf"] = df["sequence_id"].map(canon_orf)
    return df[["orf", "index"]]


def load_pbert_index(index_csv: Path) -> pd.DataFrame:
    """cols: orf (canonical), row (row in the .npy).

    Auto-detects the two index layouts: metag's single `orf_id` column vs
    fosmid/epi300's (container, orf) pair joined as `{container}_{orf}`.
    """
    df = pd.read_csv(index_csv)
    if "orf_id" in df.columns:                      # metag X_metag.index.csv
        df = df.rename(columns={"orf_id": "orf"})
    else:                                            # fosmid (fosmid, orf) / epi300 (contig, orf)
        c0 = df.columns[0]
        df["orf"] = df[c0].astype(str) + "_" + df["orf"].astype(str)
    df["orf"] = df["orf"].map(canon_orf)
    df["row"] = np.arange(len(df))
    return df[["orf", "row"]]


# =====================================================================
# Embedding readers (port of _shared.py)
# =====================================================================

def _read_parquet_rows(path: Path, want_rows: np.ndarray, dim: int) -> np.ndarray:
    """Stream a (possibly multi-GB) ESM-C parquet and gather `want_rows` (global
    row offsets), returning a (len(want_rows), dim) float32 array in the order of
    `want_rows`. Peak memory is one batch + the accumulator."""
    want = np.asarray(want_rows, dtype=np.int64)
    order = {int(r): i for i, r in enumerate(want.tolist())}
    out = np.empty((len(want), dim), dtype=np.float32)
    pf = pq.ParquetFile(path)
    base = 0
    for batch in pf.iter_batches(batch_size=131072):
        n = batch.num_rows
        sel = want[(want >= base) & (want < base + n)]
        if len(sel):
            bm = batch.to_pandas().to_numpy(dtype=np.float32)
            dst = [order[int(r)] for r in sel.tolist()]
            out[dst] = bm[sel - base]
        base += n
    return out


def load_esmc_vectors(parquet: Path, index_csv: Path, needed: set,
                      dim: int = ESMC_DIM):
    """Return (orfs, Nxdim float32) for the needed ORFs present in this source."""
    idx = load_esmc_index(index_csv)
    idx = idx[idx["orf"].isin(needed)]
    if idx.empty:
        return [], np.zeros((0, dim), dtype=np.float32)
    arr = _read_parquet_rows(parquet, idx["index"].to_numpy(), dim)
    return idx["orf"].tolist(), arr


def load_pbert_vectors(npy_path: Path, index_csv: Path, needed: set,
                       dim: int = PBERT_DIM):
    """Return (orfs, Nxdim float32) for the needed ORFs present in this source.
    The npy is mmap'd and fancy-indexed, so metag's 1.44M-row file is cheap."""
    idx = load_pbert_index(index_csv)
    idx = idx[idx["orf"].isin(needed)]
    if idx.empty:
        return [], np.zeros((0, dim), dtype=np.float32)
    arr = np.load(npy_path, mmap_mode="r")
    sub = np.asarray(arr[idx["row"].to_numpy()], dtype=np.float32)
    return idx["orf"].tolist(), sub


# =====================================================================
# Context: reference pool + label matrix (port of _eval.Context, lean)
# =====================================================================

class Context:
    """Reference table + label vocabulary + on-device label indicator matrix.

    Faithful port of _eval.Context, trimmed to what the `apply` producer needs:
    the reference split, the sorted MNXR vocabulary, and the (Nref x V) label
    matrix. Cluster-leakage codes and the DIAMOND m8 (validation-only) are omitted
    because `apply` runs with NO leakage removal on genuinely unlabeled ORFs.
    """

    def __init__(self, orf_index_path: Path, device: str = "cpu"):
        self.dev = device
        idx = pd.read_parquet(orf_index_path)
        ref = idx[idx["role"] == "reference"].reset_index(drop=True).copy()
        ref["refpos"] = np.arange(len(ref))
        self.ref = ref

        # ---- label vocabulary + reference label indicator (row, col) coords ----
        ref_label_lists = [s.split(";") if s else [] for s in ref["mnxr_list"]]
        vocab = sorted({m for ls in ref_label_lists for m in ls})
        self.vocab = vocab
        self.V = len(vocab)
        vidx = {m: i for i, m in enumerate(vocab)}
        rows, cols = [], []
        for r, ls in enumerate(ref_label_lists):
            for m in ls:
                rows.append(r); cols.append(vidx[m])
        self._L_rows = np.asarray(rows, dtype=np.int64)
        self._L_cols = np.asarray(cols, dtype=np.int64)
        self._L_gpu = None

    def label_matrix(self):
        """(Nref, V) fp16 label indicator on-device (cached)."""
        if self._L_gpu is None:
            L = torch.zeros((len(self.ref), self.V), dtype=torch.float16, device=self.dev)
            L[torch.as_tensor(self._L_rows, device=self.dev),
              torch.as_tensor(self._L_cols, device=self.dev)] = 1.0
            self._L_gpu = L
        return self._L_gpu


# =====================================================================
# Stage T1 -- build reference pool (port of 10_build_reference_pool.py)
# =====================================================================

def aggregate_labels(ev: pd.DataFrame) -> pd.DataFrame:
    """Per (source, orf): mnxr_list, ec_list, channels, n_channels_max."""
    # n_channels_max: max over the ORF's MNXRs of how many channels co-nominate it
    conom = (
        ev.groupby(["source", "orf", "mnxr"])["channel"].nunique()
          .groupby(level=[0, 1]).max()
          .rename("n_channels_max").reset_index()
    )
    ec = ev[ev["channel"] == "dl_ec"]
    agg = ev.groupby(["source", "orf"]).agg(
        mnxr_list=("mnxr", lambda s: ";".join(sorted(set(s)))),
        channels=("channel", lambda s: ";".join(sorted(set(s)))),
    ).reset_index()
    ec_agg = ec.groupby(["source", "orf"]).agg(
        ec_list=("intermediate_id", lambda s: ";".join(sorted(set(s)))),
    ).reset_index()
    agg = agg.merge(ec_agg, on=["source", "orf"], how="left")
    agg["ec_list"] = agg["ec_list"].fillna("")
    agg = agg.merge(conom, on=["source", "orf"], how="left")
    return agg


def build_reference_pool(evidence_path: Path, sources: dict, query_source: str,
                         out_dir: Path, esmc_dim: int, pbert_dim: int,
                         write_fasta: bool = True):
    """Port of 10_build_reference_pool.main, parameterized by an explicit source
    manifest. `sources` maps src -> {esmc_parquet, esmc_index, pbert_npy,
    pbert_index, fasta}; manifest key order defines SOURCES."""
    out_dir.mkdir(parents=True, exist_ok=True)
    SOURCES = tuple(sources.keys())

    print("[10] loading evidence table...")
    ev = pd.read_parquet(evidence_path,
                         columns=["source", "orf", "channel", "mnxr", "intermediate_id"])
    lab = aggregate_labels(ev)
    print(f"     labeled ORFs: {len(lab):,}  "
          + ", ".join(f"{s}={(lab['source']==s).sum():,}" for s in SOURCES))

    labeled_by_src = {s: set(lab.loc[lab["source"] == s, "orf"]) for s in SOURCES}

    # Dark query set = query-source ORFs (ESM-C universe) with no evidence row.
    q_conf = sources[query_source]
    query_universe = set(load_esmc_index(Path(q_conf["esmc_index"]))["orf"])
    dark_query = query_universe - labeled_by_src[query_source]
    print(f"     {query_source} universe {len(query_universe):,}; "
          f"dark (query) {len(dark_query):,}")

    records, esmc_blocks, pbert_blocks = [], [], []
    for src in SOURCES:
        conf = sources[src]
        needed = set(labeled_by_src[src])
        if src == query_source:
            needed |= dark_query
        print(f"[10] {src}: loading embeddings for {len(needed):,} ORFs...")
        e_orfs, e_arr = load_esmc_vectors(Path(conf["esmc_parquet"]),
                                          Path(conf["esmc_index"]), needed, esmc_dim)
        p_orfs, p_arr = load_pbert_vectors(Path(conf["pbert_npy"]),
                                           Path(conf["pbert_index"]), needed, pbert_dim)
        e_row = {o: i for i, o in enumerate(e_orfs)}
        p_row = {o: i for i, o in enumerate(p_orfs)}
        common = [o for o in e_orfs if o in p_row]   # both backbones present
        missing = len(needed) - len(common)
        print(f"     esmc {len(e_orfs):,}  pbert {len(p_orfs):,}  "
              f"both {len(common):,}  (dropped {missing:,})")
        esmc_blocks.append(e_arr[[e_row[o] for o in common]])
        pbert_blocks.append(p_arr[[p_row[o] for o in common]])
        for o in common:
            records.append({"orf": o, "source": src,
                            "role": "query" if (src == query_source and o in dark_query)
                                    else "reference"})

    idx = pd.DataFrame.from_records(records)
    emb_esmc = np.concatenate(esmc_blocks, axis=0)
    emb_pbert = np.concatenate(pbert_blocks, axis=0)
    assert len(idx) == len(emb_esmc) == len(emb_pbert)

    # Attach labels; query rows get empty label fields / n_channels_max 0.
    idx = idx.merge(lab, on=["source", "orf"], how="left")
    for c in ("mnxr_list", "ec_list", "channels"):
        idx[c] = idx[c].fillna("")
    idx["n_channels_max"] = idx["n_channels_max"].fillna(0).astype(int)
    idx["is_gold"] = (idx["role"] == "reference") & (idx["n_channels_max"] >= 2)
    idx["row"] = np.arange(len(idx))

    print(f"[10] total ORFs {len(idx):,}  reference {(idx['role']=='reference').sum():,}  "
          f"query {(idx['role']=='query').sum():,}  gold {idx['is_gold'].sum():,}")
    print("     gold by source: " + ", ".join(
        f"{s}={int(((idx['source']==s)&idx['is_gold']).sum())}" for s in SOURCES))

    np.save(out_dir / "emb_esmc.npy", emb_esmc)
    np.save(out_dir / "emb_pbert.npy", emb_pbert)
    idx.to_parquet(out_dir / "orf_index.parquet", index=False)

    if not write_fasta:
        print("[10] done (fasta skipped).")
        return

    # FASTAs for the DIAMOND identity step: reference pool + gold queries.
    print("[10] writing reference.faa / gold.faa ...")
    ref_by_src = {s: set(idx.loc[(idx["source"] == s) & (idx["role"] == "reference"), "orf"])
                  for s in SOURCES}
    gold_by_src = {s: set(idx.loc[(idx["source"] == s) & idx["is_gold"], "orf"])
                   for s in SOURCES}
    nref = ngold = 0
    ref_fa, gold_fa = out_dir / "reference.faa", out_dir / "gold.faa"
    ref_fa.write_text(""); gold_fa.write_text("")
    for s in SOURCES:
        fasta = Path(sources[s]["fasta"])
        with open(ref_fa, "a") as rf, open(gold_fa, "a") as gf:
            for name, seq in iter_fasta(fasta):
                c = canon_orf(name)
                if c in ref_by_src[s]:
                    rf.write(f">{c}\n{seq}\n"); nref += 1
                if c in gold_by_src[s]:
                    gf.write(f">{c}\n{seq}\n"); ngold += 1
    print(f"     reference.faa {nref:,} seqs; gold.faa {ngold:,} seqs")
    print("[10] done.")


# =====================================================================
# Stage T3 -- train projector (port of 30_train_projector.py)
# =====================================================================

class Head(nn.Module):
    def __init__(self, d_in):
        super().__init__()
        self.net = nn.Sequential(nn.Linear(d_in, HID), nn.ReLU(), nn.Linear(HID, PROJ_DIM))

    def forward(self, x):
        return torch.nn.functional.normalize(self.net(x), dim=1)


def supcon_loss(z, y):
    """Supervised contrastive loss over a batch of L2-normalized z (N,d), labels y (N,)."""
    sim = (z @ z.T) / TEMP
    n = z.shape[0]
    eye = torch.eye(n, device=z.device, dtype=torch.bool)
    sim.masked_fill_(eye, -1e9)
    logits = sim - sim.max(dim=1, keepdim=True).values.detach()
    exp = torch.exp(logits)
    denom = exp.sum(dim=1, keepdim=True)
    log_prob = logits - torch.log(denom + 1e-12)
    pos = (y[:, None] == y[None, :]) & ~eye
    pos_cnt = pos.sum(dim=1)
    valid = pos_cnt > 0
    mean_lp = (pos * log_prob).sum(dim=1)[valid] / pos_cnt[valid]
    return -mean_lp.mean()


def train_head(X: np.ndarray, ec_labels: np.ndarray, name: str, dev: str,
               steps: int = STEPS, seed: int = 0):
    """X: (Ntrain, d) frozen embeddings; ec_labels: (Ntrain,) int class ids."""
    classes, counts = np.unique(ec_labels, return_counts=True)
    keep = set(classes[counts >= K_PER])
    mask = np.array([c in keep for c in ec_labels])
    X, y = X[mask], ec_labels[mask]
    classes = np.unique(y)
    by_class = {c: np.where(y == c)[0] for c in classes}
    print(f"[30] {name}: {len(X):,} trainable samples, "
          f"{len(classes):,} EC classes (>= {K_PER} each)")

    Xt = torch.as_tensor(X, device=dev, dtype=torch.float32)
    head = Head(X.shape[1]).to(dev)
    opt = torch.optim.Adam(head.parameters(), lr=LR)
    rng = np.random.default_rng(seed)
    head.train()
    for step in range(steps):
        cls = rng.choice(classes, size=min(P_CLASSES, len(classes)), replace=False)
        rows = np.concatenate([rng.choice(by_class[c], size=K_PER,
                               replace=len(by_class[c]) < K_PER) for c in cls])
        lbl = np.concatenate([[c] * K_PER for c in cls])
        z = head(Xt[rows])
        loss = supcon_loss(z, torch.as_tensor(lbl, device=dev))
        opt.zero_grad(); loss.backward(); opt.step()
        if step % 500 == 0 or step == steps - 1:
            print(f"     step {step:4d}  loss {loss.item():.4f}")
    head.eval()
    return head


def project_all(head, emb_np, dev: str):
    out = np.empty((len(emb_np), PROJ_DIM), dtype=np.float32)
    with torch.no_grad():
        for s in range(0, len(emb_np), 8192):
            xb = torch.as_tensor(emb_np[s:s + 8192], device=dev, dtype=torch.float32)
            out[s:s + 8192] = head(xb).cpu().numpy()
    return out


def train_projector(in_dir: Path, out_dir: Path, dev: str,
                    steps: int = STEPS, seed: int = 0,
                    backbones=(("esmc", "emb_esmc.npy"), ("pbert", "emb_pbert.npy"))):
    """Port of 30_train_projector.main; reads orf_index + emb stacks from in_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    torch.manual_seed(seed)
    print(f"[30] device = {dev}")
    idx = pd.read_parquet(in_dir / "orf_index.parquet")
    # training rows: reference, NOT gold, with a level-4 EC; one EC per ORF (first).
    train = idx[(idx["role"] == "reference") & (~idx["is_gold"])
                & (idx["ec_list"] != "")].copy()
    train["ec"] = train["ec_list"].str.split(";").str[0]
    ec_codes, _ = pd.factorize(train["ec"])
    train["ec_code"] = ec_codes
    print(f"[30] training ORFs with EC (non-gold): {len(train):,}; "
          f"distinct EC {train['ec'].nunique():,}")

    for backbone, npy in backbones:
        emb = np.load(in_dir / npy)
        X = emb[train["row"].to_numpy()]
        head = train_head(X, train["ec_code"].to_numpy(), backbone, dev, steps, seed)
        proj = project_all(head, emb, dev)
        np.save(out_dir / f"emb_{backbone}_proj.npy", proj)
        torch.save({"state_dict": head.state_dict(), "d_in": X.shape[1],
                    "proj_dim": PROJ_DIM, "hid": HID}, out_dir / f"proj_{backbone}.pt")
        print(f"[30] {backbone}: saved emb_{backbone}_proj.npy {proj.shape} "
              f"+ proj_{backbone}.pt")
    print("[30] done.")


# =====================================================================
# Stage T4 -- apply to dark fosmid ORFs (port of 50_apply_fosmid.py)
# =====================================================================

def apply_one(ctx: Context, q_emb_np, ref_emb_np, vocab, channel, floor, dev: str):
    """Distance-weighted kNN vote for one backbone. NO leakage removal (query ORFs
    are genuinely unlabeled -> we want their real nearest labeled neighbors)."""
    L = ctx.label_matrix()                                          # (Nref, V) fp16
    ref = torch.nn.functional.normalize(
        torch.as_tensor(ref_emb_np, device=dev, dtype=torch.float32), dim=1)
    q = torch.nn.functional.normalize(
        torch.as_tensor(q_emb_np, device=dev, dtype=torch.float32), dim=1)
    Nq = q.shape[0]
    votes = torch.zeros((Nq, ctx.V), dtype=torch.float16, device=dev)
    nn_pos = torch.zeros(Nq, dtype=torch.long, device=dev)
    nn_sim = torch.zeros(Nq, dtype=torch.float32, device=dev)
    ksup = torch.zeros(Nq, dtype=torch.long, device=dev)
    for s in range(0, Nq, BATCH):
        sim = q[s:s + BATCH] @ ref.T                               # (B, Nref) cosine
        vals, idx = torch.topk(sim, K, dim=1)
        nn_sim[s:s + BATCH] = vals[:, 0]
        nn_pos[s:s + BATCH] = idx[:, 0]
        vals = vals.clamp(min=0)
        ksup[s:s + BATCH] = (vals > 0).sum(1)
        tot = vals.sum(1, keepdim=True)
        w = (vals / tot.clamp(min=1e-9)).to(torch.float16)
        votes[s:s + BATCH] = (w.unsqueeze(-1) * L[idx]).sum(dim=1)

    vmask = votes >= floor
    qi, vj = torch.nonzero(vmask, as_tuple=True)
    qi_n, vj_n = qi.cpu().numpy(), vj.cpu().numpy()
    scores = votes[qi, vj].float().cpu().numpy()
    nn_pos_n = nn_pos.cpu().numpy(); nn_sim_n = nn_sim.cpu().numpy(); ksup_n = ksup.cpu().numpy()
    del ref, q, votes
    if dev == "cuda":
        torch.cuda.empty_cache()

    ref_orf = ctx.ref["orf"].to_numpy()
    ref_src = ctx.ref["source"].to_numpy()
    vocab_arr = np.asarray(vocab)
    return qi_n, vj_n, scores, nn_pos_n, nn_sim_n, ksup_n, ref_orf, ref_src, vocab_arr, channel


def apply_fosmid(in_dir: Path, out_path: Path, dev: str,
                 query_source: str = "fosmid", variants=VARIANTS):
    """Port of 50_apply_fosmid.main; emits embed_transfer_candidates.parquet."""
    print(f"[50] device = {dev}", flush=True)
    orf_index_path = in_dir / "orf_index.parquet"
    ctx = Context(orf_index_path, device=dev)
    idx = pd.read_parquet(orf_index_path)
    q = idx[idx["role"] == "query"].reset_index(drop=True)
    print(f"[50] dark {query_source} query ORFs: {len(q):,}", flush=True)

    all_rows = []
    for npy, channel, floor in variants:
        emb = np.load(in_dir / npy)
        q_emb = emb[q["row"].to_numpy()]
        ref_emb = emb[ctx.ref["row"].to_numpy()]
        qi, vj, sc, nnp, nns, ksup, ref_orf, ref_src, vocab_arr, ch = apply_one(
            ctx, q_emb, ref_emb, ctx.vocab, channel, floor, dev)
        df = pd.DataFrame({
            "source": query_source,
            "orf": q["orf"].to_numpy()[qi],
            "channel": ch,
            "mnxr": vocab_arr[vj],
            "intermediate_id": ref_orf[nnp][qi],
            "intermediate_name": ref_src[nnp][qi],
            "raw_score": np.round(sc, 4),
            "projection_via": "embedding_knn",
            "nn_similarity": np.round(nns[qi], 4),
            "k_support": ksup[qi],
        })
        n_orf = df["orf"].nunique()
        print(f"[50] {ch:15s} floor={floor}: {len(df):,} candidate rows over "
              f"{n_orf:,} dark ORFs ({n_orf/max(len(q),1):.1%} of query)", flush=True)
        all_rows.append(df)

    out = pd.concat(all_rows, ignore_index=True)[SCHEMA_COLS + ["nn_similarity", "k_support"]]
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"[50] wrote {out_path}  ({len(out):,} rows)", flush=True)

    # quick coverage summary by confidence tier (union across both channels)
    print("\n[50] dark-ORF coverage by confidence tier (either channel):")
    for tier in (0.1, 0.3, 0.5, 0.7):
        cov = out[out["raw_score"] >= tier]["orf"].nunique()
        print(f"   raw_score >= {tier:.1f} : {cov:,} / {len(q):,} dark ORFs "
              f"({cov/max(len(q),1):.1%})")
    return out


# =====================================================================
# CLI
# =====================================================================

def cmd_build_reference_pool(args):
    sources = json.loads(Path(args.sources).read_text())
    build_reference_pool(Path(args.evidence), sources, args.query_source,
                         Path(args.out_dir), args.esmc_dim, args.pbert_dim,
                         write_fasta=not args.no_fasta)


def cmd_train_projector(args):
    dev = resolve_device(args.device)
    train_projector(Path(args.in_dir), Path(args.out_dir), dev,
                    steps=args.steps, seed=args.seed)


def cmd_apply(args):
    dev = resolve_device(args.device)
    apply_fosmid(Path(args.in_dir), Path(args.out), dev,
                 query_source=args.query_source)


def parse_args():
    ap = argparse.ArgumentParser(
        description="ECSPr embed-transfer lane (lane 4): protein-LM kNN label transfer")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build-reference-pool", help="port 10_: reference pool + dark-query split + emb stacks")
    p.set_defaults(fn=cmd_build_reference_pool)
    p.add_argument("--evidence", required=True, help="evidence_table_dlec.parquet (label source)")
    p.add_argument("--sources", required=True, help="JSON manifest: src -> {esmc_parquet,esmc_index,pbert_npy,pbert_index,fasta}")
    p.add_argument("--out-dir", required=True, help="cache dir for orf_index.parquet + emb_*.npy + *.faa")
    p.add_argument("--query-source", default="fosmid", help="dark source whose evidence-free ORFs become the query set")
    p.add_argument("--esmc-dim", type=int, default=ESMC_DIM)
    p.add_argument("--pbert-dim", type=int, default=PBERT_DIM)
    p.add_argument("--no-fasta", action="store_true", help="skip reference.faa/gold.faa emission")

    p = sub.add_parser("train-projector", help="port 30_: CLEAN-style SupCon head on frozen embeddings")
    p.set_defaults(fn=cmd_train_projector)
    p.add_argument("--in-dir", required=True, help="cache dir with orf_index.parquet + emb_esmc.npy + emb_pbert.npy")
    p.add_argument("--out-dir", required=True, help="output dir for emb_<bk>_proj.npy + proj_<bk>.pt")
    p.add_argument("--device", default="cpu", help="cpu|cuda (falls back to cpu if cuda unavailable)")
    p.add_argument("--steps", type=int, default=STEPS)
    p.add_argument("--seed", type=int, default=0)

    p = sub.add_parser("apply", help="port 50_: lane-4 producer -> embed_transfer_candidates.parquet")
    p.set_defaults(fn=cmd_apply)
    p.add_argument("--in-dir", required=True, help="cache dir with orf_index.parquet + emb_pbert.npy + emb_esmc_proj.npy")
    p.add_argument("--out", required=True, help="output embed_transfer_candidates.parquet")
    p.add_argument("--query-source", default="fosmid")
    p.add_argument("--device", default="cpu", help="cpu|cuda (falls back to cpu if cuda unavailable)")

    return ap.parse_args()


if __name__ == "__main__":
    a = parse_args()
    a.fn(a)
