import argparse as _argparse
_p = _argparse.ArgumentParser()
_p.add_argument("--bridge", required=True)
_p.add_argument("--clean", required=True)
_p.add_argument("--deepec", required=True)
_p.add_argument("--esmc-emb", required=True)
_p.add_argument("--esmc-idx", required=True)
_p.add_argument("--ev-lib", required=True)
_p.add_argument("--ezpred", required=True)
_p.add_argument("--kofam", required=True)
_p.add_argument("--lane-set", required=True)
_p.add_argument("--orfs", required=True)
_p.add_argument("--out", required=True)
_p.add_argument("--pbert-emb", required=True)
_p.add_argument("--pbert-idx", required=True)
_p.add_argument("--pool", required=True)
_p.add_argument("--pool-esmc", required=True)
_p.add_argument("--source", required=True)
_p.add_argument("--uniref", required=True)
A = _p.parse_args()

import sys, os
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(str(A.ev_lib)))
import fabfos_evidence as fe

SCHEMA = fe.SCHEMA_COLS
LANE_SET = str(A.lane_set)
SOURCE = str(A.source)
K = 30
PBERT_FLOOR = 0.20
ESMC_FLOOR = 0.10
DL_EC_FLOOR = fe.DL_EC_SCORE_FLOOR


def finish(df, channel, projection_via=None):
    """Stamp the columns every lane sets identically and order to the schema."""
    df["source"] = SOURCE
    df["channel"] = channel
    df["score_kind"] = fe.CHANNEL_SCORE_KIND[channel]
    df["lane_set"] = LANE_SET
    if projection_via is not None:
        df["projection_via"] = projection_via
    if "intermediate_name" not in df.columns:
        df["intermediate_name"] = ""
    df["intermediate_name"] = df["intermediate_name"].fillna("")
    if "evidence_quality" not in df.columns:
        df["evidence_quality"] = "reviewed"
    return df[SCHEMA]

def orf_ids(fasta):
    out = []
    with open(fasta) as fh:
        for line in fh:
            if line.startswith(">"):
                out.append(line[1:].split()[0])
    return out

def lane_kofam(path, ko_to_mnxr):
    df = pd.read_csv(path)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["thrshld"] = pd.to_numeric(df["thrshld"], errors="coerce")
    df = df[df["score"].notna() & df["thrshld"].notna() & (df["score"] >= df["thrshld"])]
    df = df.rename(columns={"gene_name": "orf", "KO": "ko", "score": "raw_score"})
    df = df.merge(ko_to_mnxr, on="ko", how="inner")
    df["intermediate_id"] = df["ko"]
    return finish(df, "kofam", "kegg.reaction")

# `clean_score` is CLEAN's maxsep DISTANCE (lower is better); stored through
# fe.clean_distance_to_score so higher-is-stronger holds. See gpr_4lane.py.
def lane_clean(path, ec_to_mnxr):
    df = pd.read_csv(path, sep="\t")
    if list(df.columns) != ["Query ID", "Predicted EC number", "clean_score"]:
        raise SystemExit(
            "[gpr] clean_predictions header is not the 3 columns this lane parses: "
            "got " + repr(list(df.columns)))
    df.columns = ["orf", "ec", "clean_dist"]
    df["clean_dist"] = pd.to_numeric(df["clean_dist"], errors="coerce")
    df = df[df["clean_dist"].notna() & (df["clean_dist"] >= 0)].copy()
    df["raw_score"] = fe.clean_distance_to_score(df["clean_dist"])
    df = df[df["ec"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]
    df = df.merge(ec_to_mnxr, on="ec", how="inner")
    df["intermediate_id"] = df["ec"]
    return finish(df, "clean", "ec")

# DeepEC: dev2 deepec_predictions is a TSV; col0 = gene id, col1 = predicted EC.
# score-less -> raw_score = NaN (accepted).
def lane_deepec(path, ec_to_mnxr):
    rows = []
    with open(path) as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            gene, ec = parts[0].strip(), parts[1].strip()
            if not ec or ec.lower().startswith("predicted") or ec.count(".") < 1:
                continue
            rows.append((gene, ec))
    df = pd.DataFrame(rows, columns=["orf", "ec"])
    # DeepEC writes its calls PREFIXED -- `EC:4.2.1.47`, not `4.2.1.47`. Without this
    # strip the level-4 match below rejects every row, the lane contributes nothing,
    # and the only thing standing between that and a published 7-lane table missing a
    # channel is validate_gpr's per-channel refusal. (CLEAN's wrapper strips the same
    # prefix at the source; this lane reads DeepEC's file as written.)
    df["ec"] = df["ec"].str.replace(r"^EC:", "", regex=True, case=False)
    df = df[df["ec"].str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]
    df = df.merge(ec_to_mnxr, on="ec", how="inner")
    df["intermediate_id"] = df["ec"]
    # Presence, not NaN -- see the header.
    df["raw_score"] = 1.0
    return finish(df, "deepec", "ec")

# EZpred: dev2 ezpred_predictions = sequence_id, ec_number, score, head_kind.
def lane_ezpred(path, ec_to_mnxr):
    df = pd.read_csv(path)
    df = df[df["head_kind"] == "enzyme"]
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df = df[df["score"] >= DL_EC_FLOOR]
    df = df[df["ec_number"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]
    df = df.merge(ec_to_mnxr, left_on="ec_number", right_on="ec", how="inner")
    df = df.rename(columns={"sequence_id": "orf", "score": "raw_score"})
    df["intermediate_id"] = df["ec_number"]
    return finish(df, "ezpred", "ec")

def lane_uniref(path, uniprot_to_mnxr):
    df = pd.read_csv(path, sep="\t", header=None, names=fe._BLAST6_BSR_COLS, dtype=str)
    df["evalue"] = pd.to_numeric(df["evalue"], errors="coerce")
    df["bitscore"] = pd.to_numeric(df["bitscore"], errors="coerce")
    df["bsr"] = pd.to_numeric(df["bsr"], errors="coerce")
    df = df[df["bsr"].notna()]
    df = (df.sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
            .drop_duplicates(subset=["qseqid"], keep="first"))
    df["uniprot_accession"] = df["sseqid"].str.replace(r"^UniRef50_", "", regex=True)
    df["intermediate_name"] = df["stitle"].apply(fe._clean_stitle)
    df = df.rename(columns={"qseqid": "orf", "bsr": "raw_score"})
    joined = df.merge(uniprot_to_mnxr, on="uniprot_accession", how="inner")
    joined["intermediate_id"] = joined["uniprot_accession"]
    joined["projection_via"] = joined["dr_source"]
    return finish(joined, "uniref50")

def _query_ids(index_csv):
    """The embedding index's id column -- `sequence_id` after the producer
    normalises it, `id` as the embedder itself writes it. Both read, neither guessed."""
    idx = pd.read_csv(index_csv)
    for cand in ("sequence_id", "id"):
        if cand in idx.columns:
            return idx[cand].to_numpy()
    raise SystemExit(
        "[gpr] the embedding index " + index_csv + " has no id column (it holds "
        + repr(list(idx.columns)) + "); the kNN lane keys its ORFs off it")

def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)

def lane_embed(parquet, index_csv, pool_dir, emb_name, channel, floor):
    stack = os.path.join(pool_dir, emb_name)
    if not os.path.exists(stack):
        raise SystemExit(
            "[gpr] the reference label pool carries no " + emb_name + " (it holds "
            + repr(sorted(os.listdir(pool_dir))) + "). The " + channel + " lane votes "
            "against embeddings from its own model -- cosine distance between two "
            "embedding spaces is a number with no referent -- so there is no "
            "degraded mode here; the pool must be rebuilt with this embedder")
    pool_idx = pd.read_parquet(os.path.join(pool_dir, "orf_index.parquet"))
    ref = pool_idx[pool_idx["role"] == "reference"].reset_index(drop=True)
    emb = np.load(stack, mmap_mode="r")
    ref_emb = _norm(np.asarray(emb[ref["row"].to_numpy()], dtype=np.float32))
    ref_orf = ref["orf"].to_numpy()
    label_lists = [s.split(";") if s else [] for s in ref["mnxr_list"]]
    vocab = sorted({m for ls in label_lists for m in ls})
    vidx = {m: i for i, m in enumerate(vocab)}
    L = np.zeros((len(ref), len(vocab)), dtype=np.float32)
    for r, ls in enumerate(label_lists):
        for m in ls:
            L[r, vidx[m]] = 1.0
    q_orf = _query_ids(index_csv)
    q_emb = _norm(pd.read_parquet(parquet).to_numpy(dtype=np.float32))
    rows = []
    for s in range(0, len(q_emb), 256):
        sim = q_emb[s:s+256] @ ref_emb.T
        top = np.argpartition(-sim, min(K, sim.shape[1]-1), axis=1)[:, :K]
        for bi in range(sim.shape[0]):
            nn = top[bi]
            vals = np.clip(sim[bi, nn], 0, None)
            tot = vals.sum()
            if tot <= 0:
                continue
            w = vals / tot
            votes = w @ L[nn]
            best = int(np.argmax(sim[bi, nn]))
            for j in np.nonzero(votes >= floor)[0]:
                # intermediate_id names the DONOR neighbour: label transfer IS the
                # projection, so there is no KO or EC in between.
                rows.append((q_orf[s+bi], vocab[j], ref_orf[nn[best]],
                             float(min(votes[j], 1.0))))
    df = pd.DataFrame(rows, columns=["orf", "mnxr", "intermediate_id", "raw_score"])
    df["evidence_quality"] = "reviewed"   # the pool IS the bridge's reviewed cut
    return finish(df, channel, "embedding_knn")

def load_bridge(path):
    """One table, three id spaces. Sliced by id_source into the per-lane frames the
    lane functions expect. The spaces share no ids, so the slice is exact."""
    b = pd.read_parquet(path, columns=["id", "id_source", "mnxr", "evidence_quality"])
    def slice_as(src, name):
        s = b[b["id_source"] == src][["id", "mnxr", "evidence_quality"]].drop_duplicates()
        return s.rename(columns={"id": name})
    return (slice_as("ko", "ko"),
            slice_as("ec", "ec"),
            slice_as("uniprot", "uniprot_accession").assign(dr_source="rhea"))

def main():
    ids = orf_ids(str(A.orfs))
    if not ids:
        raise SystemExit("[gpr] the input ORF FASTA has no records")
    ko_to_mnxr, ec_to_mnxr, up_to_mnxr = load_bridge(str(A.bridge))
    frames = [
        lane_kofam(str(A.kofam), ko_to_mnxr),
        lane_clean(str(A.clean), ec_to_mnxr),
        lane_deepec(str(A.deepec), ec_to_mnxr),
        lane_ezpred(str(A.ezpred), ec_to_mnxr),
        lane_uniref(str(A.uniref), up_to_mnxr),
        lane_embed(str(A.pbert_emb), str(A.pbert_idx), str(A.pool), "emb_pbert.npy", "pbert", PBERT_FLOOR),
        lane_embed(str(A.esmc_emb), str(A.esmc_idx), str(A.pool_esmc), "emb_esmc.npy", "esmc", ESMC_FLOOR),
    ]
    gpr = pd.concat(frames, ignore_index=True)
    gpr = gpr[gpr["orf"].isin(set(ids))]
    key = ["source", "orf", "channel", "intermediate_id", "mnxr"]
    gpr = gpr.drop_duplicates(subset=key).sort_values(key, kind="mergesort").reset_index(drop=True)
    fe.validate_gpr(gpr, LANE_SET, ids, SOURCE)
    gpr.to_parquet(str(A.out), index=False)
    print("[gpr_7lane] wrote " + str(len(gpr)) + " rows -> " + str(A.out), flush=True)

main()
