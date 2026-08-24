"""GPR mapper (full 7 lanes): every annotation lane folded into one
gene-attributed GPR table.

Same shape and contract as gpr_4lane (see that module, and
`lib::fabfos_evidence.py` for the schema itself); this is the full-coverage
variant that adds DeepEC, EZpred and the ESM-C embedding lane.

The ESM-C lane votes against its OWN landmarks: two embedders in two images, one
of which needs a GPU, so they are two references. `lane_embed` refuses a query and
a landmark table of different widths, because a kNN vote across two embedding
spaces is a number with no referent.
"""
import os
import sys


def _args():
    import argparse
    p = argparse.ArgumentParser(description="full-7 GPR mapper")
    p.add_argument("--bridge", required=True)
    p.add_argument("--clean", required=True)
    p.add_argument("--deepec", required=True)
    p.add_argument("--esmc-emb", required=True)
    p.add_argument("--esmc-idx", required=True)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--ezpred", required=True)
    p.add_argument("--kofam", required=True)
    p.add_argument("--landmarks", required=True)
    p.add_argument("--lane-set", required=True)
    p.add_argument("--lm-esmc", required=True)
    p.add_argument("--orfs", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--pbert-emb", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--uniref", required=True)
    p.add_argument("--uniref-descriptions", required=True)
    return p.parse_args()


if __name__ == "__main__":
    A = _args()

import numpy as np
import pandas as pd


def load_evidence(ev_lib):
    """The staged `lib::fabfos_evidence.py`, imported from wherever it landed."""
    sys.path.insert(0, os.path.dirname(str(ev_lib)))
    import fabfos_evidence
    return fabfos_evidence


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

# `clean_score` is CLEAN's GMM-calibrated CONFIDENCE (higher is better), stored
# unchanged, and the lane abstains below fe.CLEAN_MIN_SCORE at parse time the way
# kofam drops a hit below its family threshold. See gpr_4lane.py.
def lane_clean(path, ec_to_mnxr):
    df = pd.read_csv(path, sep="\t")
    if list(df.columns) != ["Query ID", "Predicted EC number", "clean_score"]:
        raise SystemExit(
            "[gpr] clean_predictions header is not the 3 columns this lane parses: "
            "got " + repr(list(df.columns)))
    df.columns = ["orf", "ec", "clean_score"]
    df["clean_score"] = pd.to_numeric(df["clean_score"], errors="coerce")
    n_in = len(df)
    df = df[df["clean_score"].notna() & (df["clean_score"] >= CLEAN_MIN_SCORE)].copy()
    print("[gpr] clean: " + str(n_in) + " calls, " + str(n_in - len(df))
          + " below the abstain at " + str(CLEAN_MIN_SCORE), flush=True)
    df["raw_score"] = df["clean_score"]
    df = df[df["ec"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]
    df = df.merge(ec_to_mnxr, on="ec", how="inner")
    df["intermediate_id"] = df["ec"]
    return finish(df, "clean", "ec")

# DeepEC: dev2 deepec_predictions is a TSV; col0 = gene id, col1 = predicted EC.
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
    # A score-less tool asserts PRESENCE, and 1.0 is how that is spelled. NaN made
    # the downstream share-of-sum read the whole lane's total as zero and fall back
    # to a uniform split with nothing raised.
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

def lane_uniref(path, descriptions, uniprot_to_mnxr):
    df = pd.read_csv(path, sep="\t", dtype=str)
    if list(df.columns) != fe._BLAST6_BSR_COLS:
        raise SystemExit(
            "[gpr] diamond_uniref50_results header is not the "
            + str(len(fe._BLAST6_BSR_COLS)) + " columns this lane parses: got "
            + repr(list(df.columns)) + ". diamond_uniref50 writes the header, "
            "so a drift here silently renames every column and empties the lane")
    df["evalue"] = pd.to_numeric(df["evalue"], errors="coerce")
    df["bitscore"] = pd.to_numeric(df["bitscore"], errors="coerce")
    df["bsr"] = pd.to_numeric(df["bsr"], errors="coerce")
    df = df[df["bsr"].notna()]
    df = (df.sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
            .drop_duplicates(subset=["qseqid"], keep="first"))
    df["uniprot_accession"] = df["sseqid"].str.replace(r"^UniRef50_", "", regex=True)
    titles = fe.read_uniref50_titles(descriptions)
    df["intermediate_name"] = df["sseqid"].map(titles).fillna("").apply(fe._clean_stitle)
    df = df.rename(columns={"qseqid": "orf", "bsr": "raw_score"})
    joined = df.merge(uniprot_to_mnxr, on="uniprot_accession", how="inner")
    joined["intermediate_id"] = joined["uniprot_accession"]
    joined["projection_via"] = joined["dr_source"]
    return finish(joined, "uniref50")

# An embedding table's ids and its vectors, taken from the SAME rows. The dim columns
# must run contiguously from 0: a table missing `dim_7` still stacks into a matrix of
# the wrong width rather than failing.
def _dims(df, path):
    dims = [c for c in df.columns if c.startswith("dim_")]
    want = ["dim_" + str(i) for i in range(len(dims))]
    if sorted(dims, key=lambda c: int(c[4:])) != want:
        raise SystemExit(
            "[gpr] " + path + " does not carry dim_0..dim_" + str(len(dims) - 1)
            + " contiguously")
    return df[want].to_numpy(dtype=np.float32)

def _read_query(parquet, index_csv):
    df = pd.read_parquet(parquet)
    emb = _dims(df, parquet)
    if index_csv is None:
        if "sequence_id" not in df.columns:
            raise SystemExit(
                "[gpr] " + parquet + " has no sequence_id column; this embedding type "
                "carries its ids beside its vectors")
        return df["sequence_id"].to_numpy(), emb
    # THE PRODUCER CHECKS THIS AND THE CONSUMER DID NOT. An index LONGER or shorter
    # than the stack does not raise on its own -- it attributes every vote to the
    # wrong ORF and yields a full, schema-valid, confidently wrong table.
    idx = pd.read_csv(index_csv)
    for cand in ("sequence_id", "id"):
        if cand in idx.columns:
            ids = idx[cand].to_numpy()
            break
    else:
        raise SystemExit(
            "[gpr] the embedding index " + index_csv + " has no id column (it holds "
            + repr(list(idx.columns)) + "); the kNN lane keys its ORFs off it")
    if len(ids) != len(emb):
        raise SystemExit(
            "[gpr] the embedding index has " + str(len(ids)) + " rows and the stack "
            "has " + str(len(emb)) + "; the lane addresses the stack BY ROW, so these "
            "cannot be paired")
    return ids, emb

def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)

def lane_embed(parquet, index_csv, landmark_dir, channel, floor,
               nn_min, tau, k_max):
    # Zero is a meaningful vote floor -- "every label an admitted neighbour carries" --
    # now that `nn_min` decides which neighbours are admitted. Only a negative one is
    # refused. See gpr_4lane.py for the dense vote form this used to guard against.
    if floor < 0:
        raise SystemExit("[gpr] the " + channel + " vote floor must be >= 0")
    if not 0.0 <= tau <= 1.0:
        raise SystemExit("[gpr] the " + channel + " tau is a fraction of this ORF's "
                         "own best cosine and must lie in [0, 1]")
    if k_max < 1:
        raise SystemExit("[gpr] the " + channel + " k_max must be at least 1")
    table = os.path.join(landmark_dir, "landmarks.parquet")
    if not os.path.exists(table):
        raise SystemExit(
            "[gpr] the landmark set carries no landmarks.parquet (it holds "
            + repr(sorted(os.listdir(landmark_dir))) + "). The " + channel + " lane "
            "votes against embeddings from its own model -- cosine distance between "
            "two embedding spaces is a number with no referent -- so there is no "
            "degraded mode here; the landmarks must be rebuilt with this embedder")
    ref = pd.read_parquet(table)
    ref_emb = _norm(_dims(ref, table))
    ref_orf = ref["accession"].to_numpy()

    # CSR-shaped label lists rather than a dense (reference x MNXR) indicator: at
    # 222,019 x ~13,112 float32 that matrix is 10.84 GiB of 99.97% zeros, and the
    # vote only ever touches at most K neighbours' short lists. Each list is
    # DEDUPLICATED -- the dense form wrote 1.0 idempotently, so an accumulation over
    # a repeated label is the one place the two forms could disagree.
    label_lists = [
        sorted(set(m for m in (s.split(";") if s else []) if m))
        for s in ref["mnxr_list"].fillna("")
    ]
    vocab = sorted({m for ls in label_lists for m in ls})
    vidx = {m: i for i, m in enumerate(vocab)}
    vocab_arr = np.asarray(vocab, dtype=object)
    counts = np.fromiter((len(ls) for ls in label_lists), dtype=np.int64,
                         count=len(label_lists))
    lab_ptr = np.zeros(len(label_lists) + 1, dtype=np.int64)
    np.cumsum(counts, out=lab_ptr[1:])
    lab_idx = np.fromiter((vidx[m] for ls in label_lists for m in ls),
                          dtype=np.int32, count=int(lab_ptr[-1]))

    q_orf, q_raw = _read_query(parquet, index_csv)
    if q_raw.shape[1] != ref_emb.shape[1]:
        raise SystemExit(
            "[gpr] the " + channel + " query embeddings are " + str(q_raw.shape[1])
            + " dims and its landmarks are " + str(ref_emb.shape[1]) + ". A cosine "
            "between two embedding spaces is a number with no referent")
    q_emb = _norm(q_raw)
    kk = min(k_max, ref_emb.shape[0])
    rows = []
    n_refused = 0
    n_admitted = 0
    n_voting = 0
    for s in range(0, len(q_emb), 256):
        sim = q_emb[s:s+256] @ ref_emb.T
        top = np.argpartition(-sim, min(kk, sim.shape[1]-1), axis=1)[:, :kk]
        for bi in range(sim.shape[0]):
            cand = top[bi]
            cs = np.clip(sim[bi, cand], 0, None)
            order = np.argsort(-cs, kind="stable")
            cand, cs = cand[order], cs[order]
            # The ORF-level abstain: `floor` gates the vote, which is normalised
            # within the admitted set and so measures AGREEMENT; this measures
            # PROXIMITY. See gpr_4lane.py.
            if cs[0] <= 0 or cs[0] < nn_min:
                n_refused += 1
                continue
            # The quota: absolute floor and a band relative to this ORF's own best
            # match, so a dense neighbourhood votes with many neighbours and a thin
            # one with a few or with exactly one.
            keep = cs >= max(nn_min, tau * cs[0])
            nn, vals = cand[keep], cs[keep]
            n_admitted += len(nn)
            n_voting += 1
            tot = vals.sum()
            if tot <= 0:
                continue
            w = vals / tot
            cnt = lab_ptr[nn + 1] - lab_ptr[nn]
            total = int(cnt.sum())
            if total == 0:
                continue
            base = np.repeat(lab_ptr[nn], cnt)
            within = np.arange(total, dtype=np.int64) - np.repeat(
                np.cumsum(cnt) - cnt, cnt)
            uniq, inv = np.unique(lab_idx[base + within], return_inverse=True)
            votes = np.bincount(inv, weights=np.repeat(w, cnt), minlength=len(uniq))
            for j in np.nonzero(votes >= floor)[0]:
                # intermediate_id names the DONOR neighbour: label transfer IS the
                # projection, so there is no KO or EC in between. `cs` is sorted
                # descending, so the donor is the first admitted neighbour.
                rows.append((q_orf[s+bi], vocab_arr[uniq[j]], ref_orf[nn[0]],
                             float(min(votes[j], 1.0))))
    print("[gpr] " + channel + ": " + str(len(q_emb)) + " ORFs, " + str(n_refused)
          + " with no landmark at cosine " + ("%.4f" % nn_min) + " or better (abstain), "
          + str(n_voting) + " voting on "
          + ("%.1f" % (n_admitted / n_voting) if n_voting else "0")
          + " admitted neighbours on average, " + str(len(rows)) + " calls",
          flush=True)
    df = pd.DataFrame(rows, columns=["orf", "mnxr", "intermediate_id", "raw_score"])
    df["evidence_quality"] = "reviewed"   # the landmarks ARE the bridge's reviewed cut
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
        lane_uniref(str(A.uniref), str(A.uniref_descriptions), up_to_mnxr),
        lane_embed(str(A.pbert_emb), None, str(A.landmarks), "pbert", PBERT_FLOOR,
                   PBERT_NN_MIN, PBERT_TAU, PBERT_K_MAX),
        lane_embed(str(A.esmc_emb), str(A.esmc_idx), str(A.lm_esmc), "esmc", ESMC_FLOOR,
                   ESMC_NN_MIN, ESMC_TAU, ESMC_K_MAX),
    ]
    gpr = pd.concat(frames, ignore_index=True)
    gpr = gpr[gpr["orf"].isin(set(ids))]
    key = ["source", "orf", "channel", "intermediate_id", "mnxr"]
    gpr = gpr.drop_duplicates(subset=key).sort_values(key, kind="mergesort").reset_index(drop=True)
    fe.validate_gpr(gpr, LANE_SET, ids, SOURCE)
    gpr.to_parquet(str(A.out), index=False)
    print("[gpr_7lane] wrote " + str(len(gpr)) + " rows -> " + str(A.out), flush=True)


if __name__ == "__main__":
    fe = load_evidence(A.ev_lib)
    SCHEMA = fe.SCHEMA_COLS
    LANE_SET = str(A.lane_set)
    SOURCE = str(A.source)
    # Every lane cut-off is fe's. See the block there for what each one gates and why
    # the ESM-C quota knobs are the pre-quota no-ops rather than pbert's tuned numbers.
    PBERT_NN_MIN = fe.PBERT_NN_MIN
    PBERT_TAU = fe.PBERT_TAU
    PBERT_K_MAX = fe.PBERT_K_MAX
    PBERT_FLOOR = fe.PBERT_FLOOR
    ESMC_NN_MIN = fe.ESMC_NN_MIN
    ESMC_TAU = fe.ESMC_TAU
    ESMC_K_MAX = fe.ESMC_K_MAX
    ESMC_FLOOR = fe.ESMC_FLOOR
    CLEAN_MIN_SCORE = fe.CLEAN_MIN_SCORE
    DL_EC_FLOOR = fe.DL_EC_SCORE_FLOOR
    main()
