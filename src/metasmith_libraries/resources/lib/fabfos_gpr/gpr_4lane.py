"""GPR mapper (chosen 4 lanes): kofam + CLEAN + uniref50 + ProteinBERT -> one
gene-attributed GPR table.

The schema, the channel vocabulary, the per-channel score_kind and every lane
cut-off live in `lib::fabfos_evidence.py` and are read from it. Two copies, one
per mapper, is how the tree came to hold two different channel vocabularies at
once.

Every lane abstains before the table exists: a threshold applied by a reader is a
threshold the published table did not have. The cost of that placement is that
retuning a deployed cut-off means re-running this mapper rather than re-filtering
a table.
"""
import os
import sys


def _args():
    import argparse
    p = argparse.ArgumentParser(description="chosen-4 GPR mapper")
    p.add_argument("--bridge", required=True)
    p.add_argument("--clean", required=True)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--kofam", required=True)
    p.add_argument("--landmarks", required=True)
    p.add_argument("--lane-set", required=True)
    p.add_argument("--orfs", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--pbert-emb", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--threads", type=int, required=True)
    p.add_argument("--uniref", required=True)
    p.add_argument("--uniref-descriptions", required=True)
    return p.parse_args()


# BEFORE numpy. Its BLAS reads these once, at import, and the dominant arithmetic
# in this transform is the query-vs-landmark similarity matmul -- on the order of
# 2.3e13 operations for a 100,000-ORF shard, which is a couple of minutes across
# 16 cores and closer to forty single-threaded. The workflow config sets these to
# 1 and its own comment doubts they reach inside the container, so the number was
# simply unknown; it is declared here, from the allocation the scheduler actually
# granted, with the transform's own cpus as the floor.
if __name__ == "__main__":
    A = _args()
    _T = (os.environ.get("GPR_THREADS")
          or os.environ.get("SLURM_CPUS_PER_TASK")
          or str(A.threads))
    for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
               "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ[_v] = str(_T)

import numpy as np
import pandas as pd


def load_evidence(ev_lib):
    """The staged `lib::fabfos_evidence.py`, imported from wherever it landed.

    It carries the bridge loaders, the stitle cleaner, the schema, the score
    contract, the validator, and the parquet reader and writer every table here
    goes through -- `fe.read_parquet` / `fe.write_parquet` pick the engine the
    interpreter actually has, which is polars in this image and pyarrow outside
    it. Nothing in this file may touch parquet by another route.
    """
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

# Each lane PARSES first and joins to the bridge later. The order matters at this
# scale: the bridge is 30.5 million rows over three id spaces, and reading it
# whole into pandas -- which is what this transform used to do, once per task,
# ~1000 times over -- costs gigabytes of object-dtype strings to then throw
# almost all of them away. Parsing first means the shard's OWN hit ids are known
# before the read, so the read is bounded by the shard rather than by the
# reference. See `bridge_slice`.

# ---- kofam lane: dev2 kofamscan_results = gene_name,KO,thrshld,score,E-value,best
def parse_kofam(path):
    df = pd.read_csv(path)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["thrshld"] = pd.to_numeric(df["thrshld"], errors="coerce")
    df = df[df["score"].notna() & df["thrshld"].notna() & (df["score"] >= df["thrshld"])]
    return df.rename(columns={"gene_name": "orf", "KO": "ko", "score": "raw_score"})

def lane_kofam(df, ko_to_mnxr):
    df = df.merge(ko_to_mnxr, on="ko", how="inner")
    df["intermediate_id"] = df["ko"]
    return finish(df, "kofam", "kegg.reaction")

# ---- CLEAN lane: dev2 clean_predictions = Query ID \t Predicted EC number \t clean_score
#
# `clean_score` IS A CONFIDENCE -- higher is better, bounded by 1 -- not the raw
# maxsep distance this lane used to take it for. Stored through a 1/(1+d) inversion
# it ranked every CLEAN call backwards; the retired `clean_maxsep_inv` in
# fe.SCORE_KINDS carries the measurement. It needs no transform at all.
#
# CLEAN NEVER DECLINES -- a full level-4 EC for ~99% of ORFs -- so its coverage
# measures willingness, not reach, and the lane abstains here at parse time, in the
# same place and for the same reason kofam drops a hit below its family's threshold.
def parse_clean(path):
    df = pd.read_csv(path, sep="\t")
    if list(df.columns) != ["Query ID", "Predicted EC number", "clean_score"]:
        raise SystemExit(
            "[gpr] clean_predictions header is not the 3 columns this lane parses: "
            "got " + repr(list(df.columns)) + ". CLEAN's wrapper writes the header, "
            "so a drift here silently renames every column and empties the lane")
    df.columns = ["orf", "ec", "clean_score"]
    df["clean_score"] = pd.to_numeric(df["clean_score"], errors="coerce")
    n_in = len(df)
    df = df[df["clean_score"].notna() & (df["clean_score"] >= CLEAN_MIN_SCORE)].copy()
    print("[gpr] clean: " + str(n_in) + " calls, " + str(n_in - len(df))
          + " below the abstain at " + str(CLEAN_MIN_SCORE), flush=True)
    df["raw_score"] = df["clean_score"]
    return df[df["ec"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]

def lane_clean(df, ec_to_mnxr):
    df = df.merge(ec_to_mnxr, on="ec", how="inner")
    df["intermediate_id"] = df["ec"]
    return finish(df, "clean", "ec")

# ---- uniref50 lane: diamond_uniref50_results = BLAST6 + bsr, headered (13 col),
# with the subject title in diamond_uniref50_descriptions beside it.
def parse_uniref(path, descriptions):
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
    return df.rename(columns={"qseqid": "orf", "bsr": "raw_score"})

def lane_uniref(df, uniprot_to_mnxr):
    # evidence_quality is carried, not dropped: it is the only thing separating a
    # Swiss-Prot-backed reaction call from a TrEMBL one, and the bridge's keep-first
    # dedup already retained the stronger of the two per (id, mnxr).
    joined = df.merge(uniprot_to_mnxr, on="uniprot_accession", how="inner")
    joined["intermediate_id"] = joined["uniprot_accession"]
    joined["projection_via"] = joined["dr_source"]
    return finish(joined, "uniref50")

# ---- embedding lane: numpy kNN label transfer vs the labelled landmarks ----
# lightweight port of fabfos_embed_transfer.apply_one (cosine vote), plus the
# distance quota and the ORF-level abstain that port never had.
# An embedding table's ids and its vectors, taken from the SAME rows. Both the query
# and the landmark tables carry their id beside their vector, so there is no ordering
# here to get wrong -- which is the whole reason they were collapsed into one file
# each. What is checked is that the dim columns run contiguously from 0, because a
# table missing `dim_7` still stacks into a matrix of the wrong width rather than
# failing.
def _read_embeddings(path, id_col):
    df = fe.read_parquet(path)
    if id_col not in df.columns:
        raise SystemExit(
            "[gpr] " + path + " has no " + id_col + " column (it holds "
            + repr(list(df.columns)[:8]) + "...); the kNN lane keys its rows off it")
    dims = [c for c in df.columns if c.startswith("dim_")]
    want = ["dim_" + str(i) for i in range(len(dims))]
    if sorted(dims, key=lambda c: int(c[4:])) != want:
        raise SystemExit(
            "[gpr] " + path + " does not carry dim_0..dim_" + str(len(dims) - 1)
            + " contiguously; a gap would stack into a matrix of the wrong width "
            "rather than failing")
    return df[id_col].to_numpy(), df[want].to_numpy(dtype=np.float32), df


# Two names over one reader, because the query side is the half a driver replaces --
# see research/fabfos/examples/scadc_metag_gpr_4lane.py, which feeds `lane_embed` one
# slab of a legacy stack at a time. Overriding a shared reader would swap the
# landmarks out from under it too.
def _read_landmarks(path):
    return _read_embeddings(path, "accession")


def _read_query(path):
    return _read_embeddings(path, "sequence_id")


def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)


def lane_embed(parquet, landmark_dir, channel, floor, nn_min, tau, k_max):
    # `floor` is a share of a vote normalised within the admitted set, so a negative
    # one would admit labels no neighbour voted for. ZERO IS MEANINGFUL AND IS NOT
    # REFUSED: it means "every label an admitted neighbour carries", which is a real
    # setting now that `nn_min` decides which neighbours are admitted at all. The
    # refusal here used to cover a dense vote form that emitted the ENTIRE vocabulary
    # at score 0 for an unlabelled neighbourhood; that form is gone.
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
    ref_orf, ref_raw, ref = _read_landmarks(table)
    ref_emb = _norm(ref_raw)

    # THE LABEL MATRIX IS SPARSE AND IS STORED THAT WAY. This used to be a dense
    # (reference x MNXR) float32 indicator -- 222,019 x ~13,112 = 10.84 GiB that
    # is 99.97% zeros -- built so the vote could be a matmul. The vote is a
    # weighted sum over at most PBERT_K_MAX neighbours' label lists, and those are
    # short, so the matrix bought nothing but the 48 GB the step had to declare
    # and a (30 x 13,112) fancy-index COPY per query, 100,000 times per shard.
    #
    # Stored instead as CSR-shaped flat arrays: `lab_ptr` bounds each reference's
    # slice of `lab_idx`, which holds its labels' vocabulary indices. A few
    # megabytes. Each reference's list is DEDUPLICATED, because the dense form
    # wrote 1.0 idempotently and an accumulation would otherwise count a repeated
    # label twice -- the one place the two forms could disagree.
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
    print("[gpr] landmarks: " + str(len(ref)) + " references, " + str(len(vocab))
          + " MNXR, " + str(int(lab_ptr[-1])) + " label edges ("
          + "%.1f" % (lab_idx.nbytes / 2**20) + " MiB sparse vs "
          + "%.2f" % (len(ref) * len(vocab) * 4 / 2**30) + " GiB dense)", flush=True)

    q_orf, q_raw, _ = _read_query(parquet)
    if q_raw.shape[1] != ref_emb.shape[1]:
        raise SystemExit(
            "[gpr] the query embeddings are " + str(q_raw.shape[1]) + " dims and the "
            "landmarks are " + str(ref_emb.shape[1]) + ". A cosine between two "
            "embedding spaces is a number with no referent")
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
            # THE ORF-LEVEL ABSTAIN, and the thing this lane could not say before.
            # `floor` gates the VOTE, and the vote is normalised within whatever
            # neighbours are admitted, so it measures agreement and not proximity --
            # thirty neighbours at cosine 0.15 that agree score 1.0. Nothing in the
            # landmark set being close enough for a transferred label to mean
            # anything is a different claim, and it is made here.
            if cs[0] <= 0 or cs[0] < nn_min:
                n_refused += 1
                continue
            # THE QUOTA. Absolute (`nn_min`) and relative (`tau` of this ORF's own
            # best match) at once: the first says a neighbour is close enough to
            # speak at all, the second stops a mediocre one voting at full weight
            # beside a near-perfect one. So a dense neighbourhood votes with many
            # neighbours and a thin one with a few or with exactly one, where a
            # fixed K gave every ORF K votes whether or not it had K worth having.
            keep = cs >= max(nn_min, tau * cs[0])
            nn, vals = cand[keep], cs[keep]
            n_admitted += len(nn)
            n_voting += 1
            tot = vals.sum()
            if tot <= 0:
                continue
            w = vals / tot
            # Ragged gather of the admitted neighbours' label slices, then one bincount
            # over only the labels they actually carry. Exactly `w @ L[nn]`
            # restricted to its non-zero support, and `np.unique` returns the
            # support sorted, so the emitted row order is the same ascending
            # vocabulary order `np.nonzero` gave.
            cnt = lab_ptr[nn + 1] - lab_ptr[nn]
            total = int(cnt.sum())
            if total == 0:
                continue
            base = np.repeat(lab_ptr[nn], cnt)
            within = np.arange(total, dtype=np.int64) - np.repeat(
                np.cumsum(cnt) - cnt, cnt)
            gathered = lab_idx[base + within]
            uniq, inv = np.unique(gathered, return_inverse=True)
            votes = np.bincount(inv, weights=np.repeat(w, cnt),
                                minlength=len(uniq))
            donor = ref_orf[nn[0]]
            qid = q_orf[s+bi]
            for j in np.nonzero(votes >= floor)[0]:
                # intermediate_id names the DONOR neighbour, not a projected
                # intermediate: label transfer IS the projection, so there is no
                # KO or EC in between. `projection_via` says so.
                rows.append((qid, vocab_arr[uniq[j]], donor,
                             float(min(votes[j], 1.0))))
    print("[gpr] " + channel + ": " + str(len(q_emb)) + " ORFs, " + str(n_refused)
          + " with no landmark at cosine " + ("%.4f" % nn_min) + " or better (abstain), "
          + str(n_voting) + " voting on "
          + ("%.1f" % (n_admitted / n_voting) if n_voting else "0")
          + " admitted neighbours on average, " + str(len(rows)) + " calls",
          flush=True)
    df = pd.DataFrame(rows, columns=["orf", "mnxr", "intermediate_id", "raw_score"])
    # The landmarks are the bridge's `reviewed` cut by construction (see
    # compile/label_transfer_landmarks.py), so every transferred label inherits it.
    df["evidence_quality"] = "reviewed"
    return finish(df, channel, "embedding_knn")

def bridge_slice(path, src, name, wanted):
    """One id space of the bridge, cut down to the ids this shard actually hit.

    The bridge is 30.5 million rows over three id spaces and the read used to be
    the whole thing, into pandas, in every task -- gigabytes of object-dtype
    strings, ~1000 times, to keep a fraction of a percent of them. Both cuts are
    pushed into the parquet reader instead, so only the surviving rows are ever
    handed to pandas.

    `wanted` is the lane's own parsed ids, so the result is bounded by the SHARD.
    That is the whole point: the reference does not grow with the corpus and the
    per-task cost should not either.
    """
    if len(wanted) == 0:
        return pd.DataFrame(columns=[name, "mnxr", "evidence_quality"])
    df = fe.read_parquet(
        path, columns=["id", "mnxr", "evidence_quality"],
        filters=[("id_source", "==", src), ("id", "in", sorted(set(wanted)))])
    return df.drop_duplicates().rename(columns={"id": name})

def main():
    ids = orf_ids(str(A.orfs))
    if not ids:
        raise SystemExit("[gpr] the input ORF FASTA has no records")
    # Parse every lane FIRST, so the bridge read below knows which ids matter.
    kof = parse_kofam(str(A.kofam))
    cln = parse_clean(str(A.clean))
    uni = parse_uniref(str(A.uniref), str(A.uniref_descriptions))
    frames = [
        lane_kofam(kof, bridge_slice(str(A.bridge), "ko", "ko", kof["ko"].unique())),
        lane_clean(cln, bridge_slice(str(A.bridge), "ec", "ec", cln["ec"].unique())),
        lane_uniref(uni, bridge_slice(str(A.bridge), "uniprot", "uniprot_accession",
                                      uni["uniprot_accession"].unique())
                         .assign(dr_source="rhea")),
        lane_embed(str(A.pbert_emb), str(A.landmarks), "pbert", PBERT_FLOOR,
                   PBERT_NN_MIN, PBERT_TAU, PBERT_K_MAX),
    ]
    del kof, cln, uni
    gpr = pd.concat(frames, ignore_index=True)
    del frames
    # REFUSE THE STRAYS; DO NOT QUIETLY DROP THEM. The filter below was the only
    # treatment, which meant `validate_gpr`'s stray-id check could never fire --
    # a lane paired with the wrong shard was silently emptied here and then
    # surfaced downstream as "channel X contributed 0 rows", which reads as *the
    # lane failed* rather than *the lane was mispaired*. The ids are
    # `{sample}::`-prefixed and disjoint between shards, so a mispairing is
    # total: the count below is either ~0 or ~everything, and either way it
    # names the right problem.
    id_set = set(ids)
    for ch, grp in gpr.groupby("channel"):
        stray = grp.loc[~grp["orf"].isin(id_set), "orf"]
        if len(stray):
            raise SystemExit(
                "[gpr] the " + str(ch) + " lane contributed " + str(len(stray))
                + " of " + str(len(grp)) + " rows for ORF ids that are not in this "
                "shard's FASTA, e.g. " + repr(sorted(set(stray))[:3]) + ". That "
                "lane's product belongs to a DIFFERENT shard -- check the run's "
                "lineage_of_given.json before spending any more queue time")
    gpr = gpr[gpr["orf"].isin(id_set)]
    key = ["source", "orf", "channel", "intermediate_id", "mnxr"]
    gpr = gpr.drop_duplicates(subset=key).sort_values(key, kind="mergesort").reset_index(drop=True)
    # Refuse before writing, not after: an empty concat, an ORF-id mismatch and a
    # lane whose reference never staged all used to produce a zero-row parquet and
    # report success.
    fe.validate_gpr(gpr, LANE_SET, ids, SOURCE)
    fe.write_parquet(gpr, str(A.out))
    print("[gpr_4lane] wrote " + str(len(gpr)) + " rows -> " + str(A.out), flush=True)


if __name__ == "__main__":
    fe = load_evidence(A.ev_lib)
    SCHEMA = fe.SCHEMA_COLS
    LANE_SET = str(A.lane_set)
    SOURCE = str(A.source)
    # The lane cut-offs are fe's, not this module's. They were literals here, in the
    # 7-lane driver and in fe at once, so a retuned floor shipped in whichever of the
    # three the tuning session happened to edit.
    PBERT_NN_MIN = fe.PBERT_NN_MIN
    PBERT_TAU = fe.PBERT_TAU
    PBERT_K_MAX = fe.PBERT_K_MAX
    PBERT_FLOOR = fe.PBERT_FLOOR
    CLEAN_MIN_SCORE = fe.CLEAN_MIN_SCORE
    main()
