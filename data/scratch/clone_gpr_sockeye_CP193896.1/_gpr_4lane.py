import sys, os

# BEFORE numpy. Its BLAS reads these once, at import, and the dominant arithmetic
# in this transform is the query-vs-pool similarity matmul -- on the order of
# 2.3e13 operations for a 100,000-ORF shard, which is a couple of minutes across
# 16 cores and closer to forty single-threaded. The workflow config sets these to
# 1 and its own comment doubts they reach inside the container, so the number was
# simply unknown; it is now declared here, from the allocation the scheduler
# actually granted, with the transform's own cpus as the floor.
_T = (os.environ.get("GPR_THREADS")
      or os.environ.get("SLURM_CPUS_PER_TASK")
      or "8")
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = str(_T)

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# reuse the ported bridge loaders / stitle cleaner / SCHEMA + score contract +
# validator (pandas-only)
sys.path.insert(0, os.path.dirname("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/_metasmith/task/data/67s9CEUuzyy0/fabfos_evidence.py"))
import fabfos_evidence as fe

SCHEMA = fe.SCHEMA_COLS
LANE_SET = "chosen_4"
SOURCE = "CP193896.1"
K = 30
PBERT_FLOOR = 0.20


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
# `clean_score` is CLEAN's maxsep value, which is a DISTANCE to the EC cluster
# centre -- lower is better, and unbounded above (its own parser's worked example
# is 8.06). Stored through fe.clean_distance_to_score so the schema's
# higher-is-stronger contract holds; the transform is monotone and lossless.
def parse_clean(path):
    df = pd.read_csv(path, sep="\t")
    if list(df.columns) != ["Query ID", "Predicted EC number", "clean_score"]:
        raise SystemExit(
            "[gpr] clean_predictions header is not the 3 columns this lane parses: "
            "got " + repr(list(df.columns)) + ". CLEAN's wrapper writes the header, "
            "so a drift here silently renames every column and empties the lane")
    df.columns = ["orf", "ec", "clean_dist"]
    df["clean_dist"] = pd.to_numeric(df["clean_dist"], errors="coerce")
    df = df[df["clean_dist"].notna() & (df["clean_dist"] >= 0)].copy()
    df["raw_score"] = fe.clean_distance_to_score(df["clean_dist"])
    return df[df["ec"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]

def lane_clean(df, ec_to_mnxr):
    df = df.merge(ec_to_mnxr, on="ec", how="inner")
    df["intermediate_id"] = df["ec"]
    return finish(df, "clean", "ec")

# ---- uniref50 lane: dev2 diamond_uniref50_results = BLAST6 + stitle + bsr (14 col)
def parse_uniref(path):
    df = pd.read_csv(path, sep="\t", header=None, names=fe._BLAST6_BSR_COLS, dtype=str)
    df["evalue"] = pd.to_numeric(df["evalue"], errors="coerce")
    df["bitscore"] = pd.to_numeric(df["bitscore"], errors="coerce")
    df["bsr"] = pd.to_numeric(df["bsr"], errors="coerce")
    df = df[df["bsr"].notna()]
    df = (df.sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
            .drop_duplicates(subset=["qseqid"], keep="first"))
    df["uniprot_accession"] = df["sseqid"].str.replace(r"^UniRef50_", "", regex=True)
    df["intermediate_name"] = df["stitle"].apply(fe._clean_stitle)
    return df.rename(columns={"qseqid": "orf", "bsr": "raw_score"})

def lane_uniref(df, uniprot_to_mnxr):
    # evidence_quality is carried, not dropped: it is the only thing separating a
    # Swiss-Prot-backed reaction call from a TrEMBL one, and the bridge's keep-first
    # dedup already retained the stronger of the two per (id, mnxr).
    joined = df.merge(uniprot_to_mnxr, on="uniprot_accession", how="inner")
    joined["intermediate_id"] = joined["uniprot_accession"]
    joined["projection_via"] = joined["dr_source"]
    return finish(joined, "uniref50")

# ---- embedding lane: numpy kNN label transfer vs the reference pool ----
# lightweight port of fabfos_embed_transfer.apply_one (cosine top-K distance vote).
def _query_ids(index_csv):
    """The embedding index's id column.

    The transform that writes `annotation::proteinbert_index` normalises it to
    `sequence_id`; the embedder itself writes `id`, so an artifact predating that
    normalisation carries the other name. Both are read, neither is guessed at.
    """
    idx = pd.read_csv(index_csv)
    for cand in ("sequence_id", "id"):
        if cand in idx.columns:
            return idx[cand].to_numpy()
    raise SystemExit(
        "[gpr] the embedding index " + index_csv + " has no id column (it holds "
        + repr(list(idx.columns)) + "); the kNN lane keys its ORFs off it")

def _load_query(parquet, index_csv):
    q = pd.read_parquet(parquet).to_numpy(dtype=np.float32)
    ids = _query_ids(index_csv)
    # THE PRODUCER CHECKS THIS AND THE CONSUMER DID NOT. `proteinbert.py`'s
    # combiner refuses an index/stack length mismatch because "pairing them
    # would misindex every row silently" -- but this transform then read the two
    # back separately and never re-checked. An index LONGER than the stack does
    # not raise here, it just labels every vote with the wrong ORF, and the
    # result is a full, schema-valid, confidently wrong table. That matters more
    # now than it did: the embeddings can arrive as a repack of an earlier pass
    # rather than from the producer in the same run.
    if len(ids) != len(q):
        raise SystemExit(
            "[gpr] the embedding index has " + str(len(ids)) + " rows and the "
            "stack has " + str(len(q)) + ". The lane addresses the stack BY ROW, "
            "so these cannot be paired -- every kNN vote would be attributed to "
            "the wrong ORF, with nothing raised")
    return ids, q

def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)

def lane_embed(parquet, index_csv, pool_dir, emb_name, channel, floor):
    # The sparse vote skips a query whose whole neighbourhood is unlabelled,
    # where the dense form computed an all-zero vote vector. Those agree for any
    # positive floor and diverge at floor <= 0, where the dense form would emit
    # the ENTIRE vocabulary at score 0 for such a query. Refused rather than
    # silently reinterpreted.
    if floor <= 0:
        raise SystemExit("[gpr] the " + channel + " floor must be > 0")
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

    # THE LABEL MATRIX IS SPARSE AND IS STORED THAT WAY. This used to be a dense
    # (reference x MNXR) float32 indicator -- 222,019 x ~13,112 = 10.84 GiB that
    # is 99.97% zeros -- built so the vote could be a matmul. The vote is a
    # weighted sum over at most K=30 neighbours' label lists, and those lists are
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
        for s in ref["mnxr_list"]
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
    print("[gpr] label pool: " + str(len(ref)) + " references, " + str(len(vocab))
          + " MNXR, " + str(int(lab_ptr[-1])) + " label edges ("
          + "%.1f" % (lab_idx.nbytes / 2**20) + " MiB sparse vs "
          + "%.2f" % (len(ref) * len(vocab) * 4 / 2**30) + " GiB dense)", flush=True)

    q_orf, q_emb = _load_query(parquet, index_csv)
    q_emb = _norm(q_emb)
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
            # Ragged gather of the K neighbours' label slices, then one bincount
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
            best = int(np.argmax(sim[bi, nn]))
            donor = ref_orf[nn[best]]
            qid = q_orf[s+bi]
            for j in np.nonzero(votes >= floor)[0]:
                # intermediate_id names the DONOR neighbour, not a projected
                # intermediate: label transfer IS the projection, so there is no
                # KO or EC in between. `projection_via` says so.
                rows.append((qid, vocab_arr[uniq[j]], donor,
                             float(min(votes[j], 1.0))))
    df = pd.DataFrame(rows, columns=["orf", "mnxr", "intermediate_id", "raw_score"])
    # The pool is the bridge's `reviewed` cut by construction (see
    # compile/reference_label_pool.py), so every transferred label inherits it.
    df["evidence_quality"] = "reviewed"
    return finish(df, channel, "embedding_knn")

def bridge_slice(path, src, name, wanted):
    """One id space of the bridge, cut down to the ids this shard actually hit.

    The bridge is 30.5 million rows over three id spaces and the read used to be
    the whole thing, into pandas, in every task -- gigabytes of object-dtype
    strings, ~1000 times, to keep a fraction of a percent of them. Both cuts
    happen in arrow, where a string column is dictionary-backed and a filter is a
    kernel rather than a Python loop; only the surviving rows are ever handed to
    pandas.

    `wanted` is the lane's own parsed ids, so the result is bounded by the SHARD.
    That is the whole point: the reference does not grow with the corpus and the
    per-task cost should not either.
    """
    if len(wanted) == 0:
        return pd.DataFrame(columns=[name, "mnxr", "evidence_quality"])
    t = pq.read_table(path, columns=["id", "id_source", "mnxr", "evidence_quality"],
                      filters=[("id_source", "==", src)])
    t = t.filter(pc.is_in(t.column("id"), value_set=pa.array(sorted(set(wanted)))))
    df = t.select(["id", "mnxr", "evidence_quality"]).to_pandas().drop_duplicates()
    return df.rename(columns={"id": name})

def main():
    ids = orf_ids("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/_metasmith/task/data/o1CrMPhlIMSn/CP193896.1.faa")
    if not ids:
        raise SystemExit("[gpr] the input ORF FASTA has no records")
    # Parse every lane FIRST, so the bridge read below knows which ids matter.
    kof = parse_kofam("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/results/7_annotation-kofamscan_results/1-1-1.LmJeSgsiB9DGe6Jg-bE0uTgBK.csv")
    cln = parse_clean("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/results/2_annotation-clean_predictions/1-1-1.rAGTmpWsJlwFJrcm-i8IGm53H.tsv")
    uni = parse_uniref("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/results/6_annotation-diamond_uniref50_results/1-1-1.K1mjYu3MmIvi8Lj3-gA4taOg9.tsv")
    frames = [
        lane_kofam(kof, bridge_slice("/arc/project/st-shallam-1/fabfos_refs/processed/mnxr_lookup/mnxr_lookup.parquet", "ko", "ko", kof["ko"].unique())),
        lane_clean(cln, bridge_slice("/arc/project/st-shallam-1/fabfos_refs/processed/mnxr_lookup/mnxr_lookup.parquet", "ec", "ec", cln["ec"].unique())),
        lane_uniref(uni, bridge_slice("/arc/project/st-shallam-1/fabfos_refs/processed/mnxr_lookup/mnxr_lookup.parquet", "uniprot", "uniprot_accession",
                                      uni["uniprot_accession"].unique())
                         .assign(dr_source="rhea")),
        lane_embed("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/results/3_annotation-proteinbert_embeddings_chunk/1-1-1.ZjTacq4lQKhxlBzn-8YkSc5PN.parquet", "/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/eUzXfepL/results/3_annotation-proteinbert_index_chunk/1-1-1.ZjTacq4lQKhxlBzn-fRy8YUSc.csv", "/arc/project/st-shallam-1/fabfos_refs/processed/reference_label_pool/pool", "emb_pbert.npy", "pbert", PBERT_FLOOR),
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
    gpr.to_parquet("/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/routeB/gpr_denovo.parquet", index=False)
    print("[gpr_4lane] wrote " + str(len(gpr)) + " rows -> /scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/routeB/gpr_denovo.parquet", flush=True)

main()