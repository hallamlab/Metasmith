"""Annotation table: every annotation lane's calls on one ORF set, in one long table.

One row per (lane, ORF or contig feature, term). Each lane is read as its tool wrote it and
nothing is abstained here: a cut-off belongs to the consumer that needs it, so one table can
serve every subset of lanes. `gpr_table.py` is the reaction consumer. The one lane that is
computed rather than read is ProteinBERT's, whose calls do not exist until the kNN vote
against the labelled landmarks makes them.

    --lane NAME PATH [PATH ...]   once per lane; LANES lists the names and their files
"""
import json
import os
import re
import sys


def _args():
    import argparse
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--orfs", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--lane", nargs="+", action="append", default=[], metavar="NAME PATH")
    p.add_argument("--landmarks", help="ref::label_transfer_landmarks; the pbert lane needs it")
    p.add_argument("--threads", type=int, default=1)
    return p.parse_args()


# BEFORE numpy. Its BLAS reads these once, at import, and the dominant arithmetic here is
# the pbert query-vs-landmark similarity matmul -- on the order of 2.3e13 operations for a
# 100,000-ORF shard, a couple of minutes across 16 cores and closer to forty
# single-threaded. Declared from the allocation the scheduler granted, with the
# transform's own cpus as the floor.
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


ANNOTATION_COLS = [
    "source", "orf", "contig", "start", "end",
    "lane", "namespace", "term", "description",
    "score", "score_kind", "evalue", "via", "provenance",
]


def load_evidence(ev_lib):
    sys.path.insert(0, os.path.dirname(str(ev_lib)))
    import fabfos_evidence
    return fabfos_evidence


# `[gene=thrA]`, as NCBI writes it in a `_protein.faa` header. A prodigal contig has no
# such tag and gets None, which is the honest answer for an ORF on an unnamed contig.
_GENE_TAG = re.compile(r"\[gene=([^\]]+)\]")


def orf_ids(fasta):
    out, genes = [], {}
    with open(fasta) as fh:
        for line in fh:
            if not line.startswith(">"):
                continue
            oid = line[1:].split()[0]
            out.append(oid)
            m = _GENE_TAG.search(line)
            genes[oid] = m.group(1) if m else None
    return out, genes


def _refuse_header(got, want, product, writer):
    raise SystemExit(
        "[annotation] " + product + " header is not the " + str(len(want)) + " columns "
        "this lane parses: got " + repr(list(got)) + ", want " + repr(list(want)) + ". "
        + writer + " writes the header, so a drift here silently renames every column "
        "and empties the lane")


def _pick(df, product, *candidates):
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise SystemExit("[annotation] " + product + " carries none of " + repr(candidates)
                     + "; its columns are " + repr(list(df.columns)))


def _split_terms(values, sep):
    return [t.strip() for t in str(values).split(sep)
            if t.strip() and t.strip() not in ("-", "nan")]


def _read_tsv_headered(path, header_prefix):
    # The tool writes comment lines first; the header is the one that starts with
    # `header_prefix`, and every later `#` line is a comment or a footer.
    header, rows = None, []
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if header is None and line.startswith(header_prefix):
                header = line.lstrip("#").split("\t")
            elif line and not line.startswith("#") and header is not None:
                rows.append(line.split("\t"))
    if header is None:
        return None
    return pd.DataFrame([r + [""] * (len(header) - len(r)) for r in rows],
                        columns=header, dtype=str)


# ---- kofam: kofamscan_results = gene_name,KO,thrshld,score,E-value,best
_KOFAM_COLS = ["gene_name", "KO", "thrshld", "score", "E-value", "best"]


def parse_kofam(results, descriptions=None):
    df = pd.read_csv(results, dtype=str)
    if list(df.columns) != _KOFAM_COLS:
        _refuse_header(df.columns, _KOFAM_COLS, "kofamscan_results", "kofamscan")
    out = pd.DataFrame({
        "orf": df["gene_name"], "namespace": "ko", "term": df["KO"],
        # THE RATIO, NOT THE BITSCORE. A bitscore is a length-and-family quantity, and the
        # family's own threshold is the only thing that makes two of them comparable --
        # so the calibration is fitted on score/thrshld and the lane hands it the same.
        # A row kofamscan kept with no threshold gets NaN here, not a made-up ratio.
        "score": (pd.to_numeric(df["score"], errors="coerce")
                  / pd.to_numeric(df["thrshld"], errors="coerce")),
        "score_kind": "kofam_score_over_threshold",
        "evalue": pd.to_numeric(df["E-value"], errors="coerce"),
    })
    if descriptions:
        d = pd.read_csv(descriptions, dtype=str).fillna("")
        if list(d.columns) != ["KO", "definition"]:
            _refuse_header(d.columns, ["KO", "definition"], "kofamscan_descriptions",
                           "kofamscan")
        out["description"] = out["term"].map(dict(zip(d["KO"], d["definition"])))
    return out


# ---- CLEAN: clean_predictions = Query ID \t Predicted EC number \t clean_score
#
# `clean_score` IS A CONFIDENCE -- higher is better, bounded by 1 -- not the raw maxsep
# distance it was once taken for. Every call is kept; CLEAN never declines, so the
# abstain that makes its coverage mean anything is the consumer's.
_CLEAN_COLS = ["Query ID", "Predicted EC number", "clean_score"]


def parse_clean(path):
    df = pd.read_csv(path, sep="\t", dtype=str)
    if list(df.columns) != _CLEAN_COLS:
        raise SystemExit(
            "[annotation] clean_predictions header is not the 3 columns this lane parses: "
            "got " + repr(list(df.columns)) + ". CLEAN's wrapper writes the header, "
            "so a drift here silently renames every column and empties the lane")
    return pd.DataFrame({
        "orf": df["Query ID"], "namespace": "ec", "term": df["Predicted EC number"],
        "score": pd.to_numeric(df["clean_score"], errors="coerce"),
        "score_kind": "clean_confidence",
    })


# ---- DIAMOND vs a protein database (UniRef50/90, Swiss-Prot, MetaCyc): headered BLAST6 +
# bsr, subject titles in the sibling table. diamond_uniref50 keeps the cluster id at the head
# of its titles, which _clean_stitle drops; lib::diamond_bsr.py writes titles without it.
def _parse_bsr_hits(results, descriptions, namespace, product):
    fe = EV
    df = pd.read_csv(results, sep="\t", dtype=str)
    if list(df.columns) != fe._BLAST6_BSR_COLS:
        _refuse_header(df.columns, fe._BLAST6_BSR_COLS, product, product.replace("_results", ""))
    titles = fe.read_uniref50_titles(descriptions)
    return pd.DataFrame({
        "orf": df["qseqid"], "start": df["qstart"], "end": df["qend"],
        "namespace": namespace, "term": df["sseqid"],
        "description": df["sseqid"].map(titles).fillna("").apply(fe._clean_stitle),
        "score": pd.to_numeric(df["bsr"], errors="coerce"), "score_kind": "bsr",
        "evalue": pd.to_numeric(df["evalue"], errors="coerce"),
    })


def _bsr_lane(namespace):
    product = "diamond_" + namespace + "_results"
    return lambda results, descriptions=None: _parse_bsr_hits(results, descriptions, namespace, product)


# ---- DIAMOND against a gene database, headerless outfmt 6. The four protein-resistome
# lanes add `stitle` as a 13th column; CarveMe's BiGG search writes the plain 12.
_BLAST6 = ["qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
           "qstart", "qend", "sstart", "send", "evalue", "bitscore"]


def _parse_diamond(path, namespace, with_title):
    names = _BLAST6 + (["stitle"] if with_title else [])
    if os.path.getsize(path) == 0:
        return pd.DataFrame(columns=["orf", "term"])
    df = pd.read_csv(path, sep="\t", header=None, dtype=str, quoting=3)
    if df.shape[1] != len(names):
        raise SystemExit(
            "[annotation] " + path + " has " + str(df.shape[1]) + " columns; the "
            + namespace + " lane reads DIAMOND outfmt 6 with " + str(len(names)))
    df.columns = names
    return pd.DataFrame({
        "orf": df["qseqid"], "start": df["qstart"], "end": df["qend"],
        "namespace": namespace, "term": df["sseqid"],
        "description": df["stitle"] if with_title else None,
        "score": pd.to_numeric(df["bitscore"], errors="coerce"), "score_kind": "bitscore",
        "evalue": pd.to_numeric(df["evalue"], errors="coerce"),
    })


def _diamond_lane(namespace, with_title):
    return lambda path: _parse_diamond(path, namespace, with_title)


# ---- hmmsearch --domtblout (dbCAN, VOGDB): one row per ORF x model. hmmsearch's
# `target` is the ORF and its `query` the model. The score is the model coverage of the
# best-covering domain, (hmm to - hmm from) / model length, because that and the
# full-sequence E-value are the two things DRAM's significance test reads; the bitscore is
# left in the domtblout.
def _parse_domtblout(path, namespace, descriptions, strip=""):
    rows = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            f = line.split()
            if len(f) < 22:
                raise SystemExit("[annotation] " + path + " has a " + str(len(f))
                                 + "-field row; the " + namespace + " lane reads hmmsearch "
                                 "--domtblout, which has 22 before the description")
            hmm_len = float(f[5])
            rows.append((f[0], f[3].removesuffix(strip), float(f[6]),
                         (int(f[16]) - int(f[15])) / hmm_len if hmm_len else float("nan"),
                         int(f[19]), int(f[20])))
    df = pd.DataFrame(rows, columns=["orf", "term", "evalue", "score", "start", "end"])
    df = (df.sort_values("score", ascending=False, kind="stable")
            .drop_duplicates(["orf", "term"]))
    names = {}
    if descriptions:
        d = pd.read_csv(descriptions, sep="\t", dtype=str).fillna("")
        names = dict(zip(d["term"], d["description"]))
    return df.assign(namespace=namespace, score_kind="hmm_coverage",
                     description=df["term"].map(names))


def parse_dbcan(results, descriptions=None):
    return _parse_domtblout(results, "cazy", descriptions, strip=".hmm")


def parse_vogdb(results, descriptions=None):
    return _parse_domtblout(results, "vogdb", descriptions)


# ---- PathoFact 2.0's toxin and virulence-factor HMMs: hmmsearch --tblout, one row per ORF
# x CDD model it reported at bitscore 40. The bitscore is the score: PathoFact reports it
# and cuts on nothing else.
def _parse_tblout(path, namespace, names):
    rows = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            f = line.split()
            rows.append((f[0], f[2], float(f[5]), float(f[4])))
    df = pd.DataFrame(rows, columns=["orf", "term", "score", "evalue"])
    return df.assign(namespace=namespace, score_kind="bitscore",
                     description=df["term"].map(names))


def parse_pathofact_hmm(tox, vf, descriptions=None):
    names = {}
    if descriptions:
        d = pd.read_csv(descriptions, sep="\t", dtype=str).fillna("")
        names = dict(zip(d["term"], d["description"]))
    return pd.concat([_parse_tblout(tox, "pathofact.tox_hmm", names),
                      _parse_tblout(vf, "pathofact.vf_hmm", names)], ignore_index=True)


# ---- PathoFact 2.0's two classifiers: Sequence, Prediction, Probability for every ORF.
# The term is the class the classifier called, TOX or non-TOX, VF or non-VF; the
# probability is the score, and PathoFact's own 0.8 (toxin) and 0.9 (VF) are the
# consumer's. The call is kept beside the score because the toxin script rounds the
# probability to two decimals, so 0.50 can be either class.
_PATHOFACT_ML_COLS = ["Sequence", "Prediction", "Probability"]


def _parse_pathofact_ml(path, label):
    df = pd.read_csv(path, sep="\t", dtype=str)
    if list(df.columns) != _PATHOFACT_ML_COLS:
        _refuse_header(df.columns, _PATHOFACT_ML_COLS, "pathofact_" + label + "_ml",
                       "PathoFact's " + label + " classifier")
    return pd.DataFrame({
        "orf": df["Sequence"], "namespace": "pathofact." + label + "_ml",
        "term": (pd.to_numeric(df["Prediction"], errors="coerce") == 1).map(
            {True: label.upper(), False: "non-" + label.upper()}),
        "score": pd.to_numeric(df["Probability"], errors="coerce"),
        "score_kind": "classifier_probability",
    })


def parse_pathofact_ml(tox, vf):
    return pd.concat([_parse_pathofact_ml(tox, "tox"), _parse_pathofact_ml(vf, "vf")],
                     ignore_index=True)


# ---- DeepARG .mapping.ARG: `#ARG`-headed. The term is DeepARG's database gene, which is
# what argNorm maps to an ARO; the ARG name and its class are the description.
def parse_deeparg(path):
    df = pd.read_csv(path, sep="\t", dtype=str)
    product = "deeparg_results"
    arg = _pick(df, product, "#ARG")
    return pd.DataFrame({
        "orf": _pick(df, product, "read_id"), "namespace": "deeparg",
        "term": _pick(df, product, "best-hit"),
        "description": arg + " (" + _pick(df, product, "predicted_ARG-class") + ")",
        "score": pd.to_numeric(_pick(df, product, "probability"), errors="coerce"),
        "score_kind": "deeparg_probability",
        "evalue": pd.to_numeric(_pick(df, product, "alignment-evalue"), errors="coerce"),
    })


# ---- DeepEC: two columns, `EC:`-prefixed calls, no score. A score-less tool asserts
# presence; the consumer decides what that is worth.
def parse_deepec(path):
    rows = []
    with open(path) as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            gene, ec = parts[0].strip(), parts[1].strip()
            if not ec or ec.lower().startswith("predicted"):
                continue
            rows.append((gene, re.sub(r"^EC:", "", ec, flags=re.I)))
    df = pd.DataFrame(rows, columns=["orf", "term"])
    df["namespace"] = "ec"
    df["score_kind"] = "presence"
    return df


# ---- eggNOG-mapper: `#query`-headed emapper.annotations. One row per term in each
# column below; a multi-valued cell is comma-separated and `-` means none.
_EGGNOG_TERMS = {
    "seed_ortholog": "eggnog.seed",
    "KEGG_ko": "ko",
    "EC": "ec",
    "KEGG_Reaction": "kegg.reaction",
    "BiGG_Reaction": "bigg.reaction",
    "PFAMs": "pfam",
    "GOs": "go",
}


def parse_eggnog(path):
    df = _read_tsv_headered(path, "#query")
    if df is None:
        raise SystemExit("[annotation] " + path + " has no `#query` header line; "
                         "eggnog-mapper writes one above its first row")
    missing = [c for c in ["query", "evalue", "score", "Description", *_EGGNOG_TERMS]
               if c not in df.columns]
    if missing:
        raise SystemExit("[annotation] eggnog_results lacks " + repr(missing)
                         + "; its columns are " + repr(list(df.columns)))
    frames = []
    for col, ns in _EGGNOG_TERMS.items():
        exploded = df.assign(term=df[col].map(lambda v: _split_terms(v, ","))).explode("term")
        exploded = exploded[exploded["term"].notna()]
        if ns == "ko":
            exploded["term"] = exploded["term"].str.replace(r"^ko:", "", regex=True)
        frames.append(pd.DataFrame({
            "orf": exploded["query"], "namespace": ns, "term": exploded["term"],
            "description": exploded["Description"] if col == "seed_ortholog" else None,
            "score": pd.to_numeric(exploded["score"], errors="coerce"),
            "score_kind": "bitscore",
            "evalue": pd.to_numeric(exploded["evalue"], errors="coerce"),
        }))
    return pd.concat(frames, ignore_index=True)


# ---- InterProScan: protein_match rows of its GFF3, as CSV; signature descriptions beside.
# The member-database signature, its InterPro entry and its GO terms are three rows.
_IPS_COLS = ["orf", "start", "stop", "score", "database", "Name", "Dbxref", "Ontology_term"]


def parse_interproscan(results, descriptions=None):
    df = pd.read_csv(results, dtype=str).fillna("")
    if list(df.columns) != _IPS_COLS:
        _refuse_header(df.columns, _IPS_COLS, "interproscan_results", "interproscan")
    names = {}
    if descriptions:
        d = pd.read_csv(descriptions, dtype=str).fillna("")
        names = dict(zip(d["Name"], d["description"]))
    base = dict(orf=df["orf"], start=df["start"], end=df["stop"],
                evalue=pd.to_numeric(df["score"], errors="coerce"))
    frames = [pd.DataFrame({**base, "namespace": df["database"].str.lower(),
                            "term": df["Name"], "description": df["Name"].map(names)})]
    for col, ns, sep in (("Dbxref", "interpro", ","), ("Ontology_term", "go", "|")):
        x = pd.DataFrame(base).assign(term=df[col].map(lambda v: _split_terms(v, sep)))
        x = x.explode("term")
        frames.append(x[x["term"].notna()].assign(namespace=ns))
    return pd.concat(frames, ignore_index=True)


# ---- RGI: rgi_out.txt, headered. `ARO` is the accession, `Best_Hit_ARO` its name.
def parse_rgi(path):
    df = pd.read_csv(path, sep="\t", dtype=str, quoting=3).fillna("")
    product = "rgi_results"
    return pd.DataFrame({
        "orf": _pick(df, product, "ORF_ID").str.split().str[0],
        "namespace": "aro",
        "term": _pick(df, product, "ARO"),
        "description": _pick(df, product, "Best_Hit_ARO"),
        "score": pd.to_numeric(_pick(df, product, "Best_Hit_Bitscore"), errors="coerce"),
        "score_kind": "bitscore",
    })


# ---- AMRFinderPlus: headered TSV. 4.x renamed the 3.x columns, so either spelling is read.
def parse_amrfinderplus(path):
    df = pd.read_csv(path, sep="\t", dtype=str, quoting=3).fillna("")
    product = "amrfinderplus_results"
    return pd.DataFrame({
        "orf": _pick(df, product, "Protein id", "Protein identifier"),
        "namespace": "amrfinderplus",
        "term": _pick(df, product, "Element symbol", "Gene symbol"),
        "description": _pick(df, product, "Element name", "Sequence name"),
        "score": pd.to_numeric(_pick(df, product, "% Identity to reference",
                                     "% Identity to reference sequence"), errors="coerce"),
        "score_kind": "percent_identity",
        "via": _pick(df, product, "Closest reference accession",
                     "Accession of closest sequence"),
    })


# ---- Bakta, non-coding only: `#Sequence Id`-headed TSV. Contig features, not ORFs.
def parse_bakta_nc(path):
    df = _read_tsv_headered(path, "#Sequence Id")
    if df is None:
        raise SystemExit("[annotation] " + path + " has no `#Sequence Id` header line")
    product = "bakta_tsv"
    gene = _pick(df, product, "Gene").fillna("")
    prod = _pick(df, product, "Product").fillna("")
    return pd.DataFrame({
        "contig": _pick(df, product, "Sequence Id"),
        "start": _pick(df, product, "Start"), "end": _pick(df, product, "Stop"),
        "namespace": "bakta." + _pick(df, product, "Type"),
        "term": gene.where(gene != "", prod),
        "description": prod,
    })


# ---- antiSMASH: one row per product of each region. antiSMASH calls its own genes, so
# its CDS ids name nothing in the ORF FASTA; a region is named by contig and span instead.
def parse_antismash(path):
    with open(path) as fh:
        doc = json.load(fh)
    if "records" not in doc:
        raise SystemExit("[annotation] " + path + " has no `records`; it is not an "
                         "antiSMASH results JSON")
    rows = []
    for rec in doc["records"]:
        for i, area in enumerate(rec.get("areas", []), start=1):
            for prod in area.get("products", []):
                rows.append((rec["id"], area["start"], area["end"], prod,
                             "region " + str(i)))
    return pd.DataFrame(rows, columns=["contig", "start", "end", "term", "description"]
                        ).assign(namespace="antismash.product")


# ---- ProteinBERT: numpy kNN label transfer against the labelled landmarks, with the
# distance quota and the ORF-level abstain.
# An embedding table's ids and its vectors, taken from the SAME rows. What is checked is
# that the dim columns run contiguously from 0, because a table missing `dim_7` still
# stacks into a matrix of the wrong width rather than failing.
def _read_embeddings(path, id_col):
    df = EV.read_parquet(path)
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
    # setting now that `nn_min` decides which neighbours are admitted at all.
    if floor < 0:
        raise SystemExit("[gpr] the " + channel + " vote floor must be >= 0")
    if not 0.0 <= tau <= 1.0:
        raise SystemExit("[gpr] the " + channel + " tau is a fraction of this ORF's "
                         "own best cosine and must lie in [0, 1]")
    if k_max < 1:
        raise SystemExit("[gpr] the " + channel + " k_max must be at least 1")
    if landmark_dir is None:
        raise SystemExit("[gpr] the " + channel + " lane votes against landmarks and "
                         "none were given (--landmarks)")
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

    # THE LABEL MATRIX IS SPARSE AND IS STORED THAT WAY. A dense (reference x MNXR)
    # float32 indicator is 222,019 x ~13,112 = 10.84 GiB at the pinned pool and 99.97%
    # zeros, and the vote is a weighted sum over at most PBERT_K_MAX neighbours' short
    # label lists, so the matrix bought nothing but memory.
    #
    # Stored instead as CSR-shaped flat arrays: `lab_ptr` bounds each reference's
    # slice of `lab_idx`, which holds its labels' vocabulary indices. Each reference's
    # list is DEDUPLICATED, because the dense form wrote 1.0 idempotently and an
    # accumulation would otherwise count a repeated label twice -- the one place the
    # two forms could disagree.
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
            # THE ORF-LEVEL ABSTAIN. `floor` gates the VOTE, and the vote is normalised
            # within whatever neighbours are admitted, so it measures agreement and not
            # proximity -- thirty neighbours at cosine 0.15 that agree score 1.0.
            # Nothing in the landmark set being close enough for a transferred label to
            # mean anything is a different claim, and it is made here.
            if cs[0] <= 0 or cs[0] < nn_min:
                n_refused += 1
                continue
            # THE QUOTA. Absolute (`nn_min`) and relative (`tau` of this ORF's own best
            # match) at once: the first says a neighbour is close enough to speak at
            # all, the second stops a mediocre one voting at full weight beside a
            # near-perfect one.
            keep = cs >= max(nn_min, tau * cs[0])
            nn, vals = cand[keep], cs[keep]
            n_admitted += len(nn)
            n_voting += 1
            tot = vals.sum()
            if tot <= 0:
                continue
            w = vals / tot
            # Ragged gather of the admitted neighbours' label slices, then one bincount
            # over only the labels they actually carry. Exactly `w @ L[nn]` restricted
            # to its non-zero support, in ascending vocabulary order.
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
                rows.append((qid, vocab_arr[uniq[j]], donor,
                             float(min(votes[j], 1.0))))
    print("[gpr] " + channel + ": " + str(len(q_emb)) + " ORFs, " + str(n_refused)
          + " with no landmark at cosine " + ("%.4f" % nn_min) + " or better (abstain), "
          + str(n_voting) + " voting on "
          + ("%.1f" % (n_admitted / n_voting) if n_voting else "0")
          + " admitted neighbours on average, " + str(len(rows)) + " calls",
          flush=True)
    return pd.DataFrame(rows, columns=["orf", "term", "via", "score"])


def parse_pbert(embeddings):
    fe = EV
    df = lane_embed(embeddings, LANDMARKS, "pbert", fe.PBERT_FLOOR, fe.PBERT_NN_MIN,
                    fe.PBERT_TAU, fe.PBERT_K_MAX)
    # `via` names the DONOR neighbour: label transfer IS the projection, so there is no
    # KO or EC in between.
    return df.assign(namespace="mnxr", score_kind="knn_vote")


# lane -> (parser, the product type it reads, whether its rows name an ORF of the FASTA).
# A lane taking a second file (descriptions) takes it as its second PATH.
LANES = {
    "kofam":         (parse_kofam, "annotation::kofamscan_results", True),
    "clean":         (parse_clean, "annotation::clean_predictions", True),
    "uniref50":      (_bsr_lane("uniref50"), "annotation::diamond_uniref50_results", True),
    "uniref90":      (_bsr_lane("uniref90"), "annotation::diamond_uniref90_results", True),
    "swissprot":     (_bsr_lane("swissprot"), "annotation::diamond_swissprot_results", True),
    "metacyc":       (_bsr_lane("metacyc"), "annotation::diamond_metacyc_results", True),
    "pbert":         (parse_pbert, "annotation::proteinbert_embeddings", True),
    "deepec":        (parse_deepec, "annotation::deepec_predictions", True),
    "eggnog":        (parse_eggnog, "annotation::eggnog_results", True),
    "interproscan":  (parse_interproscan, "annotation::interproscan_results", True),
    "mobileog":      (_diamond_lane("mobileog", True), "annotation::mobileog_diamond_results", True),
    "megares":       (_diamond_lane("megares", True), "annotation::megares_diamond_results", True),
    "bacmet":        (_diamond_lane("bacmet", True), "annotation::bacmet_diamond_results", True),
    "vfdb":          (_diamond_lane("vfdb", True), "annotation::vfdb_diamond_results", True),
    "bigg":          (_diamond_lane("bigg.gene", False), "annotation::bigg_diamond_hits", True),
    "dbcan":         (parse_dbcan, "annotation::dbcan_hmmsearch_results", True),
    "merops":        (_diamond_lane("merops", True), "annotation::merops_diamond_results", True),
    "vogdb":         (parse_vogdb, "annotation::vogdb_hmmsearch_results", True),
    "refseq_viral":  (_diamond_lane("refseq_viral", True), "annotation::refseq_viral_diamond_results", True),
    "rgi":           (parse_rgi, "annotation::rgi_results", True),
    "amrfinderplus": (parse_amrfinderplus, "annotation::amrfinderplus_results", True),
    "pathofact_hmm": (parse_pathofact_hmm, "annotation::pathofact_tox_hmm", True),
    "pathofact_ml":  (parse_pathofact_ml, "annotation::pathofact_tox_ml", True),
    "deeparg":       (parse_deeparg, "annotation::deeparg_results", True),
    "bakta_nc":      (parse_bakta_nc, "annotation::bakta_tsv", False),
    "antismash":     (parse_antismash, "annotation::antismash_json", False),
}


def _finish(df, lane, source, provenance):
    df = df.copy()
    for c in ANNOTATION_COLS:
        if c not in df.columns:
            df[c] = None
    df["source"] = source
    df["lane"] = lane
    df["provenance"] = provenance
    # Coordinates are floats so a missing one is NaN: polars, the engine inside the
    # container, has no column type for pandas' nullable-integer NA.
    for c in ("start", "end", "score", "evalue"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    for c in ("orf", "contig", "namespace", "term", "description", "score_kind", "via"):
        df[c] = df[c].astype("object").where(df[c].notna(), None)
    return df[ANNOTATION_COLS]


# The batch splitter that feeds the DIAMOND, RGI and AMRFinderPlus lanes renames every
# record `<stem>~<id>`, and no merge takes the prefix off. The id after it is the FASTA's.
def _unprefix(orfs, ids):
    tail = orfs.str.split("~", n=1).str[-1]
    return orfs.where(orfs.isin(ids) | ~tail.isin(ids), tail)


def build(lanes, fasta, source):
    ids, _ = orf_ids(fasta)
    if not ids:
        raise SystemExit("[annotation] the input ORF FASTA has no records")
    id_set = set(ids)
    seen = [lane[0] for lane in lanes]
    if len(set(seen)) != len(seen):
        raise SystemExit("[annotation] a lane was given twice: " + repr(seen))
    frames = []
    for name, *paths in lanes:
        if name not in LANES:
            raise SystemExit("[annotation] unknown lane " + repr(name) + "; known: "
                             + ", ".join(LANES))
        parse, product, orf_keyed = LANES[name]
        df = _finish(parse(*paths), name, source, product)
        if orf_keyed:
            df["orf"] = _unprefix(df["orf"].astype(str), id_set)
            # REFUSE THE STRAYS; DO NOT QUIETLY DROP THEM. A lane paired with the wrong
            # shard would otherwise be emptied here and surface downstream as "lane X
            # contributed 0 rows", which reads as the lane failing rather than the lane
            # being mispaired.
            stray = df.loc[~df["orf"].isin(id_set), "orf"]
            if len(stray):
                raise SystemExit(
                    "[annotation] the " + name + " lane contributed " + str(len(stray))
                    + " of " + str(len(df)) + " rows for ORF ids that are not in this "
                    "shard's FASTA, e.g. " + repr(sorted(set(stray))[:3]) + ". That "
                    "lane's product belongs to a DIFFERENT shard -- check the run's "
                    "lineage_of_given.json before spending any more queue time")
        print("[annotation]   " + "%-13s" % name + " " + "%9d" % len(df) + " rows  "
              + "%7d" % df["orf"].nunique() + " ORFs  " + "%7d" % df["term"].nunique()
              + " terms", flush=True)
        frames.append(df)
    table = pd.concat(frames, ignore_index=True).drop_duplicates(ignore_index=True)
    if len(table) == 0:
        raise SystemExit("[annotation] the table is empty; every lane parsed to nothing, "
                         "which is a staging failure rather than a finding")
    return table


if __name__ == "__main__":
    EV = load_evidence(A.ev_lib)
    LANDMARKS = A.landmarks
    t = build(A.lane, A.orfs, str(A.source))
    EV.write_parquet(t, str(A.out))
    print("[annotation] wrote " + str(len(t)) + " rows over " + str(t["lane"].nunique())
          + " lanes -> " + str(A.out), flush=True)
