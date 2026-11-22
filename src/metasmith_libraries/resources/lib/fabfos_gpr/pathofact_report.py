"""Render PathoFact 2.0's reports for one contig sample from the library's own lanes.

PathoFact's report rules (src/reports.py, reportwithMGE.py, Create_Summary.py,
combined_summaries.py and get_bgc.py at commit bd5d527), applied to: the annotation table's
pathofact_hmm and pathofact_ml lanes for its toxin and virulence-factor arms; DeepARG and
AMRFinderPlus after argNorm, and RGI, for its AMR arm; geNomad's gene tables for its MGE
markers; antiSMASH's CDS gene kinds for its BGC column. Rows are dereplicated proteins, as
PathoFact reports the representatives of its 100%-identity clusters.

    --signalp PREDICTIONS adds PathoFact's SignalP column to a finished combined report.
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dram_annotations import _PRODIGAL, fasta_records  # noqa: E402

# Config.yaml threshold_vf and threshold_tox.
VF_THRESHOLD = 0.9
TOX_THRESHOLD = 0.8

# geNomad's --conservative preset (cli.py use_preset), which PathoFact runs and the
# library's geNomad does not; every bound is at least as strict as the default's, so
# the conservative calls are a subset of the default run's and can be cut from it.
CONSERVATIVE = dict(min_score=0.8, min_marker_enrichment=1.5, min_hallmarks=1, max_uscg=2)

# An ORF is a geNomad or antiSMASH gene when it lies on the same contig and strand and the
# two overlap over at least half the ORF. Both tools call their own genes with Prodigal
# variants, so a shared stop codon is the usual case and this is its tolerance.
MIN_GENE_OVERLAP = 0.5

COMBINED_COLS = [
    "Sample", "ProteinID",
    "Query_tox", "bit_score_tox", "Rfprediction_tox", "Rfprob_tox",
    "Query_vf", "bit_score_vf", "Rfprediction_vf", "Rfprob_vf",
    "DeepARG:ARO", "DeepARG:ARO_name", "DeepARG:probability", "DeepARG:Cut_Off",
    "RGI_Accession:ARO", "RGI:Cut_Off", "RGI:AMR_Gene_family",
    "amrfinder:ARO", "amrfinder:ARO_name", "amrfinder:Cut_Off",
    "SignalP",
    "Plasmid_marker", "Virus_marker", "BGC", "Antismash_Description",
    "Other_proteins_in_cluster",
]
AMR_COLS = COMBINED_COLS[10:20]
MGE_COLS = ["Plasmid_marker", "Virus_marker"]
NOHIT = "-"


def arm_cols(label):
    return ["Query_" + label, "bit_score_" + label, "Rfprediction_" + label, "Rfprob_" + label]


# PathoFact's own row order: the digits of the last three `_`/`.`-separated fields.
def order_key(name):
    n = re.sub(r"\.", "_", name).split("_")
    digits = lambda s: int(re.sub(r"[^\d]", "", s)) if re.sub(r"[^\d]", "", s) else 0
    return (digits(n[-3]) if len(n) > 2 else 0, digits(n[-2]) if len(n) > 1 else 0,
            digits(n[-1]))


def ordered(names):
    try:
        return sorted(names, key=order_key)
    except Exception:
        return sorted(names)


# ---- the proteins: 100%-identity clusters (mmseqs -c 1 --min-seq-id 1 --cov-mode 0 is
# equal sequences), the first in FASTA order standing for its cluster.
def proteins(orfs):
    by_seq, genes = defaultdict(list), {}
    for oid, header, seq in fasta_records(orfs):
        by_seq[seq.upper().replace("*", "")].append(oid)
        m = _PRODIGAL.search(header)
        if m:
            start, end, strand = (int(g) for g in m.groups())
            genes[oid] = (oid.rpartition("_")[0], start, end, strand)
    others = {ids[0]: ids[1:] for ids in by_seq.values()}
    return others, genes


def unprefix(ids, known):
    # The batch splitters name a record `<stem>~<id>` and no merge takes the prefix off.
    return [i if i in known or "~" not in i or i.split("~", 1)[1] not in known
            else i.split("~", 1)[1] for i in ids]


# ---- toxin and virulence-factor arms (reports.py parse_hmmer, parse_RF)
def arm(table, label, reps):
    hmm = table[(table["lane"] == "pathofact_hmm")
                & (table["namespace"] == "pathofact." + label + "_hmm")
                & table["orf"].isin(reps)]
    rows = {}
    for orf, df in hmm.groupby("orf", sort=False):
        df = df.sort_values("score", ascending=False, kind="stable")
        rows[orf] = {"Query_" + label: ",".join(df["term"]),
                     "bit_score_" + label: ",".join("%.1f" % s for s in df["score"])}
    ml = table[(table["lane"] == "pathofact_ml")
               & (table["namespace"] == "pathofact." + label + "_ml")
               & (table["term"] == label.upper()) & table["orf"].isin(reps)]
    for orf, prob in zip(ml["orf"], ml["score"]):
        # predict.py writes the toxin probability at two decimals and the VF script writes
        # it in full; Create_Summary compares the text it reads to the threshold.
        rows.setdefault(orf, {}).update({
            "Rfprediction_" + label: label.upper(),
            "Rfprob_" + label: "%.2f" % prob if label == "tox" else repr(float(prob))})
    return rows


# ---- AMR arm (reports.py parse_deeparg, parse_rgi, parse_amrfinder)
def _read(path):
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False, quoting=3)


def amr(deeparg, rgi, amrfinder, reps):
    rows = defaultdict(dict)
    d = _read(deeparg)
    for orf, aro, name, prob, cut in zip(unprefix(d["read_id"], reps), d["ARO"], d["ARO_name"],
                                         d["probability"], d["Cut_Off"]):
        if orf in reps:
            rows[orf].update(zip(AMR_COLS[0:4], (aro, name, str(round(float(prob), 2)), cut)))
    # PathoFact runs RGI without --include_loose; the library's lane includes Loose hits.
    r = _read(rgi)
    r = r[r["Cut_Off"] != "Loose"]
    for orf, aro, cut, family in zip(unprefix(r["ORF_ID"].str.split().str[0], reps),
                                     r["ARO"], r["Cut_Off"], r["AMR Gene Family"]):
        if orf in reps:
            rows[orf].update(zip(AMR_COLS[4:7], ("ARO:" + aro, cut, family)))
    # PathoFact runs AMRFinderPlus without --plus, which reports the core scope alone.
    a = _read(amrfinder)
    pid = "Protein id" if "Protein id" in a.columns else "Protein identifier"
    a = a[a["Scope"] == "core"]
    for orf, aro, name, cut in zip(unprefix(a[pid], reps), a["ARO"], a["ARO_name"],
                                   a["Cut_Off"]):
        if orf in reps:
            rows[orf].update(zip(AMR_COLS[7:10], (aro, name, cut)))
    return rows


# ---- MGE markers (reportwithMGE.py over geNomad --conservative)
def _gene_seq(gene):
    return gene.rsplit("_", 1)[0]


def mge_genes(summaries, gene_tables, score_col, contigs):
    genes = pd.concat([_read(p) for p in gene_tables], ignore_index=True)
    seqs = pd.concat([_read(p) for p in summaries], ignore_index=True)
    seq_of = genes["gene"].map(_gene_seq)
    n_uscg = seq_of[~genes["uscg"].isin(["NA", ""])].value_counts()
    c = CONSERVATIVE
    kept = seqs[(pd.to_numeric(seqs[score_col]) >= c["min_score"])
                & (pd.to_numeric(seqs["marker_enrichment"]) >= c["min_marker_enrichment"])
                & (pd.to_numeric(seqs["n_hallmarks"]) >= c["min_hallmarks"])
                & (seqs["seq_name"].map(n_uscg).fillna(0) <= c["max_uscg"])]
    genes = genes[seq_of.isin(set(kept["seq_name"]))]
    contig = unprefix([s.split("|")[0] for s in genes["gene"].map(_gene_seq)], contigs)
    return pd.DataFrame({
        "contig": contig, "start": pd.to_numeric(genes["start"]).values,
        "end": pd.to_numeric(genes["end"]).values,
        "strand": pd.to_numeric(genes["strand"]).values, "value": genes["marker"].values,
    })


def match_genes(orf_genes, features):
    # orf -> the value of the feature it is, by MIN_GENE_OVERLAP on contig and strand.
    if features.empty:
        return {}
    orfs = pd.DataFrame([(o, *g) for o, g in orf_genes.items()],
                        columns=["orf", "contig", "o_start", "o_end", "strand"])
    m = orfs.merge(features, on=["contig", "strand"])
    overlap = (m[["o_end", "end"]].min(axis=1) - m[["o_start", "start"]].max(axis=1) + 1)
    m = m.assign(overlap=overlap)
    m = m[m["overlap"] >= MIN_GENE_OVERLAP * (m["o_end"] - m["o_start"] + 1)]
    m = m.sort_values("overlap", ascending=False, kind="stable").drop_duplicates("orf")
    return dict(zip(m["orf"], m["value"]))


# ---- BGC column (get_bgc.py over regions.js): antiSMASH's CDS in a region whose gene
# kind is not `other`, described by its rule-based-clusters functions.
_LOCATION = re.compile(r"\[<?(\d+):>?(\d+)\]\(([+-])\)")
_RULE_FUNCTION = re.compile(r"^biosynthetic \(rule-based-clusters\) (.*?): (.*)$")


def bgc_genes(antismash_jsons, contigs):
    rows = []
    for path in antismash_jsons:
        with open(path) as fh:
            doc = json.load(fh)
        for rec in doc.get("records", []):
            areas = [(a["start"], a["end"]) for a in rec.get("areas", [])]
            contig = unprefix([rec["id"]], contigs)[0]
            for f in rec.get("features", []):
                kind = f.get("qualifiers", {}).get("gene_kind", [None])[0]
                spans = _LOCATION.findall(f.get("location", ""))
                if f.get("type") != "CDS" or not kind or "other" in kind or not spans:
                    continue
                start = min(int(s) for s, _, _ in spans)
                end = max(int(e) for _, e, _ in spans)
                if not any(a <= start and end <= b for a, b in areas):
                    continue
                desc = sorted({f"{m.group(1)}({m.group(2)})"
                               for g in f["qualifiers"].get("gene_functions", [])
                               if (m := _RULE_FUNCTION.match(g))})
                rows.append((contig, start + 1, end, 1 if spans[0][2] == "+" else -1,
                             (kind, ",".join(desc) or NOHIT)))
    return pd.DataFrame(rows, columns=["contig", "start", "end", "strand", "value"])


# ---- the tables
def _frame(rows, cols, sample, others, mge):
    names = ordered(rows)
    df = pd.DataFrame([[rows[n].get(c, NOHIT) for c in cols] for n in names], columns=cols)
    df.insert(0, "ProteinID", names)
    df.insert(0, "Sample", sample)
    if mge is not None:
        for col in MGE_COLS:
            df[col] = [mge[col].get(n, NOHIT) for n in names]
    df["Other_proteins_in_cluster"] = [",".join(others.get(n, [])) for n in names]
    return df


def split_by_probability(df, label, threshold):
    prob = pd.to_numeric(df["Rfprob_" + label].where(df["Rfprob_" + label] != NOHIT),
                         errors="coerce")
    high = prob >= threshold
    return df[high], df[~high]


def combine(paths, bgc, sample):
    # combined_summaries.py: the arm tables as pandas reads them back, first value per
    # protein in the order VF, toxin, AMR, BGC; a cell pandas reads as missing becomes `-`.
    frames = [pd.read_csv(p, sep="\t") for p in paths]
    frames.append(pd.DataFrame([(sample, orf, kind, desc) for orf, (kind, desc) in bgc.items()],
                               columns=["Sample", "ProteinID", "BGC", "Antismash_Description"]))
    df = pd.concat(frames, axis=0, ignore_index=True, sort=False)
    df = df.groupby("ProteinID", as_index=False).first()
    try:
        df = df.sort_values(by="ProteinID", key=lambda x: x.apply(order_key))
    except Exception:
        df = df.sort_values(by="ProteinID")
    df = df[[c for c in COMBINED_COLS if c in df.columns]].astype(object).fillna(NOHIT)
    count = (df != NOHIT).sum()
    df.loc["Count"] = count
    df.iloc[-1, 0] = "#Total"
    return df


def write(df, path):
    df.to_csv(path, sep="\t", index=False)


def build(a):
    sys.path.insert(0, os.path.dirname(a.ev_lib))
    import fabfos_evidence as ev

    others, orf_genes = proteins(a.orfs)
    reps = set(others)
    contigs = {g[0] for g in orf_genes.values()}
    table = ev.read_parquet(a.annotation_table, columns=["orf", "lane", "namespace", "term", "score"],
                            filters=[("lane", "in", ["pathofact_hmm", "pathofact_ml"])])
    rep_genes = {o: g for o, g in orf_genes.items() if o in reps}
    mge = {
        "Plasmid_marker": match_genes(rep_genes, mge_genes(
            a.plasmid_summary, a.plasmid_genes, "plasmid_score", contigs)),
        "Virus_marker": match_genes(rep_genes, mge_genes(
            a.virus_summary, a.virus_genes, "virus_score", contigs)),
    }

    paths = []
    for label, threshold, high_out, low_out in (
            ("vf", a.vf_threshold, a.out_vf, a.out_vf_ambiguous),
            ("tox", a.tox_threshold, a.out_tox, a.out_tox_ambiguous)):
        df = _frame(arm(table, label, reps), arm_cols(label), a.sample, others, mge)
        high, low = split_by_probability(df, label, threshold)
        write(high, high_out)
        write(low, low_out)
        paths.append(high_out)
        print("[pathofact] " + label + ": " + str(len(high)) + " at probability "
              + str(threshold) + " or more, " + str(len(low)) + " ambiguous", flush=True)
    df = _frame(amr(a.deeparg, a.rgi, a.amrfinderplus, reps), AMR_COLS, a.sample, others, mge)
    write(df, a.out_amr)
    paths.append(a.out_amr)
    print("[pathofact] amr: " + str(len(df)) + " proteins", flush=True)

    bgc = match_genes(orf_genes, bgc_genes(a.antismash_json, contigs))
    combined = combine(paths, bgc, a.sample)
    write(combined, a.out_combined)
    print("[pathofact] combined: " + str(len(combined) - 1) + " proteins; "
          + str(len(mge["Plasmid_marker"])) + " representatives on geNomad plasmids, "
          + str(len(mge["Virus_marker"])) + " on viruses, " + str(len(bgc))
          + " ORFs are antiSMASH genes", flush=True)


# ---- SignalP (reports.py parse_sp, over SignalP 6's output.gff3)
# The gff3 names two types and keeps Tat and pilin in its Note column, so a prediction
# maps to one of the two; NO_SP (OTHER in older builds) is no row at all.
SIGNALP_GFF_TYPE = {"SP": "signal_peptide", "LIPO": "lipoprotein_signal_peptide",
                    "TAT": "signal_peptide", "TATLIPO": "lipoprotein_signal_peptide",
                    "PILIN": "signal_peptide"}


def read_signalp(path, known):
    calls = {}
    with open(path) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            f = line.rstrip("\n").split("\t")
            kind = SIGNALP_GFF_TYPE.get(f[1])
            if kind:
                calls[unprefix([f[0].split()[0]], known)[0]] = kind
    return calls


def add_signalp(report, predictions, out):
    # PathoFact runs SignalP on each arm's candidates (Filter_Proteins.py: an HMM hit for
    # the toxin and VF arms, an RGI or AMRFinderPlus hit for the AMR arm) and the combined
    # report keeps the column from the first arm table the protein is in.
    df = pd.read_csv(report, sep="\t", dtype=str, keep_default_na=False)
    total = df.iloc[-1:]
    df = df.iloc[:-1]
    calls = read_signalp(predictions, set(df["ProteinID"]))
    num = lambda c: pd.to_numeric(df[c].where(df[c] != NOHIT), errors="coerce")
    arms = [
        (num("Rfprob_vf") >= VF_THRESHOLD, df["Query_vf"] != NOHIT),
        (num("Rfprob_tox") >= TOX_THRESHOLD, df["Query_tox"] != NOHIT),
        (df[AMR_COLS].ne(NOHIT).any(axis=1), df[AMR_COLS[4:]].ne(NOHIT).any(axis=1)),
    ]
    signalp = pd.Series(NOHIT, index=df.index)
    decided = pd.Series(False, index=df.index)
    for member, candidate in arms:
        take = member & ~decided
        signalp[take & candidate] = df.loc[take & candidate, "ProteinID"].map(calls).fillna(NOHIT)
        decided |= member
    df.insert(df.columns.get_loc("Plasmid_marker"), "SignalP", signalp)
    total = total.copy()
    total.insert(df.columns.get_loc("SignalP"), "SignalP", str((signalp != NOHIT).sum()))
    write(pd.concat([df, total], ignore_index=True), out)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--signalp", help="SignalP 6 prediction_results.txt; with --report, "
                   "adds the SignalP column and writes --out")
    p.add_argument("--report", help="a combined report this script wrote")
    p.add_argument("--out")
    p.add_argument("--ev-lib")
    p.add_argument("--annotation-table")
    p.add_argument("--orfs", help="the ORF FASTA the table was built on")
    p.add_argument("--sample", help="PathoFact's Sample column")
    p.add_argument("--deeparg", help="deeparg_results after `argnorm deeparg`")
    p.add_argument("--rgi")
    p.add_argument("--amrfinderplus", help="amrfinderplus_results after `argnorm amrfinderplus`")
    p.add_argument("--plasmid-summary", nargs="+", default=[])
    p.add_argument("--plasmid-genes", nargs="+", default=[])
    p.add_argument("--virus-summary", nargs="+", default=[])
    p.add_argument("--virus-genes", nargs="+", default=[])
    p.add_argument("--antismash-json", nargs="+", default=[])
    p.add_argument("--vf-threshold", type=float, default=VF_THRESHOLD)
    p.add_argument("--tox-threshold", type=float, default=TOX_THRESHOLD)
    for name in ("vf", "vf-ambiguous", "tox", "tox-ambiguous", "amr", "combined"):
        p.add_argument("--out-" + name)
    a = p.parse_args()
    if a.signalp:
        add_signalp(a.report, a.signalp, a.out)
    else:
        build(a)


if __name__ == "__main__":
    main()
