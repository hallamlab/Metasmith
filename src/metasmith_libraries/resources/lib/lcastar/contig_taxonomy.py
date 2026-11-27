import argparse
import csv
import gzip
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

from lcastar import gtdb_rank, lca, lcastar, parse_gtdb, to_text

csv.field_size_limit(sys.maxsize)
_TAXID = re.compile(r"\bTaxID=(\d+)")
_MATCH = re.compile(r"^(\d+):(\d+)$")
_ORF_SUFFIX = re.compile(r"[_-]\d+$")
_RANK_LETTER = {"superkingdom": "d", "domain": "d", "phylum": "p", "class": "c", "order": "o",
                "family": "f", "genus": "g", "species": "s"}


def contig_of(orf: str) -> str:
    return _ORF_SUFFIX.sub("", orf)


def read_uniref_taxids(descriptions):
    taxids = {}
    with open(descriptions) as fh:
        next(fh, None)
        for line in fh:
            sseqid, _, desc = line.rstrip("\n").partition("\t")
            m = _TAXID.search(desc)
            if m:
                taxids[sseqid] = int(m.group(1))
    return taxids


def read_map(path, wanted):
    lineages = {}
    with gzip.open(path, "rt") as fh:
        next(fh, None)
        for line in fh:
            taxid, lineage, _ = line.split("\t", 2)
            if int(taxid) in wanted:
                lineages[int(taxid)] = parse_gtdb(lineage)
    return lineages


def orf_lcas(hits_path, sseq_taxid, taxid_lineage, a):
    by_orf = defaultdict(list)
    with open(hits_path) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            bits, bsr = float(row["bitscore"]), float(row["bsr"])
            if (bits < a.min_bitscore or bsr < a.min_bsr or int(row["length"]) < a.min_length
                    or float(row["evalue"]) > a.max_evalue):
                continue
            lineage = taxid_lineage.get(sseq_taxid.get(row["sseqid"]))
            by_orf[row["qseqid"]].append((bits, lineage))

    out = {}
    for orf, hits in by_orf.items():
        eligible = [h for h in hits if h[0] >= a.lca_min_bitscore]
        if not eligible:
            continue
        floor = max(h[0] for h in eligible) * (1 - a.lca_top_percent / 100)
        lineages = [lin for bits, lin in eligible if bits >= floor and lin]
        if lineages:
            out[orf] = lca(lineages)
    return out


def lift_to_support(orf_lineage, min_support):
    support = Counter()
    for lineage in orf_lineage.values():
        for depth in range(1, len(lineage) + 1):
            support[lineage[:depth]] += 1
    lifted = {}
    for orf, lineage in orf_lineage.items():
        while lineage and support[lineage] < min_support:
            lineage = lineage[:-1]
        lifted[orf] = lineage
    return lifted


def read_metabuli_taxonomy(ref_dir):
    names_dmp = next(iter(sorted(Path(ref_dir).glob("**/names.dmp"))), None)
    nodes_dmp = next(iter(sorted(Path(ref_dir).glob("**/nodes.dmp"))), None)
    if names_dmp is None or nodes_dmp is None:
        return {}
    names, parent, rank = {}, {}, {}
    with open(names_dmp) as fh:
        for line in fh:
            f = [x.strip() for x in line.split("|")]
            if len(f) > 3 and f[3] == "scientific name":
                names[int(f[0])] = f[1]
    with open(nodes_dmp) as fh:
        for line in fh:
            f = [x.strip() for x in line.split("|")]
            parent[int(f[0])], rank[int(f[0])] = int(f[1]), f[2]

    def label(t):
        name, letter = names.get(t, ""), _RANK_LETTER.get(rank.get(t, ""))
        if not letter or not name:
            return None
        return name if "__" in name else f"{letter}__{name}"

    memo = {}

    def lineage(taxid):
        if taxid in memo:
            return memo[taxid]
        path, seen, t = [], set(), taxid
        while t in parent and t not in seen:
            seen.add(t)
            lab = label(t)
            if lab:
                path.append(lab)
            if parent[t] == t:
                break
            t = parent[t]
        memo[taxid] = tuple(reversed(path))
        return memo[taxid]

    return {t: parse_gtdb(to_text(lineage(t))) for t in parent}


def read_metabuli(path):
    calls, matches = {}, {}
    with open(path) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            f = line.rstrip("\n").split("\t")
            if f[0] != "1":
                continue
            contig = f[1]
            lineage = next((x for x in f if "d_" in x and "__" in x), "")
            calls[contig] = (int(f[2]), parse_gtdb(lineage))
            pairs = (_MATCH.match(tok) for tok in re.split(r"[ ,]", f[-1]))
            matches[contig] = [(int(m.group(1)), int(m.group(2))) for m in pairs if m]
    return calls, matches


def read_rrna(path):
    by_contig = defaultdict(list)
    with open(path) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            lineage = parse_gtdb(row["lineage"])
            if lineage:
                by_contig[row["contig"]].append(lineage)
    return by_contig


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hits", required=True)
    p.add_argument("--descriptions", required=True)
    p.add_argument("--map", required=True)
    p.add_argument("--metabuli", required=True)
    p.add_argument("--metabuli-ref", required=True)
    p.add_argument("--rrna", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--min-bitscore", type=float, default=20)
    p.add_argument("--min-bsr", type=float, default=0.4)
    p.add_argument("--min-length", type=int, default=45)
    p.add_argument("--max-evalue", type=float, default=1e-6)
    p.add_argument("--lca-min-bitscore", type=float, default=50)
    p.add_argument("--lca-top-percent", type=float, default=10)
    p.add_argument("--lca-min-support", type=int, default=2)
    p.add_argument("--alpha", type=float, default=0.51)
    p.add_argument("--orf-weight", type=float, default=1.0)
    p.add_argument("--metabuli-weight", type=float, default=1.0)
    p.add_argument("--rrna-weight", type=float, default=1.0)
    a = p.parse_args()

    sseq_taxid = read_uniref_taxids(a.descriptions)
    taxid_lineage = read_map(a.map, set(sseq_taxid.values()))
    orf_lineage = lift_to_support(orf_lcas(a.hits, sseq_taxid, taxid_lineage, a), a.lca_min_support)
    mb_taxonomy = read_metabuli_taxonomy(a.metabuli_ref)
    mb_calls, mb_matches = read_metabuli(a.metabuli)
    # Without the database's own taxonomy dump, a match taxID resolves only if
    # some contig was called to it.
    for taxid, lineage in mb_calls.values():
        mb_taxonomy.setdefault(taxid, lineage)
    rrna = read_rrna(a.rrna)

    votes = defaultdict(lambda: {"orf": [], "metabuli": [], "rrna": []})
    for orf, lineage in orf_lineage.items():
        votes[contig_of(orf)]["orf"].append((lineage, a.orf_weight))
    unresolved = 0
    fallback = 0
    for contig, pairs in mb_matches.items():
        resolved = []
        for taxid, n in pairs:
            lineage = mb_taxonomy.get(taxid)
            if lineage is None:
                unresolved += n
            else:
                resolved.append((lineage, n))
        # A contig averages ~58 matches against ~1.6 ORF votes (SG8W14 pilot), so
        # raw counts would outvote the ORFs; Metabuli casts one vote per contig,
        # split across its taxa by match share.
        total = sum(n for _, n in resolved)
        resolved = [(lin, a.metabuli_weight * n / total) for lin, n in resolved]
        # Metabuli 1.2.0 often leaves taxID:match_count empty on contigs, and a
        # database without its taxonomy dump cannot resolve most match taxIDs, so
        # the contig's own call is then its one vote.
        if not resolved and mb_calls[contig][1]:
            resolved = [(mb_calls[contig][1], a.metabuli_weight)]
            fallback += 1
        votes[contig]["metabuli"].extend(resolved)
    for contig, lineages in rrna.items():
        votes[contig]["rrna"].extend((lin, a.rrna_weight) for lin in lineages)

    cols = ["contig", "lineage", "rank", "p_value", "fraction", "votes", "total", "majority",
            "orf_votes", "metabuli_votes", "rrna_votes", "orf_call", "metabuli_call", "rrna_call"]
    n = 0
    with open(a.out, "w") as fh:
        fh.write("\t".join(cols) + "\n")
        for contig in sorted(votes):
            v = votes[contig]
            r = lcastar(v["orf"] + v["metabuli"] + v["rrna"], a.alpha)
            if r is None:
                continue
            orf_call = lcastar(v["orf"], a.alpha)
            row = [contig, to_text(r.lineage), gtdb_rank(r.lineage), f"{r.p_value:.4g}",
                   f"{r.fraction:.4f}", f"{r.votes:g}", f"{r.total:g}", str(r.majority),
                   f"{sum(w for _, w in v['orf']):g}", f"{sum(w for _, w in v['metabuli']):g}",
                   f"{sum(w for _, w in v['rrna']):g}",
                   to_text(orf_call.lineage) if orf_call else "",
                   to_text(mb_calls[contig][1]) if contig in mb_calls else "",
                   to_text(lca(lin for lin, _ in v["rrna"])) if v["rrna"] else ""]
            fh.write("\t".join(row) + "\n")
            n += 1
    print(f"[contig_lcastar] {n:,} contigs voted; {len(orf_lineage):,} ORF LCAs; "
          f"{unresolved:,} Metabuli matches unresolved; {fallback:,} contigs vote their Metabuli call", flush=True)


if __name__ == "__main__":
    main()
