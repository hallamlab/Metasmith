"""PathoLogic input from an annotation table, by MetaPathways 4.0.3's rules.

One FUNCTION per ORF (the product name), plus EC rows and METACYC reaction rows. Rule sources in
MP4: MetaPathways_parse_blast.py (hit cut-offs, hit ECs), MetaPathways_annotate_fast.py (product
choice) and MetaPathways_create_genbank_ptinput.py (EC and METACYC lines of the .pf).
"""
import argparse
import re
from pathlib import Path

import fabfos_evidence as fe
import pandas as pd

COLUMNS = ["orf_id", "kind", "value", "score", "confidence"]
# MP4 searches these in this order, and on a tie the earlier database names the ORF. MetaCyc
# hits weigh double.
DATABASES = ["metacyc", "uniref90", "uniref50", "swissprot"]
WEIGHT = {"metacyc": 2}
MIN_ORF_LENGTH = 60
# MP4's parse cut-offs. Its bitscore >= 20 is implied by bsr >= 0.4 on a 60 aa protein, and its
# identity >= 20 is not checked here: the annotation table does not carry identity.
MIN_ALIGNED, MAX_EVALUE, MIN_BSR, MAX_HITS = 45, 1e-6, 0.4, 5
STOPWORDS = {"", "is", "have", "has", "will", "can", "should", "in", "at", "upon", "the", "a", "an",
             "on", "for", "of", "by", "with", "and", ">"}
UNINFORMATIVE = STOPWORDS | {"predicted", "protein", "conserved", "unannotated", "protein>", "<unannotated"}
SKIPPED_PRODUCTS = {"hypothetical protein", "<unannotated protein>", "NA", "N/A"}


def orf_lengths(path):
    lengths, sid = {}, None
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                sid = line[1:].split()[0]
                lengths[sid] = 0
            elif sid is not None:
                lengths[sid] += sum(c.isalpha() for c in line)
    return lengths


def product_name(description, db):
    p = description.replace("MULTISPECIES: ", "")
    cut = {"metacyc": " (", "swissprot": " OS=", "uniref90": " n=", "uniref50": " n="}[db]
    if cut in p:
        p = p.rsplit(cut, 1)[0]
    # MP4's remove_repeats drops stopwords and every repeat of a word.
    seen, words = set(), []
    for w in p.split():
        if w not in seen and w not in STOPWORDS:
            seen.add(w)
            words.append(w)
    return " ".join(words).replace(";", "")


def annotation_value(product, has_ec):
    words = {w for w in product.split() if w.lower() not in UNINFORMATIVE and "_" not in w}
    return 10 * has_ec + (0 if "hypothetical protein" in product else len(words)) + 1


def accession(db, target):
    if db == "metacyc":
        return target.split("|", 2)[2]
    if db == "swissprot":
        return target.split("|", 2)[1]
    return target.split("_", 1)[1]


def read_ec_maps(directory):
    df = pd.concat([pd.read_csv(p, sep="\t", dtype=str) for p in sorted(Path(directory).glob("EC_map.*.tsv"))])
    df = df.dropna(subset=["EC"])
    df = df[~df["EC"].str.contains(".-", regex=True)]
    return df.assign(EC=df["EC"].str.replace("EC:", "")).groupby("AccID")["EC"].apply(list).to_dict()


def best_hits(hits, db, ec_map):
    """One hit per ORF in one database, as annotate_fast picks it: the strongest hit, a tied one
    replacing it only on a higher annotation value."""
    hits = hits[(hits["end"] - hits["start"] + 1 >= MIN_ALIGNED) & (hits["evalue"] <= MAX_EVALUE)
                & (hits["score"] >= MIN_BSR)].copy()
    hits = hits.sort_values(["orf", "score"], ascending=[True, False], kind="stable")
    hits = hits.groupby("orf").head(MAX_HITS)
    # The parse step looks the bare accession up in every map, so a UniRef hit whose representative
    # is a Swiss-Prot entry carries that entry's EC.
    hits["ec"] = [ec_map.get(accession(db, t), []) for t in hits["term"]]
    hits["product"] = [product_name(d or "", db) for d in hits["description"]]
    hits["value"] = [annotation_value(p, bool(e)) * WEIGHT.get(db, 1) for p, e in zip(hits["product"], hits["ec"])]
    best = {}
    for row in hits.itertuples(index=False):
        cur = best.get(row.orf)
        if cur is None or (cur.score <= row.score and cur.value < row.value):
            best[row.orf] = row
    return best


def gather(table, lengths, ec_map, reactions):
    keep = {o for o, n in lengths.items() if n >= MIN_ORF_LENGTH}
    table = table[table["orf"].isin(keep)]
    best = {db: best_hits(table[table["lane"] == db], db, ec_map) for db in DATABASES}
    rows = []
    for orf in (o for o in lengths if o in keep):
        chosen, top = None, 0
        for db in DATABASES:
            hit = best[db].get(orf)
            if hit is not None and hit.value * WEIGHT.get(db, 1) > top:
                chosen, top = hit, hit.value * WEIGHT.get(db, 1)
        if chosen is None or chosen.product in SKIPPED_PRODUCTS:
            continue
        rows.append((orf, "FUNCTION", chosen.product, chosen.score, chosen.lane))
        ecs = list(chosen.ec)
        for db in ("swissprot", "uniref50", "uniref90"):
            hit = best[db].get(orf)
            if hit is not None:
                ecs += ec_map.get(hit.term if db != "swissprot" else accession(db, hit.term), [])
        for ec in dict.fromkeys(t.strip() for e in ecs for t in re.split(r"[,;|]", e) if t.strip()):
            rows.append((orf, "EC", ec, chosen.score, chosen.lane))
        hit = best["metacyc"].get(orf)
        for rxn in reactions.get(accession("metacyc", hit.term), []) if hit is not None else []:
            rows.append((orf, "METACYC", rxn, hit.score, "metacyc"))
    return pd.DataFrame(rows, columns=COLUMNS).astype({"score": "float64"})


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--orfs", required=True)
    p.add_argument("--annotations", nargs="+", required=True, help="the four lanes' lane tables")
    p.add_argument("--ec-maps", required=True, help="ref::uniprot_ec_maps")
    p.add_argument("--metacyc-reactions", required=True, help="ref::metacyc_monomer_reactions")
    p.add_argument("--out", required=True)
    a = p.parse_args()

    table = fe.read_lane_tables(a.annotations, columns=["orf", "lane", "term", "description", "score", "start", "end", "evalue"])
    table = table[table["lane"].isin(DATABASES)]
    rxn = pd.read_csv(a.metacyc_reactions, sep="\t", dtype=str)
    reactions = rxn.groupby("MC")["RXN"].apply(lambda s: list(dict.fromkeys(s))).to_dict()
    df = gather(table, orf_lengths(a.orfs), read_ec_maps(a.ec_maps), reactions)
    fe.write_parquet(df, a.out)
    counts = df["kind"].value_counts()
    print(f"[ptools_gather] {counts.get('FUNCTION', 0):,} named ORFs, {counts.get('EC', 0):,} EC rows, "
          f"{counts.get('METACYC', 0):,} METACYC rows", flush=True)


if __name__ == "__main__":
    main()
