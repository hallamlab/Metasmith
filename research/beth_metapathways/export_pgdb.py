#!/usr/bin/env python3
"""Export the gene-level inferences of Pathway Tools PGDBs to one table per assembly.

    export_pgdb.py --sample S --out-dir DIR [--pipeline port|mp] [--bins MAG_MAP]
                   (--port-run RUN_DIR | --mp-out SAMPLE_DIR | PGDB ...)

A PGDB is a directory or a .tgz, .tar.bz2, .tar or .zip archive holding the flat files
genes.dat, proteins.dat, enzrxns.dat, reactions.dat and pathways.dat. --port-run finds the
community and MAG archives under a metasmith run's results/; --mp-out finds MetaPathways'
results/pgdb/{community,MAGs/*}/*cyc.tar.bz2 and expands the community PGDB through
ptools/orf_map.txt, because MetaPathways builds it from one representative ORF per product.

Each PGDB is identified from its kb/<id>base.ocelot name and organism.dat ID, matched against
the bin IDs of --bins (headerless contig<TAB>bin) after removing non-alphanumerics. A PGDB that
matches no bin is the community PGDB; two such PGDBs in one assembly is an error.

Writes pgdb_genes.parquet and pgdb_genes.tsv.gz (one row per ORF x reaction x pathway; an ORF
with no reaction or no pathway keeps one row with those cells empty) and pgdb_manifest.tsv
(one row per PGDB).
"""

import argparse
import csv
import re
import sys
import tarfile
import zipfile
from collections import defaultdict
from pathlib import Path

import pandas as pd

FLAT = ("genes.dat", "proteins.dat", "enzrxns.dat", "reactions.dat", "pathways.dat")
ORGANISM = ("organism.dat", "organism-params.dat")
OCELOT = re.compile(r"(?:^|/)kb/([^/]+)base\.ocelot$")
COLUMNS = ["sample", "pipeline", "entity", "entity_type", "pgdb_id", "orf_id", "gene_id", "product",
           "reaction", "reaction_name", "ec", "enzrxn_name", "pathway", "pathway_name", "represented_by"]


def read_dat(text):
    records, rec, last = {}, defaultdict(list), None
    for line in text.splitlines():
        if line.startswith("#") or line.startswith("^"):
            continue
        if line == "//":
            if rec.get("UNIQUE-ID"):
                records[rec["UNIQUE-ID"][0]] = rec
            rec, last = defaultdict(list), None
        elif line.startswith("/") and last:
            rec[last][-1] += " " + line[1:]
        elif " - " in line:
            key, value = line.split(" - ", 1)
            rec[key].append(value)
            last = key
    return records


def read_tab_attrs(text):
    attrs = {}
    for line in text.splitlines():
        if line.startswith(";") or "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        attrs.setdefault(key.strip().upper(), value.strip())
    return attrs


def wanted(name):
    parts = name.rstrip("/").split("/")
    base = parts[-1]
    if base in FLAT and len(parts) >= 2 and parts[-2] == "data":
        return "flat"
    if base in ORGANISM and len(parts) >= 2 and parts[-2] in ("input", "data"):
        return "organism"
    return None


def load_pgdb(path):
    path = Path(path)
    files, organism, ocelot = {}, {}, set()

    def take(name, read):
        m = OCELOT.search(name)
        if m:
            ocelot.add(m.group(1).lower())
        kind = wanted(name)
        if kind == "flat":
            files[name.rsplit("/", 1)[-1]] = read().decode("latin-1")
        elif kind == "organism":
            organism.update(read_tab_attrs(read().decode("latin-1")))

    if path.is_dir():
        for p in sorted(path.rglob("*")):
            if p.is_file():
                take(p.relative_to(path).as_posix(), p.read_bytes)
    elif zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            for name in z.namelist():
                take(name, lambda n=name: z.read(n))
    else:
        with tarfile.open(path, "r|*") as tar:
            for m in tar:
                if m.isfile():
                    take(m.name, lambda m=m: tar.extractfile(m).read())
    missing = [f for f in FLAT if f not in files]
    if missing:
        sys.exit(f"{path}: no {', '.join(missing)} under a data/ directory")
    return {k[:-4]: read_dat(v) for k, v in files.items()}, organism, ocelot


def key(s):
    return re.sub(r"[^a-z0-9]", "", s.lower())


def id_forms(bin_id):
    k = key(bin_id)
    forms = {k, k[:40], k[:35]}
    if k and k[0].isdigit():
        forms |= {"p" + k, ("p" + k)[:40], "e" + k, ("e" + k)[:35]}
    return forms


def identify(organism, ocelot, bins):
    seen = {key(x) for x in ocelot | {organism.get("ID", "")} if x}
    exact = sorted(b for b in bins if {key(b), "p" + key(b), "e" + key(b)} & seen)
    hits = exact or sorted(b for b in bins if id_forms(b) & seen)
    pgdb_id = organism.get("ID") or next(iter(ocelot), "")
    return pgdb_id, hits


def genes_of(protein, proteins, depth=0):
    p = proteins.get(protein, {})
    out = list(p.get("GENE", []))
    if depth < 4:
        for comp in p.get("COMPONENTS", []):
            out += genes_of(comp, proteins, depth + 1)
    return out


def first(rec, attr):
    return (rec.get(attr) or [""])[0]


def gene_rows(db):
    genes, proteins, enzrxns = db["genes"], db["proteins"], db["enzrxns"]
    reactions, pathways = db["reactions"], db["pathways"]

    rxn_pwys = defaultdict(list)
    for pid, pwy in pathways.items():
        for rid in pwy.get("REACTION-LIST", []):
            if rid in reactions:
                rxn_pwys[rid].append(pid)

    gene_enz = defaultdict(list)
    for eid, er in enzrxns.items():
        for enz in er.get("ENZYME", []):
            for g in set(genes_of(enz, proteins)):
                gene_enz[g].append((eid, enz))

    for gid, gene in genes.items():
        orf = first(gene, "COMMON-NAME") or first(gene, "ACCESSION-1") or gid
        products = gene.get("PRODUCT", [])
        product = "; ".join(sorted({first(proteins.get(p, {}), "COMMON-NAME") for p in products} - {""}))
        links = gene_enz.get(gid, [])
        if not links:
            yield orf, gid, product, "", "", "", "", "", ""
            continue
        for eid, enz in sorted(set(links)):
            er = enzrxns[eid]
            prod = product or first(proteins.get(enz, {}), "COMMON-NAME")
            for rid in er.get("REACTION", []) or [""]:
                rxn = reactions.get(rid, {})
                ec = ",".join(e.strip("|").replace("EC-", "") for e in rxn.get("EC-NUMBER", []))
                for pid in sorted(set(rxn_pwys.get(rid, []))) or [""]:
                    yield (orf, gid, prod, rid, first(rxn, "COMMON-NAME"), ec, first(er, "COMMON-NAME"),
                           pid, first(pathways.get(pid, {}), "COMMON-NAME"))


def read_bins(path):
    bins = set()
    if path:
        with open(path) as fh:
            for line in fh:
                f = line.rstrip("\n").split("\t")
                if len(f) >= 2 and f[1]:
                    bins.add(f[1])
    return bins


def read_orf_map(path):
    groups = {}
    if path and Path(path).is_file():
        with open(path) as fh:
            for line in fh:
                members = [m for m in line.rstrip("\n").split("\t") if m]
                for m in members:
                    groups[m] = members
    return groups


def discover(args):
    if args.port_run:
        res = Path(args.port_run) / "results"
        return (sorted(res.glob("annotation-pgdb_archive/*")) + sorted(res.glob("annotation-bin_pgdb_archive/*")), None)
    if args.mp_out:
        out = Path(args.mp_out)
        found = sorted(out.glob("results/pgdb/community/*cyc.tar.bz2")) + sorted(out.glob("results/pgdb/MAGs/*/*cyc.tar.bz2"))
        return found, out / "ptools" / "orf_map.txt"
    return [Path(p) for p in args.pgdb], args.orf_map


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pgdb", nargs="*")
    ap.add_argument("--sample", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--pipeline", default="")
    ap.add_argument("--bins", help="headerless contig<TAB>bin map; its bin IDs are the MAG candidates")
    ap.add_argument("--port-run")
    ap.add_argument("--mp-out")
    ap.add_argument("--orf-map", help="MetaPathways ptools/orf_map.txt, to expand the community PGDB")
    args = ap.parse_args()

    paths, orf_map_path = discover(args)
    if not paths:
        sys.exit("no PGDB found")
    pipeline = args.pipeline or ("port" if args.port_run else "mp" if args.mp_out else "")
    bins = read_bins(args.bins)
    orf_groups = read_orf_map(orf_map_path)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    frames, manifest, communities = [], [], []
    for path in paths:
        db, organism, ocelot = load_pgdb(path)
        pgdb_id, hits = identify(organism, ocelot, bins)
        if len(hits) > 1:
            sys.exit(f"{path}: ID {pgdb_id} matches several bins: {hits}")
        entity, etype = (hits[0], "mag") if hits else ("community", "community")
        if etype == "community":
            communities.append(path)
        rows = pd.DataFrame(gene_rows(db), columns=COLUMNS[5:-1])
        rows["represented_by"] = ""
        if etype == "community" and orf_groups:
            present = set(rows["orf_id"])
            pairs = pd.DataFrame([(orf, other) for orf in present for other in orf_groups.get(orf, ())
                                  if other not in present], columns=["orf_id", "duplicate"])
            if len(pairs):
                dup = rows.merge(pairs, on="orf_id")
                dup["represented_by"], dup["orf_id"] = dup["orf_id"], dup.pop("duplicate")
                rows = pd.concat([rows, dup[rows.columns]], ignore_index=True)
        rows.insert(0, "pgdb_id", pgdb_id)
        rows.insert(0, "entity_type", etype)
        rows.insert(0, "entity", entity)
        rows.insert(0, "pipeline", pipeline)
        rows.insert(0, "sample", args.sample)
        frames.append(rows)
        with_rxn = rows["reaction"] != ""
        manifest.append({
            "sample": args.sample, "pipeline": pipeline, "entity": entity, "entity_type": etype,
            "pgdb_id": pgdb_id, "kb_ocelot": ";".join(sorted(ocelot)), "organism_name": organism.get("NAME", ""),
            "source": str(path), "genes": len(db["genes"]), "proteins": len(db["proteins"]),
            "enzrxns": len(db["enzrxns"]), "reactions_in_pgdb": len(db["reactions"]), "pathways": len(db["pathways"]),
            "rows": len(rows), "orfs": rows["orf_id"].nunique(), "orfs_with_reaction": rows.loc[with_rxn, "orf_id"].nunique(),
            "orfs_with_pathway": rows.loc[rows["pathway"] != "", "orf_id"].nunique(),
            "orfs_expanded": int((rows["represented_by"] != "").groupby(rows["orf_id"]).any().sum()),
            "pathways_with_orf": rows.loc[rows["pathway"] != "", "pathway"].nunique(),
        })
        print(f"{entity}\t{pgdb_id}\t{len(db['pathways'])} pathways\t{len(rows)} rows", flush=True)
    if len(communities) > 1:
        sys.exit(f"{len(communities)} PGDBs match no bin: {[str(p) for p in communities]}")

    table = pd.concat(frames, ignore_index=True)[COLUMNS]
    table.to_parquet(out / "pgdb_genes.parquet", index=False)
    tsv = table.apply(lambda c: c.str.replace(r"[\t\r\n]", " ", regex=True) if c.dtype == object else c)
    tsv.to_csv(out / "pgdb_genes.tsv.gz", sep="\t", index=False, compression="gzip", quoting=csv.QUOTE_NONE, quotechar="\x01")
    pd.DataFrame(manifest).to_csv(out / "pgdb_manifest.tsv", sep="\t", index=False)
    print(f"{len(frames)} PGDBs, {len(table)} rows -> {out}")


if __name__ == "__main__":
    main()
