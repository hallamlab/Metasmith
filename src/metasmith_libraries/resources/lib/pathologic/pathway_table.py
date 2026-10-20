import argparse
from collections import defaultdict
from pathlib import Path


def read_dat(path):
    # Pathway Tools attribute-value files: "KEY - VALUE" lines, "/" continuation
    # lines, records ended by "//". Some releases export an error sentence in
    # place of an empty class, which parses to no records.
    records, rec, last = {}, defaultdict(list), None
    if not Path(path).exists():
        return records
    with open(path, encoding="latin-1") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith("#"):
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


def genes_of(protein, proteins, depth=0):
    p = proteins.get(protein, {})
    genes = list(p.get("GENE", []))
    if depth < 3:
        for comp in p.get("COMPONENTS", []):
            genes += genes_of(comp, proteins, depth + 1)
    return genes


def rows(pgdb):
    data = Path(pgdb) / "data"
    pathways = read_dat(data / "pathways.dat")
    reactions = read_dat(data / "reactions.dat")
    enzrxns = read_dat(data / "enzrxns.dat")
    proteins = read_dat(data / "proteins.dat")
    genes = read_dat(data / "genes.dat")

    def orf(gene):
        return (genes.get(gene, {}).get("COMMON-NAME") or [gene])[0]

    for pid, pwy in sorted(pathways.items()):
        name = (pwy.get("COMMON-NAME") or [""])[0]
        for rid in pwy.get("REACTION-LIST", []):
            rxn = reactions.get(rid)
            if rxn is None:
                continue
            ec = ",".join(e.replace("EC-", "") for e in rxn.get("EC-NUMBER", []))
            orfs = sorted({orf(g) for er in rxn.get("ENZYMATIC-REACTION", [])
                           for enz in enzrxns.get(er, {}).get("ENZYME", [])
                           for g in genes_of(enz, proteins)})
            for o in orfs or [""]:
                yield pid, name, o, rid, ec


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pgdb", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    n, pwys = 0, set()
    with open(a.out, "w") as fh:
        fh.write("pathway\tname\torf\treaction\tec\n")
        for row in rows(a.pgdb):
            fh.write("\t".join(row) + "\n")
            n += 1
            pwys.add(row[0])
    print(f"[pathway_table] {len(pwys):,} pathways, {n:,} rows", flush=True)


if __name__ == "__main__":
    main()
