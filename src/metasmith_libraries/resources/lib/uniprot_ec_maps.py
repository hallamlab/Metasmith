"""MetaPathways 4's EC maps and FASTA for one pinned UniProt release.

MP4 (build_DBs/*_mapper.py) maps a Swiss-Prot accession to the ECs whose ExPASy ENZYME
record cites it, and a UniRef cluster to the ECs of its UniProtKB members, which it reads
from idmapping.dat. A release's UniRef XML lists the same members, so it serves for both
the cluster's FASTA record and its membership.

    uniprot_ec_maps.py swissprot --enzyme enzyme.dat --out EC_map.swissprot.tsv
    uniprot_ec_maps.py uniref --enzyme enzyme.dat --xml uniref90.xml.gz --name uniref90 \
        --fasta uniref90.fasta --out EC_map.uniref90.tsv
"""
import argparse
import gzip
import re
import xml.etree.ElementTree as ET


def enzyme_xrefs(path):
    ecs = {}
    with open(path) as fh:
        for rec in fh.read().split("//"):
            if "ID " not in rec:
                continue
            ec = rec.split("ID")[1].split("\n")[0].strip(" ")
            for line in rec.split("\n"):
                if line[:2] == "DR":
                    for acc in re.split("; |, | ", line.strip("DR").strip(" ").strip(";")):
                        if acc:
                            ecs.setdefault(acc, []).append(ec)
    return ecs


def write_map(rows, out):
    with open(out, "w") as fh:
        fh.write("AccID\tEC\n")
        for acc, ec in rows:
            fh.write(acc + "\t" + ec + "\n")


def swissprot(a):
    write_map(((acc, ec) for acc, ecs in enzyme_xrefs(a.enzyme).items() for ec in ecs), a.out)


def _local(tag):
    return tag.rsplit("}", 1)[-1]


# One <entry> per cluster: its representative member's sequence and every member's
# UniProtKB accession. The FASTA header is UniProt's own UniRef FASTA header.
def uniref(a):
    ecs = enzyme_xrefs(a.enzyme)
    rows, n = [], 0
    with gzip.open(a.xml, "rb") as src, open(a.fasta, "w") as fasta:
        for _, el in ET.iterparse(src, events=("end",)):
            if _local(el.tag) != "entry":
                continue
            cid = el.get("id")
            name, rep, seq, tax, taxid, count, accs = "", "", "", "", "", "", set()
            for sub in el.iter():
                tag = _local(sub.tag)
                if tag == "name" and not name:
                    name = (sub.text or "").removeprefix("Cluster: ")
                elif tag == "property":
                    k, v = sub.get("type"), sub.get("value")
                    if k == "member count":
                        count = v
                    elif k == "common taxon":
                        tax = v
                    elif k == "common taxon ID":
                        taxid = v
                    elif k == "UniProtKB accession":
                        accs.add(v)
                elif tag == "representativeMember":
                    ref = next((r for r in sub.iter() if _local(r.tag) == "dbReference"), None)
                    rep = ref.get("id") if ref is not None else ""
                    s = next((r for r in sub.iter() if _local(r.tag) == "sequence"), None)
                    seq = "".join((s.text or "").split()) if s is not None else ""
            fasta.write(f">{cid} {name} n={count} Tax={tax} TaxID={taxid} RepID={rep}\n{seq}\n")
            rows.extend((cid, ec) for acc in accs for ec in ecs.get(acc, ()))
            n += 1
            el.clear()
    if n == 0:
        raise SystemExit("[uniprot] " + a.xml + " holds no UniRef <entry>")
    write_map(rows, a.out)
    print(f"[uniprot] {a.name}: {n} clusters, {len(rows)} cluster-EC pairs", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("swissprot")
    s.add_argument("--enzyme", required=True)
    s.add_argument("--out", required=True)
    u = sub.add_parser("uniref")
    for k in ("--enzyme", "--xml", "--name", "--fasta", "--out"):
        u.add_argument(k, required=True)
    a = p.parse_args()
    {"swissprot": swissprot, "uniref": uniref}[a.cmd](a)


if __name__ == "__main__":
    main()
