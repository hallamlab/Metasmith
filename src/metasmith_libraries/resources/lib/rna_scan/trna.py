import argparse
import re

from gff import Feature, write_gff

# ARAGORN -w batch rows: "1   tRNA-Ala              c[12345,12420]      35      (tgc)"
_ARAGORN = re.compile(r"^\s*\d+\s+(tRNA-\S+)\s+(c?)\[(-?\d+),(\d+)\]\s+\S+\s+\((\w+)\)")


def read_aragorn(path):
    feats, contig = [], None
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                contig = line[1:].split()[0]
                continue
            m = _ARAGORN.match(line)
            if m and contig is not None:
                product, comp, start, end, anticodon = m.groups()
                feats.append(Feature(contig, max(1, int(start)), int(end), "-" if comp else "+", "tRNA",
                                     product, None, "aragorn", extra=f"anticodon={anticodon.upper()}"))
    return feats


def read_trnascan(path):
    feats = []
    with open(path) as fh:
        for line in fh:
            f = [x.strip() for x in line.rstrip("\n").split("\t")]
            if len(f) < 9 or not f[1].isdigit():
                continue
            begin, end = int(f[2]), int(f[3])
            strand = "+" if begin <= end else "-"
            note = f[9] if len(f) > 9 else ""
            if "pseudo" in note:
                continue
            feats.append(Feature(f[0], min(begin, end), max(begin, end), strand, "tRNA", f"tRNA-{f[4]}",
                                 float(f[8]), "tRNAscan-SE", extra=f"anticodon={f[5].upper()}"))
    return feats


def main():
    p = argparse.ArgumentParser()
    p.add_argument("tool", choices=["aragorn", "trnascan"])
    p.add_argument("--raw", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    feats = (read_aragorn if a.tool == "aragorn" else read_trnascan)(a.raw)
    feats.sort(key=lambda x: (x.contig, x.start))
    for i, feat in enumerate(feats, 1):
        feat.id = f"trna_{i}"
    write_gff(feats, a.out)
    print(f"[trna] {a.tool}: {len(feats)} tRNA genes", flush=True)


if __name__ == "__main__":
    main()
