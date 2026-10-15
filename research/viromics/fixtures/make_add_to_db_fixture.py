"""Build a stand-in GTDB-Tk de_novo directory so iphop_add_to_db can be exercised locally.

`iphop add_to_db` reads two things out of --gtdb_dir: a decorated tree, and a
tree-taxonomy table it globs for ONLY under `infer/`. It then rebuilds the database's
Host_Genomes.tsv, keeping a row only when that row's representative appears in the
taxonomy table -- so a taxonomy table listing just the new genomes silently empties the
database rather than augmenting it. The fixture therefore lists every leaf of the
database's own tree alongside the new genomes.

This exercises our chaining and our output handling. It is not a scientific result: the
new leaves are grafted at the root, so their placement is arbitrary. The real input is
gtdbtk de_novo_wf's own output.
"""

import argparse
import re
import shutil
from pathlib import Path

LEAF = re.compile(r"[(,]\s*([^(),:;]+)\s*:")


def leaves(newick: str) -> list[str]:
    return [m.group(1).strip() for m in LEAF.finditer(newick)]


def graft(newick: str, names: list[str], length: float = 0.1) -> str:
    body = newick.strip().rstrip(";").strip()
    extra = ",".join(f"{n}:{length}" for n in names)
    return f"({body},{extra});\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, required=True, help="an unpacked iPHoP database")
    ap.add_argument("--out", type=Path, required=True, help="the gtdb_dir to write")
    ap.add_argument("--bacteria", nargs="*", default=[], metavar="ID=TAXONOMY",
                    help="new bacterial genomes, each id=semicolon-separated GTDB lineage")
    ap.add_argument("--archaea", nargs="*", default=[], metavar="ID=TAXONOMY",
                    help="new archaeal genomes, each id=semicolon-separated GTDB lineage")
    args = ap.parse_args()

    def split(items):
        out = []
        for item in items:
            assert "=" in item, f"expected ID=TAXONOMY, got [{item}]"
            out.append(tuple(item.split("=", 1)))
        return out

    infer = args.out/"infer"
    infer.mkdir(parents=True, exist_ok=True)

    for domain, new in (
        ("bac120", split(args.bacteria)),
        ("ar122", split(args.archaea)),
    ):
        src = args.db/"db_infos"/f"gtdbtk.{domain}.decorated.tree"
        if not src.exists():
            continue
        newick = src.read_text()
        names = leaves(newick)
        if new:
            (infer/f"gtdbtk.{domain}.decorated.tree").write_text(
                graft(newick, [n for n, _ in new]))
        else:
            shutil.copyfile(src, infer/f"gtdbtk.{domain}.decorated.tree")
        with open(infer/f"gtdbtk.{domain}.decorated.tree-taxonomy", "w") as f:
            for name in names:
                f.write(f"{name}\td__;p__;c__;o__;f__;g__\n")
            for name, taxon in new:
                f.write(f"{name}\t{taxon}\n")
        print(f"{domain}: {len(names)} existing leaves + {len(new)} new")


if __name__ == "__main__":
    main()
