# Grafts new MAGs into an iPHoP database's own GTDB-Tk trees, for `iphop add_to_db`. Runs
# inside the iPHoP image, which carries Bio.Phylo. Writes <out>/infer/ with one decorated
# tree and tree-taxonomy per domain the base database has, and prints a JSON summary.
import argparse
import json
import sys
from pathlib import Path

from Bio import Phylo
from Bio.Phylo.Newick import Clade

# iPHoP's predict walks these trees to a common ancestor and sums branch lengths on the
# way, so every clade but the root needs one. A MAG is grafted as the sister of the base
# leaf nearest to it in its de_novo tree, under a new clade that takes over that leaf's
# branch. A MAG whose de_novo tree reaches no base leaf at all hangs off the root.
NEAREST_CANDIDATES = 64


def domain_of(marker: str) -> str:
    return "ar" if marker.startswith("ar") else "bac"


def parents_of(tree) -> dict:
    parent = {}
    for clade in tree.find_clades(order="level"):
        for child in clade.clades:
            parent[id(child)] = clade
    return parent


def nearest_base_leaf(denovo, parent, leaf, base_names):
    node = leaf
    while id(node) in parent:
        node = parent[id(node)]
        found = [t for t in node.get_terminals() if t.name in base_names]
        if found:
            return min(found[:NEAREST_CANDIDATES], key=lambda t: denovo.distance(leaf, t))
    return None


def denovo_files(dirs, prefix, suffix):
    for d in dirs:
        for parent in (d, d/"infer"):
            yield from sorted(parent.glob(f"gtdbtk.{prefix}[0-9]*.decorated.{suffix}"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--mags", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("denovo", type=Path, nargs="+")
    args = ap.parse_args()
    sys.setrecursionlimit(1_000_000)

    mags = {p.stem for p in args.mags.iterdir()}
    repr_taxonomy = {}
    with open(args.base/"db_infos"/"Host_Genomes.tsv") as f:
        next(f)
        for line in f:
            r = line.rstrip("\n").split("\t")
            repr_taxonomy.setdefault(r[4], r[5])

    out = args.out/"infer"
    out.mkdir(parents=True, exist_ok=True)
    summary = {}
    for base_tree_file in sorted((args.base/"db_infos").glob("gtdbtk.*.decorated.tree")):
        marker = base_tree_file.name.split(".")[1]
        prefix = domain_of(marker)
        tree = Phylo.read(base_tree_file, "newick")
        leaves = {t.name: t for t in tree.get_terminals()}
        base_parent = parents_of(tree)

        mag_taxonomy = {}
        for table in denovo_files(args.denovo, prefix, "tree-taxonomy"):
            with open(table) as f:
                for line in f:
                    gid, _, taxon = line.rstrip("\n").partition("\t")
                    if gid in mags:
                        mag_taxonomy.setdefault(gid, taxon)

        grafted, at_root = [], []
        for denovo_file in denovo_files(args.denovo, prefix, "tree"):
            denovo = Phylo.read(denovo_file, "newick")
            denovo_parent = parents_of(denovo)
            for leaf in denovo.get_terminals():
                if leaf.name not in mag_taxonomy or leaf.name in leaves:
                    continue
                near = nearest_base_leaf(denovo, denovo_parent, leaf, leaves.keys())
                new_leaf = Clade(name=leaf.name, branch_length=leaf.branch_length or 0.0)
                if near is None:
                    new_leaf.branch_length = denovo.distance(leaf)
                    tree.root.clades.append(new_leaf)
                    base_parent[id(new_leaf)] = tree.root
                    at_root.append(leaf.name)
                else:
                    sister = leaves[near.name]
                    holder = base_parent[id(sister)]
                    joint = Clade(branch_length=sister.branch_length or 0.0, clades=[sister, new_leaf])
                    sister.branch_length = 0.0
                    holder.clades[holder.clades.index(sister)] = joint
                    base_parent[id(joint)] = holder
                    base_parent[id(sister)] = joint
                    base_parent[id(new_leaf)] = joint
                    grafted.append(leaf.name)
                leaves[leaf.name] = new_leaf

        Phylo.write(tree, out/base_tree_file.name, "newick")
        with open(out/f"{base_tree_file.name}-taxonomy", "w") as f:
            for name in leaves:
                taxon = mag_taxonomy.get(name, repr_taxonomy.get(name, ""))
                f.write(f"{name}\t{taxon}\n")
        summary[marker] = {
            "base_leaves": len(leaves) - len(grafted) - len(at_root),
            "grafted": len(grafted), "at_root": len(at_root),
            "mags": grafted + at_root,
        }
    json.dump(summary, sys.stdout)
    print()


if __name__ == "__main__":
    main()
