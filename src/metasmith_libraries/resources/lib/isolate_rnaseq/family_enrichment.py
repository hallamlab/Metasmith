"""Test whether gene families sit disproportionately in some dose-response clusters.

    family_enrichment.py <clusters.csv> <annotation_dir> <out_dir> [--tag T] [--stable-only]

`<clusters.csv>` is cluster_k.py output: gene_id, gene_name, cluster, membership_confidence,
stable. `<annotation_dir>` holds EcoCyc's GO annotation of E. coli K-12 (ecocyc.gaf.gz),
go-basic.obo, and KEGG's eco gene list, gene-pathway links and pathway names. Genes map to
K-12 annotation by gene name, which RefSeq shares with K-12 for an EPI300 genome.

Three family definitions: GO biological process (propagated to every ancestor over is_a and
part_of), KEGG pathway (the global overview maps left out), and the three-letter gene-name
prefix, which in E. coli mostly tracks an operon or system (y-genes, named for their map
position, left out). The background is the clustered genes themselves, so a family is tested
for where it sits among the clusters, not for being variable at all. Per family, a
permutation test of the cluster labels (G statistic) asks whether its members are spread
non-randomly; per family and cluster, a one-sided Fisher's exact test says where it
concentrates. Both are Benjamini-Hochberg corrected within each source.
"""
import argparse
import gzip
import re
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import hypergeom

MIN_GENES = 4
PERMUTATIONS = 10000
FDR = 0.05
PER_CLUSTER_SHOWN = 6
SEED = 20261029
KEGG_OVERVIEW = re.compile(r"^eco01[12]\d\d$")
NAME_FAMILY = re.compile(r"^([a-xz][a-z]{2})[A-Z]")


def bh(p: np.ndarray) -> np.ndarray:
    p = np.asarray(p, dtype=float)
    order = np.argsort(p)
    ranked = p[order] * len(p) / (np.arange(len(p)) + 1)
    q = np.minimum.accumulate(ranked[::-1])[::-1].clip(max=1)
    out = np.empty_like(q)
    out[order] = q
    return out


def go_ancestors(obo: Path) -> tuple[dict, dict]:
    parents, names, bp = defaultdict(set), {}, set()
    for stanza in obo.read_text().split("\n\n"):
        lines = stanza.strip().splitlines()
        if not lines or lines[0] != "[Term]":
            continue
        fields = defaultdict(list)
        for line in lines[1:]:
            key, _, value = line.partition(": ")
            fields[key].append(value)
        if fields.get("is_obsolete") == ["true"]:
            continue
        term = fields["id"][0]
        names[term] = fields["name"][0]
        if fields["namespace"] == ["biological_process"]:
            bp.add(term)
        parents[term] |= {v.split()[0] for v in fields["is_a"]}
        parents[term] |= {v.split()[1] for v in fields["relationship"] if v.startswith("part_of ")}
    memo = {}

    def up(t):
        if t not in memo:
            memo[t] = {t}
            for p in parents.get(t, ()):
                memo[t] |= up(p)
        return memo[t]

    return {t: up(t) & bp for t in bp}, names


def go_families(ann: Path, genes: dict[str, str]) -> tuple[dict, dict]:
    ancestors, names = go_ancestors(ann / "go-basic.obo")
    by_symbol = defaultdict(set)
    with gzip.open(ann / "ecocyc.gaf.gz", "rt") as fh:
        for line in fh:
            if line.startswith("!"):
                continue
            f = line.rstrip("\n").split("\t")
            if f[8] != "P" or "NOT" in f[3] or f[4] not in ancestors:
                continue
            by_symbol[f[2].lower()] |= ancestors[f[4]]
    families = defaultdict(set)
    for gid, name in genes.items():
        for t in by_symbol.get(name.lower(), ()):
            families[t].add(gid)
    return families, {t: names.get(t, t) for t in families}


def kegg_families(ann: Path, genes: dict[str, str]) -> tuple[dict, dict]:
    symbol_of = {}
    for line in (ann / "kegg_eco_genes.tsv").read_text().splitlines():
        f = line.split("\t")
        symbols = f[-1].split(";")[0]
        symbol_of[f[0]] = {s.strip().lower() for s in symbols.split(",")}
    pathway_of = defaultdict(set)
    for line in (ann / "kegg_eco_gene_pathway.tsv").read_text().splitlines():
        gene, path = line.split("\t")
        path = path.removeprefix("path:")
        if not KEGG_OVERVIEW.match(path):
            for s in symbol_of.get(gene, ()):
                pathway_of[s].add(path)
    names = {}
    for line in (ann / "kegg_eco_pathways.tsv").read_text().splitlines():
        pid, name = line.split("\t")
        names[pid] = name.split(" - Escherichia")[0]
    families = defaultdict(set)
    for gid, name in genes.items():
        for p in pathway_of.get(name.lower(), ()):
            families[p].add(gid)
    return families, {p: names.get(p, p) for p in families}


def name_families(genes: dict[str, str]) -> tuple[dict, dict]:
    families = defaultdict(set)
    for gid, name in genes.items():
        m = NAME_FAMILY.match(name)
        if m:
            families[m.group(1)].add(gid)
    return families, {f: f"{f}* genes" for f in families}


def g_statistic(observed: np.ndarray, expected: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        terms = np.where(observed > 0, observed * np.log(observed / expected), 0.0)
    return 2 * terms.sum(axis=-1)


def test_source(source: str, families: dict, names: dict, labels: pd.Series, perms: np.ndarray) -> pd.DataFrame:
    index = {g: i for i, g in enumerate(labels.index)}
    clusters = np.sort(labels.unique())
    k_index = {c: i for i, c in enumerate(clusters)}
    coded = labels.map(k_index).to_numpy()
    sizes = np.bincount(coded, minlength=len(clusters))
    n = len(labels)
    rows = []
    for fam, members in families.items():
        idx = np.array([index[g] for g in members if g in index])
        f = len(idx)
        if f < MIN_GENES or f == n:
            continue
        observed = np.bincount(coded[idx], minlength=len(clusters))
        expected = np.stack([f * sizes / n, (n - f) * sizes / n])
        g_obs = g_statistic(np.stack([observed, sizes - observed]), expected).sum()
        member_labels = perms[:, idx]
        in_family = (member_labels[:, :, None] == np.arange(len(clusters))).sum(axis=1)
        g_perm = g_statistic(in_family, expected[0]) + g_statistic(sizes - in_family, expected[1])
        p_perm = (1 + (g_perm >= g_obs - 1e-9).sum()) / (1 + len(perms))
        for c, ci in k_index.items():
            a = observed[ci]
            rows.append({
                "source": source, "family": fam, "name": names[fam], "genes_clustered": f,
                "permutation_p": p_perm, "cluster": c, "cluster_size": sizes[ci],
                "in_cluster": a, "expected": round(expected[0, ci], 2),
                "fold": round(a / expected[0, ci], 2),
                "fisher_p": hypergeom.sf(a - 1, n, f, sizes[ci]),
                "genes": ",".join(sorted(labels.index[idx][coded[idx] == ci].map(GENE_NAME))),
            })
    table = pd.DataFrame(rows)
    if table.empty:
        return table
    fam_p = table.drop_duplicates("family").set_index("family").permutation_p
    table["permutation_q"] = table.family.map(pd.Series(bh(fam_p.values), index=fam_p.index))
    table["fisher_q"] = bh(table.fisher_p.values)
    return table


GENE_NAME: dict[str, str] = {}


def plot(table: pd.DataFrame, title: str, out: Path) -> None:
    hits = table[(table.fisher_q < FDR) & (table.fold > 1)]
    if hits.empty:
        return
    # Propagated GO terms repeat one gene set under many names, so each cluster shows its
    # strongest families with distinct member sets only.
    seen, keep = set(), []
    for _, h in hits.sort_values(["fisher_q", "genes_clustered"]).iterrows():
        key = (h.cluster, h.genes)
        if key in seen or sum(k[0] == h.cluster for k in seen) >= PER_CLUSTER_SHOWN:
            continue
        seen.add(key)
        keep.append(h)
    fams = pd.DataFrame(keep).drop_duplicates(["source", "family"]).sort_values(["cluster", "fisher_q"])
    keys = list(zip(fams.source, fams.family))
    labels = [f"{s}: {n[:55]} ({f})" if s != "name" else f"name: {n}" for s, f, n in zip(fams.source, fams.family, fams.name)]
    clusters = sorted(table.cluster.unique())
    sub = table.set_index(["source", "family", "cluster"])
    q = np.array([[sub.loc[(s, f, c), "fisher_q"] for c in clusters] for s, f in keys])
    count = np.array([[sub.loc[(s, f, c), "in_cluster"] for c in clusters] for s, f in keys])
    fold = np.array([[sub.loc[(s, f, c), "fold"] for c in clusters] for s, f in keys])
    score = np.where(fold > 1, -np.log10(q), 0)
    fig, ax = plt.subplots(figsize=(1.0 * len(clusters) + 6.5, 0.3 * len(keys) + 1.8))
    im = ax.imshow(score, aspect="auto", cmap="Reds", vmin=0, vmax=max(3, score.max()))
    for i in range(len(keys)):
        for j in range(len(clusters)):
            if count[i, j]:
                ax.text(j, i, int(count[i, j]), ha="center", va="center", fontsize=7,
                        color="white" if score[i, j] > 2 else "#333", fontweight="bold" if q[i, j] < FDR and fold[i, j] > 1 else None)
    ax.set_xticks(range(len(clusters)), [f"cluster {c}" for c in clusters], rotation=30, ha="right")
    ax.set_yticks(range(len(keys)), labels, fontsize=7)
    fig.colorbar(im, ax=ax, fraction=0.03, label="-log10 FDR (over-representation)")
    ax.set_title(title + "\nnumbers: family genes in the cluster; bold: FDR < 0.05", fontsize=10)
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("clusters")
    ap.add_argument("annotation")
    ap.add_argument("out")
    ap.add_argument("--tag", default="")
    ap.add_argument("--stable-only", action="store_true")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    ann = Path(args.annotation)
    tag = args.tag + ("_stable_only" if args.stable_only else "")

    clusters = pd.read_csv(args.clusters, index_col=0)
    if args.stable_only:
        clusters = clusters[clusters.stable]
    names = clusters.gene_name.fillna("").astype(str)
    GENE_NAME.update({g: (n or g) for g, n in names.items()})
    genes = {g: n for g, n in names.items() if n and not n.startswith("ACP6EX_")}
    labels = clusters.cluster

    rng = np.random.default_rng(SEED)
    coded = labels.map({c: i for i, c in enumerate(np.sort(labels.unique()))}).to_numpy()
    perms = np.stack([rng.permutation(coded) for _ in range(PERMUTATIONS)])

    sources = {"GO": go_families(ann, genes), "KEGG": kegg_families(ann, genes), "name": name_families(genes)}
    tables, lines = [], [f"{len(labels)} clustered genes in {labels.nunique()} clusters; "
                         f"{len(genes)} carry a gene name; {PERMUTATIONS} permutations; seed {SEED}"]
    for source, (families, fam_names) in sources.items():
        mapped = len(set().union(*families.values())) if families else 0
        t = test_source(source, families, fam_names, labels, perms)
        tables.append(t)
        tested = t.family.nunique() if not t.empty else 0
        lines.append(f"{source}: {mapped} genes annotated, {tested} families with >= {MIN_GENES} genes tested")
        if t.empty:
            continue
        spread = t.drop_duplicates("family")
        lines.append(f"  spread non-randomly across clusters (permutation FDR < {FDR}): {int((spread.permutation_q < FDR).sum())}")
        hits = t[(t.fisher_q < FDR) & (t.fold > 1)].sort_values("fisher_q")
        lines.append(f"  over-represented family x cluster cells (Fisher FDR < {FDR}): {len(hits)}")
        for _, h in hits.head(25).iterrows():
            lines.append(f"    cluster {h.cluster}: {h['name'][:60]} ({h.family}) {h.in_cluster}/{h.genes_clustered} "
                         f"(expected {h.expected}), q={h.fisher_q:.2g}: {h.genes[:120]}")
    table = pd.concat(tables, ignore_index=True)
    table.sort_values(["fisher_q", "permutation_q"]).to_csv(out / f"family_enrichment{tag}.csv", index=False)
    plot(table, f"Gene families over-represented per cluster{tag.replace('_', ' ')}", out / f"family_enrichment{tag}.png")
    (out / f"family_enrichment_summary{tag}.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
