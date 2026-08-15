#!/usr/bin/env python3
"""Reaction distance from the Eydallin carbohydrate hits to glycogen, on the carbon graph.

Three bases, reported side by side, because the hub policy and the host restriction each
decide the answer more than the chemistry does:
  bake/full      -- every reaction with a carbon atom-pair row in data/fabfos/processed/metabolism_bake
  bake/host      -- the same table restricted to the reactions iML1515 gives K-12
  bake/host+glc  -- host, cofactors barred but D-glucose left in, to show what that
                    single hub does to every distance in the table

The retired tier4 reference is deliberately absent: on that basis this whole column came
back empty except glgB, because glgA and glgP carried no carbon rows there at all.

Distance counts REACTIONS: 0 = the reaction touches a glycogen node itself, 1 = it
shares a carbon-carrying metabolite with such a reaction, and so on. Named cofactor
hubs are barred as intermediates -- without that, everything is two steps from
everything through the ATP->ADP adenosine carbons. PEP, pyruvate and the sugar
phosphates are deliberately NOT barred: they are the actual chemistry here.

Gene -> b-number resolution goes through NC_000913.3, never through names: nagD is
umpH/b0675 and aspP is nudF/b3034 in the annotation, and both read as "absent from
the model" if you match iML1515's gene column by name.
"""
import collections
import re
from pathlib import Path

import pandas as pd

# Everything this reads is pinned in this repository. It used to be read out of
# the fabfos bench-aska worktree, which is archived and read-only now.
R = str(Path(__file__).resolve().parents[4])
BAKE = f"{R}/data/fabfos/processed/metabolism_bake"

GENES = ["ptsI", "ptsN", "nagB", "nagD", "malP", "gntT", "xylG", "rpiB", "talA", "rbsR",
         "glgA", "glgC", "glgB", "glgP", "aspP"]
GLYCOGEN = ["MNXM738130", "MNXM738131", "MNXM8348"]

# Hub policies. Ids resolved by exact chem_prop name below, and printed, because a
# guessed MNXM is how you silently fail to bar a hub (MNXM26 is acetate, not glutamate).
#   cofactor -- the carriers whose carbon skeleton rides through unchanged. Without
#               these barred, every reaction is 2 hops from every other via ATP->ADP.
#   central  -- adds the free-metabolite hubs above ~300 reactions. D-glucose is the
#               one that matters here: it is one hop from glycogen via a hydrolase, so
#               leaving it in makes every sugar transporter look adjacent.
COFACTOR_NAMES = [
    "ATP", "ADP", "AMP", "CoA", "acetyl-CoA", "malonyl-CoA",
    "NAD(+)", "NADH", "NADP(+)", "NADPH", "FAD", "FADH2", "Flavin", "Reduced flavin",
    "CO2", "hydrogencarbonate", "UDP", "CMP", "GDP", "UMP", "UTP", "GTP", "CTP",
    "dTTP", "dTDP", "dTMP", "S-adenosyl-L-methionine", "S-adenosyl-L-homocysteine",
    "glutathione", "adenosine 3',5'-bisphosphate",
]
CENTRAL_NAMES = [
    "D-glucose", "pyruvate", "acetate", "succinate", "L-glutamate", "glycine",
    "L-alanine", "formate", "L-methionine", "2-oxoglutarate",
]

# ---------------------------------------------------------------- reference tables
v = pd.read_parquet(f"{BAKE}/vocab.parquet")
sym = v[v.kind == "met"].set_index("code").symbol.to_dict()
rsym = v[v.kind == "rxn"].set_index("code").symbol.to_dict()
ap = pd.read_parquet(f"{BAKE}/atom_pairs.parquet")
bake_c = (ap[ap.element == 0][["rxn", "tail_met", "head_met"]]
          .assign(rxn=lambda d: d.rxn.map(rsym),
                  tail_met=lambda d: d.tail_met.map(sym),
                  head_met=lambda d: d.head_met.map(sym))
          .drop_duplicates())

gpr = pd.read_parquet(f"{R}/data/fabfos/benchmarks/hosts/e_coli_k12/gpr_gem.parquet")
host_rxn = set(gpr.mnxr)

cp = pd.read_csv(f"{R}/data/fabfos/originals/metanetx/4.5/chem_prop.tsv", sep="\t", comment="#",
                 header=None, usecols=[0, 1], names=["id", "name"], dtype=str,
                 low_memory=False).set_index("id").name.to_dict()

name_to_id = {}
for _id, _nm in cp.items():
    name_to_id.setdefault(_nm, _id)
COFACTOR = {name_to_id[n] for n in COFACTOR_NAMES if n in name_to_id}
CENTRAL = COFACTOR | {name_to_id[n] for n in CENTRAL_NAMES if n in name_to_id}
print("barred as cofactor hubs:", ", ".join(sorted(f"{cp[i]}" for i in COFACTOR)))
print("central policy also bars:", ", ".join(sorted(cp[i] for i in CENTRAL - COFACTOR)))
print()

gbk = open(f"{R}/data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk",
           errors="ignore").read()
# PRIMARY /gene= NAMES WIN OVER SYNONYMS, and the two maps are kept apart. nagE
# (b0679) carries `ptsN` as an obsolete synonym while the real ptsN is b3204, so a
# synonym-first map hands the nitrogen-PTS regulator NagE's transport reactions and
# a plausible-looking distance. Same class of trap as rfaY/waaY, opposite direction.
PRIMARY, SYN = {}, {}
for blk in re.findall(r"\n     gene {12}.*?(?=\n     \w)", gbk, re.S):
    lt = re.search(r'/locus_tag="(b\d+)"', blk)
    if not lt:
        continue
    for n in set(re.findall(r'/gene="([^"]+)"', blk)):
        PRIMARY.setdefault(n, lt.group(1))
    syn = re.search(r'/gene_synonym="([^"]*)"', blk, re.S)
    if syn:
        for n in (x.strip() for x in syn.group(1).replace("\n", " ").split(";")):
            if n:
                SYN.setdefault(n, lt.group(1))
BNUM = {**SYN, **PRIMARY}


# ---------------------------------------------------------------- graph + BFS
def basis(sub, hubs):
    m2r, r2m = collections.defaultdict(set), collections.defaultdict(set)
    for rx, t, h in sub.itertuples(index=False):
        for m in (t, h):
            if m in hubs or m is None:
                continue
            m2r[m].add(rx)
            r2m[rx].add(m)
    dist, prev = {}, {}
    frontier = set()
    for g in GLYCOGEN:
        for r in m2r.get(g, ()):
            dist[r] = 0
            prev[r] = (g, None)
            frontier.add(r)
    d = 0
    while frontier:
        d += 1
        nxt = set()
        for r in frontier:
            for m in r2m[r]:
                for r2 in m2r[m]:
                    if r2 not in dist:
                        dist[r2] = d
                        prev[r2] = (m, r)
                        nxt.add(r2)
        frontier = nxt
    return dist, prev, m2r, r2m


def route(rx, dist, prev):
    """rendered shortest path: rxn -> met -> rxn -> ... -> glycogen"""
    out = []
    cur = rx
    while cur is not None:
        m, nxt = prev[cur]
        out.append(f"{cur} -[{str(cp.get(m, m))[:22]}]->")
        cur = nxt
    return " ".join(out) + " glycogen"


_bake_host = bake_c[bake_c.rxn.isin(host_rxn)]
BASES = {
    "bake/full": basis(bake_c, CENTRAL),
    "bake/host": basis(_bake_host, CENTRAL),
    "bake/host+glc": basis(_bake_host, COFACTOR),
}

rows = []
for gene in GENES:
    b = BNUM.get(gene)
    how = "primary" if gene in PRIMARY else ("synonym" if gene in SYN else "-")
    sel = gpr[gpr.feature_id == b] if b else gpr.iloc[:0]
    rxns = sorted(set(sel.mnxr))
    out = {"gene": gene, "b": b or "-", "via": how, "n_rxn": len(rxns)}
    for name, (dist, prev, _, _) in BASES.items():
        ds = [(dist[r], r) for r in rxns if r in dist]
        out[name] = min(ds)[0] if ds else None
        if name == "bake/host" and ds:
            out["_r"] = min(ds)[1]
    rows.append(out)

df = pd.DataFrame(rows)
OUT = Path(__file__).resolve().parent / "cache"
OUT.mkdir(exist_ok=True)
df.drop(columns=["_r"], errors="ignore").to_csv(OUT / "reach_to_glycogen.tsv", sep="\t", index=False)
print("=== reaction hops to glycogen (0 = the reaction touches a glycogen node) ===")
print(df.drop(columns=["_r"]).fillna("-").to_string(index=False))

print("\n=== shortest route on bake/host ===")
for r in rows:
    if r.get("_r"):
        dist, prev, _, _ = BASES["bake/host"]
        print(f"{r['gene']:6} {route(r['_r'], dist, prev)}")

print("\n=== glycogen's neighbourhood (bake/full) ===")
_, _, m2r, r2m = BASES["bake/full"]
gene_of = gpr.groupby("mnxr").feature_name.apply(lambda s: ",".join(sorted({x for x in s if x})))
for g in GLYCOGEN:
    inc = sorted(m2r.get(g, ()))
    print(f"\n{g} ({cp.get(g)}): {len(inc)} incident carbon reactions")
    for rx in inc:
        partners = ", ".join(f"{str(cp.get(p, p))[:24]}" for p in sorted(r2m[rx]) if p != g)
        print(f"   {rx:12} host={'Y' if rx in host_rxn else '.'} "
              f"{gene_of.get(rx, '-'):12} -> {partners}")

# Carbon coverage of the glycogen machinery itself. A reaction with zero carbon rows
# contributes no edge, so this table is what decides whether the polymer is reachable
# at all -- it is the quantity the retired tier4 basis got wrong.
print("\n=== the glg machinery: carbon rows on the bake ===")
print(f"{'reaction':12} {'gene':10} {'in_host':8} {'bake_C':8}")
rc_of = {s: c for c, s in rsym.items()}
for mn, g in [("MNXR145046", "glgA"), ("MNXR145050", "glgC"), ("MNXR145021", "glgB"),
              ("MNXR145036", "glgP/malP"), ("MNXR145038", "glgP/malP"),
              ("MNXR132767", "KEGG-side"), ("MNXR172038", "-"), ("MNXR145023", "-")]:
    rc = rc_of.get(mn)
    nb = int(((ap.rxn == rc) & (ap.element == 0)).sum()) if rc is not None else 0
    print(f"{mn:12} {g:10} {'Y' if mn in host_rxn else 'N':8} {nb:8}")
