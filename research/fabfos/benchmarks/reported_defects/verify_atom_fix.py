"""End-to-end: cached maps -> new extractor gate -> fusion, for the two reported reactions."""
import sys
import pandas as pd
sys.path.insert(0, "src")
from rdkit import Chem, RDLogger
RDLogger.DisableLog("rdApp.*")
from ecspr.bake import atom_pairs as AP
from ecspr.bake.aam import layers as L

MNX = "data/fabfos/originals/metanetx/4.5"
CACHE = "data/fabfos/processed/metabolism_bake/aam_cache"
RXNS = ["MNXR166070", "MNXR206779"]
NAME = {"MNXM1104186": "L-rhamnulose-1-P", "MNXM731755": "L-rhamnulose",
        "MNXM731756": "L-rhamnulose(alt)", "MNXM3": "ATP", "MNXM40333": "ADP",
        "MNXM1": "H+", "MNXM38657": "L-rhamnulose(twin frame)"}

eqs = AP.load_equations(__import__("pathlib").Path(f"{MNX}/reac_prop.tsv"), set(RXNS))
parsed = {r: AP.parse_equation(e) for r, e in eqs.items()}
want = set()
for s, p in parsed.values():
    want |= set(s) | set(p)
raw = AP.load_mnxm_smiles(__import__("pathlib").Path(f"{MNX}/chem_prop.tsv"), want)
canon, conn = {}, {}
for m, smi in raw.items():
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        continue
    cs, k = AP.frag_keys(mol)
    if cs: canon[m] = cs
    if k: conn[m] = k

for mode in ("trim",):
    per_member = {}
    for mem in ("indigo", "rxnmapper", "localmapper"):
        c = pd.read_csv(f"{CACHE}/{mem}/cache.tsv", sep="\t")
        c = c[c["mnxr"].isin(RXNS)]
        rows = []
        for r in c.itertuples(index=False):
            subs, prods = parsed[r.mnxr]
            st = {}
            pairs, status = AP.pairs_from_mapped(
                r.mapped_rxn_smiles, subs, prods, canon, conn=conn, align="strict",
                on_noninjective=mode, stats=st)
            print(f"  {mem:<12} {r.mnxr}  status={status:<22} dup_numbers={st.get('n_dup_map',0)}")
            for (el, sm, pm), v in pairs.items():
                for a, b, w in v:
                    rows.append(dict(mnxr=r.mnxr, element=el, substrate=sm, product=pm,
                                     sub_idx=int(a), prod_idx=int(b), pair_w=float(w),
                                     method=mem, source=mem, confidence=float(r.confidence)))
        if rows:
            per_member[mem] = pd.DataFrame(rows)[list(L.ATOM_COLS)]
    coef = L.load_coefficients(f"{MNX}/reac_prop.tsv")
    fused, _t = L.fuse_members(per_member, coefficients=coef)
    for r in RXNS:
        g = fused[(fused.mnxr == r) & (fused.element == "C")
                  & (fused.substrate == "MNXM1104186")]
        print(f"\n=== {r}  carbon out of L-rhamnulose-1-phosphate ({mode})")
        for _i, row in g.sort_values(["sub_idx", "product"]).iterrows():
            print(f"    rank {row.sub_idx:<3} -> {NAME.get(row['product'], row['product']):<24}"
                  f" pair_w {row.pair_w:.3f}   {row.method}")
        atp = g[g["product"] == "MNXM3"]
        print(f"    carbons reaching ATP: {len(atp)}   distinct sugar ranks: {g.sub_idx.nunique()}")
        adp = fused[(fused.mnxr == r) & (fused.element == "C")
                    & (fused.substrate == "MNXM40333")]
        print(f"    ADP within-molecule rows kept: {len(adp)}  "
              f"(diluted: {int((adp.pair_w < 1.0).sum())})")
