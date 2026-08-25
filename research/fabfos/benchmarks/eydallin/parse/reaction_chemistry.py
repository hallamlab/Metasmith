"""Reaction chemistry for a cohort table: the equation, the carbon-bond call, and the
rules for choosing one reaction per gene out of a channel that offers several.

Shared by `build_gof_reactions.py` (2010, ASKA overexpression, DH1's GEM) and
`build_lof_reactions.py` (2007, Keio deletions, BW25113's iML1515). Everything here takes
frames and strings rather than paths, so a cohort's own driver owns which tables it reads
and this module owns what the answers mean. Two copies of these rules is how two halves of
one benchmark come to disagree about what `breaks` means.
"""
from __future__ import annotations

import re

import pandas as pd

EQUATION_TERM_RE = re.compile(r"(\d+(?:\.\d+)?) (\S+?)@\S+")

DENOVO_MAX_CANDIDATES = 10


class _UnionFind:
    def __init__(self):
        self.parent: dict = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def carbon_bond_change(pairs_for_mnxr: pd.DataFrame) -> str:
    """Does this reaction change a carbon skeleton, read off the baked atom map rather
    than guessed from the equation string?

    `pairs_for_mnxr` is `atom_pairs`'s rows for one `mnxr`. That table already pairs each
    carbon atom on the substrate side to the carbon atom it becomes on the product side.
    Group those pairs into connected components by shared atoms: a component spanning more
    than one SUBSTRATE molecule means those molecules' carbons became bonded (`creates`);
    a component spanning more than one PRODUCT molecule means one molecule's carbons came
    apart (`breaks`); a component touching more than one molecule on both sides is `both`;
    every component 1:1 is `no_change`.

    A reaction with no carbon atoms in the map at all -- nothing in `in_atom_universe`, or
    no carbon on either side, e.g. a proton antiport -- returns the empty string. That is
    UNKNOWN, not "no change", and the two must not be collapsed.

    Checked against three reactions: HSK (homoserine kinase, `MNXR100737`) is a clean 1:1
    phosphorylation -> `no_change`; 5'-deoxyadenosine nucleosidase (`MNXR152703`) splits
    one molecule into adenine + ribose -> `breaks`; glucosamine-6-phosphate deaminase
    (`MNXR115917`) is a 1:1 deamination -> `no_change`.
    """
    c = pairs_for_mnxr[pairs_for_mnxr.element == "C"]
    if c.empty:
        return ""

    uf = _UnionFind()
    for sub_met, prod_met in c[["substrate", "product"]].drop_duplicates().itertuples(index=False):
        s_node, p_node = ("S", sub_met), ("P", prod_met)
        uf.find(s_node)
        uf.find(p_node)
        uf.union(s_node, p_node)

    components: dict = {}
    for node in uf.parent:
        components.setdefault(uf.find(node), []).append(node)

    creates = breaks = False
    for members in components.values():
        n_sub = sum(1 for k, _ in members if k == "S")
        n_prod = sum(1 for k, _ in members if k == "P")
        if n_sub > 1:
            creates = True
        if n_prod > 1:
            breaks = True

    if creates and breaks:
        return "both"
    if creates:
        return "creates"
    if breaks:
        return "breaks"
    return "no_change"


def legible_equation(equation: str, names: dict[str, str]) -> str:
    """MetaNetX writes `1 MNXM1105977@MNXD1 = 1 MNXM40333@MNXD1 + ...` -- ids and a bare
    `=`, compartment tag included even though every compound in these cohorts' reactions
    sits in one compartment. Swap each id for `metabolites.parquet`'s name, drop the
    compartment tag, drop a bare `1` coefficient, and write the arrow a stoichiometry
    reader actually reads left-to-right as consumed -> produced."""
    lhs, rhs = equation.split(" = ")

    def side(text: str) -> str:
        terms = []
        for coeff, mid in EQUATION_TERM_RE.findall(text):
            name = names.get(mid, mid)
            terms.append(name if coeff == "1" else f"{coeff} {name}")
        return " + ".join(terms)

    return f"{side(lhs)} -> {side(rhs)}"


def pick_primary(candidates: pd.DataFrame) -> tuple[pd.Series | None, str]:
    """One reaction per gene out of a GEM channel's candidates, plus the reason.

    THIS IS AN in_atom_universe TIE-BREAK, NOT AN EVIDENCE RANKING, BECAUSE THERE IS NO
    EVIDENCE TO RANK ON YET: every row in a `gpr_gem.parquet` today carries
    `raw_score=1.0` and `evidence_quality="unknown"` -- presence/absence GPR calls, not
    scored ones. Ranking on a constant would be an arbitrary row order dressed up as a
    ranking. `in_atom_universe` is the one real discriminator available -- it says whether
    the reaction has atom-mapping data behind it at all, which is also exactly what
    `carbon_bond_change` needs -- so the pick prefers `in_atom_universe=True`, then breaks
    any remaining tie on `mnxr` for determinism. When these tables grow real per-reaction
    scores this tie-break is the first thing to replace."""
    if candidates.empty:
        return None, ""
    ordered = candidates.sort_values(
        by=["in_atom_universe", "raw_score", "mnxr"],
        ascending=[False, False, True],
    )
    reason = ("only candidate" if len(ordered) == 1 else
              "in_atom_universe tie-break" if ordered.iloc[0]["in_atom_universe"] else
              "mnxr tie-break (no in_atom_universe candidate)")
    return ordered.iloc[0], reason


def denovo_gapfill(gpr_denovo: pd.DataFrame, genes: set[str],
                   key: str = "gene") -> dict[str, tuple[str, str]]:
    """The de-novo channel is noisy by design -- ~3 independent projection methods (HMM/KO
    bitscore, BLAST reciprocal-best-hit, an embedding k-NN vote) each throw candidates at
    a gene, on scales that aren't comparable to each other (a bitscore of 295 and a
    knn_vote of 0.04 aren't the same kind of number), so ranking by `raw_score` the way
    `pick_primary` does for the GEM channel would just reward whichever method happens to
    score highest, not the gene's actual reaction. What IS comparable across methods is
    agreement: gap-fill a gene only when >=2 of those independent methods land on the exact
    same `mnxr`, the way `resolve_gene_manual.py`'s own two-method-agreement check works
    for the GEM channel. And only when the candidate pool is small (<=10) -- `ylcG` has 99
    candidates because its EC hit (3.1.1.4, a broad phospholipase class) enumerates every
    reaction MetaNetX carries under that EC number, so its top knn_vote pick landing inside
    that huge pool is a coincidence of pool size, not real convergence, and is excluded
    here even though it technically clears the 2-method bar.

    `key` is the column holding the caller's gene identity -- the two cohorts resolve their
    de-novo tables on different ids, and this rule does not care which."""
    out: dict[str, tuple[str, str]] = {}
    sub = gpr_denovo[gpr_denovo[key].isin(genes)]
    for gene, g in sub.groupby(key):
        if g["mnxr"].nunique() > DENOVO_MAX_CANDIDATES:
            continue
        agreement = g.groupby("mnxr")["score_kind"].nunique()
        qualifying = agreement[agreement >= 2]
        if len(qualifying) == 1:
            mnxr = qualifying.index[0]
            methods = sorted(g[g["mnxr"] == mnxr]["score_kind"].unique())
            out[gene] = (mnxr, f"de-novo channel, {len(methods)} independent "
                         f"methods agree ({', '.join(methods)})")
    return out
