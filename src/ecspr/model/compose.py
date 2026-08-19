"""Compose several organisms into ONE atom network: private copies joined by bridges.

    {org: gpr_table} + the reference atom-pair table
        -> a composed atom-pair table + a composed GPR table + a bridge report

A COMBINED NETWORK IS NOT A UNION OF REACTION SETS. Two organisms that both carry MNXR1
are not one reaction at a summed conductance; they are two reactions, one inside each
organism, and a molecule made by one is only available to the other if it crosses between
them. So each member is built in ISOLATION with its own copy of every metabolite, and the
copies are joined by explicit BRIDGE edges. Union semantics would have made every
community measurement a measurement of one well-mixed pot with a bigger gene set, which is
the null this experiment is trying to reject.

THE WHOLE CONSTRUCTION IS A PAIR-TABLE TRANSFORM
------------------------------------------------
A graph node is the plain tuple `(metabolite_string, atom_rank)`, and every terminal,
leak and report path in :mod:`ecspr.model.graph` keys off that string. So a private copy is
bought by PREFIXING the metabolite ids (`NOS:MNXM1364061`), and a bridge is one more pair row with a
pseudo-reaction id. Nothing in the graph builder, the solver or either measurement
transform learns that communities exist -- which is the same indifference that lets those
transforms treat a knockout and a null draw as "another GPR table".

Two consequences worth stating because they are load-bearing rather than incidental:

  * The composed pair table is NOT network-agnostic, unlike the reference bake it is cut
    from. It must be staged per network, never shared -- see `ecspr::atom_pairs`.
  * A bridge carries its conductance in `pair_w`, because `graph_from_pairs` forms
    `gp = E_r * pair_w` and drops any reaction absent from the weight dict. One synthetic
    ORF per bridge reaction puts it in the weight dict through the ordinary evidence path,
    so `sum(belief_mass) == n_orfs` still holds exactly and the driver's conservation
    assert stays meaningful instead of being weakened for us. That ORF spends its whole
    1.0 on one pseudo-reaction, so every bridge pools to the SAME `E_r` --
    `evidence.pooled_E_of_mass(1.0)`, not 1.0 -- and `pair_w` is divided by it so the
    product is the `g_bridge` the bridge table computed.

THE BRIDGE WEIGHT IS A GENE-LEVEL MISMATCH, DILUTED TWICE
---------------------------------------------------------
The unit is the GENE, not the reaction, and a gene's belief is diluted on the way down:

    e_g(r)  ORF g's belief on reaction r -- `ecspr.model.evidence.nomination_contributions`,
            summing to exactly 1.0 per ORF. The first dilution: a gene spread over many
            reactions contributes proportionally less to any one of them.

    c_g(m) = SUM_r e_g(r) * 1[m in mets_E(r)] / |mets_E(r)|
            The second dilution, splitting a reaction's belief across the metabolites it
            actually touches in THIS element. Bridges only: the reaction conductances
            `E_full` are never diluted this way.

    M_o(m) = SUM_{g in o} c_g(m)                    the diluted gene count
    D(m)   = |M_A - M_B| / (M_A + M_B)              RELATIVE asymmetry, in [0, 1]

`M` counts GENES, so it reads `belief_mass` -- the pre-pooling ledger, in ORF units. `G`
is a CONDUCTANCE and reads the pooled `E_r`, the same numbers the member edges carry.
They were one quantity before pooling and are two now, and the map between them is not a
rescale `g0` could absorb: it is compressive at the top and floored at `sigma(lam0)` at
the bottom, so feeding mass into `G` mis-scales each bridge by a factor that runs from
0.009 to 103 across the carbon metabolites of one genome. A metabolite whose neighbours
are well-corroborated would get a bridge ~10x too strong against a network that had
moved, while a sparsely-annotated one got 100x too weak.

`D` is relative rather than a raw difference because the raw difference is 96% correlated
with metabolite size (corr = 0.963 on this community): Nostoc carries 1.86x
Erythrobacter's ORFs and so leads almost everywhere, and ATP's raw gap of 103 against a
mean of 0.21 would have made one cofactor carry ~500x the typical bridge. Relative
asymmetry is genome-size-free, and what it surfaces is specialization -- biliverdin IX
alpha and L-histidine at the top, ATP and CoA down at 0.26 and 0.24.

    g_bridge(m) = g0 * D(m) * min(G_A(m), G_B(m)) / k(m)

`D` is dimensionless by construction so the SCALE has to come from somewhere: it comes
from the weaker partner's own incident conductance, i.e. exchange is limited by the side
less able to carry it. `k(m)` is the number of atom ranks the two copies share, and the
bridge is emitted once per shared rank, so the total bridge conductance for a metabolite
is `g0 * D * min(G_A, G_B)` however many atoms it has -- the same fanout dilution
`attach_leak` applies, and for the same reason: otherwise the readout is a proxy for
molecule size. `g0` is a free parameter and is reported, never hidden.

Zero mismatch means zero exchange, as written. That is strong -- two organisms using a
metabolite identically arguably exchange it most freely -- but a floor term would be a
second free parameter, so the sweep over `g0` is the honest way to find out whether it
matters.

BRIDGES ARE SYMMETRIC, DELIBERATELY. `ratio = 1.0`, so the edge is an ordinary resistor.
There is no evidence about which way a metabolite crosses between two organisms, and
handing the direction ensemble's ratios to a bridge would let DIRECTION evidence
manufacture a claim about TRANSPORT that nothing in the bake ever made.

THE CARRIER BLACKLIST, AND WHY THE EXPERIMENT RUNS TWICE
--------------------------------------------------------
Reaction degree is savagely skewed: the median metabolite is touched by 2 reactions, but
ATP, NADP(+), NADPH, CoA and phosphate sit between 4,300 and 5,400. Bridge those and
current crosses A -> B through the cofactor pool and returns through another, and every
solve reports the two organisms as one well-mixed pot while looking entirely healthy.
`carrier_blacklist` is the answer: one GLOBAL list -- a metabolite is a carrier or it is
not, in every element -- built on the principle *blacklist the carrier, not the cargo*,
i.e. what does not cross a membrane. CO2, acetate, amino acids and free biotin are kept
bridgeable on exactly that test.

The list is authored as name RULES and materialised against a metabolite name table, and
the resolved ids are written out with the composition so the reviewable artifact is the id
list rather than the patterns. And because the blacklist is a modelling choice and not a
fact, the whole experiment is run twice, with it and without it. Two arms that come back
indistinguishable would mean either the blacklist does nothing or the bridges carry no
current -- both worth knowing, and neither visible from one arm.

Env: numpy + pandas (CPU).
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

from . import conditions as cond_mod
from . import evidence as _net

ELEMENTS = ("C", "N", "P", "S")
SEP = ":"
BRIDGE_CHANNEL = "bridge"

BRIDGE_COLS = ["element", "org_a", "org_b", "metabolite", "name",
               "M_a", "M_b", "asymmetry", "G_a", "G_b", "k_atoms",
               "g_bridge", "blacklisted", "bridged"]


def tag(org: str, mid: str) -> str:
    return f"{org}{SEP}{mid}"


def untag(tagged: str) -> tuple:
    org, sep, mid = str(tagged).partition(SEP)
    return (org, mid) if sep else ("", str(tagged))


# =====================================================================
# The carrier blacklist
# =====================================================================
#
# Authored as rules, materialised as ids. Each entry is (class, kind, pattern) where kind
# is `exact` (case-insensitive full name) or `sub` (case-insensitive substring). Substring
# rules are what cover MetaNetX's many synonym rows for one carrier -- thioredoxin alone
# has nine -- which an exact list silently misses, leaving a degree-450 hub bridged.
CARRIER_RULES = [
    # Phosphoryl / energy. Exact, because a substring on "ATP" also catches dATP, which is
    # a biomass precursor and cargo.
    *[("phosphoryl", "exact", n) for n in (
        "ATP", "ADP", "AMP", "GTP", "GDP", "GMP", "UTP", "UDP", "UMP",
        "CTP", "CDP", "CMP", "ITP", "IDP", "IMP",
        "phosphate", "diphosphate", "triphosphate")],
    # Nicotinamide.
    *[("nicotinamide", "exact", n) for n in (
        "NAD(+)", "NADH", "NADP(+)", "NADPH", "NAD(P)", "NAD(P)H", "NAD(P)(+)")],
    # Flavin. FAD/FMN carry no "flavin" in the name, so both forms are needed.
    *[("flavin", "exact", n) for n in ("FAD", "FADH2", "FMN", "FMNH2")],
    ("flavin", "sub", "flavin"),
    # Quinone pool. NOT a bare "quinone" substring -- that catches anthraquinone and the
    # asterriquinones, which are metabolites in their own right.
    *[("quinone", "sub", n) for n in (
        "ubiquinone", "ubiquinol", "menaquinone", "menaquinol",
        "plastoquinone", "plastoquinol", "demethylmenaquin")],
    *[("quinone", "exact", n) for n in ("a quinone", "a quinol")],
    # Protein and thiol redox carriers.
    *[("redox_protein", "sub", n) for n in (
        "ferredoxin", "thioredoxin", "glutaredoxin", "rubredoxin",
        "lipoamide", "dihydrolipoamide")],
    *[("redox_protein", "exact", n) for n in ("glutathione", "glutathione disulfide")],
    # Acyl carriers. The [ACP] series entire; on the CoA side only free CoA and acetyl-CoA
    # -- a narrower cut than strict consistency would give, taken deliberately, because
    # the longer acyl-CoAs are cargo more than carrier.
    ("acyl", "sub", "acyl-carrier protein"),
    *[("acyl", "exact", n) for n in ("CoA", "acetyl-CoA")],
    # One-carbon carriers.
    *[("one_carbon", "sub", n) for n in ("tetrahydrofolate", "cobalamin", "tetrahydromethanopterin")],
    *[("one_carbon", "exact", n) for n in (
        "S-adenosyl-L-methionine", "S-adenosyl-L-homocysteine")],
    # Sulfuryl.
    ("sulfuryl", "sub", "adenylyl sulfate"),
    # Aldehyde (thiamine diphosphate and its adducts).
    ("aldehyde", "sub", "thiamine diphosphate"),
    # Glycosyl donors. Exact: UDP-N-acetyl-alpha-D-glucosamine is a cell-wall PRECURSOR
    # this experiment reads as an endpoint, so a "UDP-" substring would blacklist a sink.
    *[("glycosyl", "exact", n) for n in (
        "UDP-alpha-D-glucose", "ADP-alpha-D-glucose", "GDP-alpha-D-mannose",
        "UDP-alpha-D-galactose", "dTDP-alpha-D-glucose",
        "a CDP-diacylglycerol", "di-trans,poly-cis-Undecaprenyl phosphate")],
]

# Kept bridgeable on the *does it cross a membrane* test, regardless of degree. CO2 is
# degree-2,803 and the single most important exchange in a phototroph community; free
# biotin is the one independent check this experiment has (the ad-hoc annotation found
# Allorhizobium cannot complete its biosynthesis alone), and its carrier role is
# protein-bound, not free.
CARRIER_KEEP = ("CO2", "acetate", "biotin", "thiamine", "L-glutamate", "L-glutamine",
                "ammonium", "NH4(+)", "sulfate", "H2S", "hydrogen sulfide")


def carrier_blacklist(names: pd.DataFrame, *, extra=(), keep=CARRIER_KEEP) -> pd.DataFrame:
    uniq = names.drop_duplicates("mnxm")[["mnxm", "name"]].copy()
    uniq["_lc"] = uniq["name"].astype(str).str.lower()
    keep_lc = {str(k).lower() for k in keep}

    hits = {}
    for cls, kind, pat in list(CARRIER_RULES) + list(extra):
        p = str(pat).lower()
        if kind == "exact":
            sel = uniq["_lc"] == p
        elif kind == "sub":
            sel = uniq["_lc"].str.contains(p, regex=False, na=False)
        else:
            raise ValueError(f"unknown rule kind {kind!r} (want 'exact' or 'sub')")
        for m, n, lc in uniq.loc[sel, ["mnxm", "name", "_lc"]].itertuples(index=False):
            if lc in keep_lc:
                continue
            hits.setdefault(m, (n, cls))
    return (pd.DataFrame([(m, n, c) for m, (n, c) in hits.items()],
                         columns=["mnxm", "name", "carrier_class"])
            .sort_values(["carrier_class", "name"], ignore_index=True))


# =====================================================================
# The gene-level mismatch
# =====================================================================

def reaction_beliefs(gpr: pd.DataFrame) -> pd.Series:
    return _net.belief_mass(gpr, "compose")


def reaction_conductances(gpr: pd.DataFrame) -> pd.Series:
    return _net.compute_E(gpr, "compose")


def diluted_gene_counts(beliefs: pd.Series, pairs: pd.DataFrame) -> pd.Series:
    # `M_o(m)` -- the SECOND dilution, spreading each reaction's belief across the
    # metabolites it touches in this element's atom-pair table.
    #
    # Per-gene and per-reaction forms agree exactly (the sum over genes commutes with the
    # split over metabolites), so this takes the reaction aggregate and is O(pair rows).
    #
    # A reaction with no atom-pair row in this element contributes nothing, which is the AAM
    # gap leaking out where it should: for carbon on Nostoc the metabolite-level mass comes
    # to 4,162 against 5,921 ORFs, and the shortfall is coverage, not a bug.
    df = pairs[pairs.mnxr.isin(beliefs.index)]
    if not len(df):
        return pd.Series(dtype=float)
    touch = pd.concat([df[["mnxr", "substrate"]].rename(columns={"substrate": "met"}),
                       df[["mnxr", "product"]].rename(columns={"product": "met"})])
    touch = touch.drop_duplicates()
    n_mets = touch.groupby("mnxr")["met"].transform("size")
    touch["c"] = touch["mnxr"].map(beliefs).astype(float) / n_mets
    return touch.groupby("met")["c"].sum()


def incident_conductance(weights: pd.Series, pairs: pd.DataFrame) -> pd.Series:
    df = pairs[pairs.mnxr.isin(weights.index)]
    if not len(df):
        return pd.Series(dtype=float)
    g = df.mnxr.map(weights).astype(float).to_numpy() * \
        pd.to_numeric(df.get("pair_w", 1.0), errors="coerce").fillna(1.0).to_numpy()
    both = pd.concat([pd.Series(g, index=df["substrate"].to_numpy()),
                      pd.Series(g, index=df["product"].to_numpy())])
    return both.groupby(level=0).sum()


def atom_ranks(beliefs: pd.Series, pairs: pd.DataFrame) -> dict:
    df = pairs[pairs.mnxr.isin(beliefs.index)]
    out = {}
    for met, idx in (list(zip(df["substrate"].to_numpy(), df["sub_idx"].to_numpy()))
                     + list(zip(df["product"].to_numpy(), df["prod_idx"].to_numpy()))):
        out.setdefault(met, set()).add(int(idx))
    return out


def bridge_table(members: dict, pairs: pd.DataFrame, element: str, conductances: dict, *,
                 g0=1.0, blacklist=frozenset(), names=None, ranks=None) -> pd.DataFrame:
    # One row per (organism pair, metabolite) candidate bridge, for one element.
    #
    # Every candidate is reported, including the ones that get no edge -- blacklisted, or
    # present in only one copy. A skipped bridge that leaves no trace would make a coverage
    # hole read as a modelling choice.
    #
    # `members` is `{org: belief_mass}` and sets the mismatch `D`; `conductances` is
    # `{org: E_r}` and sets the scale `G`. See the module docstring for why those are two
    # maps and not one.
    #
    # `ranks` is the `{org: {met: ranks}}` map from `atom_ranks`; it is passed in rather than
    # recomputed because the caller needs the same map to emit one edge per shared rank, and
    # the two must agree exactly or `k` divides by an atom count no edge was built for.
    orgs = sorted(members)
    el = pairs[pairs.element == element] if "element" in pairs.columns else pairs
    name_of = ({} if names is None else
               dict(names.drop_duplicates("mnxm")[["mnxm", "name"]].itertuples(index=False)))

    M, G = {}, {}
    K = ranks if ranks is not None else {o: atom_ranks(members[o], el) for o in orgs}
    for o in orgs:
        M[o] = diluted_gene_counts(members[o], el)
        G[o] = incident_conductance(conductances[o], el)

    rows = []
    for i, a in enumerate(orgs):
        for b in orgs[i + 1:]:
            shared = set(K[a]) & set(K[b])
            union = set(K[a]) | set(K[b])
            for met in sorted(union):
                ma, mb = float(M[a].get(met, 0.0)), float(M[b].get(met, 0.0))
                tot = ma + mb
                d = abs(ma - mb) / tot if tot > 0 else float("nan")
                ga, gb = float(G[a].get(met, 0.0)), float(G[b].get(met, 0.0))
                k = len(K[a].get(met, set()) & K[b].get(met, set()))
                bl = met in blacklist
                ok = (met in shared) and k > 0 and not bl and tot > 0 and d > 0
                gbr = (g0 * d * min(ga, gb) / k) if ok else 0.0
                rows.append((element, a, b, met, name_of.get(met, ""), ma, mb, d,
                             ga, gb, k, gbr, int(bl), int(ok and gbr > 0)))
    return pd.DataFrame(rows, columns=BRIDGE_COLS)


# =====================================================================
# Composition
# =====================================================================

def _synthetic_orf_rows(mnxr: str, source: str) -> dict:
    return dict(source=source, orf=f"__bridge__{SEP}{mnxr}", channel=BRIDGE_CHANNEL,
                mnxr=mnxr, intermediate_id=mnxr, intermediate_name=mnxr,
                raw_score=1.0, score_kind="bridge", projection_via="compose",
                evidence_quality="synthetic", lane_set=BRIDGE_CHANNEL)


def compose(members: dict, pairs: pd.DataFrame, direction: pd.DataFrame | None = None, *,
            elements=ELEMENTS, g0=1.0, blacklist=frozenset(), names=None,
            network_id=None) -> dict:
    orgs = sorted(members)
    network_id = network_id or "-".join(orgs)
    beliefs = {o: reaction_beliefs(members[o]) for o in orgs}
    conduct = {o: reaction_conductances(members[o]) for o in orgs}
    # Every bridge is one synthetic ORF spending 1.0 on one pseudo-reaction, so they all
    # pool to this same E_r. Dividing it out of pair_w is what makes E_r * pair_w equal
    # the g_bridge the bridge table computed.
    e_bridge = _net.pooled_E_of_mass(1.0)

    dup = set()
    seen = {}
    for o in orgs:
        for orf in members[o]["orf"].unique():
            if orf in seen and seen[orf] != o:
                dup.add(orf)
            seen[orf] = o
    if dup:
        raise ValueError(
            f"{len(dup):,} ORF ids appear in more than one member (e.g. {sorted(dup)[:3]}). "
            f"Belief conservation is per-ORF, so a shared id would silently merge two "
            f"organisms' genes into one.")

    # --- the private copies -------------------------------------------------
    keep_el = pairs[pairs.element.isin(elements)] if "element" in pairs.columns else pairs
    pair_parts, gpr_parts = [], []
    for o in orgs:
        sub = keep_el[keep_el.mnxr.isin(beliefs[o].index)].copy()
        sub["mnxr"] = o + SEP + sub["mnxr"].astype(str)
        sub["substrate"] = o + SEP + sub["substrate"].astype(str)
        sub["product"] = o + SEP + sub["product"].astype(str)
        pair_parts.append(sub)
        g = members[o].copy()
        g["mnxr"] = o + SEP + g["mnxr"].astype(str)
        gpr_parts.append(g)

    # --- the bridges --------------------------------------------------------
    bridge_parts, bridge_pairs, bridge_gpr, bridge_dir = [], [], [], []
    for el in elements:
        el_pairs = keep_el[keep_el.element == el] if "element" in keep_el.columns else keep_el
        ranks = {o: atom_ranks(beliefs[o], el_pairs) for o in orgs}
        bt = bridge_table(beliefs, el_pairs, el, conduct, g0=g0,
                          blacklist=blacklist, names=names, ranks=ranks)
        bridge_parts.append(bt)
        live = bt[bt.bridged == 1]
        if not len(live):
            continue
        for (a, b), grp in live.groupby(["org_a", "org_b"]):
            rxn = f"BRIDGE{SEP}{a}-{b}{SEP}{el}"
            bridge_gpr.append(_synthetic_orf_rows(rxn, network_id))
            bridge_dir.append(dict(mnxr=rxn, ratio=1.0))
            ka, kb = ranks[a], ranks[b]
            for r in grp.itertuples(index=False):
                for i in sorted(ka.get(r.metabolite, set()) & kb.get(r.metabolite, set())):
                    bridge_pairs.append((rxn, el, tag(a, r.metabolite),
                                         tag(b, r.metabolite), i, i,
                                         r.g_bridge / e_bridge))

    out_pairs = pd.concat(pair_parts, ignore_index=True)
    if bridge_pairs:
        bp = pd.DataFrame(bridge_pairs, columns=["mnxr", "element", "substrate",
                                                 "product", "sub_idx", "prod_idx", "pair_w"])
        for c in out_pairs.columns:
            if c not in bp.columns:
                bp[c] = np.nan
        out_pairs = pd.concat([out_pairs, bp[out_pairs.columns]], ignore_index=True)
    out_pairs["sub_idx"] = out_pairs["sub_idx"].astype(int)
    out_pairs["prod_idx"] = out_pairs["prod_idx"].astype(int)

    out_gpr = pd.concat(gpr_parts + ([pd.DataFrame(bridge_gpr)] if bridge_gpr else []),
                        ignore_index=True)
    out_gpr["source"] = network_id

    out_dir = None
    if direction is not None:
        parts = []
        for o in orgs:
            d = direction[direction.mnxr.isin(beliefs[o].index)].copy()
            d["mnxr"] = o + SEP + d["mnxr"].astype(str)
            parts.append(d)
        if bridge_dir:
            bd = pd.DataFrame(bridge_dir)
            for c in parts[0].columns:
                if c not in bd.columns:
                    bd[c] = np.nan
            parts.append(bd[parts[0].columns])
        out_dir = pd.concat(parts, ignore_index=True)

    bridges = pd.concat(bridge_parts, ignore_index=True) if bridge_parts else \
        pd.DataFrame(columns=BRIDGE_COLS)
    report = dict(
        network_id=network_id, members=orgs, g0=g0, n_blacklist=len(blacklist),
        n_orfs=int(out_gpr["orf"].nunique()), n_reactions=int(out_gpr["mnxr"].nunique()),
        n_pair_rows=int(len(out_pairs)),
        n_bridge_rows=int(len(bridge_pairs)),
        n_bridged=int((bridges.bridged == 1).sum()) if len(bridges) else 0,
        n_blocked_blacklist=int(((bridges.blacklisted == 1) & (bridges.bridged == 0)).sum())
        if len(bridges) else 0,
        n_one_copy_only=int(((bridges.blacklisted == 0) & (bridges.bridged == 0)
                             & ((bridges.M_a == 0) | (bridges.M_b == 0))).sum())
        if len(bridges) else 0,
    )
    return dict(pairs=out_pairs, gpr=out_gpr, direction=out_dir,
                bridges=bridges, report=report)


# =====================================================================
# Conditions
# =====================================================================

def make_conditions(*, network_id, source_org, sink_orgs, substrates, precursors,
                    media, elements=ELEMENTS, two_terminal_precursors=None) -> tuple:
    tt_prec = two_terminal_precursors or {}
    ground, two_terminal = [], []
    for el in elements:
        src = substrates.get(el)
        if src is None:
            continue
        prec = precursors.get(el, ())
        if not prec:
            continue
        ground.append(cond_mod.Condition(
            condition_id=f"{network_id}|{source_org}->{'+'.join(sink_orgs)}|{el}|ground",
            element=el, source_hub=tag(source_org, src),
            sinks=tuple(tag(so, p) for so in sink_orgs for p in prec),
            media=media,
        ))
        for p in tt_prec.get(el, ()):
            for so in sink_orgs:
                two_terminal.append(cond_mod.Condition(
                    condition_id=f"{network_id}|{source_org}->{so}|{el}|2t|{p}",
                    element=el, source_hub=tag(source_org, src),
                    sinks=(tag(so, p),), media=media,
                ))
    return ground, two_terminal


# =====================================================================
# Self-tests
# =====================================================================

def _toy_gpr(source, rxns):
    # One ORF per reaction, one lane -- so every reaction's belief is exactly 1.0 and the
    # arithmetic under test is the composition's, not the allocation's.
    return pd.DataFrame([dict(source=source, orf=f"{source}_g{i}", channel="clean",
                              mnxr=r, intermediate_id=f"i{i}", intermediate_name="x",
                              raw_score=1.0, score_kind="p", projection_via="t",
                              evidence_quality="q", lane_set="clean")
                         for i, r in enumerate(rxns)])


def _toy_pairs():
    #  R1: S -> X    R2: X -> P1    R3: X -> P2    R4: P1 -> Y
    rows = [("R1", "S", "X"), ("R2", "X", "P1"), ("R3", "X", "P2"), ("R4", "P1", "Y")]
    return pd.DataFrame([dict(mnxr=r, element="C", substrate=s, product=p,
                              sub_idx=0, prod_idx=0, pair_w=1.0) for r, s, p in rows])


def _selftest_copies():
    print("[compose] private copies: no reaction weight crosses between members")
    pairs = _toy_pairs()
    a, b = _toy_gpr("A", ["R1", "R2"]), _toy_gpr("B", ["R2", "R3"])
    out = compose({"A": a, "B": b}, pairs, elements=("C",), g0=0.0)
    mets = set(out["pairs"].substrate) | set(out["pairs"]["product"])
    print(f"  metabolites: {sorted(mets)}")
    assert "A:X" in mets and "B:X" in mets, "each member must get its own copy of X"
    assert "X" not in mets, "an untagged metabolite means the prefixing did not take"
    # R2 is in both members and must appear as two separate reactions, never one summed.
    r2 = set(out["pairs"][out["pairs"].mnxr.str.endswith("R2")].mnxr)
    print(f"  R2 -> {sorted(r2)}")
    assert r2 == {"A:R2", "B:R2"}, r2
    assert out["report"]["n_bridge_rows"] == 0, "g0=0 must produce no bridge edges"
    print("  PASS\n")
    return 0


def _selftest_singleton():
    print("[compose] a singleton is the uncomposed organism, prefixed")
    pairs = _toy_pairs()
    a = _toy_gpr("A", ["R1", "R2", "R3"])
    out = compose({"A": a}, pairs, elements=("C",))
    assert len(out["bridges"]) == 0 or (out["bridges"].bridged == 0).all()
    assert out["report"]["n_bridge_rows"] == 0
    assert out["report"]["n_orfs"] == 3, out["report"]
    # R4 is in the reference but not in this organism, so it must NOT be in the copy: the
    # composed table is cut to the member's own reactions, which is what keeps a
    # three-member composition from being three times the whole reference universe.
    base = set(zip(*[pairs[pairs.mnxr != "R4"][c] for c in ("substrate", "product")]))
    got = {(untag(s)[1], untag(p)[1])
           for s, p in zip(out["pairs"].substrate, out["pairs"]["product"])}
    print(f"  edges {sorted(got)}")
    assert got == base, (got, base)
    assert not out["pairs"].mnxr.str.endswith("R4").any()
    print("  PASS\n")
    return 0


def _selftest_mismatch():
    print("[compose] the mismatch is relative, gene-unit, and doubly diluted")
    pairs = _toy_pairs()
    # A carries R2 only; B carries R2 and R3. X is touched by both, P2 only by B.
    a, b = _toy_gpr("A", ["R2"]), _toy_gpr("B", ["R2", "R3"])
    bt = bridge_table({"A": reaction_beliefs(a), "B": reaction_beliefs(b)},
                      pairs, "C",
                      {"A": reaction_conductances(a), "B": reaction_conductances(b)},
                      g0=1.0)
    row = bt[bt.metabolite == "X"].iloc[0]
    # Second dilution: R2 touches {X, P1} so contributes 1/2 to each; R3 touches {X, P2}.
    # M_A(X) = 0.5, M_B(X) = 0.5 + 0.5 = 1.0  ->  D = 0.5/1.5 = 1/3.
    print(f"  X: M_A={row.M_a:.4f} M_B={row.M_b:.4f} D={row.asymmetry:.6f}")
    assert abs(row.M_a - 0.5) < 1e-12, row.M_a
    assert abs(row.M_b - 1.0) < 1e-12, row.M_b
    assert abs(row.asymmetry - 1 / 3) < 1e-12, row.asymmetry
    # Relative asymmetry must not move when one member's genome is scaled up wholesale.
    b2 = pd.concat([b, _toy_gpr("B2", ["R2", "R3"]).assign(source="B")], ignore_index=True)
    bt2 = bridge_table({"A": reaction_beliefs(a), "B": reaction_beliefs(b2)},
                       pairs, "C",
                       {"A": reaction_conductances(a), "B": reaction_conductances(b2)},
                       g0=1.0)
    r2 = bt2[bt2.metabolite == "X"].iloc[0]
    print(f"  X after doubling B's genes: M_B={r2.M_b:.4f} D={r2.asymmetry:.6f}")
    assert r2.M_b > row.M_b, "the raw count must grow"
    assert r2.asymmetry > row.asymmetry, "and so must the asymmetry -- B really is bigger"
    # P2 exists only in B: no bridge, and it is REPORTED rather than dropped.
    p2 = bt[bt.metabolite == "P2"].iloc[0]
    print(f"  P2: M_A={p2.M_a} M_B={p2.M_b} bridged={p2.bridged}")
    assert p2.bridged == 0 and p2.M_a == 0.0, p2
    print("  PASS\n")
    return 0


def _selftest_bridge_conductance():
    print("[compose] a bridge is symmetric, split across atoms, and scaled by g0")
    pairs = _toy_pairs()
    # B additionally carries R4 (P1 -> Y), so P1 is the one metabolite the two use
    # DIFFERENTLY. X is used identically by both and gets no bridge at all: zero mismatch
    # is zero exchange, which is the scheme's strongest claim and is pinned here.
    a, b = _toy_gpr("A", ["R1", "R2"]), _toy_gpr("B", ["R2", "R3", "R4"])
    out = compose({"A": a, "B": b}, pairs, elements=("C",), g0=1.0)
    assert not len(out["pairs"][out["pairs"].substrate == "A:X"].query("mnxr.str.startswith('BRIDGE')")), \
        "X is used identically by both members: D == 0, so no bridge"
    br = out["pairs"][out["pairs"].mnxr.str.startswith("BRIDGE")]
    print(f"  {len(br)} bridge rows: {sorted(set(zip(br.substrate, br['product'])))}")
    assert len(br) > 0
    assert (br.substrate.str.startswith("A:") & br["product"].str.startswith("B:")).all()
    # Every bridge reaction must be present in the composed GPR carrying belief mass
    # EXACTLY 1.0, or the driver's conservation assert is measuring something other than
    # what it says. Its POOLED E is not 1.0 and cannot be -- but it is the same constant
    # for every bridge, which is what lets pair_w carry g_bridge exactly.
    w = _net.compute_weights(out["gpr"])
    n_orf = out["gpr"]["orf"].nunique()
    print(f"  sum(belief_mass)={w.belief_mass.sum():.9f} over {n_orf} ORFs")
    assert abs(w.belief_mass.sum() - n_orf) < 1e-9
    e_br = w.loc[w.mnxr.isin(br.mnxr.unique())]
    print(f"  {len(e_br)} bridge reactions, mass {e_br.belief_mass.unique()}, "
          f"E {e_br.E_full.unique()}")
    assert (e_br.belief_mass.sub(1.0).abs() < 1e-12).all()
    assert e_br.E_full.std(ddof=0) < 1e-15 or len(e_br) == 1
    assert abs(float(e_br.E_full.iloc[0]) - _net.pooled_E_of_mass(1.0)) < 1e-12
    assert (w.E_full > 0).all() and (w.E_full < 1).all()
    # g0 scales the conductance linearly and nothing else -- the sweep that must converge
    # to the isolated members as g0 -> 0 is only meaningful if this holds.
    out2 = compose({"A": a, "B": b}, pairs, elements=("C",), g0=2.5)
    b2 = out2["pairs"][out2["pairs"].mnxr.str.startswith("BRIDGE")]
    print(f"  g0=1 -> {br.pair_w.sum():.6f}   g0=2.5 -> {b2.pair_w.sum():.6f}")
    assert abs(b2.pair_w.sum() - 2.5 * br.pair_w.sum()) < 1e-12
    # And the direction table must say symmetric, not inherit a ratio.
    assert out["direction"] is None
    print("  PASS\n")
    return 0


def _selftest_blacklist():
    print("[compose] a blacklisted carrier is blocked but still reported")
    pairs = _toy_pairs()
    a, b = _toy_gpr("A", ["R1", "R2"]), _toy_gpr("B", ["R2", "R3", "R4"])
    out = compose({"A": a, "B": b}, pairs, elements=("C",), g0=1.0, blacklist={"P1"})
    br = out["pairs"][out["pairs"].mnxr.str.startswith("BRIDGE")]
    assert not len(br), "P1 was the only bridgeable metabolite and it is blacklisted"
    row = out["bridges"][out["bridges"].metabolite == "P1"].iloc[0]
    print(f"  P1: blacklisted={row.blacklisted} bridged={row.bridged} g={row.g_bridge}")
    assert row.blacklisted == 1 and row.bridged == 0 and row.g_bridge == 0.0
    assert out["report"]["n_blocked_blacklist"] >= 1
    # The rules must resolve carriers by SYNONYM, not just by the one canonical name.
    names = pd.DataFrame([("m1", "ATP"), ("m2", "an oxidized thioredoxin"),
                          ("m3", "a reduced ferredoxin [iron-sulfur] cluster"),
                          ("m4", "CO2"), ("m5", "dATP"), ("m6", "biotin"),
                          ("m7", "UDP-N-acetyl-alpha-D-glucosamine")],
                         columns=["mnxm", "name"])
    bl = set(carrier_blacklist(names).mnxm)
    print(f"  blacklist over 7 toy names -> {sorted(bl)}")
    assert {"m1", "m2", "m3"} <= bl, bl
    assert not ({"m4", "m5", "m6", "m7"} & bl), \
        "CO2, dATP, biotin and UDP-GlcNAc are cargo or endpoints, never carriers"
    print("  PASS\n")
    return 0


def _selftest_conditions():
    print("[compose] conditions name the copies, and carry the arm in their strings")
    g, t = make_conditions(network_id="A-B", source_org="A", sink_orgs=["B"],
                           substrates={"C": "GLC"}, precursors={"C": ["P1", "P2"]},
                           media="M9|g0=1.0|bl=on", elements=("C",),
                           two_terminal_precursors={"C": ["P1"]})
    for c in g + t:
        print(f"  {c.condition_id}  {c.source_hub} -> {'|'.join(c.sinks)}")
    assert {c.source_hub for c in g} == {"A:GLC"}, "glucose is injected into the SOURCE copy"
    assert set(g[0].sinks) == {"B:P1", "B:P2"}, "endpoints are read in the SINK copy"
    assert len(g) == 1, "a ground condition names every endpoint at once"
    assert len(t) == 1 and t[0].sinks == ("B:P1",)
    assert all(c.media == "M9|g0=1.0|bl=on" for c in g + t)
    print("  PASS\n")
    return 0


def _selftest() -> int:
    print("=" * 70)
    print("ecspr.model.compose self-tests")
    print("=" * 70)
    _selftest_copies()
    _selftest_singleton()
    _selftest_mismatch()
    _selftest_bridge_conductance()
    _selftest_blacklist()
    _selftest_conditions()
    print("ALL PASS")
    return 0


# =====================================================================
# CLI
# =====================================================================

def cmd_blacklist(a):
    names = pd.read_parquet(a.names)
    bl = carrier_blacklist(names)
    if a.out:
        bl.to_parquet(a.out, index=False)
        print(f"{len(bl):,} carriers -> {a.out}")
    print(bl.groupby("carrier_class").size().to_string())
    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("blacklist", help="materialise the carrier blacklist")
    b.add_argument("--names", required=True, help="ecspr::metabolite_names parquet")
    b.add_argument("--out", default=None)
    b.set_defaults(func=cmd_blacklist)
    s = sub.add_parser("selftest", help="run the module self-tests")
    s.set_defaults(func=lambda a: _selftest())
    return p.parse_args(argv)


def main(argv=None):
    a = parse_args(argv)
    return a.func(a) or 0


if __name__ == "__main__":
    sys.exit(main())
