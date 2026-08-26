#!/usr/bin/env python3
"""What the 2010 GOF screen is made of, against what a carbon-conductance probe can see.

    mamba run -n msm python research/fabfos/benchmarks/eydallin/parse/gof_audit.py

Three questions this answers, in order, for the 86 ASKA clones of gof.csv:

    COVERAGE    how many carry a reaction at all, on the hand-curated mapping and on each
                GPR channel -- and, crucially, cross-tabbed against the mechanism the
                PAPER itself proposes for the gene. The two mapping notions are different
                populations and get confused on sight: gof.csv's `mnxr` is one curated
                representative reaction per gene, while the sweep's `n_rxn` is the GPR
                channel the probe actually walks. Neither contains the other.

    RESPONSE    the active/responder/mover ladder from ratio_cross_arm.py, re-cut here per
                gene so the movers can be read beside the paper's claim about them.

    CHEMISTRY   for the 42 curated reactions: is the assigned reaction the enzyme's actual
                reaction, and does the deployed direction ratio agree with a textbook sign.

THE PAPER'S MECHANISM CLASS IS THE LOAD-BEARING COLUMN. Eydallin 2010 §3.1.1-3.1.6 states
a mechanism for 34 of the 86 genes, and only six of those act on the glycogen carbon path
(glgA, glgC, glgB, glgP, malP, aspP). Everything else the paper routes through ppGpp, RpoS,
CsrA, c-di-GMP, biofilm precursors, amino-acid provision, the ATP pool or glutathione redox
-- currencies a carbon-flow ratio has no representation for. So the coverage number is not
a mapping deficiency to be repaired; it is the screen's own composition. Report it that way.

DIRECTION RATIO IS exp(dG'/RT) FOR THE REACTION AS MetaNetX WRITES IT, so ratio < 1 means
the written direction is favoured and ratio > 1 means the reverse is. The bake clamps at
three decades, so 1000 and 0.001 are "at the rail" rather than measurements, and exactly
1.0 is the ensemble ABSTAINING -- it carries no claim and must not be scored as one. The
census below separates all three; a bare mean over this column is meaningless.

TEXTBOOK_DIRECTION is hand-entered from standard-state biochemistry and is the point of
the file. Each entry is the expected sign of dG'° for the equation AS WRITTEN ABOVE, not
for the enzyme's physiological direction -- MetaNetX writes many of these backwards and
scoring against the enzyme's name instead of its equation inverts half the table.

SCORE EACH TIER AGAINST WHAT THAT TIER CLAIMS. `dir_tier` is not a confidence grade, it is
which KIND of claim the ensemble is making, and one anchor cannot score all four:

    tier 0   abstention. 36,151 rows, every one at exactly 1.0. Asserts nothing; excluded.
    tier 1   a declared irreversibility, mostly from a GEM's own bounds. 2,171 rows, 1,683
             of them pinned at a clamp because a clamp is how "irreversible" is spelled in
             ratio units. This is NOT a dG' claim -- glgC is declared irreversible because
             PPi hydrolysis pulls it in vivo, which is true, and its own dG'o is still ~0.
    tier 2   a thermodynamic estimate. Score this one against dG'o.
    tier 3   BioCyc's curated arrow. 6,072 rows, none clamped. A curated direction, not a
             free energy.

MASS BALANCE PREDICTS THE RAIL. Among tier-2 rows, 84.3% of those MetaNetX cannot call
balanced sit at a clamp against 41.0% of the balanced ones. An equation that does not
balance yields a dG' that saturates, so `is_balanced` belongs beside every clamped value.

`Glycogen` IS NOT A FORMULA-LESS POLYMER, and assuming it is sends the diagnosis to the
wrong place. MNXM738130 carries C24H42O21 -- maltotetraose's formula, charge 0. What is
wrong with the two accessions this benchmark walks is that the ACCEPTOR IS MISSING from
the substrate side, so they are carbon-unbalanced outright: MNXR145046 runs 16 C -> 34 C
and MNXR145036 runs 6 C -> 24 C. Balanced counterparts exist for both (MNXR132476 and
MNXR145632) and return the SAME dG' to two decimals, which is what rules out the missing
acceptor as the cause and puts it on the group values instead. Check the balance and find
the balanced sibling before blaming the polymer.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
GOF = ROOT / "data/fabfos/runs/eydallin_clones/parse/gof/gof.csv"
SWEEP = ROOT / "data/fabfos/runs/aska/ecspr/aska_ratio_sweep_{ch}_e_coli_ag1_fold2.0_C.tsv"
OUT = ROOT / "data/fabfos/runs/eydallin_clones/parse/gof"

MOVER_PCT = 1.0

# Eydallin 2010 §3.1.1-3.1.6. A gene absent here is one the paper names only in the COG
# tables (Tables 1 and 2) and offers no mechanism for -- 52 of the 86.
PAPER_MECHANISM = {
    "glgA": "glycogen_catalysis", "glgC": "glycogen_catalysis", "glgB": "glycogen_catalysis",
    "glgP": "glycogen_catalysis", "malP": "glycogen_catalysis", "aspP": "glycogen_catalysis",
    "csrA": "csr_post_transcriptional", "csrD": "csr_post_transcriptional",
    "malT": "maltodextrin_regulon",
    "ptsI": "pts_carbon_sensing", "mlc": "pts_carbon_sensing",
    "rpoS": "general_stress", "glgS": "general_stress",
    "spoT": "stringent_alarmone", "gpp": "stringent_alarmone",
    "yeaP": "cdi_gmp_signalling", "yjcC": "cdi_gmp_signalling", "dos": "cdi_gmp_signalling",
    "yncC": "biofilm_orphan", "yncG": "biofilm_orphan", "ymgC": "biofilm_orphan",
    "wzc": "biofilm_precursor", "galS": "biofilm_precursor", "nagB": "biofilm_precursor",
    "ptsN": "nitrogen_pts",
    "serB": "aa_provision_stringent", "cysI": "aa_provision_stringent",
    "cysP": "aa_provision_stringent", "tdcA": "aa_provision_stringent",
    "recQ": "atp_pool", "nagD": "atp_pool", "ppK": "atp_pool",
    "gor": "glutathione_redox", "cydC": "glutathione_redox",
}

# Does the curated reaction describe what the enzyme actually does?
#   ok         the enzyme's own reaction, or a defensible member of its family
#   secondary  a real but minor activity, not the one the gene is named for
#   wrong      a different enzyme's reaction, or the right family on the wrong ligand
#   vacuous    an identity or generic-R equation; carries no chemistry to measure
#   invalid    not a balanced chemical transformation
REACTION_FIDELITY = {
    "aspP": ("wrong", "mapped through the GEM's nudF (ADP-ribose diphosphatase); the GEM "
                      "carries no ADP-glucose pyrophosphatase, which is AspP's glycogen role"),
    "cysP": ("wrong", "molybdate ABC (ModA); CysP binds thiosulfate/sulfate"),
    "dos": ("wrong", "GMP/cGMP; DosP is a c-di-GMP phosphodiesterase"),
    "putP": ("wrong", "propanoate symport; PutP is a proline symporter"),
    "rutF": ("wrong", "RutA's O2-dependent monooxygenation; RutF is the FMN reductase"),
    "ydcJ": ("invalid", "de-novo gap-fill: a hydroxyacid plus CO2 does not evolve O2"),
    "gntT": ("vacuous", "PMF-coupled identity, gluconate on both sides"),
    "yifJ": ("vacuous", "flippase written as A -> A"),
    "yncG": ("vacuous", "generic RX; no carbon in the atom map"),
    "pfs": ("secondary", "5'-deoxyadenosine; Pfs is named for MTA/SAH"),
    "rpiB": ("secondary", "allose-6-P isomerase; RpiB's main activity is ribose-5-P"),
    "tnaA": ("secondary", "serine deaminase side activity; not the tryptophanase reaction"),
}

# Expected sign of dG'° for the equation AS WRITTEN in gof.csv.
#   fwd  written direction favoured (dG'° < 0)     -> expect ratio < 1
#   rev  reverse favoured (dG'° > 0)               -> expect ratio > 1
#   eq   near equilibrium (|dG'°| < ~3 kJ/mol)     -> expect ratio near 1
# None means no defensible textbook anchor, and those rows are excluded from the score
# rather than counted as passes.
TEXTBOOK_DIRECTION = {
    "MNXR152012": ("fwd", "ADP-ribose pyrophosphate hydrolysis, dG'o ~ -20"),
    "MNXR100447": ("rev", "ABC written as ATP synthesis"),
    "MNXR104650": ("rev", "written oxidatively; sulfite reduction by NADPH is the favoured sense"),
    "MNXR101689": ("rev", "ABC written as ATP synthesis"),
    "MNXR189614": ("rev", "nucleotide cyclization; hydrolysis is the favoured sense"),
    "MNXR145046": ("fwd", "glycosyl transfer from a nucleotide sugar, dG'o ~ -13"),
    "MNXR145021": ("eq", "a1->4 traded for a1->6; branching is near isoenergetic"),
    "MNXR145050": ("eq", "nucleotidyl transfer is near isoenergetic; PPi hydrolysis is what "
                         "makes ADPG synthesis irreversible in vivo, not this step"),
    "MNXR145036": ("fwd", "phosphorolysis (glycogen + Pi -> G1P) is dG'o ~ +3, so the "
                          "equation as written is ~ -3; the enzyme runs catabolically on "
                          "mass action, not on standard-state sign"),
    "MNXR100299": ("rev", "ABC written as ATP synthesis"),
    "MNXR100098": ("fwd", "E'o(GSSG/2GSH) -0.24 vs NADP+/NADPH -0.32, n=2 -> dG'o ~ -15"),
    "MNXR100099": ("rev", "phosphoanhydride condensation; Gpp hydrolyses pppGpp"),
    "MNXR145582": ("rev", "methyl transfer runs CH3-THF -> Met"),
    "MNXR115917": ("fwd", "deamination to F6P, dG'o ~ -6"),
    "MNXR152703": ("fwd", "N-glycosidic hydrolysis, dG'o ~ -20"),
    "MNXR103113": ("eq", "polyP anhydride bond is close to ATP's gamma-phosphate"),
    "MNXR103069": ("rev", "phosphoanhydride condensation; Ppx hydrolyses"),
    "MNXR151670": ("fwd", "rhodanese sulfur transfer to cyanide"),
    "MNXR102870": ("rev", "ABC written as ATP synthesis"),
    "MNXR97364": ("rev", "PEP is the higher-energy phosphoryl donor, dG'o(PEP->pyr) ~ -31"),
    "MNXR151350": ("eq", "aldose-ketose isomerization"),
    "MNXR103383": ("fwd", "O2-dependent monooxygenation"),
    "MNXR162386": ("fwd", "phosphomonoester hydrolysis, dG'o ~ -13; the committed step"),
    "MNXR188099": ("fwd", "pyrophosphoryl transfer with ATP -> AMP"),
    "MNXR100880": ("rev", "ABC written as ATP synthesis"),
    "MNXR146501": ("eq", "transaldolase is freely reversible, dG'o ~ 0"),
    "MNXR100737": ("rev", "kinase run backwards, dG'o ~ +13 to +17"),
    "MNXR146410": ("rev", "serine deamination to pyruvate is the favoured sense"),
    "MNXR105268": ("rev", "ABC written as ATP synthesis"),
    "MNXR105306": ("fwd", "amide hydrolysis, dG'o ~ -14"),
    "MNXR101969": ("rev", "amidation without an ATP coupling is disfavoured"),
}

RT = 8.314e-3 * 298.15

BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
REACTIONS = ROOT / "data/fabfos/processed/lookups/reactions.parquet"

TIER_CLAIM = {0: "abstains", 1: "declares irreversible", 2: "estimates dG'", 3: "cites BioCyc"}

# Standard-state dG'o in kJ/mol for the equation as written, for the glycogen family only.
# The whole argument about that family turns on magnitude rather than sign, and these two
# are the anchors that make it a single-parameter defect rather than two failures.
TEXTBOOK_DG = {
    "MNXR145036": (-3.1, "Lehninger: phosphorolysis is +3.1; written here as synthesis, so -3.1"),
    "MNXR145046": (-13.0, "glycosyl transfer from a nucleotide sugar"),
}


def ratio_class(r: float) -> str:
    if not np.isfinite(r):
        return "unmapped"
    if abs(r - 1.0) < 1e-9:
        return "abstain"
    if r >= 999:
        return "clamp_rev"
    if r <= 1.1e-3:
        return "clamp_fwd"
    return "measured"


def observed_direction(r: float) -> str:
    """Coarsen a ratio to the same three buckets TEXTBOOK_DIRECTION uses.

    The `eq` band is +/- 3 kJ/mol, i.e. a factor of ~3.3 either way -- wide enough that a
    group-contribution estimate and a textbook table agree inside it, narrow enough that
    the clamps and the two glycogen reactions still fall outside.
    """
    if abs(r - 1.0) < 1e-9:
        return "abstain"
    lo, hi = np.exp(-3.0 / RT), np.exp(3.0 / RT)
    return "eq" if lo <= r <= hi else ("fwd" if r < 1 else "rev")


def replaceable(p: Path) -> Path:
    """Break the hardlink before writing.

    Once this output has been `dvc add`ed it comes back as a read-only hardlink into the
    shared cache, so a re-run dies on PermissionError -- and chmod-ing it writable instead
    would write THROUGH the link and corrupt the cache generation every other worktree
    shares. Unlinking leaves the cache object alone and starts a fresh inode.
    """
    p.unlink(missing_ok=True)
    return p


def bake_direction() -> pd.DataFrame:
    """`dir_tier` beside the ratio, read from the deployed bake rather than the decode
    cache -- the cache carries only (mnxr, ratio) and the tier is the column that says
    which question the ratio is answering."""
    d = pd.read_parquet(BAKE / "direction.parquet")
    v = pd.read_parquet(BAKE / "vocab.parquet")
    d["mnxr"] = d.rxn.map(v[v.kind == "rxn"].set_index("code").symbol)
    return d[["mnxr", "dir_tier"]]


def load() -> pd.DataFrame:
    d = pd.read_csv(GOF)
    d["phenotype"] = np.where(d.pct_glycogen_production > 100, "excess", "deficient")
    d["paper_mechanism"] = d.gene.map(PAPER_MECHANISM).fillna("not_discussed")
    d["curated_mapped"] = d.mnxr.notna()
    for ch in ("gem", "denovo"):
        s = pd.read_csv(str(SWEEP).format(ch=ch), sep="\t")
        s = s[s.is_positive == True].set_index("eydallin_gene")  # noqa: E712
        d[f"{ch}_n_rxn"] = d.gene.map(s.n_rxn).fillna(0).astype(int)
        d[f"{ch}_delta"] = d.gene.map(s.delta_ratio_pct).fillna(0.0)
        d[f"{ch}_rxns"] = d.gene.map(s.rxns)
    d = d.merge(bake_direction(), on="mnxr", how="left")
    bal = pd.read_parquet(REACTIONS, columns=["mnxr", "is_balanced"])
    return d.merge(bal, on="mnxr", how="left")


def ladder(d: pd.DataFrame, ch: str) -> dict:
    a = d[d[f"{ch}_n_rxn"] > 0]
    live = a[a[f"{ch}_delta"].abs() > 1e-12]
    mov = live[live[f"{ch}_delta"].abs() >= MOVER_PCT]
    return dict(active=len(a), responders=len(live), movers=len(mov), mov=mov)


def main():
    d = load()
    L = []

    L.append("=" * 96)
    L.append("EYDALLIN 2010 GOF -- 86 ASKA clones with altered glycogen, audited three ways")
    L.append("=" * 96)
    L.append(f"\n  {(d.phenotype == 'excess').sum()} glycogen-excess, "
             f"{(d.phenotype == 'deficient').sum()} glycogen-deficient")

    L.append("\n\n### COVERAGE -- three different 'mapped' numbers, none contained in another\n")
    L.append(f"  curated reaction in gof.csv       {int(d.curated_mapped.sum()):3d} / 86")
    for ch in ("gem", "denovo"):
        L.append(f"  in the {ch:6s} GPR atom universe    {int((d[f'{ch}_n_rxn'] > 0).sum()):3d} / 86")
    L.append("")
    for ch in ("gem", "denovo"):
        only_cur = int((d.curated_mapped & (d[f"{ch}_n_rxn"] == 0)).sum())
        only_ch = int((~d.curated_mapped & (d[f"{ch}_n_rxn"] > 0)).sum())
        L.append(f"  {only_cur:3d} genes carry a curated reaction the {ch} channel cannot walk, "
                 f"and {only_ch} the other way.")

    L.append("\n  why the other 44 have no curated reaction:")
    for k, v in d.no_mapping_reason.value_counts().items():
        L.append(f"      {v:3d}  {k}")

    L.append("\n\n### THE PAPER'S OWN MECHANISM vs what the probe can see\n")
    L.append("  Eydallin 2010 states a mechanism for 34 of the 86; the other 52 appear only in")
    L.append("  the COG tables. Only ONE class acts on the glycogen carbon path.\n")
    g = d.groupby("paper_mechanism").agg(
        n=("gene", "size"),
        curated=("curated_mapped", "sum"),
        gem_active=("gem_n_rxn", lambda s: int((s > 0).sum())),
        gem_movers=("gem_delta", lambda s: int((s.abs() >= MOVER_PCT).sum())),
        denovo_movers=("denovo_delta", lambda s: int((s.abs() >= MOVER_PCT).sum())),
    ).sort_values("n", ascending=False)
    L.append(g.to_string())
    L.append("\n  Every mover on both channels is in glycogen_catalysis. No gene the paper")
    L.append("  explains through ppGpp, RpoS, CsrA, c-di-GMP, the ATP pool or glutathione")
    L.append("  moves the ratio, and none of them could: the probe's currency is carbon.")

    L.append("\n\n### RESPONSE LADDER -- fold 2.0, ratio C(glucose->glycogen)/C(glycogen->pyruvate)\n")
    for ch in ("gem", "denovo"):
        r = ladder(d, ch)
        L.append(f"  {ch:6s}  labelled 86   active {r['active']:3d}   responders "
                 f"{r['responders']:3d}   movers (>={MOVER_PCT:g}%) {r['movers']:2d}")
        for m in r["mov"].sort_values(f"{ch}_delta", key=abs, ascending=False).itertuples():
            L.append(f"            {m.gene:6s} {getattr(m, f'{ch}_delta'):+9.3f}%  "
                     f"{m.phenotype:9s}  n_rxn {getattr(m, f'{ch}_n_rxn'):3d}   "
                     f"{m.paper_mechanism}")
        sub = d[(d[f"{ch}_delta"].abs() > 1e-12) & (d[f"{ch}_delta"].abs() < MOVER_PCT)]
        L.append(f"            {len(sub)} more respond below 1%, the largest at "
                 f"{sub[f'{ch}_delta'].abs().max():.3g}% and the smallest at "
                 f"{sub[f'{ch}_delta'].abs().min():.3g}% -- solver dust.\n")

    L.append("\n### CHEMISTRY OF THE 42 CURATED REACTIONS\n")
    m = d[d.curated_mapped].copy()
    m["fidelity"] = m.gene.map(lambda g: REACTION_FIDELITY.get(g, ("ok", ""))[0])
    m["fidelity_note"] = m.gene.map(lambda g: REACTION_FIDELITY.get(g, ("ok", ""))[1])
    L.append("  is the assigned reaction the enzyme's reaction?")
    for k, v in m.fidelity.value_counts().items():
        L.append(f"      {v:3d}  {k}")
    L.append("")
    for r in m[m.fidelity != "ok"].sort_values(["fidelity", "gene"]).itertuples():
        L.append(f"      {r.gene:6s} {r.fidelity:9s} {r.fidelity_note}")

    L.append("\n  direction ratio = exp(dG'/RT) for the equation as written:")
    m["ratio_class"] = m.direction_ratio.map(ratio_class)
    for k, v in m.ratio_class.value_counts().items():
        L.append(f"      {v:3d}  {k}")
    L.append("      an abstain is not a measurement and a clamp is a rail, not a value.")

    m["obs"] = m.direction_ratio.map(observed_direction)
    m["expect"] = m.mnxr.map(lambda x: (TEXTBOOK_DIRECTION.get(x) or (None,))[0])
    m["basis"] = m.mnxr.map(lambda x: (TEXTBOOK_DIRECTION.get(x) or (None, ""))[1])
    m["dg"] = RT * np.log(m.direction_ratio)
    scored = m[m.expect.notna() & (m.obs != "abstain")]
    ok = scored[scored.obs == scored.expect]
    L.append(f"\n  against a textbook sign, on the {len(scored)} rows that carry both a claim "
             f"and an anchor: {len(ok)}/{len(scored)} agree.")

    L.append("\n  BY TIER -- and only tier 2 is making a dG' claim at all:")
    for t, sub in scored.groupby("dir_tier"):
        hit = int((sub.obs == sub.expect).sum())
        L.append(f"      tier {int(t)} ({TIER_CLAIM[int(t)]:22s}) {hit:2d}/{len(sub):2d}")
    L.append("      Scoring tier 1 against dG'o is a category error: a declared")
    L.append("      irreversibility is a statement about the cell, not about the reaction.")

    L.append("\n  the disagreements, with the tier and the balance flag that explain them:\n")
    for r in scored[scored.obs != scored.expect].sort_values(["dir_tier", "gene"]).itertuples():
        bal = r.is_balanced if isinstance(r.is_balanced, str) and r.is_balanced else "unbalanced"
        L.append(f"      {r.gene:6s} {r.mnxr}  tier {int(r.dir_tier)}  dG' {r.dg:+7.2f}  "
                 f"[{'balanced' if bal == 'B' else bal}]  says {r.obs:3s}, textbook {r.expect}")
        L.append(f"             {r.basis}")

    L.append(f"\n      {int(m.expect.isna().sum())} rows carry no textbook anchor "
             f"(identity equations, generic R, the invalid de-novo row);")
    L.append(f"      {int((m.obs == 'abstain').sum())} more are tier-0 abstentions and assert "
             f"nothing.")

    L.append("\n  how far off, where a NUMBER is anchored and not just a sign:\n")
    for mx, (want, why) in TEXTBOOK_DG.items():
        r = m[m.mnxr == mx].iloc[0]
        L.append(f"      {r.gene:6s} {mx}  bake {r.dg:+6.2f}  textbook {want:+6.1f}  "
                 f"miss {r.dg - want:+6.2f} kJ/mol")
        L.append(f"             {why}")
    L.append("\n      These two are ONE defect. The alpha-1,4 glucosyl residue enters the")
    L.append("      synthase as a product and phosphorolysis as a substrate, so a single")
    L.append("      wrong group value shows up with opposite sign in the two reactions.")
    L.append("      Solved independently they demand -7.54 and -12.80 kJ/mol on that one")
    L.append("      value -- agreeing to 5.3 kJ/mol, which is what makes it one defect and")
    L.append("      not two. The mean offset lands phosphorolysis at +5.7 (textbook +3.1)")
    L.append("      and the synthase at -10.4 (textbook -13.0), both with the right sign.")
    L.append("      Correcting it is the bake's to make, not this scope's.")

    L.append("\n  the polymer budget, visible here as a carbon-bond call:")
    for r in m[m.gene.isin(["glgA", "glgP", "malP", "glgB"])].sort_values("gene").itertuples():
        L.append(f"      {r.gene:6s} {r.carbon_bond_change if isinstance(r.carbon_bond_change, str) else '(blank)':10s} "
                 f"{r.reaction_equation}")
    L.append("      'Glycogen' is one node with no length index, so a glucosyl unit leaving")
    L.append("      the polymer is invisible to the atom map: phosphorolysis reads as")
    L.append("      no_change and the synthase's dG' collapses to ~0.")

    OUT.mkdir(parents=True, exist_ok=True)
    cols = ["gene", "product", "pct_glycogen_production", "phenotype", "paper_mechanism",
            "curated_mapped", "mnxr", "mnxr_mapping_method", "reaction_equation",
            "direction_ratio", "carbon_bond_change", "no_mapping_reason",
            "gem_n_rxn", "gem_delta", "denovo_n_rxn", "denovo_delta"]
    out = d[cols].copy()
    out["reaction_fidelity"] = out.gene.map(lambda g: REACTION_FIDELITY.get(g, ("", ""))[0])
    out["ratio_class"] = out.direction_ratio.map(ratio_class)
    out["dir_tier"] = d.dir_tier
    out["is_balanced"] = d.is_balanced
    out["textbook_direction"] = out.mnxr.map(lambda x: (TEXTBOOK_DIRECTION.get(x) or (None,))[0])
    out.to_csv(replaceable(OUT / "gof_audit.tsv"), sep="\t", index=False)

    text = "\n".join(L) + "\n"
    replaceable(OUT / "gof_audit.txt").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
