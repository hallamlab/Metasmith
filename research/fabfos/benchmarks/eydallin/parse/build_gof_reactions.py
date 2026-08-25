#!/usr/bin/env python3
"""Attach reaction chemistry to `gof.csv`: which MetaNetX reaction each gene's ORF
was scored against by the ECSPr GEM channel, its equation, its baked direction
ratio, and whether it changes a carbon skeleton -- plus the full candidate list
that choosing one reaction per gene throws away.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/parse/build_gof_reactions.py
    ... --publish        # rewrites gof.csv in place, writes gof_reaction_edges.csv beside it

FOUR OUTCOMES PER GENE. `mnxr_mapping_method` says which: `GEM` (the curated model
has it), `LLM review` (the model doesn't, but a specific MetaNetX reaction was
hand-picked and cited -- see `MANUAL_MNXR` below), `denovo` (neither of the above,
but the de-novo GPR channel's independent projection methods agree on one reaction
-- see `denovo_gapfill` below), or blank, in which case `no_mapping_reason` says
why, as one of eleven categories (see `NO_MAPPING_REASON` below). A blank `mnxr` cell
and a blank `no_mapping_reason` cell never both happen; every one of the 86 genes
lands in exactly one of the four outcomes, on purpose -- neither the LLM review nor
the denovo channel stretches to cover a gene with no real reaction, and the reason
category does not stand in for one that was simply never looked up.

GENE -> REACTION IS MANY-TO-MANY, AND gof.csv IS ONE ROW PER GENE. The GEM channel
(`data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet`, `iECDH1ME8569_1439`'s GPR
against the 86 clone ORFs) nominates more than one `mnxr` for 16 of its 34 covered
genes -- isozyme entries, or one ORF sitting in more than one GPR rule (`ptsI` alone
carries 19). Only 34 of the 86 genes are covered by this channel at all; the rest
have no curated-GEM reaction to attach and get blank cells here, same "nothing
silently dropped" rule as `build_gof_table.py`. So two things get written: `gof.csv`
gets exactly one PRIMARY reaction per gene (still 86 rows), and every candidate
that lost -- with the evidence it lost on -- goes to `gof_reaction_edges.csv`
beside it, so the pick is checkable rather than silently swallowed.

THE JOIN KEY IS `condition_id`, NOT `feature_name`. `gpr_gem.parquet`'s
`feature_name` is the model's CURRENT gene symbol after `resolve_gene_manual.py`'s
b-number resolution (`pfs` arrives there as `mtn`, `yifJ` as `wzxE`, `aspP` as
`nudF`) -- joining `gof.csv`'s paper-spelling `gene` column against it silently
drops every renamed gene. `condition_id` (`eydallin:<gene>`) is written straight
from `extraction.tsv`'s own spelling by the same script, so it is the stable key;
this script joins on `condition_id.split(":")[1]`, not `feature_name`.

THE PICK IS AN in_atom_universe TIE-BREAK, NOT AN EVIDENCE RANKING, BECAUSE THERE
IS NO EVIDENCE TO RANK ON YET: every row in `gpr_gem.parquet` today carries
`raw_score=1.0` and `evidence_quality="unknown"` -- presence/absence GPR calls,
not scored ones. Ranking on a constant would be an arbitrary row order dressed up
as a ranking. `in_atom_universe` is the one real discriminator available -- it says
whether the reaction has atom-mapping data behind it at all, which is also exactly
what the carbon-bond column below needs -- so the primary pick prefers
`in_atom_universe=True`, then breaks any remaining tie on `mnxr` for determinism.
When `gpr_gem.parquet` grows real per-reaction scores this tie-break is the first
thing to replace.

`reaction_equation` IS NAMES, NOT MNXM IDS. `reactions.parquet`'s own `equation`
column is MetaNetX's internal notation (`1 MNXM1105977@MNXD1 = 1 MNXM40333@MNXD1 +
...`); `legible_equation()` swaps every id for `metabolites.parquet`'s name, drops
the compartment tag (every compound here sits in one), and writes `->` instead of
`=` so it reads substrates-consumed -> products-made. All 131 compound ids this
cohort's reactions use resolve to a name; nothing here silently falls back to the
raw id.

CARBON-BOND CHANGE IS READ OFF THE BAKED ATOM MAP, NOT GUESSED FROM THE EQUATION
STRING. `atom_pairs.parquet` (via `bake_pairs.py`, this tree's shared decode of
`data/fabfos/processed/metabolism_bake`) already pairs each carbon atom on the
substrate side of a reaction to the carbon atom it becomes on the product side.
Group those pairs into connected components by shared atoms: a component spanning
more than one SUBSTRATE molecule means those molecules' carbons became bonded
(`creates`); a component spanning more than one PRODUCT molecule means one
molecule's carbons came apart (`breaks`); a component touching more than one
molecule on both sides is `both`; every component 1:1 is `no_change`. A reaction
with no carbon atoms in the map at all (nothing in `in_atom_universe`, or a
reaction with no carbon on either side, e.g. a proton antiport) is left blank --
unknown, not "no change". This was checked against three of the resolved
reactions: HSK (homoserine kinase, `MNXR100737`) is a clean 1:1 phosphorylation
-> `no_change`; 5'-deoxyadenosine nucleosidase (`MNXR152703`) splits one molecule
into adenine + ribose -> `breaks`; glucosamine-6-phosphate deaminase
(`MNXR115917`) is a 1:1 deamination -> `no_change`.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(HERE.parent))
import bake_pairs                                                             # noqa: E402

GOF_DIR = REPO / "data/fabfos/runs/eydallin_clones/parse/gof"
GOF_CSV = GOF_DIR / "gof.csv"
EDGES_CSV = GOF_DIR / "gof_reaction_edges.csv"
GPR_GEM = REPO / "data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet"
GPR_DENOVO = REPO / "data/fabfos/runs/eydallin_clones/gpr/gpr_denovo.parquet"
DENOVO_MAX_CANDIDATES = 10
REACTIONS = REPO / "data/fabfos/processed/lookups/reactions.parquet"
METABOLITES = REPO / "data/fabfos/processed/lookups/metabolites.parquet"

EQUATION_TERM_RE = re.compile(r"(\d+(?:\.\d+)?) (\S+?)@\S+")

# LLM REVIEW, THE THIRD CHANNEL, FOR THE 52 GENES `iECDH1ME8569_1439` HAS NO TRACE OF AT
# ALL (`resolve_gene_manual.py`'s exhaustive search -- every gene identifier plus a
# raw text grep of every gene_reaction_rule -- returns nothing for any of them). Not
# in the curated GEM does not mean not a real enzyme: MetaNetX's full reaction
# universe (`reactions.parquet`, ~84k reactions, independent of any one organism's
# model) still carries some of their textbook chemistry. Each entry here is a
# specific `mnxr` chosen by hand -- product identity checked by name against the
# characterized activity, not by grepping a keyword and taking the first hit -- and
# is the ONLY source outside the GEM/de-novo channels this pipeline lets write a
# gene straight to an `mnxr`, same as `gpr_manual.parquet`'s own `--decisions` gate
# does for the GEM channel.
MANUAL_MNXR = {
    # c-di-GMP phosphodiesterase PdeC: c-di-GMP + H2O -> pGpG. Textbook reaction;
    # the curated GEM has no c-di-GMP metabolite or reaction of any kind.
    "yjcC": ("MNXR96524", "c-di-GMP phosphodiesterase: c-di-GMP + H2O -> pGpG"),
    # diguanylate cyclase DgcP: 2 GTP -> c-di-GMP + 2 PPi. Same gap, opposite step.
    "yeaP": ("MNXR97353", "diguanylate cyclase: 2 GTP -> c-di-GMP + 2 diphosphate"),
    # omega-amidase / 2-oxoglutaramate amidase: 2-oxoglutaramate + H2O ->
    # 2-oxoglutarate + NH4+. EC 3.5.1.3, gene's own annotated name.
    "yafV": ("MNXR105306", "2-oxoglutaramate amidase: 2-oxoglutaramate + H2O -> "
             "2-oxoglutarate + NH4+"),
    # D-phenylhydantoinase: ring-opening hydrolysis of the hydantoin the gene is
    # named for. Substrate here is the unsubstituted phenylhydantoin MetaNetX
    # carries; the paper's clone is the broader enzyme, substrate-general in vivo.
    "hyuA": ("MNXR160637", "D-phenylhydantoinase: phenylhydantoin + H2O -> "
             "phenylureidoacetic acid"),
    # 2',3'-cyclic-nucleotide 2'-phosphodiesterase, cpdB's own annotated activity
    # (EC 3.1.4.16) -- generic-nucleotide form, since cpdB is broad-specificity and
    # MetaNetX's compound is the generic "2',3'-cyclic nucleotide".
    "cpdB": ("MNXR148152", "2',3'-cyclic-nucleotide 2'-phosphodiesterase: "
             "2',3'-cyclic nucleotide + H2O -> nucleoside 2'-phosphate"),
    # promoted from a denovo_gapfill review (see below): glutathione S-transferase,
    # EC 2.5.1.18, matches the gene's own annotated family exactly. The generic "RX"
    # substrate is how MetaNetX represents every GST reaction -- GSTs are
    # textbook-broad-specificity enzymes, so a non-specific substrate here is the
    # correct representation, not the vagueness that sank ucpA/yabI/ylcG/yqjA below.
    "yncG": ("MNXR165804", "glutathione S-transferase (EC 2.5.1.18): RX + "
             "glutathione -> a halide anion + an S-substituted glutathione"),
}

# DE-NOVO CANDIDATES REVIEWED BY HAND AND NOT PROMOTED, for the 4 remaining
# `uncharacterized` genes with candidates -- so "not promoted" reads as a checked
# decision, not an omission the next reader has to redo the same search to explain.
#   ucpA -- two methods disagree on WHICH reaction: hmm_bitscore (295, strong)
#           names a fatty-acid-biosynthesis SDR reductase (K00059/FabG-like),
#           knn_vote (0.29, weak) names an unrelated L-xylulose reductase. The
#           strong hit is a short-chain-dehydrogenase FOLD match, not evidence
#           ucpA does FabG's specific reaction -- E. coli already has a fabG.
#   yabI  -- contradictory: an ABC-transporter ATPase (weak) vs. a self-to-self
#           lipid entry (perfect knn score, but a degenerate placeholder, not a
#           reaction). yqjA below hits the identical UniProt accession by knn --
#           the embedding model is finding a DedA-family paralog, not a function.
#   ylcG  -- contradictory across 98 candidates: a protease vs. a lysophospholipase,
#           neither corroborated by the other method.
#   yqjA  -- one method only (knn_vote), and the hit is a same-compound-both-sides
#           entry, degenerate as written; it IS a MetaNetX transport reaction
#           (compartment differs, stripped by `legible_equation`) for a plausible
#           DedA-family flippase role, but nothing here pins WHICH phospholipid,
#           and a single uncorroborated method isn't enough to promote regardless.

# FOR EVERYTHING ELSE, ONE CATEGORY FROM AN ELEVEN-WAY SPLIT -- so a blank `mnxr` cell
# says WHY rather than looking unchecked, without multiplying into one sentence per
# gene or a second near-duplicate column. `resolve_gene_manual.py` already
# confirmed all these genes have no trace in the curated GEM; this is what that
# absence means, checked against each gene's own product string in `gof.csv`. Genes
# `denovo_gapfill` resolves (see above) are removed from here at runtime -- this
# table is the reason for the genes IT leaves uncovered, not a static list.
#
#   regulatory                -- a transcription factor, sigma factor, or other
#                                 regulatory component with no substrate to react
#                                 at all (15 genes)
#   transport                 -- a transporter with no single defined substrate
#                                 (broad/multidrug efflux, an uncharacterized
#                                 cation channel) (4 genes)
#   proteolysis                -- degrades protein substrates (1 gene: `clpA`)
#   dna_replication             -- part of the replisome (1 gene: `holC`)
#   dna_repair                 -- repairs DNA lesions (2 genes: `phr`, `recQ`)
#   rna_processing              -- degrades/processes RNA (1 gene: `pnp`)
#   translation                -- acts on the ribosome/nascent peptide (1 gene:
#                                 `prfB`)
#   toxin_antitoxin             -- a toxin-antitoxin system toxin, target is a
#                                 macromolecule (membrane or mRNA), not a
#                                 metabolite (2 genes: `hokA`, `yoeB`)
#   envelope_assembly_or_secretion -- structural or assembly machinery for the
#                                 cell envelope/secretion apparatus, no metabolite
#                                 substrate (4 genes: `gspD`, `napF`, `ppdB`, `wzc`)
#   uncharacterized             -- function or substrate not established in the
#                                 literature, including the one confirmed
#                                 pseudogene (no product to characterize at all)
#   ambiguous_metabolic_reaction -- IS a real, specific metabolic enzyme, and
#                                 MetaNetX has the reaction, but it can't be told
#                                 apart from a paralog or a broad substrate class
#                                 by compound name alone (2 genes: `erfK`, `nagD`)
#
# These are the genes' own established functional categories, not a paraphrase of
# "why no reaction" invented for this column -- so a class of one (`clpA`, `holC`,
# `pnp`, `prfB`) is still the right class, not a signal to merge it into a vaguer
# neighbor. `transport`, `dna_repair`, `toxin_antitoxin`, and
# `envelope_assembly_or_secretion` hold >=2 genes each on their own; nothing here
# is split finer than the biology actually supports.
NO_MAPPING_REASON = {
    "csrA": "regulatory", "csrD": "regulatory", "exuR": "regulatory",
    "galS": "regulatory", "glgS": "regulatory", "malT": "regulatory",
    "mlc": "regulatory", "ptsN": "regulatory", "rbsR": "regulatory",
    "rpoS": "regulatory", "tdcA": "regulatory", "yfdN": "regulatory",
    "yfeD": "regulatory", "yfjR": "regulatory", "yncC": "regulatory",

    "mdtG": "transport", "yegH": "transport", "yjcQ": "transport", "yoaE": "transport",

    "clpA": "proteolysis",
    "holC": "dna_replication",
    "phr": "dna_repair", "recQ": "dna_repair",
    "pnp": "rna_processing",
    "prfB": "translation",
    "hokA": "toxin_antitoxin", "yoeB": "toxin_antitoxin",
    "gspD": "envelope_assembly_or_secretion", "napF": "envelope_assembly_or_secretion",
    "ppdB": "envelope_assembly_or_secretion", "wzc": "envelope_assembly_or_secretion",

    "smg": "uncharacterized", "ucpA": "uncharacterized", "yabI": "uncharacterized",
    "ybcV": "uncharacterized", "ycbJ": "uncharacterized", "yciN": "uncharacterized",
    "ydcJ": "uncharacterized", "yfaY": "uncharacterized", "ylcG": "uncharacterized",
    "ymgC": "uncharacterized", "ynbD": "uncharacterized",
    "yqjA": "uncharacterized", "yhcE": "uncharacterized",

    "erfK": "ambiguous_metabolic_reaction", "nagD": "ambiguous_metabolic_reaction",
}

GOF_IDENTITY_COLS = ("gene", "source_genome", "locus_tag", "product",
                      "pct_glycogen_production")
GOF_REACTION_COLS = ("mnxr", "mnxr_mapping_method", "reaction_equation",
                      "direction_ratio", "carbon_bond_change", "no_mapping_reason")
EDGE_COLS = ("gene", "channel", "model_gene_symbol", "mnxr", "is_primary",
             "primary_reason", "intermediate_id", "intermediate_name", "raw_score",
             "evidence_quality", "in_atom_universe", "gpr_rule", "reaction_equation",
             "direction_ratio", "carbon_bond_change")


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
    """`pairs_for_mnxr` is `atom_pairs`'s rows for one `mnxr`, `element == "C"` only.
    See the module docstring for what each outcome means."""
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
    """MetaNetX writes `1 MNXM1105977@MNXD1 = 1 MNXM40333@MNXD1 + ...` -- ids and a
    bare `=`, compartment tag included even though every compound in this cohort's
    reactions sits in one compartment. Swap each id for `metabolites.parquet`'s name,
    drop the compartment tag, drop a bare `1` coefficient, and write the arrow a
    stoichiometry-reader actually reads left-to-right as consumed -> produced."""
    lhs, rhs = equation.split(" = ")

    def side(text: str) -> str:
        terms = []
        for coeff, mid in EQUATION_TERM_RE.findall(text):
            name = names.get(mid, mid)
            terms.append(name if coeff == "1" else f"{coeff} {name}")
        return " + ".join(terms)

    return f"{side(lhs)} -> {side(rhs)}"


def pick_primary(candidates: pd.DataFrame) -> tuple[pd.Series | None, str]:
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


def denovo_gapfill(gpr_denovo: pd.DataFrame, genes: set[str]) -> dict[str, tuple[str, str]]:
    """The de-novo channel is noisy by design -- ~3 independent projection methods
    (HMM/KO bitscore, BLAST reciprocal-best-hit, an embedding k-NN vote) each throw
    candidates at a gene, on scales that aren't comparable to each other (a bitscore
    of 295 and a knn_vote of 0.04 aren't the same kind of number), so ranking by
    `raw_score` the way `pick_primary` does for the GEM channel would just reward
    whichever method happens to score highest, not the gene's actual reaction. What
    IS comparable across methods is agreement: gap-fill a gene only when >=2 of
    those independent methods land on the exact same `mnxr`, the way
    `resolve_gene_manual.py`'s own two-method-agreement check works for the GEM
    channel. And only when the candidate pool is small (<=10) -- `ylcG` has 99
    candidates because its EC hit (3.1.1.4, a broad phospholipase class) enumerates
    every reaction MetaNetX carries under that EC number, so its top knn_vote pick
    landing inside that huge pool is a coincidence of pool size, not real
    convergence, and is excluded here even though it technically clears the
    2-method bar."""
    out: dict[str, tuple[str, str]] = {}
    sub = gpr_denovo[gpr_denovo["gene"].isin(genes)]
    for gene, g in sub.groupby("gene"):
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                     help=f"rewrite {GOF_CSV.relative_to(REPO)} and write "
                          f"{EDGES_CSV.relative_to(REPO)}")
    a = ap.parse_args()

    # keep only the identity columns from whatever's on disk -- a prior run of this
    # script may have written a now-retired reaction column, and re-reading its
    # output must not carry that stale column into DictWriter's strict fieldnames.
    gof_rows = [{k: row[k] for k in GOF_IDENTITY_COLS}
                for row in csv.DictReader(GOF_CSV.open())]
    gpr = pd.read_parquet(GPR_GEM)
    gpr["gene"] = gpr["condition_id"].str.split(":").str[1]
    gpr_denovo = pd.read_parquet(GPR_DENOVO)
    gpr_denovo["gene"] = gpr_denovo["condition_id"].str.split(":").str[1]
    met_names = pd.read_parquet(METABOLITES).set_index("mnxm")["name"].to_dict()
    raw_equations = pd.read_parquet(REACTIONS).set_index("mnxr")["equation"].to_dict()
    reactions = {mnxr: legible_equation(eq, met_names) for mnxr, eq in raw_equations.items()}
    ratios = pd.read_parquet(bake_pairs.direction_ratios()).set_index("mnxr")["ratio"].to_dict()
    atom_pairs = pd.read_parquet(bake_pairs.atom_pairs())
    atom_pairs_by_mnxr = {m: g for m, g in atom_pairs.groupby("mnxr")}

    # Gap-fill candidates are the `uncharacterized` bucket ONLY, not every gene the
    # GEM/manual channels miss. Trying this against the full miss-list first showed
    # why: `clpA` and `recQ` -- a protease and a DNA helicase, both well-established
    # as non-metabolic -- converged on the exact SAME generic ATP-hydrolysis
    # reaction, a domain-similarity artifact (any ATPase's catalytic domain matches
    # it), not a real gene-specific hit; `csrD`/`holC` were similarly spurious. A
    # gene the literature has already placed outside metabolism needs a real
    # dissenting citation to move, the way `MANUAL_MNXR` requires one -- not an
    # algorithmic coincidence. `uncharacterized` genes carry no such prior claim, so
    # convergent de-novo evidence is actually informative there.
    uncharacterized = {g for g, cat in NO_MAPPING_REASON.items() if cat == "uncharacterized"}
    denovo_picks = denovo_gapfill(gpr_denovo, uncharacterized)

    print(f"{len(gof_rows)} genes in gof.csv, {len(gpr)} gene-reaction rows in "
          f"{GPR_GEM.name} covering {gpr['gene'].nunique()} genes, "
          f"{len(reactions):,} reaction equations, {len(ratios):,} direction "
          f"ratios, {len(atom_pairs_by_mnxr):,} reactions with atom-mapped pairs")
    print(f"{len(uncharacterized)} genes are uncharacterized; the de-novo channel "
          f"gap-fills {len(denovo_picks)} of those with >=2-method agreement on one "
          f"reaction: {sorted(denovo_picks)}")

    bond_cache: dict[str, str] = {}

    def bond_for(mnxr: str) -> str:
        if mnxr not in bond_cache:
            pairs = atom_pairs_by_mnxr.get(mnxr)
            bond_cache[mnxr] = carbon_bond_change(pairs) if pairs is not None else ""
        return bond_cache[mnxr]

    edge_rows, n_multi, n_gem, n_manual, n_denovo, n_no_reaction = [], 0, 0, 0, 0, 0
    for r in gof_rows:
        gene = r["gene"]
        candidates = gpr[gpr["gene"] == gene]
        primary, reason = pick_primary(candidates)
        if not candidates.empty and len(candidates) > 1:
            n_multi += 1

        for _, c in candidates.iterrows():
            mnxr = c["mnxr"]
            is_primary = primary is not None and c["mnxr"] == primary["mnxr"] and \
                c["intermediate_id"] == primary["intermediate_id"]
            edge_rows.append({
                "gene": gene,
                "channel": "GEM",
                "model_gene_symbol": c["feature_name"],
                "mnxr": mnxr,
                "is_primary": "yes" if is_primary else "no",
                "primary_reason": reason if is_primary else "",
                "intermediate_id": c["intermediate_id"],
                "intermediate_name": c["intermediate_name"],
                "raw_score": c["raw_score"],
                "evidence_quality": c["evidence_quality"],
                "in_atom_universe": c["in_atom_universe"],
                "gpr_rule": c["gpr_rule"],
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })

        channel, no_mapping_reason = "", ""
        if primary is not None:
            channel = "GEM"
            n_gem += 1
        elif gene in MANUAL_MNXR:
            mnxr, note = MANUAL_MNXR[gene]
            channel = "LLM review"
            n_manual += 1
            primary = pd.Series({"mnxr": mnxr})
            edge_rows.append({
                "gene": gene, "channel": "LLM review", "model_gene_symbol": gene,
                "mnxr": mnxr, "is_primary": "yes", "primary_reason": note,
                "intermediate_id": "", "intermediate_name": "", "raw_score": "",
                "evidence_quality": "LLM review", "in_atom_universe": "", "gpr_rule": "",
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })
        elif gene in denovo_picks:
            mnxr, note = denovo_picks[gene]
            channel = "denovo"
            n_denovo += 1
            primary = pd.Series({"mnxr": mnxr})
            edge_rows.append({
                "gene": gene, "channel": "denovo", "model_gene_symbol": gene,
                "mnxr": mnxr, "is_primary": "yes", "primary_reason": note,
                "intermediate_id": "", "intermediate_name": "", "raw_score": "",
                "evidence_quality": "denovo", "in_atom_universe": "", "gpr_rule": "",
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })
        else:
            no_mapping_reason = NO_MAPPING_REASON.get(gene, "uncharacterized")
            n_no_reaction += 1

        r["mnxr"] = primary["mnxr"] if primary is not None else ""
        r["mnxr_mapping_method"] = channel
        r["reaction_equation"] = reactions.get(r["mnxr"], "") if primary is not None else ""
        r["direction_ratio"] = ratios.get(r["mnxr"], "") if primary is not None else ""
        r["carbon_bond_change"] = bond_for(r["mnxr"]) if primary is not None else ""
        r["no_mapping_reason"] = no_mapping_reason

    print(f"\n{n_gem}/{len(gof_rows)} genes resolved via gpr_gem.parquet, "
          f"{n_manual} more via LLM review, {n_denovo} more via de-novo "
          f"gap-fill, {n_no_reaction} have no reaction (see no_mapping_reason) -> "
          f"{n_gem + n_manual + n_denovo}/{len(gof_rows)} covered")
    print(f"{n_multi} GEM-channel genes had more than one candidate reaction -> "
          f"{len(edge_rows)} edge rows")
    n_bond = sum(1 for r in edge_rows if r["carbon_bond_change"])
    print(f"{n_bond}/{len(edge_rows)} edges have an atom-mapped carbon-bond call "
          f"({len(edge_rows) - n_bond} have no carbon atom-mapping to call it from)")
    reason_counts = Counter(r["no_mapping_reason"] for r in gof_rows if r["no_mapping_reason"])
    print("no_mapping_reason breakdown:", dict(reason_counts))

    if not a.publish:
        print(f"\n(dry run -- pass --publish to rewrite {GOF_CSV.relative_to(REPO)} "
              f"and write {EDGES_CSV.relative_to(REPO)})")
        return 0

    with GOF_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(GOF_IDENTITY_COLS) + list(GOF_REACTION_COLS))
        w.writeheader()
        w.writerows(gof_rows)
    with EDGES_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(EDGE_COLS))
        w.writeheader()
        w.writerows(edge_rows)
    print(f"\n-> {GOF_CSV}: {len(gof_rows)} rows")
    print(f"-> {EDGES_CSV}: {len(edge_rows)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
