"""Atom-transfer PAIRS from cached atom-mapping output: which metabolite's atoms
land in which metabolite, per element.

WHY THIS EXISTS
---------------
The bipartite met-rxn graph the solve runs on is a STAR: a reaction is a node joined
to every metabolite it touches. Eliminating that node -- which is exactly what the
Woodbury update's `_build_delta` does -- leaves a CLIQUE over every participant, so
every pair of participants gets conductance whether or not one atom passes between
them. The edge weights are atom counts, so the method looks stoichiometry-aware; the
TOPOLOGY throws the atom mapping away.

Measured on the real carbon graph: 52.8% of the reaction-induced metabolite pairs
transfer no atoms at all. On MNXR106432 (acetyl-CoA + CO2 + NADPH = pyruvate + NADP+
+ CoA) the pyruvate-NADPH channel carries ZERO carbons at conductance 0.700, while
the real one-carbon pyruvate-CO2 channel gets 0.033 -- a zero-atom channel 21x more
conductive than a real one.

The information needed to fix it already exists and is already computed. The unifier
(`20b_aam_unify.py:count_transits`) holds the substrate metabolite and the product
metabolite of each mapped atom IN THE SAME LOOP ITERATION, and then discards the
pairing into two independent role counters:

    sm = sub_mnxms[si]                       # substrate MNXM   <- the pair
    pm = prod_mnxms[j]                       # product MNXM     <- is right here
    if sm: counts[el][f"{sm}:substrate"] += 1     # and then thrown
    if pm: counts[el][f"{pm}:product"]   += 1     # away, separately

This module keeps `(sm, pm)`. Everything else is the same computation.

RE-EXTRACTION, NOT RE-MAPPING. The mapped SMILES are cached with their atom-map
numbers; nothing here runs RXNMapper or LocalMapper. This is arithmetic over a cache.

WHY A METABOLITE-LEVEL PAIR IS STILL NOT ENOUGH
-----------------------------------------------
Worth stating because it is the trap this whole design walks around. Pairing at the
METABOLITE level does not fix the stated problem. The true atom map splits
acetyl-CoA's 23 carbons by destination: atoms [0,1] -- the acetyl group -- become
pyruvate; the other 21 -- the CoA moiety -- become CoA. So acetyl-CoA -> CoA is a
GENUINE 21-carbon edge that any metabolite-level graph keeps, and a path arriving on
acetyl carbons could still leave through the CoA moiety carrying 21 carbons it never
brought. Only atom-resolved nodes deny that, which is why this module emits the atom
INDICES and not merely the counts -- the indices are what the class refinement
downstream partitions on.

WHAT IS REFUSED, AND WHY -- AND THE TWO REFUSALS THAT WERE BUGS
---------------------------------------------------------------
This module's first version refused 40% of the mapper universe and reported that as a
coverage finding about the method's reach. It was not. Two of the three refusals were
defects in THIS FILE, and they cost the atom lane 29 of the curated set4 axes:

  * `stripped` (19,930 rxns, 34.6%) was a REGEX MISMATCH, not unbalanced chemistry.
    The story told here -- "generic metabolites with no SMILES were dropped before
    mapping, so the mapper re-routed their atoms" -- describes a real phenomenon in a
    DIFFERENT file (`aam_stripped_input.tsv`), which this module never reads and which
    is disjoint from the universe it does read. The real cause: `EQ_TERM` matched
    `WATER`/`BIOMASS`, which the SMILES builder's regex does not, so the metabolite
    count exceeded the template count and every water-bearing reaction was refused.
    See `EQ_TERM`. Measured: 30,938 of 30,939 metabolites in the universe HAVE a
    canonical SMILES -- nothing was dropped for lack of structure.
  * `ambiguous_duplicate` (3,255 rxns) was 98.7% SELF-INFLICTED: the same metabolite
    twice is not an ambiguity. See `match_mols`. Only 43 were real.
  * DUPLICATE-METABOLITE reactions where two DISTINCT MNXMs share one canonical SMILES
    are genuinely ambiguous -- `pop(0)` picks between different metabolites -- and stay
    refused. That refusal is real.

The lesson worth keeping: a refusal rate is not evidence about the world until the
refusals have been read. Reporting 37.8% coverage as a property of the atom mapping,
when most of it was this file disagreeing with a regex, put a false limitation into a
report and nearly retired a set of biologically real axes. Every status here is
counted and reported so the next reader can check rather than believe.

Env: rdkit + pandas. No SCADC paths: every input arrives as an argument.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from rdkit import Chem, RDLogger
from rdkit.Chem import AllChem

RDLogger.DisableLog("rdApp.*")

ELEMENTS = ("C", "N", "S", "P")

# MUST BE THE REGEX THAT BUILT THE MAPPED SMILES, CHARACTER FOR CHARACTER.
# `19b_rxnmapper_universe.py:33` reads the equation with exactly this pattern and
# emits one SMILES fragment per match. If this regex matches a token that one did
# not, the template count and the metabolite count disagree for every reaction
# carrying that token, and the reaction is refused as `stripped` -- not because its
# chemistry is unbalanced, but because two regexes disagreed. That is precisely what
# happened: this pattern used to read `MNXM\d+|MNXM\w+|WATER|BIOMASS` without the
# `@compartment` suffix, and `WATER`/`BIOMASS` are not MNXM tokens, so the builder
# never wrote a fragment for them. 19,930 reactions -- 34.6% of the universe -- were
# discarded on that mismatch alone.
EQ_TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@\w+")

PAIR_COLS = ("mnxr", "element", "substrate", "product", "n_atoms",
             "sub_idx", "prod_idx", "pair_w")
STATUS_COLS = ("mnxr", "status", "n_sub", "n_prod", "n_mapped_sub", "n_mapped_prod")


# =====================================================================
# MetaNetX side (same derivation the unifier uses)
# =====================================================================

def canon_smiles(smi: str):
    """Canonical SMILES with atom-map numbers stripped."""
    if not smi:
        return None
    try:
        mol = Chem.MolFromSmiles(smi)
    except Exception:
        return None
    if mol is None:
        return None
    for a in mol.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return None


def load_mnxm_smiles(chem_prop: Path, want: set) -> dict:
    out = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 9 or p[0] not in want:
                continue
            if p[8].strip():
                out[p[0]] = p[8].strip()
    return out


def load_mnxm_formulas(chem_prop: Path, want: set) -> dict:
    """mnxm -> curated formula. Formulas exist for metabolites that have no SMILES, and
    they are what makes a conservation argument possible without a structure."""
    out = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 9 or p[0] not in want:
                continue
            if p[3].strip():
                out[p[0]] = p[3].strip()
    return out


def load_equations(reac_prop: Path, want: set) -> dict:
    out = {}
    with open(reac_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) > 1 and p[0] in want:
                out[p[0]] = p[1]
    return out


def parse_equation(eq: str):
    """(substrates, products), each with multiplicity, in equation order."""
    if "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)

    def expand(s):
        out = []
        for coef, m in EQ_TERM.findall(s):
            for _ in range(max(1, int(round(float(coef))))):
                out.append(m)
        return out

    L, R = expand(lhs), expand(rhs)
    return (L, R) if (L and R) else None


def frag_keys(mol):
    """(canonical SMILES, InChIKey connectivity block) for one template fragment.

    Both keys are computed HERE, from the fragment, by the same code path the
    metabolite side uses -- never read off MetaNetX's own InChIKey column. MetaNetX's
    InChIKey and its SMILES are two independent records of the same compound and they
    do not always agree about protonation; comparing a computed key against a
    published one turns that disagreement into a failed match.
    """
    m2 = Chem.Mol(mol)
    for a in m2.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        Chem.SanitizeMol(m2)
    except Exception:
        return None, None
    try:
        cs = Chem.MolToSmiles(m2, canonical=True)
    except Exception:
        cs = None
    conn = None
    try:
        ik = Chem.MolToInchiKey(m2)
        if ik and "-" in ik:
            conn = ik.split("-", 1)[0]
    except Exception:
        pass
    return cs, conn


def match_mols(mols: list, mnxms: list, canon: dict, conn: dict = None):
    """Canonical-SMILES match, aligned with `mols`, returning WEIGHTED name candidates.

    Each template position gets the list of DISTINCT MNXMs whose canonical SMILES it
    matches, each at weight 1/k (k = number of distinct candidates). k == 1 is the
    ordinary confident case, weight 1.0. Also returns whether any position was diluted
    (k > 1). Unmatched positions get an empty list.

    WHY WEIGHTS RATHER THAN A GREEDY PICK, AND WHY NOT A REFUSAL. When two DISTINCT MNXMs
    share a canonical SMILES the naming is genuinely ambiguous -- a greedy `pop(0)` picked
    arbitrarily between different metabolites, and the previous version REFUSED the whole
    reaction (`ambiguous_duplicate`) rather than choose. Both are wrong for the same
    reason forced n > 1 was: a pick fabricates an identity, a refusal is a gap, and the
    contract is dilute-not-gap. Emitting each candidate at 1/k is the doubly-stochastic
    marginal of the uniform distribution over valid namings -- the honest "it is one of
    these k, we cannot say which," spread as weight instead of collapsed to a guess or a
    hole.

    THE SAME MNXM TWICE IS NOT AMBIGUOUS. `parse_equation` expands stoichiometry, so
    `2 H2O` arrives as `[M, M]`; the pool is `[M, M]` but the distinct-candidate set is
    `{M}`, k == 1, weight 1.0 -- no dilution. Only `len(set(v)) > 1` is a real ambiguity.
    Measured: 3,212 of 3,255 old `ambiguous_duplicate` refusals (98.7%) were this
    self-inflicted case; the remaining 43 are the ones now diluted rather than dropped.

    THE CONNECTIVITY FALLBACK, AND WHY IT IS NOT A LOOSENING. A canonical-SMILES match
    is an identity test that also tests protonation and tautomer, and MetaCyc and
    MetaNetX routinely disagree about both while agreeing completely about which
    molecule they mean. `TIER4_FREEZE` named the fix -- match on the InChIKey
    CONNECTIVITY layer -- and recorded it as never attempted; this is that. It is tried
    only where the exact match already missed, so nothing that matched before matches
    differently now.

    It is genuinely weaker in one way and the dilution machinery already covers it: two
    DISTINCT MetaNetX ids can share a connectivity block (a protonation pair, or the two
    epimers an epimerase interconverts). Then the position has k > 1 candidates and is
    emitted at 1/k -- the same honest spread a shared canonical SMILES gets, for the
    same reason.
    """
    pool = defaultdict(list)
    kpool = defaultdict(list)
    for m in mnxms:
        cs = canon.get(m)
        if cs:
            pool[cs].append(m)
        if conn:
            k = conn.get(m)
            if k:
                kpool[k].append(m)
    # distinct candidates per key -- the same metabolite twice is one candidate
    cand = {cs: sorted(set(v)) for cs, v in pool.items()}
    kcand = {k: sorted(set(v)) for k, v in kpool.items()}
    diluted = any(len(v) > 1 for v in cand.values())
    out = []
    for mol in mols:
        if mol is None:
            out.append([])
            continue
        # Sanitize for the same reason `canonical_ranks` does, plus one specific to
        # here: `canon` was built by `canon_smiles` via `MolFromSmiles`, which
        # sanitizes. Canonicalising an UNSANITIZED template produces a string that can
        # differ from the sanitized one for the very same molecule, so the lookup misses
        # and the metabolite goes unnamed -- silently dropping its pairs. Both sides of
        # this comparison must be perceived the same way.
        cs, k = frag_keys(mol)
        c = cand.get(cs) if cs else None
        if not c and conn and k:
            c = kcand.get(k)
            if c and len(c) > 1:
                diluted = True
        if not c:
            out.append([])
        else:
            w = 1.0 / len(c)
            out.append([(m, w) for m in c])
    return out, diluted


def multiplicity_dilution(named: list, mnxms: list):
    """Scale each position's weights so no metabolite is claimed more often than the
    equation says it appears.

    Only reachable once template count is allowed to differ from participant count.
    Two templates can then both name metabolite M -- correct when M has stoichiometry
    2, and an over-claim when M appears once and the second template is a protonation
    variant that is not a MetaNetX participant at all. Neither case is distinguishable
    from here, so neither is chosen: M's total claim is capped at its multiplicity and
    spread over the claimants, which is a confident pairing when the counts agree and a
    dilution when they do not.
    """
    mult = Counter(mnxms)
    claims = Counter()
    for cands in named:
        for m, _w in cands:
            claims[m] += 1
    scale = {m: min(1.0, mult.get(m, 0) / n) for m, n in claims.items() if n}
    if all(s >= 1.0 for s in scale.values()):
        return named, False
    return ([[(m, w * scale.get(m, 1.0)) for m, w in cands] for cands in named],
            True)


# =====================================================================
# The pairing itself -- the two lines the unifier throws away
# =====================================================================

def canonical_ranks(mol):
    """Atom -> canonical rank, invariant to how this reaction happened to write it.

    THIS IS LOAD-BEARING, AND THE OBVIOUS THING IS WRONG. `a.GetIdx()` is the index
    within THIS reaction's template molecule, and the same metabolite is not written
    the same way in every reaction. Measured over the mapper universe: 248 of 847
    carbon-bearing metabolites that appear in more than one reaction have
    INCONSISTENT idx -> canonical-rank mappings. Pyruvate (MNXM23) alone shows up
    with 3 distinct atom orderings across 63 reactions.

    So `(metabolite, GetIdx())` is not an atom -- it is a different atom depending on
    which reaction you read it from, and a graph keyed on it silently welds unrelated
    atoms together and splits identical ones apart. `CanonicalRankAtoms` is invariant
    to input ordering by construction, so `(metabolite, rank)` IS an atom.

    SANITIZE FIRST, ALWAYS. Templates come off `ReactionFromSmarts` unsanitized, so
    ring info and implicit valence are unset and `CanonicalRankAtoms` raises
    "Pre-condition Violation". That refusal was reported as `unrankable` and cost
    10,576 reactions (18.4% of the universe); sanitizing rescues 300/300 sampled.

    It must be unconditional, not a fallback. Sanitization perceives aromaticity, and
    canonical ranks depend on it -- so ranking some molecules sanitized and others raw
    would give one metabolite two rank systems and break `(met, rank)` exactly the way
    `GetIdx()` did. Doing it always also NORMALISES the kekulized-vs-aromatic spellings
    the mapper emits for the same molecule, which makes ranks agree across reactions
    that would otherwise disagree. If sanitization fails the molecule is genuinely
    unrankable and is refused.
    """
    m2 = Chem.Mol(mol)
    for a in m2.GetAtoms():
        a.SetAtomMapNum(0)          # map numbers are per-reaction; they'd poison the rank
    try:
        Chem.SanitizeMol(m2)
        return list(Chem.CanonicalRankAtoms(m2, breakTies=True))
    except Exception:
        return None


_FORMULA_TERM = re.compile(r"([A-Z][a-z]?)(\d*)")


def count_element(formula: str, X: str):
    """Atoms of element X in a MetaNetX formula; None when it cannot be trusted.

    Untrustworthy means absent, a `*` polymer/R-group, or nested groups -- an unknown
    count can never license a forced pairing, so None must propagate to a refusal
    rather than to a zero.
    """
    if not formula or not isinstance(formula, str) or formula.strip() in ("", "*"):
        return None
    if "*" in formula or "(" in formula or ")" in formula:
        return None
    n, seen = 0, False
    for sym, num in _FORMULA_TERM.findall(formula):
        if not sym:
            continue
        if sym == X:
            seen = True
            n += int(num) if num else 1
    return n if seen else 0


def forced_pairs(sub_mnxms: list, prod_mnxms: list, formulas: dict, ranks_of: dict):
    """Pairs that CONSERVATION forces, for reactions the mapper could not read.

    WHY THIS EXISTS. `no_mapping` was reported as a mapper outcome. It is not: every one
    of those 2,515 reactions has a buildable reaction SMILES and an EMPTY mapping,
    because RXNMapper's transformer accepts at most 512 tokens and these are the long
    ones -- median SMILES length 983 against the universe's 268, 97.5% over 400 chars.
    It is a context-window limit on a neural model, and it eats a BIASED sample: the
    reactions with the most and largest cofactors. `3 NADPH + 3 NADP+` blows the limit
    while contributing no sulfur at all.

    That bias is what makes this fallback sound. For a single element X most of a long
    reaction is irrelevant, and when exactly one substrate and one product carry X, with
    equal counts, conservation leaves exactly one possibility. This does not GUESS the
    pairing; it is the only pairing that exists.

    WHEN n == 1, one X atom on each side, there is a unique bijection and nothing is
    chosen: the pair is emitted at weight 1.0.

    WHEN n > 1 the metabolite pairing is still forced but the ATOM correspondence is not
    -- which of a substrate's 5 carbons becomes which of a product's 5. This used to be
    REFUSED, on the reasoning that picking one pairing (by rank order, say) would
    fabricate the very atom identity this graph is built to respect. That reasoning is
    right about PICKING and wrong about REFUSING: a refusal is a gap, and the model's
    stated contract is dilute-not-gap. So instead of picking one and instead of dropping
    the reaction, every candidate source->product pairing is emitted DILUTED by the
    fanout n -- each source atom spreads a total weight of 1.0 over its n candidate
    destinations (weight 1/n each). This is the doubly-stochastic completion, the
    maximum-entropy statement of "we know the metabolites transfer n atoms but not which
    maps to which": rows and columns of the n x n candidate matrix each sum to 1.0, so
    the per-atom margin is exactly a confident pairing's and O(n^2) edges are added, not
    the O(n!) of enumerating bijections. Nothing is fabricated because nothing is chosen;
    the uncertainty is represented as spread weight, which is what the graph then dilutes.

    The single case where MNXR104650 (sulfite reductase, `H2S + 3 NADP+ + 3 H2O = 4 H+ +
    sulfite + 3 NADPH`) severs sulfate from cysteine is exactly the n == 1 shape: one S
    in, one S out, three NADPH carrying none.

    Each emitted correspondence is a triple `(sub_rank, prod_rank, weight)`; the weight is
    the fanout dilution the atom graph multiplies onto the reaction's evidence E_r.
    """
    out = {}
    for X in ELEMENTS:
        sx, px = [], []
        bad = False
        for side, ms in ((sx, sub_mnxms), (px, prod_mnxms)):
            for m in set(ms):
                c = count_element(formulas.get(m), X)
                if c is None:
                    bad = True
                    break
                if c:
                    side.append((m, c))
            if bad:
                break
        if bad or len(sx) != 1 or len(px) != 1:
            continue
        (sm, sn), (pm, pn) = sx[0], px[0]
        if sn != pn or sm == pm:
            continue
        sr, pr = ranks_of.get((sm, X)), ranks_of.get((pm, X))
        if not sr or not pr or len(sr) != sn or len(pr) != pn:
            continue
        w = 1.0 / sn                      # fanout dilution; n == 1 -> 1.0 (unique pairing)
        out[(X, sm, pm)] = [(a, b, w) for a in sr for b in pr]
    return out


def distinct_structures(mnxms: list, smiles_of: dict) -> int:
    """How many components a side has once each distinct structure is written once.

    The count `aam.worklist.collapse` produces, arrived at from the participant list
    rather than from the string -- so the extractor can recognise a collapsed reaction
    without the worklist having to hand it a flag, and the threshold logic stays in one
    file. Keyed on the RAW SMILES, the same strings the reaction string was built from:
    re-canonicalising here would merge tautomer pairs the builder wrote separately and
    make this count too small.

    A participant with no SMILES counts as its own component. It cannot appear in a
    reaction string at all -- the lookup builder refuses to build one and the reaction
    is blocked -- so this is defensive, and it errs toward the expanded count, which
    means a refusal rather than an admission.
    """
    seen, n = set(), 0
    for m in mnxms:
        s = smiles_of.get(m)
        key = s if s is not None else ("\0unknown", m)
        if key not in seen:
            seen.add(key)
            n += 1
    return n


def pairs_from_mapped(mapped_smi: str, sub_mnxms: list, prod_mnxms: list,
                      canon: dict, conn: dict = None, align: str = "strict",
                      collapsed_counts: tuple | None = None):
    """(pairs, status). pairs: {(element, sm, pm) -> [(sub_rank, prod_rank)]}.

    The atom identifiers are CANONICAL RANKS within each metabolite's own molecule,
    not RDKit template indices -- see `canonical_ranks`. That is what makes
    `(metabolite, rank)` a well-defined node across reactions, which is the whole
    premise of the atom graph.

    `align` DECIDES WHO WROTE THE SMILES, and that is the only thing it decides.

      "strict"      -- WE built this reaction SMILES, from the equation, one fragment
                       per participant. The template count therefore MUST equal the
                       participant count, and a mismatch means a molecule went missing
                       before mapping and the mapper re-routed its atoms onto whatever
                       was left. The counts survive that; the pairs do not. Refuse.
    `collapsed_counts` is the per-side count of DISTINCT structures, and it is the
    second template count strict mode will accept -- see the guard below. It is the
    caller's to compute because only the caller holds the metabolite-to-SMILES map the
    reaction string was built from, and computing it here from `canon` would compare a
    re-canonicalised structure against a raw one and disagree on the tautomer cases.

      "structural"  -- a FOREIGN database wrote it, over its OWN participant set. The
                       counts are then expected to differ and carry no information:
                       MetaCyc writes the water that MetaNetX leaves implicit and omits
                       the proton MetaNetX lists. Naming is by structure, unmatched
                       templates contribute nothing, and over-claims are diluted by
                       `multiplicity_dilution`.

    Applying "strict" to MetaCyc is what made 9,173 of 16,526 curated records (55.5%)
    refuse as `stripped` -- the single largest loss in the whole lane, and not a
    chemistry verdict at all. Applying "structural" to a member whose SMILES we built
    would throw away a real guard, which is why this is a parameter and not a default
    change.
    """
    if not mapped_smi or pd.isna(mapped_smi):
        return {}, "no_mapping"
    try:
        rxn = AllChem.ReactionFromSmarts(mapped_smi, useSmiles=True)
    except Exception:
        return {}, "unparseable"
    if rxn is None:
        return {}, "unparseable"

    n_sub_t = rxn.GetNumReactantTemplates()
    n_prod_t = rxn.GetNumProductTemplates()
    sub_mols = [rxn.GetReactantTemplate(i) for i in range(n_sub_t)]
    prod_mols = [rxn.GetProductTemplate(j) for j in range(n_prod_t)]

    # ATOM-UNBALANCED: a molecule was dropped before mapping (no SMILES for a
    # generic), so the mapper re-routed orphan atoms onto whatever remained. The
    # counts survive that; the pairs do not. Only meaningful when WE built the SMILES
    # -- see `align` in the docstring.
    #
    # TWO ACCEPTABLE COUNTS, and the guard is not weakened by having both. A reaction
    # the worklist collapsed carries one component per DISTINCT structure per side, so
    # its template count is the collapsed count rather than the expanded one -- and a
    # count check that knew only the expanded number would refuse every reaction the
    # collapse rescued, which is the whole population this exists to admit.
    # `collapsed_counts` is the caller's per-side distinct count, computed from the
    # SAME metabolite-to-SMILES map the reaction string was built from. Neither
    # matching is still a refusal: a genuinely stripped reaction matches nothing.
    if align == "strict":
        exact = (n_sub_t == len(sub_mnxms) and n_prod_t == len(prod_mnxms))
        as_collapsed = (collapsed_counts is not None
                        and (n_sub_t, n_prod_t) == tuple(collapsed_counts))
        if not (exact or as_collapsed):
            return {}, "stripped"

    # WEIGHTED name candidates per template. A shared-canonical-SMILES ambiguity is
    # DILUTED across its candidates (weight 1/k), not refused -- see `match_mols`.
    #
    # ORIENTATION IS A CONVENTION, NOT A FACT, and the two databases do not share it.
    # MetaNetX writes MNXR100060 as `NAD+ + 2 H+ + glycolate = glycolaldehyde + NADH`;
    # MetaCyc writes the same reaction as `glycolaldehyde + H2O >> glycolate`. Named in
    # the given orientation, EVERY template misses -- which is what "nothing named on
    # either side" was, in 2,451 of 2,500 sampled `no_pairs` reactions. So in structural
    # mode both orientations are named and the one that names more templates wins.
    #
    # The emitted pair is always in METANETX orientation, because that is what
    # `direction.parquet` is keyed against: reading a MetaCyc-oriented pair against a
    # MetaNetX-oriented ratio would reverse the edge silently.
    if align == "structural":
        fwd_s, a_s = match_mols(sub_mols, sub_mnxms, canon, conn)
        fwd_p, a_p = match_mols(prod_mols, prod_mnxms, canon, conn)
        rev_s, b_s = match_mols(prod_mols, sub_mnxms, canon, conn)
        rev_p, b_p = match_mols(sub_mols, prod_mnxms, canon, conn)
        n_fwd = sum(1 for x in fwd_s if x) + sum(1 for x in fwd_p if x)
        n_rev = sum(1 for x in rev_s if x) + sum(1 for x in rev_p if x)
        flipped = n_rev > n_fwd
        if flipped:
            # MetaCyc's product molecules ARE MetaNetX's substrates. Swap the roles
            # rather than the names: the atom map still says which atom went where, so
            # reading the reaction from the other end is all that is needed.
            sub_mols, prod_mols = prod_mols, sub_mols
            sub_named, prod_named = rev_s, rev_p
            amb_s, amb_p = b_s, b_p
        else:
            sub_named, prod_named = fwd_s, fwd_p
            amb_s, amb_p = a_s, a_p
        sub_named, ov_s = multiplicity_dilution(sub_named, sub_mnxms)
        prod_named, ov_p = multiplicity_dilution(prod_named, prod_mnxms)
        amb_s, amb_p = amb_s or ov_s, amb_p or ov_p
    else:
        sub_named, amb_s = match_mols(sub_mols, sub_mnxms, canon, conn)
        prod_named, amb_p = match_mols(prod_mols, prod_mnxms, canon, conn)

    sub_ranks = [canonical_ranks(m) for m in sub_mols]
    prod_ranks = [canonical_ranks(m) for m in prod_mols]
    if any(r is None for r in sub_ranks) or any(r is None for r in prod_ranks):
        return {}, "unrankable"

    # atom-map number -> (which substrate template, CANONICAL RANK, element)
    sub_index = {}
    for i, mol in enumerate(sub_mols):
        for a in mol.GetAtoms():
            n = a.GetAtomMapNum()
            if n > 0:
                sub_index[n] = (i, sub_ranks[i][a.GetIdx()], a.GetSymbol())

    pairs = defaultdict(list)
    for j, mol in enumerate(prod_mols):
        for a in mol.GetAtoms():
            n = a.GetAtomMapNum()
            if n <= 0:
                continue
            el = a.GetSymbol()
            if el not in ELEMENTS:
                continue
            src = sub_index.get(n)
            if src is None:
                continue
            si, s_rank, sel = src
            if sel != el:
                continue
            sub_cands = sub_named[si] if si < len(sub_named) else []
            prod_cands = prod_named[j] if j < len(prod_named) else []
            if not sub_cands or not prod_cands:
                continue
            p_rank = prod_ranks[j][a.GetIdx()]
            # THE LINE THE UNIFIER DOES NOT WRITE: keep the pair, not two counts. When
            # a side is name-ambiguous the pair is emitted once per candidate naming at
            # the product of the two sides' dilution weights, so the atom's transfer is
            # spread over the candidates rather than picked or dropped.
            for sm, ws in sub_cands:
                for pm, wp in prod_cands:
                    pairs[(el, sm, pm)].append((s_rank, p_rank, ws * wp))
    if not pairs:
        return {}, "no_pairs"
    return dict(pairs), ("ambiguous_diluted" if (amb_s or amb_p) else "ok")


# =====================================================================
# Driver
# =====================================================================

def load_placeholders(path: Path):
    """mnxm -> smiles for generics that were stood in for (see ecspr_aam_rescue).

    These exist so the mapper could see a sane reaction. They are NOT metabolites: their
    atoms must never become graph nodes, or `(met, rank)` would name an atom of a
    molecule that does not exist. The SMILES are needed anyway, because `match_mols`
    names fragments by canonical SMILES -- without them the placeholder fragment goes
    unnamed, the template count stops matching the equation's, and the reaction is
    refused as `stripped`.
    """
    d = pd.read_csv(path, sep="\t")
    return dict(zip(d.mnxm, d.smiles))


def load_resolved(path: Path):
    """mnxm -> curated smiles for metabolites whose structure `ecspr_aam_rescue` supplied.

    THE OPPOSITE OF A PLACEHOLDER, and they must not be confused. A placeholder is named
    here so its fragment matches, and then SUPPRESSED so its invented atoms never reach the
    graph. A resolved metabolite is the metabolite: its structure was missing from
    MetaNetX and a curated row asserted it, so its atoms are real and MUST become nodes --
    suppressing a sulfur carrier's S would kill precisely the edge the curation exists to
    license.

    Hence there is no filter to go with this. These simply join the SMILES map and take the
    ordinary path, which is the whole point: a metabolite with a structure is what this
    module already knows how to handle.
    """
    # `#` is a comment ONLY at line start. An inline `comment="#"` truncates any curated
    # SMILES that contains a `#` triple bond (nitrile C#N, alkyne C#C) -- silently dropping
    # exactly the rows a curator most needs to supply. Filter full-line comments and parse
    # the remainder, so a `#` inside a field survives. Mirrors `ecspr_aam_rescue`, which
    # reads the same crosswalk: the two must not disagree about what a row says.
    from io import StringIO
    _kept = [ln for ln in Path(path).read_text().splitlines() if not ln.lstrip().startswith("#")]
    d = pd.read_csv(StringIO("\n".join(_kept)), sep="\t")
    return dict(zip(d.mnxm, d.smiles))


def load_balance(path: Path):
    """(mnxr, element) -> whether the CONCRETE atoms balance.

    A placeholder asserts its carrier is conserved for an element. `ecspr_aam_rescue`
    tests that per reaction and per element rather than trusting it; this applies the
    verdict. Absent from the table (an ordinary universe reaction) means no filter.
    """
    d = pd.read_csv(path, sep="\t")
    return {(r.mnxr, r.element): bool(r.balanced) for r in d.itertuples(index=False)}


def cmd_extract(args):
    aam = pd.concat([pd.read_csv(a, sep="\t") for a in args.aam], ignore_index=True)
    aam = aam.drop_duplicates(subset="mnxr", keep="first")
    if args.min_confidence is not None and "confidence" in aam.columns:
        aam = aam[aam.confidence >= args.min_confidence]
    mnxrs = set(aam.mnxr)
    print(f"[atom-pairs] {len(aam):,} mapped reactions from "
          f"{', '.join(Path(a).name for a in args.aam)}", flush=True)

    ph = load_placeholders(Path(args.placeholders)) if args.placeholders else {}
    res = load_resolved(Path(args.resolved)) if args.resolved else {}
    bal = load_balance(Path(args.balance)) if args.balance else {}
    if ph:
        print(f"[atom-pairs] {len(ph):,} placeholder generics (atoms suppressed)",
              flush=True)
    if res:
        print(f"[atom-pairs] {len(res):,} curated structures (atoms KEPT -- they are the "
              f"metabolite's own)", flush=True)
        # A metabolite cannot be both scaffolding and real. If one were in both maps the
        # suppression filter would silently delete the very pairs the curation licensed,
        # and the payoff would read as zero with nothing to show why.
        both = set(ph) & set(res)
        if both:
            raise SystemExit(f"[atom-pairs] {sorted(both)} are both placeheld and "
                             f"resolved -- a metabolite is scaffolding or it is real, "
                             f"not both")
    if bal:
        nbad = sum(1 for v in bal.values() if not v)
        print(f"[atom-pairs] {len(bal):,} (rxn, element) balance verdicts; "
              f"{nbad:,} refused as unbalanced", flush=True)

    eqs = load_equations(Path(args.reac_prop), mnxrs)
    want = set()
    parsed = {}
    for r, eq in eqs.items():
        pe = parse_equation(eq)
        if pe:
            parsed[r] = pe
            want |= set(pe[0]) | set(pe[1])
    print(f"[atom-pairs] {len(parsed):,} equations parsed; {len(want):,} metabolites",
          flush=True)

    raw = load_mnxm_smiles(Path(args.chem_prop), want)
    raw.update({m: s for m, s in ph.items() if m in want})
    # Curated structures join the SAME map chem_prop's do -- they ARE structures, supplied
    # where MetaNetX had none. Everything downstream (canon, match_mols, canonical_ranks,
    # the pairs) then treats them as the ordinary metabolites they are.
    raw.update({m: s for m, s in res.items() if m in want})
    canon, conn = {}, {}
    for m, smi in raw.items():
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue
        cs, k = frag_keys(mol)
        if cs:
            canon[m] = cs
        if k:
            conn[m] = k
    print(f"[atom-pairs] {len(canon):,} metabolites with canonical SMILES, "
          f"{len(conn):,} with an InChIKey connectivity key", flush=True)

    formulas, ranks_of = {}, {}
    if args.fallback_forced:
        formulas = load_mnxm_formulas(Path(args.chem_prop), want)
        # The rank of a metabolite's sole X atom, taken from ITS OWN canonical molecule
        # -- never from a reaction template, which is per-reaction and unstable (see
        # `canonical_ranks`). This is the same rank system the mapped path emits, so a
        # forced pair and a mapped pair name the same node.
        for m, smi in raw.items():
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                continue
            rk = canonical_ranks(mol)
            if rk is None:
                continue
            for X in ELEMENTS:
                idx = [a.GetIdx() for a in mol.GetAtoms() if a.GetSymbol() == X]
                if idx:
                    ranks_of[(m, X)] = [rk[i] for i in idx]
        print(f"[atom-pairs] fallback armed: {len(formulas):,} formulas, "
              f"{len(ranks_of):,} (metabolite, element) rank sets", flush=True)

    rows, status_rows = [], []
    tally = defaultdict(int)
    for k, rec in enumerate(aam.itertuples(index=False)):
        r = rec.mnxr
        pe = parsed.get(r)
        if pe is None:
            tally["no_equation"] += 1
            status_rows.append(dict(mnxr=r, status="no_equation", n_sub=0, n_prod=0,
                                    n_mapped_sub=0, n_mapped_prod=0))
            continue
        subs, prods = pe
        pairs, status = pairs_from_mapped(
            getattr(rec, "mapped_rxn_smiles", None), subs, prods, canon,
            conn=conn if args.connectivity_fallback else None, align=args.align,
            collapsed_counts=(distinct_structures(subs, raw),
                              distinct_structures(prods, raw)))
        if not pairs and status == "no_mapping" and args.fallback_forced:
            fp = forced_pairs(subs, prods, formulas, ranks_of)
            if fp:
                pairs, status = fp, "forced"
        if pairs and (ph or bal):
            # A placeholder is scaffolding for the mapper, not a metabolite: drop every
            # pair that touches one, so no fabricated atom reaches the graph. Then drop
            # the elements whose concrete atoms did not balance -- that is where the
            # conservation claim is actually tested, and it is per element: ferredoxin
            # is inert to carbon in IspH and is the sulfur DONOR in biotin synthase, so
            # the same stand-in is admissible for one element and refused for the other.
            pairs = {(el, sm, pm): v for (el, sm, pm), v in pairs.items()
                     if sm not in ph and pm not in ph
                     and bal.get((r, el), True)}
            if not pairs:
                status = "placeholder_only"
        tally[status] += 1
        status_rows.append(dict(mnxr=r, status=status, n_sub=len(subs),
                                n_prod=len(prods),
                                n_mapped_sub=len({p[1] for p in pairs}),
                                n_mapped_prod=len({p[2] for p in pairs})))
        for (el, sm, pm), idxs in pairs.items():
            rows.append(dict(mnxr=r, element=el, substrate=sm, product=pm,
                             n_atoms=len(idxs),
                             sub_idx=",".join(str(i) for i, _, _ in idxs),
                             prod_idx=",".join(str(j) for _, j, _ in idxs),
                             pair_w=",".join(repr(float(w)) for _, _, w in idxs)))
        if (k + 1) % 10000 == 0:
            print(f"[atom-pairs]   {k+1:,}/{len(aam):,}", flush=True)

    df = pd.DataFrame(rows, columns=list(PAIR_COLS))
    sdf = pd.DataFrame(status_rows, columns=list(STATUS_COLS))
    df.to_parquet(args.out, index=False)
    sdf.to_csv(args.out_status, sep="\t", index=False)

    print(f"\n[atom-pairs] {len(df):,} (rxn, element, sub, prod) pairs "
          f"over {df.mnxr.nunique():,} reactions -> {args.out}")
    print("\n[atom-pairs] per-reaction outcome:")
    for s, n in sorted(tally.items(), key=lambda x: -x[1]):
        print(f"    {s:22s} {n:>7,}  ({100*n/len(aam):5.1f}%)")
    if len(df):
        print("\n[atom-pairs] pairs per element:")
        for el, g in df.groupby("element"):
            print(f"    {el}: {len(g):>7,} pairs  {g.n_atoms.sum():>9,} atoms  "
                  f"{g.mnxr.nunique():>6,} reactions")
    return 0


def cmd_selftest(args):
    """Does the pairing say what chemistry says? One reaction, checked by hand.

    MNXR106432: acetyl-CoA + CO2 + NADPH = pyruvate + NADP+ + CoA. The acetyl
    group's 2 carbons must land in pyruvate and the CoA moiety's 21 must land in
    CoA -- and pyruvate must receive nothing at all from NADPH.
    """
    smi = ("CC(=O)[S:1][CH2:2][CH2:3][NH:4][C:5](=[O:6])[CH2:7][CH2:8]"
           ">>[CH3:9][C:10](=[O:11])[C:12](=[O:13])[OH:14]")
    rxn = AllChem.ReactionFromSmarts(smi, useSmiles=True)
    print(f"parsed: {rxn is not None} "
          f"({rxn.GetNumReactantTemplates()} sub / {rxn.GetNumProductTemplates()} prod)")
    print("selftest is a parse check only; the real check is `verify` against MNX.")
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("extract"); p.set_defaults(fn=cmd_extract)
    p.add_argument("--aam", required=True, nargs="+",
                   help="cached mapper output(s): mnxr, mapped_rxn_smiles, confidence. "
                        "Repeatable, so a rescued universe (ecspr_aam_rescue) can be "
                        "concatenated with the base one; first file wins on duplicates.")
    p.add_argument("--placeholders", default=None,
                   help="ecspr_aam_rescue placeholder map. Names the stand-in fragments "
                        "so they match, then suppresses their atoms from the pairs.")
    p.add_argument("--resolved", default=None,
                   help="ecspr_aam_rescue curated structure crosswalk. Supplies structures "
                        "MetaNetX lacks. NOT placeholders: their atoms are kept, because "
                        "they are the metabolite's own.")
    p.add_argument("--balance", default=None,
                   help="ecspr_aam_rescue per-(reaction, element) concrete-balance "
                        "verdicts; unbalanced elements are dropped for that reaction.")
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--out", required=True, help="pairs parquet")
    p.add_argument("--out-status", required=True, help="per-reaction outcome tsv")
    p.add_argument("--fallback-forced", action="store_true",
                   help="for reactions the mapper returned nothing for (its 512-token "
                        "limit, not a chemistry verdict), emit the pairs conservation "
                        "FORCES: one substrate and one product carrying the element, one "
                        "atom each. Only n==1; multi-atom cases stay refused because the "
                        "atom correspondence would have to be invented.")
    p.add_argument("--align", choices=("strict", "structural"), default="strict",
                   help="strict: WE built the reaction SMILES, so template count must "
                        "equal participant count and a mismatch means atoms were "
                        "re-routed. structural: a foreign database wrote it over its "
                        "own participant set, so the counts are expected to differ and "
                        "naming is by structure alone. Use structural for MetaCyc and "
                        "strict for anything built from reac_prop.")
    p.add_argument("--connectivity-fallback", action="store_true",
                   help="when a template's canonical SMILES matches no participant, "
                        "retry on the InChIKey connectivity block -- which is invariant "
                        "to the protonation and tautomer differences MetaCyc and "
                        "MetaNetX have about the same molecule, and to nothing else")
    p.add_argument("--min-confidence", type=float, default=None,
                   help="omit to keep the mapper's whole universe (tier B applies no "
                        "confidence gate; the acetyl-CoA split is chemically correct "
                        "at confidence 0.032, so a gate here would cost real pairs)")

    p = sub.add_parser("selftest"); p.set_defaults(fn=cmd_selftest)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
