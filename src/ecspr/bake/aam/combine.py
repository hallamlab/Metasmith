"""AAM ensemble combiner -- fuse multiple atom-atom mappers into one correspondence
table with provenance, mirroring the directionality ensemble (``direction/combine.py``).

The atom lane's universe is currently single-source: ``atom_pairs_universe.parquet`` is
RXNMapper-only with no record of who mapped what. This makes the universe an ensemble:
each member proposes, per (reaction, element, substrate-atom), a product-atom, and the
members are fused in a common identity space with the correlation structure made explicit
and the provenance recorded, never selected -- exactly the pattern the direction combiner
uses for ΔG'.

Common space. Every member's correspondence is expressed in the engine's canonical-rank
identity: ``(metabolite, CanonicalRankAtoms(breakTies=True))`` is an atom, invariant to how
a reaction happened to write the molecule. Because the identity is a property of the
metabolite, not the mapper, two mappers that map the same physical atom name the same node.
We obtain each member's correspondence by running the engine's own ``pairs_from_mapped`` on
that member's mapped SMILES -- one extractor, so the only thing that varies between members
is the mapping, not the identity bookkeeping.

Correlation structure (the load-bearing design fact, mirrored from direction's TAU_SHARED).
RXNMapper and LocalMapper are both transformers trained on reaction SMILES: when they agree,
part of the agreement is shared architectural bias, not independent confirmation. So
neural-neural agreement is DISCOUNTED (``NEURAL_SHARED_FLOOR``) -- two correlated members
cannot count as two independent ones, the direct analogue of the thermo vote flooring
eQ+dGbyG. A curated member (Rhea/MetaCyc atom maps) is a database, not a transformer -- so
when it exists it breaks ties and carries undiscounted weight. (Not wired this round; see
the README.)

Abstain-by-dilution (the shrink-to-no-op analogue). Where members DISAGREE on an atom's
fate, the correspondence is not decided by majority or confidence -- it is DILUTED across the
disputed products by member weight, so a contested atom spreads its transfer rather than
committing. This is the same fanout dilution the atom graph already applies to name-ambiguous
pairs (``pairs_from_mapped``'s ``ambiguous_diluted``), extended across mappers. An atom no
member maps simply has no pair -- the AAM analogue of ratio 1.0.

Provenance. Every emitted pair carries ``method`` / ``source`` / ``confidence`` -- a record of
which members spoke and how they agreed, NOT a tier that gates usability (every pair is used).

This driver fuses an ORDERED set of members, each ``{mnxr -> (mapped_smiles, confidence)}``,
and reports PAIRWISE agreement between every pair of members over the atoms both address.
Two families of member exist and are treated differently by design:

  * NEURAL (RXNMapper, LocalMapper) -- transformers on reaction SMILES. Their mutual
    agreement is partly shared architectural bias, so a purely-neural consensus is
    DISCOUNTED (``NEURAL_SHARED_FLOOR``) and their agreement rate measures how little
    independent information a second neural member adds.
  * CURATED (MetaCyc, ``metacyc_member.py``) -- a database, not a model, so an INDEPENDENT
    vote. A curated-inclusive consensus fuses UNDISCOUNTED (full ``1.0``) via the existing
    ``set(present) <= NEURAL_MEMBERS`` branch, and curated-vs-neural agreement is the "does
    the curated source argue with the neural mappers" number.

Pure re-extraction over cached / curated mapped SMILES; runs anywhere rdkit + the engine
import (``scadc-metabolic-model``). Drop ``--no-metacyc`` to reproduce the original
two-member neural demonstration.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

# The extractor is deliberately shared rather than reimplemented per member: every
# member's correspondence has to be expressed in ONE identity, (metabolite,
# CanonicalRankAtoms(breakTies=True)), or two mappers stop naming the same physical atom
# the same node while every table still looks well-formed.
from ..atom_pairs import (ELEMENTS, load_equations, parse_equation,           # noqa: F401
                          load_mnxm_smiles, canon_smiles, pairs_from_mapped,
                          load_placeholders, load_resolved, load_balance)
from .metacyc_member import load_member as load_metacyc

# --- ensemble tunables (committed before the run; would graduate to canon.py the way
#     DIR_TAU_SHARED did once the ensemble is canonical -- provisional, so kept local). ---
# Two transformer mappers agreeing is partly shared architectural bias, not independent
# confirmation, so a neural-neural consensus is worth less than two independent votes. We
# credit it at this fraction of "fully corroborated". 1.0 would treat them as independent
# (wrong); 0.5 treats their agreement as ~one member's worth.
NEURAL_SHARED_FLOOR = 0.5
# A correspondence carried by a single member (the other silent on that atom) is credited
# at this fraction -- present, but not corroborated.
SINGLE_MEMBER_CREDIT = 0.5

# The neural family: members whose mutual agreement is correlated and must be discounted.
NEURAL_MEMBERS = {"rxnmapper", "localmapper"}


def load_mapper(path: Path) -> dict:
    """mnxr -> (mapped_rxn_smiles, confidence) from a cached mapper tsv."""
    df = pd.read_csv(path, sep="\t").drop_duplicates(subset="mnxr", keep="first")
    out = {}
    for r in df.itertuples(index=False):
        conf = float(getattr(r, "confidence", float("nan")))
        out[r.mnxr] = (getattr(r, "mapped_rxn_smiles", None), conf)
    return out


def correspondence(pairs: dict) -> dict:
    """{(element, sm, pm, sub_rank) -> {prod_rank: weight}} from a pairs_from_mapped dict.
    A member may spread an atom over several product ranks (its own dilution); kept as-is."""
    corr = defaultdict(lambda: defaultdict(float))
    for (el, sm, pm), idxs in pairs.items():
        for s_rank, p_rank, w in idxs:
            corr[(el, sm, pm, s_rank)][p_rank] += float(w)
    return corr


def _argmax(d: dict):
    return max(d.items(), key=lambda kv: kv[1])[0] if d else None


def fuse_reaction(mnxr, subs, prods, canon, members: dict):
    """Fuse members' correspondences for one reaction into provenance-carrying rows, plus
    a PAIRWISE agreement tally and each member's re-extraction status.

    ``members`` is an ORDERED ``{name: (mapped_smiles, confidence)}``. Returns
    ``(rows, pair_tally, statuses)`` where:
      * each row is a fused atom-pair with method/source/confidence;
      * ``pair_tally`` is ``{(name_a, name_b) -> [shared, agree]}`` over atoms BOTH members
        address (every key here is already a C/N/S/P atom -- ``pairs_from_mapped`` emits
        only ``ELEMENTS`` -- so agreement is C/N/S/P by construction);
      * ``statuses`` is ``{name -> status}`` from ``pairs_from_mapped`` (ok / stripped /
        no_pairs / ...), so coverage loss is measured, never mistaken for agreement.
    """
    # per-member correspondence in the shared canonical-rank space
    corr = {}
    conf = {}
    statuses = {}
    for name, (mapped, c) in members.items():
        pairs, status = pairs_from_mapped(mapped, subs, prods, canon)
        corr[name] = correspondence(pairs) if pairs else {}
        conf[name] = c
        statuses[name] = status

    keys = set().union(*[set(c.keys()) for c in corr.values()]) if corr else set()
    rows = []
    names = list(members)
    pair_tally = defaultdict(lambda: [0, 0])

    for key in keys:
        el, sm, pm, s_rank = key
        # pairwise agreement bookkeeping over members that BOTH address this atom. Neural-
        # neural (rxnmapper,localmapper) and curated-vs-neural (metacyc,*) fall out of the
        # same loop -- the "does the curated source argue" number is one of these pairs.
        addressing = [n for n in names if key in corr[n]]
        for i in range(len(addressing)):
            for j in range(i + 1, len(addressing)):
                a, b = addressing[i], addressing[j]
                pk = (a, b) if a < b else (b, a)
                pair_tally[pk][0] += 1
                if _argmax(corr[a][key]) == _argmax(corr[b][key]):
                    pair_tally[pk][1] += 1

        present = {name: corr[name][key] for name in addressing}
        picks = {name: _argmax(w) for name, w in present.items()}
        distinct = set(picks.values())
        srcs = "+".join(sorted(present))

        if len(present) == 1:                              # single member speaks
            name = next(iter(present))
            rows.append(dict(mnxr=mnxr, element=el, substrate=sm, product=pm,
                             sub_idx=s_rank, prod_idx=picks[name],
                             pair_w=SINGLE_MEMBER_CREDIT,
                             method=f"{name}_only", source=name,
                             confidence=conf[name] * SINGLE_MEMBER_CREDIT))
        elif len(distinct) == 1:                           # members AGREE
            p_rank = distinct.pop()
            # discount ONLY a purely-neural consensus; a curated-inclusive one is undiscounted
            corr_disc = NEURAL_SHARED_FLOOR if set(present) <= NEURAL_MEMBERS else 1.0
            cbar = sum(conf[n] for n in present) / len(present)
            rows.append(dict(mnxr=mnxr, element=el, substrate=sm, product=pm,
                             sub_idx=s_rank, prod_idx=p_rank, pair_w=1.0,
                             method="consensus", source=srcs,
                             confidence=cbar * corr_disc))
        else:                                              # members DISAGREE -> dilute
            wsum = sum(conf[n] for n in present) or float(len(present))
            for name, p_rank in picks.items():
                rows.append(dict(mnxr=mnxr, element=el, substrate=sm, product=pm,
                                 sub_idx=s_rank, prod_idx=p_rank,
                                 pair_w=conf[name] / wsum,
                                 method="disagree_diluted", source=srcs,
                                 confidence=conf[name] / wsum))
    return rows, pair_tally, statuses


def build(members_src: dict, parsed: dict, canon: dict):
    """Fuse over the reactions covered by at least one member.

    ``members_src`` is an ORDERED ``{name: {mnxr:(mapped,conf)}}``. Reports pairwise
    agreement per member pair and per-member re-extraction status counts.
    """
    all_mnxr = set().union(*[set(m) for m in members_src.values()]) if members_src else set()
    rows = []
    agree = defaultdict(lambda: [0, 0])                    # (a,b) -> [shared, agree]
    status_counts = defaultdict(lambda: defaultdict(int))  # name -> {status: n}
    n_overlap = n_rxn = 0
    for mnxr in sorted(all_mnxr):
        pe = parsed.get(mnxr)
        if pe is None:
            continue
        subs, prods = pe
        members = {}
        for name, src in members_src.items():
            if mnxr in src and src[mnxr][0]:
                members[name] = src[mnxr]
        if not members:
            continue
        n_rxn += 1
        if len(members) >= 2:
            n_overlap += 1
        r, tally, statuses = fuse_reaction(mnxr, subs, prods, canon, members)
        rows.extend(r)
        for pk, (s, a) in tally.items():
            agree[pk][0] += s
            agree[pk][1] += a
        for name, st in statuses.items():
            status_counts[name][st] += 1
    return (pd.DataFrame(rows), dict(
        n_rxn=n_rxn, n_overlap=n_overlap,
        agree={k: tuple(v) for k, v in agree.items()},
        status={k: dict(v) for k, v in status_counts.items()}))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    # No default paths. The originals pointed at a sibling project's scratch cache and at
    # its data tree; here every input is staged by the planner and named explicitly.
    ap.add_argument("--rxnmapper", type=Path, required=True,
                    help="the RXNMapper member's cached tsv (neural_members --out)")
    ap.add_argument("--localmapper", type=Path, required=True,
                    help="the LocalMapper member's cached tsv (neural_members --out)")
    ap.add_argument("--mcs", type=Path, default=None,
                    help="the structural MCS member's cached tsv (mcs_member.py --out): a "
                         "fourth, INDEPENDENT (undiscounted) member. Its atoms are named "
                         "from chem_prop like any other; its placeholder stand-ins and "
                         "per-element balance verdicts arrive via --placeholders/--balance.")
    ap.add_argument("--no-mcs", action="store_true",
                    help="drop the MCS member even if --mcs is given (the 3-member ensemble)")
    ap.add_argument("--placeholders", type=Path, default=None,
                    help="placeholder map (mcs_member.py --out-placeholders): the generic "
                         "carriers stood in for. NAMED so their fragments match, then their "
                         "pairs are SUPPRESSED -- their atoms are scaffolding, not nodes.")
    ap.add_argument("--resolved", type=Path, default=None,
                    help="curated structure crosswalk (ecspr_aam_rescue schema): structures "
                         "MetaNetX lacks. NOT placeholders -- their atoms are KEPT.")
    ap.add_argument("--balance", type=Path, default=None,
                    help="per-(reaction, element) concrete-balance verdicts "
                         "(mcs_member.py --out-balance): unbalanced elements are dropped for "
                         "that reaction, so a carrier that actually donated an atom is refused "
                         "for that element while staying usable for the ones it is inert to.")
    ap.add_argument("--metacyc", type=Path, required=True,
                    help="MetaCyc atom-mappings-smiles.dat (the curated member's source)")
    ap.add_argument("--reac-xref", type=Path, required=True,
                    help="MetaNetX reac_xref for the metacyc.reaction->MNXR crosswalk")
    ap.add_argument("--no-metacyc", action="store_true",
                    help="drop the curated member -> the original two-member neural demo")
    ap.add_argument("--reac-prop", type=Path, required=True)
    ap.add_argument("--chem-prop", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--overlap-only", action="store_true",
                    help="restrict to reactions BOTH neural members cover (the demonstration set)")
    a = ap.parse_args(argv)

    # Ordered member map: neural pair first, curated last (order only affects report layout
    # and the deterministic source string, never the fusion result).
    rxnm = load_mapper(a.rxnmapper)
    lm = load_mapper(a.localmapper)
    members_src = {"rxnmapper": rxnm, "localmapper": lm}
    if not a.no_metacyc:
        mc = load_metacyc(a.metacyc, a.reac_xref)
        members_src["metacyc"] = mc
        print(f"[aam] metacyc: {len(mc):,} reactions (curated, independent)", flush=True)
    if a.mcs and not a.no_mcs:
        mcs = load_mapper(a.mcs)
        members_src["mcs"] = mcs
        print(f"[aam] mcs: {len(mcs):,} reactions (structural, independent)", flush=True)
    print(f"[aam] rxnmapper: {len(rxnm):,} reactions; localmapper: {len(lm):,} reactions; "
          f"neural overlap: {len(set(rxnm) & set(lm)):,}", flush=True)

    mnxrs = set().union(*[set(m) for m in members_src.values()])
    if a.overlap_only:
        mnxrs = set(rxnm) & set(lm)
    eqs = load_equations(a.reac_prop, mnxrs)
    parsed, want = {}, set()
    for r, eq in eqs.items():
        pe = parse_equation(eq)
        if pe:
            parsed[r] = pe
            want |= set(pe[0]) | set(pe[1])
    raw = load_mnxm_smiles(a.chem_prop, want)
    # A placeholder carrier and a curated structure are BOTH named here so their fragments
    # match the template count (an unnamed fragment makes the reaction come back `stripped`).
    # The difference is downstream: placeholder atoms are SUPPRESSED after fusion (scaffolding),
    # resolved atoms are KEPT (the metabolite's own). This mirrors ecspr_atom_pairs.cmd_extract.
    ph = load_placeholders(a.placeholders) if a.placeholders else {}
    res = load_resolved(a.resolved) if a.resolved else {}
    bal = load_balance(a.balance) if a.balance else {}
    both = set(ph) & set(res)
    if both:
        raise SystemExit(f"[aam] {sorted(both)} are both placeheld and resolved -- a "
                         f"metabolite is scaffolding or it is real, not both")
    raw.update({m: s for m, s in ph.items() if m in want})
    raw.update({m: s for m, s in res.items() if m in want})
    canon = {m: cs for m, s in raw.items() if (cs := canon_smiles(s))}
    print(f"[aam] {len(parsed):,} equations parsed; {len(canon):,} metabolites with SMILES "
          f"({len(ph):,} placeholder, {len(res):,} curated)", flush=True)

    if a.overlap_only:
        members_src = {name: {k: v for k, v in src.items() if k in mnxrs}
                       for name, src in members_src.items()}
    df, rep = build(members_src, parsed, canon)

    # Suppress placeholder atoms and drop unbalanced elements -- the same filter the
    # single-source extractor applies, so the fused reference obeys the identical
    # conservation discipline: a fabricated carrier atom never reaches the graph, and an
    # element a carrier actually perturbed is refused for that reaction alone.
    if (ph or bal) and len(df):
        n0 = len(df)
        # NB bracket-index "product": df.product is the DataFrame.product METHOD, not the column.
        keep = ~(df["substrate"].isin(ph) | df["product"].isin(ph))
        if bal:
            # vectorised: an (mnxr, element) is dropped only if explicitly unbalanced;
            # absent from the table means no filter (an ordinary concrete reaction).
            unbal = {k for k, v in bal.items() if not v}
            if unbal:
                bad = [(m, e) in unbal for m, e in zip(df["mnxr"], df["element"])]
                keep &= ~pd.Series(bad, index=df.index)
        df = df[keep].reset_index(drop=True)
        print(f"[aam] suppression/balance filter: {n0:,} -> {len(df):,} pairs "
              f"({n0 - len(df):,} dropped: placeholder-touching or unbalanced)", flush=True)

    df.to_parquet(a.out, index=False)

    print("\n" + "=" * 64)
    print("AAM ENSEMBLE -- " + " + ".join(members_src))
    print("=" * 64)
    print(f"fused reactions            : {rep['n_rxn']:,}")
    print(f"multi-member overlap rxns  : {rep['n_overlap']:,}")
    print(f"emitted atom-pairs         : {len(df):,}")
    if len(df):
        print("provenance (method counts):")
        print(df.method.value_counts().to_string())

    print("\nper-member re-extraction status (coverage vs cross-namespace loss):")
    for name in members_src:
        st = rep["status"].get(name, {})
        tot = sum(st.values()) or 1
        parts = ", ".join(f"{k}={v:,} ({v/tot:.0%})"
                          for k, v in sorted(st.items(), key=lambda x: -x[1]))
        print(f"  {name:12s}: {parts}")

    print("\nPAIRWISE AGREEMENT (argmax product-rank, atoms BOTH members address, C/N/S/P):")
    for (a_name, b_name), (sh, ag) in sorted(rep["agree"].items()):
        tag = "  [neural<->neural]" if {a_name, b_name} <= NEURAL_MEMBERS else \
              "  [curated<->neural]"
        rate = f"{ag/sh:.1%}" if sh else "n/a"
        print(f"  {a_name:11s} vs {b_name:11s}: {ag:,}/{sh:,} = {rate}{tag}")
    print("  -> neural<->neural high = correlated members (the NEURAL_SHARED_FLOOR basis).")
    print("     curated<->neural high = MetaCyc corroborates the neural pair at full weight;")
    print("     a shortfall is a finding (namespace artifact vs genuine chemistry), not noise.")
    print(f"\nwrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
