"""Adjudicate the whole reaction universe BEFORE any mapper runs.

WHY THIS STEP EXISTS
--------------------
Until now the three AAM members each derived their own universe from
`lookup::reactions` -- "every row whose rxn_smiles is not null and is under 8,000
characters" -- and everything else simply did not appear. A reaction that produced no
pairs and a reaction that was never attempted came out of the build looking identical,
so "did we cover it" was only answerable by re-running the thing that dropped it. The
deployed chain this tree ports did not work that way: it classified the whole 83,796
reaction universe first and mapped only the partition that could bank, which is the
only reason its accounting adds up reaction by reaction.

So: one row per MNXR, always, with a VERDICT from a closed set and, where the reaction
is blocked, the FAMILY of each blocker. Members read this table instead of deriving a
universe, so the three provably see one list; the rescue lanes read the families, so
each lever's target set is a number rather than a guess; and the tier-4 gate reads the
verdict, so a reaction we miss carries the reason we missed it.

THE TWO SIZE BOUNDS ARE DIFFERENT BOUNDS
----------------------------------------
`too_long` is the 8,000-CHARACTER cap the neural members already had; `oversize` is a
cap on the reaction's ATOM count. They refuse different reactions and exist for
different reasons, so merging them would make the atom threshold unmovable -- you could
not raise it without also raising a limit that is about transformer context rather than
about cost.

Each threshold's warrant is a measured yield curve, taken by joining reaction size to
the reactions the deployed bake banked. Under the expanded count: 98-99% up to 600, then
12.8% at 600-800, 1.3% at 800-1,200 and zero above. Cutting at 600 removes 0.9% of the
buildable universe while removing the reactions that cost minutes each and OOM-killed
the LocalMapper lane twice. It is a named constant because it is a judgement call, not
a fact.

AND THE COUNT ITSELF WAS WRONG, WHICH IS WHY THERE ARE NOW THREE CONSTANTS
--------------------------------------------------------------------------
`rxn_smiles` is a stoichiometric EXPANSION: a coefficient of 16 writes the metabolite
sixteen times. So the atom cap has been measuring how many times a molecule APPEARS
rather than how much distinct chemistry a mapper must attend to -- and nitrogenase,
which hydrolyses 16 ATP, was refused at 1,236 atoms while the acetylene-reduction proxy
for exactly that chemistry sailed through.

`collapse` writes each distinct molecule once per side, and a reaction the expanded
measure refuses gets a second reading under `COLLAPSED_ATOM_LIMIT`. A reaction the
expanded measure ADMITS is untouched, byte for byte, which is what makes "coverage may
only go up" a property of the construction: 57,061 mappable before, 57,515 after, none
lost. Both counts are kept on every row so either curve stays recomputable.

VERDICTS ARE ASSIGNED IN GATE ORDER and only `oversize` / `too_long` remove anything
that the current graph would otherwise map. Everything else names a reaction no mapper
could ever have seen. That asymmetry is deliberate: this step is meant to make the
build's coverage legible, not to shrink it.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# The character cap the neural members have always applied. It is stated in both places
# rather than imported, because importing it here from a member would make the
# adjudication depend on a mapper module it is meant to precede. The two are kept honest
# at RUNTIME instead: `aam.neural_members.load_universe` re-checks every
# reaction it is handed and refuses if any exceeds its own constant, so a drift is a
# loud failure at the top of the lane rather than a quiet difference in coverage.
SMILES_LEN_LIMIT = 8000

# The atom cap on the EXPANDED string.
#
# IT IS A MEMBER BOUNDARY, NOT A REFUSAL, and that is a correction rather than a
# loosening. The cap was warranted by a yield curve joined to the deployed bake -- 98-99%
# banking up to 600, 12.8% at 600-800, 1.3% above -- and that curve is CENSORED. Every
# mapper method in the deployed table stops dead at the cap (`localmapper_only` max 600,
# `indigo_only` 598, `rxnmapper_only` 563, `consensus` 581) because the same cut was
# applied upstream of all three; the only thing banked above it is `curated`, which comes
# from MetaCyc and never sees a mapper. So the curve above 600 measures the cap, not the
# mappers, and the threshold has never been tested from the other side.
#
# Testing it says the cap belongs to the NEURAL members. RXNMapper's transformer takes
# 512 tokens and LocalMapper is the lane that was OOM-killed twice -- both are real, both
# scale with the string. Indigo is a compiled maximum-common-substructure search with a
# recorded timeout, and over the refused population it maps a 1,666-atom reaction in
# under two seconds. `research/fabfos/benchmarks/aam_cap/` is the sweep.
#
# So a reaction over this cap is `oversize`, which now means "the neural members will not
# see it" rather than "nothing will". Indigo's universe admits it, its own timeout bounds
# the cost, and a pair it finds alone lands as `indigo_only` -- the ordinary single-member
# path, at half weight, which is what it is worth. Nothing under the cap moves.
ATOM_LIMIT = 600

# Which verdicts each member's universe admits. Stated once, here, because the two
# readers live in different modules and a drift between them is a member silently seeing
# a different universe from its siblings -- the exact failure the worklist exists to stop.
NEURAL_ADMITS = ("mappable",)
INDIGO_ADMITS = ("mappable", "oversize")

# The atom cap on the COLLAPSED string -- the second chance a reaction gets when the
# expanded measure would refuse it.
#
# RE-DERIVED, not carried across, and it lands on the same number -- which is a result
# rather than a coincidence, and is why the derivation is written down. 600's warrant is
# a yield curve over the EXPANDED count, and a curve stops describing a measure the
# moment you change what is being counted. So the collapsed number was taken the same
# way the original was: bin all 57,593 buildable reactions by COLLAPSED atom count, join
# to the reactions the deployed bake actually banked, and cut where the yield falls off.
#
#   collapsed atoms   400-500  500-550  550-600 | 600-650  650-700  700-800  800-1000
#   banking rate        0.821    0.935    0.812 |   0.269    0.167    0.100     0.056
#
# The knee is sharp and it is at 600 under both measures. Cutting there admits 454 of
# the 532 reactions the expanded measure refuses. `research/fabfos/benchmarks/
# aam_collapse/` reproduces the curve.
COLLAPSED_ATOM_LIMIT = 600

VERDICTS = (
    "mappable",              # goes to every mapper lane
    "oversize",              # over the atom cap: Indigo only. See ATOM_LIMIT.
    "too_long",              # buildable, but over SMILES_LEN_LIMIT characters
    "blocked_no_structure",  # >=1 participant has no structure; the rescue lanes' target
    "non_molecule",          # >=1 participant is not a molecule; nothing can stand in
    "no_transfer",           # blocked AND the two sides are the same multiset
    "pseudo_reaction",       # a side is empty -- an exchange/sink, not chemistry
    "unparseable_equation",  # reac_prop's equation did not parse
)

# Blocker families. Assigned per structureless participant, first match wins, and the
# order is the specificity order rather than an alphabet. These are what the rescue
# lanes aim at, so the names match the lanes: `carrier` takes acyl_carrier, the acceptor
# lane takes generic_rgroup, the ladder takes polymer, and `lane_carrier`'s widened arm
# takes trna_holo. `electron_carrier` is what the deterministic placeholder library
# already covers; `other_structureless` is the ordinary case the twin/transform/
# fragment/supplier lanes work on.
FAMILIES = ("non_molecule", "electron_carrier", "acyl_carrier", "trna_holo",
            "polymer", "generic_rgroup", "other_structureless")

_F = re.compile
_FAMILY_RX = [
    ("non_molecule", _F(
        r"^(unknown|carbon|e\(-\)|e-|hnu|hn|h\N{GREEK SMALL LETTER NU}|photon|light"
        r"|biomass|.*\bbiomass\b.*)$", re.I)),
    # The deterministic placeholder library's subjects: redox carriers whose body is
    # conserved and whose atoms never transit.
    ("electron_carrier", _F(
        r"ferredoxin|flavodoxin|cytochrome|rubredoxin|adrenodoxin|thioredoxin"
        r"|glutaredoxin|electron[- ]transfer flavoprotein|hemoprotein reductase"
        r"|^an? (ubi|mena|demethylmena)quino[ln]$|plastoquino[ln]|plastocyanin", re.I)),
    ("acyl_carrier", _F(r"\bacp\b|acyl.?carrier|\bcoa\b|coenzyme a", re.I)),
    ("trna_holo", _F(r"\btrna\b|\bholo-|\bapo-|charged .*rna", re.I)),
    ("polymer", _F(
        r"starch|chitin|glycogen|cellulose|dextran|amylose|amylopectin|glucan|mannan"
        r"|xylan|peptidoglycan|polymer|oligosaccharide|polysaccharide|\(n\)|\(n\+1\)"
        r"|\bn\+1\b|glycoconjugate|lipopolysaccharide|teichoic", re.I)),
    ("generic_rgroup", _F(
        r"^(a|ah2|d|dh2|nad|nadh|idh\d*)$|acceptor|donor|\bprotein\b|enzyme-\w+ complex"
        r"|^an? alcohol$|^an? aldehyde$|^an? carboxylate$|r-group|residue", re.I)),
]


def family_of(name) -> str:
    n = str(name or "").strip()
    if not n:
        return "other_structureless"
    for fam, rx in _FAMILY_RX:
        if rx.search(n):
            return fam
    return "other_structureless"


def count_atoms(rxn_smiles: str):
    """Heavy-plus-explicit atoms across the WHOLE reaction, both sides.

    `sanitize=False`: this is a size measurement, not a chemistry check, and refusing to
    count an unsanitizable reaction would silently hand it to the mapper as if it were
    small. `>>` becomes `.` so one parse covers both sides -- the mappers attend over the
    whole string, so the whole string is what costs.
    """
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    m = Chem.MolFromSmiles(rxn_smiles.replace(">>", "."), sanitize=False)
    return None if m is None else m.GetNumAtoms()


def collapse(rxn_smiles: str):
    """The same reaction with each distinct molecule written once per side.

    THE POINT. `rxn_smiles` is a stoichiometric EXPANSION: a coefficient of 16 writes
    the metabolite sixteen times, so `count_atoms` measures how many times a molecule
    APPEARS rather than how much distinct chemistry the mapper must attend to.
    Nitrogenase hydrolyses 16 ATP; its expanded string counts over a thousand atoms and
    is refused, while the acetylene-reduction proxy for exactly that chemistry sails
    through. Counting each distinct molecule once is an exact reduction rather than an
    approximation -- a repeated component contributes no structure the mapper has not
    already attended to.

    DEDUPE PER SIDE, NOT ACROSS THE REACTION. Water on the left and water on the right
    are two occurrences that both have to exist for the equation to read; the same
    molecule twice on ONE side is the copy the cap should never have counted.

    SPLIT AS TEXT, PARSE NOTHING. This is not an optimisation, it is the whole reason
    the function is safe to call on anything: MNXR144749's expanded string is 80.7 MB
    and RDKit does not return from parsing it, so a collapse that parsed first would
    reintroduce the hour-inside-one-call failure the gate ordering exists to avoid. A
    text split of 80 MB is nothing. Component boundaries are `.` at depth zero -- inside
    no bracket and no parenthesis -- which is exactly SMILES's own component separator.

    Returns the collapsed string, or None when there is nothing to split (no `>>`).
    Order is preserved within each side, so the result is a deterministic function of
    the input rather than of a set's iteration order.
    """
    if not rxn_smiles or ">>" not in rxn_smiles:
        return None
    lhs, _, rhs = rxn_smiles.partition(">>")
    return ".".join(_uniq(_components(lhs))) + ">>" + ".".join(_uniq(_components(rhs)))


def _components(side: str):
    """Split a SMILES side on the `.` that separate components, and only those.

    A `.` inside brackets or parentheses is part of a token, not a boundary. Tracking
    the two depths is cheaper than any parse and is the only correctness requirement:
    splitting on every `.` would cut molecules in half and dedupe fragments of them.
    """
    out, start, depth = [], 0, 0
    for i, ch in enumerate(side):
        if ch in "[(":
            depth += 1
        elif ch in "])":
            depth -= 1
        elif ch == "." and depth == 0:
            out.append(side[start:i])
            start = i + 1
    out.append(side[start:])
    return [c for c in out if c]


def _uniq(items):
    seen, out = set(), []
    for c in items:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def adjudicate(reactions: pd.DataFrame, name_of: dict, atom_limit: int,
               char_limit: int,
               collapsed_atom_limit: int | None = None) -> pd.DataFrame:
    if collapsed_atom_limit is None:
        collapsed_atom_limit = COLLAPSED_ATOM_LIMIT
    rows = []
    for i, r in enumerate(reactions.itertuples(index=False)):
        subs = list(r.substrates) if r.substrates is not None else []
        prods = list(r.products) if r.products is not None else []
        blockers = list(r.blockers) if r.blockers is not None else []
        fams = sorted({family_of(name_of.get(m)) for m in blockers})
        smi = r.rxn_smiles if isinstance(r.rxn_smiles, str) and r.rxn_smiles else None

        # ATOMS ARE COUNTED ONLY UNDER THE CHARACTER CAP, and that ordering is not an
        # optimisation. `rxn_smiles` is a stoichiometric EXPANSION -- a coefficient of
        # 1,000 writes the metabolite a thousand times -- so while no metabolite's SMILES
        # exceeds 3,199 characters, MNXR144749's reaction string is 80.7 MB. RDKit does
        # not return from parsing that in any useful time, and a run died on it: the lane
        # walked 20,000 reactions in three seconds and then spent an hour inside one call.
        # The char gate refuses those reactions anyway, so the parse was never needed;
        # counting first merely put the unbounded work ahead of the bound that excludes it.
        atoms, chars = None, (len(smi) if smi else None)
        if smi and chars <= char_limit:
            atoms = count_atoms(smi)

        # THE COLLAPSED MEASURE, and the rule that makes coverage monotone.
        #
        # A reaction that passes on the EXPANDED measure keeps its expanded string, byte
        # for byte -- same universe, same map, same pairs, same weights. The collapse is
        # attempted only for a reaction that would otherwise be REFUSED, so no existing
        # row can move and "coverage may only go up" is a property of the construction
        # rather than something to check afterwards. It is also why a 1,236-atom string
        # is still never handed to a mapper, which is what the cap was protecting.
        #
        # Both counts are recorded either way. Keeping them side by side is what lets the
        # yield curve be recomputed under either measure from one table, and what makes
        # the change a diff rather than a claim.
        smi_c = atoms_c = chars_c = None
        collapsed = False
        would_refuse = smi is not None and (
            chars > char_limit or atoms is None or atoms > atom_limit)
        if would_refuse:
            smi_c = collapse(smi)
            if smi_c is not None:
                chars_c = len(smi_c)
                # The character cap applies to the COLLAPSED string because that is the
                # string the mapper would see -- and it still gates the parse, for the
                # same 80.7 MB reason as above.
                if chars_c <= char_limit:
                    atoms_c = count_atoms(smi_c)

        if int(r.n_blockers) < 0:
            verdict = "unparseable_equation"
        elif not subs or not prods:
            verdict = "pseudo_reaction"
        elif blockers:
            # A non-molecule participant is terminal: there is nothing to stand in FOR,
            # so no lane and no threshold change the answer. It outranks the rest.
            if "non_molecule" in fams:
                verdict = "non_molecule"
            elif Counter(subs) == Counter(prods):
                verdict = "no_transfer"
            else:
                verdict = "blocked_no_structure"
        elif smi is None:
            # blockers empty but no SMILES: the lookup builder declined for some other
            # reason. Recorded rather than folded into a neighbouring class.
            verdict = "blocked_no_structure"
        elif chars <= char_limit and atoms is not None and atoms <= atom_limit:
            verdict = "mappable"
        elif (chars_c is not None and chars_c <= char_limit
                and atoms_c is not None and atoms_c <= collapsed_atom_limit):
            # Recovered by collapse. `mappable` and not a verdict of its own: the
            # members treat it exactly as they treat any other mappable reaction, and a
            # separate verdict would mean adding a branch to every reader of the
            # worklist to say "and also this one". WHICH string was mapped is what
            # readers actually need, and that is the `collapsed` column.
            verdict, collapsed = "mappable", True
        elif chars > char_limit:
            verdict = "too_long"
        else:
            verdict = "oversize"

        # AN `oversize` ROW IS STILL HANDED TO A MAPPER -- Indigo -- so it too carries
        # the smaller of the two readings. Nothing was ever mapped from these strings, so
        # rewriting them moves nothing; what it does is make `rxn_smiles` mean one thing
        # on every row: the string a member would be given.
        if (verdict == "oversize" and not collapsed and smi_c is not None
                and chars_c is not None and chars_c <= char_limit):
            collapsed = True

        rows.append(dict(
            mnxr=r.mnxr, verdict=verdict, atoms=atoms, chars=chars,
            atoms_collapsed=atoms_c, chars_collapsed=chars_c, collapsed=collapsed,
            n_blockers=int(r.n_blockers), blockers=blockers, blocker_families=fams,
            is_transport=str(r.is_transport), is_balanced=str(r.is_balanced),
            classifs=str(r.classifs),
            # THE STRING THE MEMBERS MAP, and `collapsed` says which one it is, so no
            # consumer has to infer it from a length.
            rxn_smiles=(smi_c if collapsed else smi),
        ))
        if (i + 1) % 20000 == 0:
            print(f"[worklist]   adjudicated {i + 1:,}/{len(reactions):,}", flush=True)
    return pd.DataFrame(rows)


SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("verdict", pa.string()),
    # `atoms`/`chars` always describe the EXPANDED string, so the pre-collapse yield
    # curve stays recomputable from a post-collapse table. The `_collapsed` pair is
    # populated only where a collapse was attempted -- i.e. where the expanded measure
    # would have refused -- so a null there means "did not need it", not "unknown".
    ("atoms", pa.int32()), ("chars", pa.int32()),
    ("atoms_collapsed", pa.int32()), ("chars_collapsed", pa.int32()),
    ("collapsed", pa.bool_()),
    ("n_blockers", pa.int32()),
    ("blockers", pa.list_(pa.string())), ("blocker_families", pa.list_(pa.string())),
    ("is_transport", pa.string()), ("is_balanced", pa.string()),
    ("classifs", pa.string()), ("rxn_smiles", pa.string()),
])


def load(path) -> pd.DataFrame:
    return pd.read_parquet(path)


def mappable(path, admits=NEURAL_ADMITS) -> dict:
    """`{mnxr -> rxn_smiles}` for the reactions a lane is allowed to attempt.

    THE ONE READER EVERY MEMBER USES. A member that derived its own universe could drift
    from its siblings by one filter and the ensemble's disagreement rate would stop
    measuring disagreement between mappers.

    `admits` is the only thing a member may vary, and it varies for one reason: the atom
    cap is a statement about the neural members' cost, not about the chemistry. Pass
    `INDIGO_ADMITS` for the lane that can take the oversized tail.
    """
    d = pd.read_parquet(path, columns=["mnxr", "verdict", "rxn_smiles"])
    d = d[d["verdict"].isin(admits) & d["rxn_smiles"].notna()]
    return dict(zip(d["mnxr"], d["rxn_smiles"]))


def cmd_build(args):
    rx = pd.read_parquet(args.reactions)
    mets = pd.read_parquet(args.metabolites, columns=["mnxm", "name"])
    name_of = dict(zip(mets["mnxm"], mets["name"]))
    print(f"[worklist] {len(rx):,} reactions, {len(name_of):,} metabolite names",
          flush=True)

    wl = adjudicate(rx, name_of, args.atom_limit, args.char_limit,
                    args.collapsed_atom_limit)
    pq.write_table(pa.Table.from_pandas(wl, schema=SCHEMA, preserve_index=False),
                   args.out, compression="zstd")

    vc = wl["verdict"].value_counts()
    fam = Counter(f for fs in wl["blocker_families"] for f in fs)
    lines = ["kind\tkey\tn"]
    for v in VERDICTS:
        lines.append(f"verdict\t{v}\t{int(vc.get(v, 0))}")
    for f in FAMILIES:
        lines.append(f"blocker_family\t{f}\t{int(fam.get(f, 0))}")
    built = wl[wl["atoms"].notna()]
    for lo, hi in ((0, 300), (300, 600), (600, 1200), (1200, 100000)):
        n = int(((built["atoms"] >= lo) & (built["atoms"] < hi)).sum())
        lines.append(f"atom_bin\t{lo}-{hi}\t{n}")
    # The collapsed count for the same bins, over the reactions a collapse was
    # attempted on. Side by side with the expanded bins above, this IS the movement --
    # a reader can see where the recovered reactions came from without a second table.
    coll = wl[wl["atoms_collapsed"].notna()]
    for lo, hi in ((0, 300), (300, 600), (600, 1200), (1200, 100000)):
        n = int(((coll["atoms_collapsed"] >= lo) & (coll["atoms_collapsed"] < hi)).sum())
        lines.append(f"collapsed_atom_bin\t{lo}-{hi}\t{n}")
    # RECOVERED is not the same as WRITTEN COLLAPSED, now that an `oversize` row carries
    # the collapsed string too. Recovered means the second reading changed the verdict.
    recovered = int(((wl["verdict"] == "mappable") & wl["collapsed"]).sum())
    lines.append(f"collapse\trecovered\t{recovered}")
    lines.append(f"collapse\twritten_collapsed\t{int(wl['collapsed'].sum())}")
    lines.append(f"collapse\tattempted\t{int(wl['atoms_collapsed'].notna().sum())}")
    lines.append(f"route\tindigo_only\t{int((wl['verdict'] == 'oversize').sum())}")
    lines.append(f"limit\tatom_limit\t{args.atom_limit}")
    lines.append(f"limit\tchar_limit\t{args.char_limit}")
    lines.append(f"limit\tcollapsed_atom_limit\t{args.collapsed_atom_limit}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("WORKLIST -- one verdict per reaction, closed set")
    print("=" * 60)
    for v in VERDICTS:
        print(f"  {v:<24} {int(vc.get(v, 0)):>8,}")
    unknown = set(wl["verdict"]) - set(VERDICTS)
    if unknown:
        raise SystemExit(f"[worklist] verdict outside the closed set: {sorted(unknown)}")
    print(f"  {'TOTAL':<24} {len(wl):>8,}")
    print(f"\n  of which mappable only after a stoichiometric collapse: {recovered:,}")
    print(f"  over the atom cap, so Indigo alone: "
          f"{int((wl['verdict'] == 'oversize').sum()):,}")
    print("\nblocker families (metabolite-blocked reactions may carry several):")
    for f in FAMILIES:
        print(f"  {f:<24} {int(fam.get(f, 0)):>8,}")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


# =====================================================================
# closing the ledger
# =====================================================================
# The spine says what each reaction was ALLOWED to do. The close says what it DID. Every
# MNXR keeps its row, so the three answers that used to look identical -- produced
# nothing, was never attempted, was refused for a named reason -- stay distinguishable
# after the build as well as before it.

OUTCOMES = (
    "banked",                  # the reaction contributed pairs, at least one from a full map
    # A PARTIAL MAP IS ITS OWN OUTCOME, and this is the half the goal names explicitly.
    # Every pair this reaction contributed came from the partial lane -- one element,
    # from a reduced submission or from conservation. It is neither `banked` (nothing
    # ever mapped the whole reaction) nor `mapped_nothing` (it did produce pairs), and
    # collapsing it into either is the stop line: a partially-mapped reaction that reads
    # as banked overstates coverage, and one that reads as dropped hides it.
    "banked_partial",
    "partial_declined",        # offered to the partial lane, still no pair survived
    # THE REDOX REPAIR'S OWN OUTCOME. This reaction DID map -- it reached the stack with
    # pairs -- and every one of them ran between a NAD(P)/FAD/FMN couple and a substrate,
    # which a hydride transfer cannot do. The repair refused them and conservation did not
    # settle the remainder once the couple was removed. It is not `mapped_nothing`: a
    # mapper answered and the answer was an artifact, which is a different fact about the
    # reaction and the only one that points at where to look next.
    "redox_emptied",
    "mapped_nothing",          # mappable, went to the lanes, no pair survived
    "rescued_nothing",         # completed by the rescue, still no pair survived
    "rescue_declined",         # blocked, and no lane could complete it
    "oversize", "too_long", "non_molecule", "no_transfer", "pseudo_reaction",
    "unparseable_equation",
)

# What `aam.partial` stamps on every pair row it produces. Named here because `close`
# reads it to tell a partial bank from a full one, and two spellings of one string is
# how an outcome silently stops being assigned.
PARTIAL_SOURCE = "partial"


def cmd_close(args):
    wl = pd.read_parquet(args.worklist, columns=["mnxr", "verdict", "blocker_families"])
    pairs = pd.read_parquet(args.pairs, columns=["mnxr", "element", "method", "source"])

    rescued = set()
    if args.rescued:
        rescued = set(pd.read_parquet(args.rescued, columns=["mnxr"])["mnxr"])

    # OFFERED TO THE PARTIAL LANE, WHICH IS THE FORECAST'S OFFER AND NOT THE LANE'S OWN
    # UNIVERSE. The two differ by exactly the reactions the lane could not build a
    # submission for -- the reduction did not balance, or it held a structureless
    # participant -- and those are the reactions `partial_declined` is defined for. Reading
    # the built universe instead made them `mapped_nothing`, which says nothing was
    # offered when something was and it was declined for a reason the lane recorded.
    offered = set()
    if args.forecast:
        f = pd.read_parquet(args.forecast, columns=["base_mnxr", "offer"])
        offered = set(f.loc[f["offer"].astype(bool), "base_mnxr"])

    # Reactions the redox repair left holding nothing at all. Read as a plain id list
    # because that is what the repair emits and because an empty file is a legitimate
    # answer -- the expected residue is a handful of reactions.
    redox_emptied = set()
    if args.redox_emptied and Path(args.redox_emptied).exists():
        redox_emptied = {l.strip() for l in Path(args.redox_emptied).read_text().splitlines()
                         if l.strip()}

    full = pairs[pairs["source"] != PARTIAL_SOURCE]
    banked_full = set(full["mnxr"])

    by_rxn = pairs.groupby("mnxr")
    n_pairs = by_rxn.size()
    els = by_rxn["element"].apply(lambda s: ",".join(sorted(set(s))))
    meths = by_rxn["method"].apply(lambda s: ",".join(sorted(set(s))))
    srcs = by_rxn["source"].apply(lambda s: ",".join(sorted(set(s))))
    banked = set(n_pairs.index)

    def outcome(mnxr, verdict):
        if mnxr in banked:
            # A reaction with even one pair from a full map is `banked`; the partial
            # provenance of its other elements survives in the per-row method/source and
            # in the `sources` column, which is where a per-element question belongs.
            return "banked" if mnxr in banked_full else "banked_partial"
        # Ahead of both the partial and the mapped answers, because it is a statement
        # about what happened LAST: this reaction had pairs and the repair took them.
        if mnxr in redox_emptied:
            return "redox_emptied"
        if mnxr in offered:
            return "partial_declined"
        if verdict == "mappable":
            return "mapped_nothing"
        if verdict == "blocked_no_structure":
            return "rescued_nothing" if mnxr in rescued else "rescue_declined"
        return verdict

    wl["rescue_completed"] = wl["mnxr"].isin(rescued)
    wl["outcome"] = [outcome(m, v) for m, v in zip(wl["mnxr"], wl["verdict"])]
    wl["n_pairs"] = wl["mnxr"].map(n_pairs).fillna(0).astype("int64")
    wl["elements"] = wl["mnxr"].map(els).fillna("")
    wl["methods"] = wl["mnxr"].map(meths).fillna("")
    wl["sources"] = wl["mnxr"].map(srcs).fillna("")
    wl.drop(columns=["blocker_families"]).to_parquet(args.out, index=False)

    unknown = set(wl["outcome"]) - set(OUTCOMES)
    if unknown:
        raise SystemExit(f"[worklist] outcome outside the closed set: {sorted(unknown)}")

    # THE NUMBER THIS BUILD EXISTS TO MOVE. In the deployed table every rescue-derived
    # reaction is `<member>_only` at half weight, because only one mapper ever saw the
    # completed reaction. Here three do, so the ones they agree on are consensus -- and
    # that count is the improvement, stated rather than assumed.
    resc = wl[wl["rescue_completed"] & (wl["outcome"] == "banked")]
    n_consensus = int(resc["methods"].str.contains("consensus").sum())

    lines = ["kind\tkey\tn"]
    oc = wl["outcome"].value_counts()
    for o in OUTCOMES:
        lines.append(f"outcome\t{o}\t{int(oc.get(o, 0))}")
    for X in ("C", "N", "S", "P"):
        lines.append(f"element_reactions\t{X}\t"
                     f"{int(pairs.loc[pairs.element == X, 'mnxr'].nunique())}")
    lines.append(f"partial\toffered\t{len(offered)}")
    lines.append(f"partial\tpair_rows\t{int((pairs['source'] == PARTIAL_SOURCE).sum())}")
    lines.append(f"redox\temptied\t{len(redox_emptied)}")
    lines.append(f"rescue\tcompleted\t{len(rescued)}")
    lines.append(f"rescue\tbanked\t{len(resc)}")
    lines.append(f"rescue\tbanked_with_consensus\t{n_consensus}")
    lines.append(f"total\treactions\t{len(wl)}")
    lines.append(f"total\tpairs\t{len(pairs)}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("LEDGER -- one outcome per reaction, closed set")
    print("=" * 60)
    for o in OUTCOMES:
        print(f"  {o:<24} {int(oc.get(o, 0)):>8,}")
    print(f"  {'TOTAL':<24} {len(wl):>8,}")
    print(f"\nrescue: {len(rescued):,} completed, {len(resc):,} banked, "
          f"{n_consensus:,} of those carry a consensus correspondence "
          f"(the deployed table has zero -- every rescued reaction there is one "
          f"member at half weight)")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("close"); p.set_defaults(fn=cmd_close)
    p.add_argument("--worklist", required=True)
    p.add_argument("--pairs", required=True, help="the stacked aam_pairs parquet")
    p.add_argument("--rescued", default=None, help="the rescued universe parquet")
    p.add_argument("--forecast", default=None,
                   help="interm::aam_forecast. Its offered `base_mnxr` set is what the "
                        "partial lane was ASKED to reach, which is what distinguishes a "
                        "reaction the lane declined from one nothing tried. The lane's "
                        "own universe is the wrong table: it holds only what the lane "
                        "managed to build.")
    p.add_argument("--redox-emptied", default=None,
                   help="the redox repair's emptied-reaction list, one MNXR per line")
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)

    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--reactions", required=True, help="lookup::reactions parquet")
    p.add_argument("--metabolites", required=True, help="lookup::metabolites parquet")
    p.add_argument("--atom-limit", type=int, default=ATOM_LIMIT)
    p.add_argument("--char-limit", type=int, default=SMILES_LEN_LIMIT)
    p.add_argument("--collapsed-atom-limit", type=int, default=COLLAPSED_ATOM_LIMIT,
                   help="the cap on the DISTINCT-molecule count, applied only to "
                        "reactions the expanded measure would refuse")
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
