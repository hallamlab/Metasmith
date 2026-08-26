# AAM injectivity: cross-molecule leaks in `atom_pairs`

## Purpose & Contents

This directory holds the audit that answers a defect report from `fabfos/bench-fba`: on
MNXR166070 (rhamnulokinase) five of L-rhamnulose-1-phosphate's six carbons split their
weight 0.5/0.5 between L-rhamnulose and ATP's ribose. It records the measured extent of
that defect class across the deployed bake, its root cause, and how much of it a curated
map already covers.

**The defect is fixed in the bake, and the deployed chunk predates the fix.** The numbers
below therefore describe what a consumer reading `metabolism_bake` today still gets, and
what re-baking removes. Nothing here is a fix — the fix is in `ecspr.bake`, named under
*The fix* below.

The violation predicate itself lives in the bake, not here:
`ecspr.bake.aam.layers.injectivity_violations`, reachable as
`python -m ecspr.bake.aam.layers audit`. `audit_pairs.py` decodes a deployed chunk into
that verb's input shape and adds the curated-twin question, which is research rather than
bake. `scan_maps.py` reads the mapper caches directly.

## The invariant

An atom map is a bijection. Each mapped substrate atom reaches exactly one product atom.
Two violations are therefore checkable on the fused table with no chemistry:

- **A** — one substrate atom whose rows name two or more destination molecules.
- **B** — one product atom whose rows name two or more source molecules.

Dilution across atom RANKS inside one molecule is the ensemble's designed abstention and
is excluded. Dilution across MOLECULES is not an abstention. `compile_atom_graph` sums the
diluted rows into a single edge set, so a solve enters on one member's map and leaves on
the other's, and traverses a path that exists in neither.

**CAUTION** — stoichiometric multiplicity makes some of both legitimate. Glutamate
synthase writes `2 L-glutamate`, and the atom identity `(metabolite, canonical rank)`
cannot tell the two copies apart, so both substrates correctly land on one node. A row is
EXPLAINED when the molecule at the contested end carries a coefficient of two or more.
Read only the unexplained half as a defect. Multiplicity accounts for almost every
`consensus` violation, which is the sign that the neural pair is not the problem there.

## Measured on the deployed `metabolism_bake`

The atom lane is unchanged since r7. The r10 promotion rewrote `direction.parquet` only,
so these numbers describe the current chunk and its predecessor alike. Vocabulary digest
`0ffd4c8c6231696e`, 2,530,589 pair rows over 69,292 reactions.

    mamba run -n ecspr python audit_pairs.py \
      --chunk data/fabfos/processed/metabolism_bake \
      --reac-prop data/fabfos/originals/metanetx/4.5/reac_prop.tsv \
      --metabolites data/fabfos/processed/lookups/metabolites.parquet \
      --out results

| | reactions | rows | weight |
|---|---|---|---|
| violates A or B | 10,757 (15.5%) | 232,889 (9.2%) | 120,755 (5.8%) |
| explained by multiplicity | 598 | 25,275 | 16,883 |
| **unexplained** | **10,159 (14.7%)** | **207,614 (8.2%)** | **103,872 (5.0%)** |

Carbon is worst hit: 13.3% of carbon-bearing reactions, against 5.8% N, 5.2% P, 2.8% S.
Unexplained rows are 178,694 `disagree_diluted`, 26,745 `rxnmapper_only`, 1,622
`indigo_only`, 285 `localmapper_only`, 160 `curated` and 108 `consensus`.

`results/top_receivers.tsv` ranks the molecules that absorb contested weight. CoA takes
6,757 units, ATP 4,615, S-adenosyl-L-methionine 3,207, ADP 1,152, AMP 790. This is the
spurious cofactor carbon current `fabfos/bench-fba` observed on its glucose to DHAP solve.
`results/affected_reactions.tsv` lists all 10,159, heaviest first.

## Root cause: Indigo reuses product-side map numbers

    mamba run -n ecspr python scan_maps.py \
      --cache data/fabfos/processed/metabolism_bake/aam_cache --out results

| member | mapped reactions | reactions reusing a map number |
|---|---|---|
| indigo | 79,415 | 44,675 (56.3%) |
| rxnmapper | 75,382 | 0 |
| localmapper | 2,422 | 0 |

Indigo emits mapped SMILES in which one map number labels two product atoms. On
MNXR166070 it labels the ATP ribose with the rhamnulose's numbers 2, 4, 6, 8 and 10, and
leaves ADP's own ribose unmapped. `atom_pairs.pairs_from_mapped` walks the product atoms
and looks each number up in a substrate index, so the repeat pairs one sugar carbon with
two products at full weight. 23,571 reactions carry an Indigo fanout that reaches two
distinct molecules. RXNMapper's map for the same reaction is clean.

Three stages passed the invalid map through.

1. `pairs_from_mapped` never checked that a map number is unique per side.
2. `layers.fuse_members` reduces a member to one row per substrate atom with
   `max(lst, key=pair_w)`. Both Indigo rows carry weight 1.0, so the tie broke on row
   order and the member's self-contradiction became one arbitrary destination.
3. That arbitrary destination then disagreed with RXNMapper, and `disagree_diluted` gave
   it 0.5 — equal standing with a valid map.

**CAUTION** — 1,349 reactions carry `indigo_only` rows and an Indigo fanout. There the
arbitrary pick was the only answer and no dilution marked it.

## Does MetaCyc carry information that would have prevented it?

Yes for this reaction, and the bake already ingests it. MetaCyc holds `RHAMNULOKIN-RXN`
in `atom-mappings-smiles.dat`. Its map is correct: six sugar carbons to L-rhamnulose, ten
nucleotide carbons to ATP, every `pair_w` 1.0.

It does not reach MNXR166070 because MetaNetX crosswalks `RHAMNULOKIN-RXN` to **MNXR116249**.
The two records are the same chemistry under two ids, differing only in which L-rhamnulose
frame the product is written as — MNXM38657 against MNXM731755, both C6H12O5. MNXR166070
carries no `metacyc.reaction:` xref at all. The curated layer claimed MNXR116249 and left
MNXR166070 and MNXR206779 to the neural ensemble. MetaNetX files EC 2.7.1.5 under four ids.

Layer L1 is keyed on `(mnxr, element)`, so a curated map is confined to the one id the
crosswalk names. The recovery is measurable over the 10,155 unexplained reactions with no
curated map of their own:

| twin test | reactions recovered | share |
|---|---|---|
| isomeric frame, participants only | 1,145 | 11.3% |
| isomeric frame and shared EC (`--strict-ec`) | 560 | 5.5% |

**CAUTION** — the participant-only test admits chemical analogues, not only frames of one
reaction. It pairs MNXR166070 with MNXR151974 (fuculokinase, EC 2.7.1.52) before the exact
twin MNXR116249. Read 5.5% as the transferable set and 11.3% as its upper bound.

`aam/twins.py` already searches MNXref for a second record of one METABOLITE. Nothing does
the same for a reaction.

## The fix

MetaCyc was the wrong lever. It bounds the recoverable share at 5–11% of exposed
reactions, and the check that covers all of them costs no external data. Three changes,
all in `src/ecspr`:

1. `atom_pairs.duplicate_map_numbers` reads both sides of a mapped reaction SMILES.
   `pairs_from_mapped` drops the atoms carrying a reused number and reports
   `injectivity_trimmed`, so a member abstains where it argues with itself and keeps the
   rest of the reaction. `--on-noninjective refuse` drops the whole reaction instead.
   `trim` is the default because it removes the leak at no cost in coverage beyond the
   contradicted atoms. On MNXR166070 Indigo keeps one sugar carbon and five ADP carbons,
   and contributes no ATP decoy for the fusion to dilute against.
2. `layers.fuse_members` breaks a pair_w tie on the destination rather than on row order,
   and counts a member that offers one substrate atom two molecules.
3. `layers audit` reports the unexplained violation count on a fused or stacked table, and
   refuses above `--max-reactions`. `bake/aam_stack.py` runs it after the stack and files
   the report as evidence. It is REPORTED, not gated: a ceiling has to come from a
   measured re-bake, and picking one before that would freeze a number nobody derived.

What remains open, and is not a leak: `fuse_members` still keeps ONE destination per
member per substrate atom, so a duplicated participant loses a real edge — the same
`2 A = B + C` case that explains 598 reactions above. That is under-coverage rather than a
fabricated transfer, and it changes conductance everywhere, so it is its own decision.

**A re-bake is needed for any of this to reach a consumer.** Re-running the audit against
the current chunk will keep reporting 10,159 until the AAM lane runs again.
