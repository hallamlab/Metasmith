# The direction lane's reaction quotient

**Purpose & Contents.** This directory holds the scripts that decided r10 and the record of
what each measurement settled. It carries the numbers that are not derivable from the code:
what was rejected and why, what the shipped benchmark cannot see, and which numbers rest on
one measurement. The mechanism itself is documented where it runs, in
`src/ecspr/bake/direction/canon.py`. The re-bake protocol is in
`../benchmarks/direction_rescue/REBAKE.md`.

## What r10 changed

r9 shipped `exp(dG'o/RT)` — the standard state, every participant at one molar — and called
it a direction. r10 ships `exp(dG'/RT)`, with a reaction quotient formed from measured
E. coli concentrations. `dir_confidence` was never wrong about r9's number. It was confident
about a quantity nobody asked for.

Against MetaCyc's own directional categories, which no lane fits and which therefore score
the number honestly, accuracy rises on all seven strata and falls on none: 89.9% → 95.7%
overall, 91.7% → 96.5% on `PHYSIOL-*`, 97.9% → 98.7% where every participant is measured.
Of 282 outright reversals, 279 land on the curated side.

Glycogen phosphorylase, `MNXR145036`, is the reaction that prompted this. It shipped 0.204 —
synthesis favoured 5:1, backwards from what glgP and malP do — and ships 6.00. A measured
[Pi]/[G1P] of 46.9 is worth +9.54 kJ/mol, which carries dG°′ −4.18 to dG′ +5.36. No override
list decides it.

## The scripts

| script | what it answers |
|---|---|
| `poc_physiological.py` | three concentration conventions against reactions whose direction is not in doubt |
| `poc_blast_radius.py` | how much of the bake a quotient moves, split into molecularity and skew |
| `poc_validate_curated.py` | whether the corrected number agrees with curated physiology better |
| `poc_prior_holdout.py` | whether the curated prior's width and scale survive a 5-fold hold-out |
| `poc_metacyc_fields.py` | every field in MetaCyc's flat files that could have voted, scored |
| `poc_constants.py` | `DIR_SIGMA_0` and `DIR_DG_CLAMP`, re-derived from a run's own artifacts |
| `poc_glgp.py` | `MNXR145036`, with the correction re-derived independently of the lane |

`work/` holds their outputs and is not pinned. Every one is reproducible from the pinned
ECMDB, BioNumbers, MetaCyc and MNXref chunks and the deployed bake's own seams.

## What MetaCyc offers, and what the lane takes

The lane reads one field of `reactions.dat`, `REACTION-DIRECTION`. Every other candidate was
scored by `poc_metacyc_fields.py` and refused. A field carried into a schema that nothing
reads is a maintenance obligation bought with nothing.

| field | disposition |
|---|---|
| `REACTION-DIRECTION` | **the curated member.** Re-expressed in MNXR orientation by `align_one`, because MNXref re-canonicalises orientation and a naive join inverts ~60% of calls |
| `GIBBS-0` | refused twice. As a member it gets 65.4% of curated signs right against eQuilibrator's 79.4%, and 25.8% against 71.7% where the two disagree. As a validity signal it is non-monotone, and the ensemble grows *more* accurate as the gap widens |
| `pathways.dat` `REACTION-LAYOUT` | refused on coverage. Of 11,494 unanimous pathway calls, zero belong to a reaction with no reaction-level arrow, and it agrees with the arrow on 10,768 of 10,774. Its only non-duplicated content is 720 reactions the arrow calls `REVERSIBLE`, which a flux convention must not override |
| `REACTION-BALANCE-STATUS` | refused as subsumed. 485 of 503 crosswalked `:UNBALANCED-UNFIXABLE` records already land on an MNXR MetaNetX calls unbalanced, which `dG_suspect` reads. It adds 18 rows out of 83,795 |
| `EC-NUMBER` | refused. 38 direction conflicts exist in the whole crosswalk and different ECs explain 17 |
| `enzrxns.dat` `REACTION-DIRECTION` | refused. 8,260 enzyme-level calls over 5,989 reactions, every one of which already carries a reaction-level arrow |
| `PHYSIOLOGICALLY-RELEVANT?` | refused. `T` on 19,572 of 19,597 records |

The negative result had a useful half. `|eq − dgbyg|` predicts a broken equation monotonically
and further than either magnitude or GIBBS-0 does — 2.5%, 8.7%, 11.1%, 18.4%, 21.9%
unbalanced across its bands — and it was already in the annotation as two columns nothing
compared. It is now `dG_suspect`'s `member_disagreement`.

## Three things the curated benchmark cannot see

`poc_validate_curated.py` is the lane's only held-out label set, and it is blind in ways that
matter. Each of these had to be decided by physics, and the cost is recorded rather than
hidden.

1. **The dissolved-oxygen decision moves it by one reaction.** O₂ is the largest participant
   in the universe after water and the proton, and excluding it prices it at 1 M — 3.58
   decades above its measured 0.264 mM.
2. **The `DIR_TAU_CUR_FLOOR` sweep is flat** from 0.25 to 3 decades. Nothing in the benchmark
   distinguishes them.
3. **`MNXR145036` is absent from the scored population entirely**, because it carries no
   curated category. The expansion cap decides whether it is fixed — at 2 the correction is
   +3.99 against a break-even of +4.175 and the reaction stays backwards — and the benchmark
   cannot see the difference.

## The prior's scale, and the arm that lost

The combiner averages the curated category's centre with the thermo vote by precision, so
both must be on one scale. r10 fits every category on the corrected number and names the
scale in `prior_quantity`.

Splitting the taxonomy by the `PHYSIOL-` prefix — fitting the irreversible bins on dG°′ and
transporting each by its own reaction's correction — was tried and lost. It moves 16 held-out
calls and every one lands off the curated side, all in the two transported bins. An
irreversibility claim is about the reaction, and pricing it per reaction injects
concentration noise into a statement that was never about concentrations.
`poc_prior_holdout.py` still scores that arm.

## What rests on one measurement

- **[Pi] is one ECMDB row at 5 mM**, and it is the heaviest single-condition value in the
  table at 3,359 in-graph incidences. Pyrophosphate is the same shape at 2,231. No measured
  source corroborates either. This does not threaten glgP — the call flips above
  [Pi]/[G1P] = 5.4 against a measured 46.9, and every competing literature estimate is
  *higher* — but the width on phosphate is a floor rather than a measurement.
- **41% of the measured metabolites rest on a single growth condition.** `DIR_CONC_SPREAD_FLOOR`
  exists for exactly this. The survey found no measured source that closes it: Gerosa 2015
  gives a second condition to zero of them and Park 2016 to sixteen.
- **The table is grounded on E. coli and Nostoc is not E. coli.** The correction is dominated
  by cofactor pool ratios, which are properties of physiological state far more than of
  species, and `sigma_conc` is where the approximation is paid for. The row schema carries an
  organism column, so a cyanobacterial source is an adapter rather than a migration.

## Two traps, both paid for here

**A one-sided quotient is worse than none.** The first pass on `MNXR145036` returned
−17.3 kJ/mol rather than +5.4, because [Pi] was measured, [G1P] was not, and the missing
participant was skipped rather than defaulted. Skipping leaves it at 1 M and invents a
50-fold pool skew that is not there.

**The InChIKey connectivity block is not a compound.** `reac_prop` writes glgP with
`MNXM1364212` while KEGG's C00103 resolves to `MNXM1364214`, the alpha anomer, so an
accession join misses the measurement on the very reaction that motivated the work. Joining
on the InChIKey first block reaches it and sixteen others, because `HXXFSFRBOHSIMQ` holds
glucose, galactose, mannose, allose and gulose 1-phosphate, every anomer and both enantiomeric
series. The join that works is MetaNetX's own alias lists in `chem_xref` column 3, guarded on
formula and charge.
