# How much of the direction gap is rescuable?

The bake gives every MetaNetX reaction a directional conductance ratio, fused from two
thermodynamic estimators and MetaCyc's curated calls. Where no member votes the ratio
defaults to 1.0 — a real no-op in the conductance model, so "no evidence" and "genuinely
reversible" are indistinguishable to every consumer downstream.

The sibling scopes had been treating that gap as an irreducible evidence problem. It is
not, mostly. This directory measures how much of it is machinery.

**r8 cashed the machinery half.** The gap was 47,266 of 83,795 under r7 and is **37,404**
now; the tables below are re-measured against r8 and read as *what is left*, not as what
was available. What remains is a workable carrier-curation problem and a residue nobody
has a lever on.

Reproduce every number below with `measure_rescue.py`; the header of that file has the
three commands. Nothing here runs a member — the two `direction forecast` tables and the
deployed annotation are enough, which is the point. `shard_cost.sh` is the one script that
does run one, and it runs it on a twentieth of the universe.

| file | what it answers |
|---|---|
| `measure_rescue.py` | the mechanism table — how much of the gap each repair reaches |
| `reassemble.py` | re-runs the assembly locally and diffs it against a bake's own annotation |
| `glycogen_delivery.py` | which reaction delivers the carbon that arrives at glycogen |
| `glycogen_share_baseline.py` | what share of the injected carbon reaches glycogen at all |
| `scan_direction_caches.py` | which bake each decoded cache in the workspace actually holds |
| `shard_cost.sh` | what the eQuilibrator member costs before and after the fix |
| `REBAKE.md` | how to re-bake direction, and what a re-bake must not rediscover |
| `SHARD_COST.md` | what the lane costs, and the two resource declarations it settled |
| `baselines/` | the r8 readings r9 is measured against, each stamped with its bake |

## The one-line answer

**Three defects in how the ensemble read its own inputs accounted for about a fifth of the
gap, and r8 is the bake that carries the fixes.** 9,862 reactions left tier 0 and none that
carried a vote in r7 lost one. The rest divides into a workable carrier-curation problem and
a residue nobody has a lever on.

## The three defects

**Water was filtered out of the compound table.** MetaNetX 4.5 files water only as the
pseudo-accession `WATER`, and `load_mnxm_props` admitted only accessions beginning `MNXM`.
Both members therefore abstained — before attempting any chemistry — on every one of the
**30,546 water-bearing reactions, 36% of the universe**. Measured on the deployed member
tables: not one water-bearing reaction has ever received a thermodynamic vote, zero in
tier 1 and zero in tier 2. `refdata.py`'s `_split_terms` documents the hazard 85 lines
above where the loader ignored it.

**`dir_method` named members that never spoke.** `combine.py` tested `is not None` against
columns produced by a pandas left merge, where an absent member arrives as `NaN` and
`NaN is not None` is `True`. 13,479 rows of **r7** named a member that was silent — 12,405
dGbyG-only rows labelled `eq_gc_x_dgbyg`, 954 eQ-only ones the same, 120 tier-1 rows
claiming a dGbyG that is not there. Ratios were never affected, which is exactly why it went
unnoticed, and exactly why no before/after member accounting could be read until it was
fixed. r8 carries **0**, and the three labels the bug made unreachable are present:
`dgbyg` 19,142, `eq_gc` 1,068, `eq_rc` 163.

Both landed in `T1: water was never a compound here, and NaN was never a vote` and reached
a bake in r8.

**A third defect was found while re-baking and is fixed in r8.** eQuilibrator returns
dG'=0 at the sigma floor when a reaction's groups cancel identically — a statement about
the equation, not a measurement of it — and the combiner was promoting those to tier 1.
4,814 of r7's 5,554 tier-1 rows were group cancellations, so the tier a consumer reads as
MEASURED was 87% no-information. r8's tier 1 is 2,171 rows, none of them at the floor. The
same rows had also contaminated `DIR_SIGMA_0`, which is why it moved 9.505 → 23.489.

## The mechanism table

Counts are reactions; `in_graph` restricts to reactions carrying at least one atom pair,
which is the only population where a direction ratio changes a conductance. Confidence is
about whether *reaching the model* turns into *getting a number*, which is a different
question from reachability and the one that decides whether a repair is worth building.

The water-fix rows were a forecast under r7 and are a *residue* under r8: the reactions the
forecast expected to move that did not. The r7 column is kept because the difference between
the two is the only honest way to read how well the forecast did.

| mechanism | r7 forecast | r8 left | in_graph | confidence |
|---|---:|---:|---:|---|
| **water fix** — tier-0 reactions dGbyG can now score | 9,677 | **24** | 24 | high |
| **water fix** — tier-0 reactions eQuilibrator can now score | 5,339 | **87** | 81 | high |
| **water fix** — tier-0 reactions either member can now score | 9,963 | **101** | 95 | high |
| **water fix** — tier-3 rows gaining a thermo vote | 3,806 | **6** | 6 | high |
| carrier table, remainder already balanced | 2,853 | 2,853 | 2,489 | medium |
| wildcard capping, remainder already balanced | 3,679 | 3,679 | 3,632 | low–medium |
| carrier crosswalk ceiling (structures *and* a rebalance) | 6,171 | 6,171 | 4,080 | low |
| wildcard ceiling | 10,772 | 10,772 | 10,626 | low |
| unbalanced with nothing else in the way | 1,145 | 1,145 | 1,081 | very low |
| element-neutral twin (the `aam_blockers` rule) | 303 | 303 | 277 | **dead end** |

The carrier and wildcard rows do not move, and under r8 that was correct rather than
suspicious: they are properties of MetaNetX's compound table, which r8 did not touch. r9
does touch them — see *r9, staged and not deployed* below.

**The water fix delivered 9,862 of the 9,963 it was forecast to, and the 101 shortfall is
the same 101 everywhere it appears.** dGbyG called them unbalanced where the forecast
expected them to balance — the `heavy_atom_tolerance 1e-9` boundary the forecast declares in
its own summary. The tier-3 row landed 3,800 of 3,806 on the same reading.

The eQuilibrator rows are measurements rather than upper bounds because `--resolution` was
supplied. Without it the eq arm over-counts badly and the forecast says so in its own
summary — the difference is 10,175 against 5,339 on the first row, which is the whole
argument for the `resolve` pass existing.

**The water fix is spent; the two closed-remainder rows are what is left of the realistic
near-term rescue: 6,532 reactions, 6,121 of them in-graph.** The stretch case reaches
~20,000 and depends on curation that does not exist yet.

## r9, staged and not deployed

r8 is still the deployed bake, so every table above remains the deployed reading. r9 is
pinned beside it at `metabolism_bake_r9` and measured with the same script against its own
rebuilt forecast (`work/r9/`, `--substitutions src/ecspr/bake/direction`).

**r9 is the first bake in which the substitution tables actually reach the members.**
Neither member lane was passed `--substitutions` before `f4642fc`, so the rows committed
for r8 were inert in it, and the r8 sentence above — *the carrier and wildcard rows do not
move, and that is correct* — expired the moment they were wired through.

| mechanism | r8 left | r9 left | in_graph r9 |
|---|---:|---:|---:|
| carrier, remainder already balanced | 2,853 | **2,086** | 1,745 |
| wildcard, remainder already balanced | 3,679 | **3,051** | 3,005 |
| carrier crosswalk ceiling | 6,171 | **5,367** | 3,332 |
| wildcard ceiling | 10,772 | **10,093** | 9,949 |
| unbalanced with nothing else in the way | 1,145 | 1,168 | 1,073 |
| element-neutral twin | 303 | 303 | 277 |

**The near-term rescue budget falls 6,532 → 5,137 reactions (6,121 → 4,750 in-graph): the
substitution lane has already cashed about a fifth of it.** The residue no named mechanism
reaches is 18,786, essentially where r8 left it — the floor is not what moved.

Tier 0 goes 37,404 → 36,151. 1,173 of the 1,253 that leave carry atom-pair edges and 1,108
of those point past tenfold, so they are calls and not nudges; 568 tier-3 rows trade the
curated prior for a measured vote. **Tier 1 is unchanged to the reaction and σ₀ is
unchanged to four decimals** — nothing substituted reaches eQuilibrator's
reactant-contribution arm, so the tier a consumer reads as *measured* gained nothing and the
prior width that shrinks every row was not set by asserted chemistry. `unbalanced` rising by
23 is the same restaging boundary the r8 shortfall came from.

## Is the forecast to be believed?

`forecast backtest` scores it against the r7 member tables, which carry the real `reason`
for all 83,795 reactions — a pure join, no run required. Built with `--mnxm-only` so the
comparison is like for like:

| member | exact mechanism | predicted silent but spoke | predicted to speak but silent |
|---|---:|---:|---:|
| dGbyG | 83,732 / 83,795 (**99.92%**) | **0** | 63 |
| eQuilibrator | 81,227 / 83,795 (**96.94%**) | **0** | 2,568 |

Re-scored against **r8**'s member seams, with the post-fix forecast — `backtest_r8.tsv`,
and the run that produced it also reproduces both `forecast_summary_*.tsv` byte for byte:

| member | exact mechanism | predicted silent but spoke | predicted to speak but silent |
|---|---:|---:|---:|
| dGbyG | 83,694 / 83,795 (**99.88%**) | **0** | 101, all `unbalanced` |
| eQuilibrator | 79,698 / 83,795 (**95.11%**) | **0** | 4,097, all `uninformative` |

Both residuals grew because the water fix admitted reactions that then failed further
down, and both stayed entirely inside the two mechanisms this lane declares it cannot
predict. **The zero is what carries over, and it is the property the mechanism table
rests on.**

`no_smiles` is 38,225 predicted against 38,225 actual; `wildcard` 18,036 against 18,036;
`no_props` 48,385 against 48,385; `unresolved` 19,120 against 19,120. Every one of those is
exact.

**The error is one-sided, and that is what makes the table above usable.** The forecast
never claims a member will be silent where it spoke, so a rescue count read off
`expected_ok` can only overstate what is *blocked*, never overstate what is *rescued*. The
two residuals are the two mechanisms this lane deliberately does not predict, named rather
than tuned away:

- **eQuilibrator's 2,568** are all `uninformative` — a returned sigma past
  `SIGMA_CEILING`, a property of its covariance matrix and reachable only by asking it for
  the number.
- **dGbyG's 63** are reactions it called unbalanced whose heavy atoms do balance. It
  transforms protonation at pH 7, so its hydrogen and charge ledger is not a function of
  the SMILES. Counting H instead gives 1,280 false positives; counting heavy atoms alone
  gives none in the 27,534 reactions that reached the check. The predicate is the
  conservative one on purpose.

  **On the post-fix run this class is 101**, measured against r8's member table: the
  forecast said 38,596 dGbyG `expected_ok` and 2,944 `unbalanced`, and the member returned
  38,495 and 3,045. It is the whole of the water fix's shortfall, 0.26% of the prediction,
  and still one-sided. `forecast_summary_postfix.tsv` carries both the prediction and the
  observation rather than being edited to agree with the outcome — a forecast quietly
  rewritten to match what happened stops being evidence about the forecast.

### Why dGbyG is the high-confidence arm

It answered **25,053 of the 25,053** reactions that got past its own guards in r7. There is
no attrition between "reaches the model" and "gets a number" — which is why the water fix's
dGbyG row can be read as a rescue count rather than as an upper bound. eQuilibrator has two
further ways to say nothing after admitting a reaction (`unresolved`, and a degenerate
sigma past the ceiling), so its arm needs the resolution pass before its numbers mean the
same thing.

## The two big residual classes are one *kind* of problem, not one set of compounds

After the water fix, **36,936 tier-0 reactions are still silent** (23,397 of them in-graph),
and dGbyG's mechanism partitions them exactly: **23,630 blocked by a participant with no
SMILES at all, 12,161 by one carrying a `*` residue, 1,145 by an unbalanced equation.** Both
distributions are headed by generic redox carriers — `reduced/oxidized [NADPH--hemoprotein
reductase]`, `Acceptor`, `Reduced acceptor`, `ACP` on the structureless side; `AH2`,
`Reduced flavin`, `Flavin`, `a reduced two electron carrier`, ferredoxins on the wildcard
side. MetaNetX underspecifies the same class of compound two different ways, and one
curation effort addresses both.

**But they are largely different accessions, and that matters to whoever builds the
table.** Of the top 200 blockers on each side, only four normalise to a shared name. A
carrier table keyed on the accessions from one population will miss most of the other, so
it has to be built over both lists at once.

| | reactions blocked | distinct blockers | of which generic carriers | reactions they cover | top 20 carriers | top 100 |
|---|---:|---:|---:|---:|---:|---:|
| no SMILES | 23,630 | 10,317 | 1,370 | 5,913 (25%) | 53% | 67% |
| `*` residue | 12,161 | 4,004 | 1,138 | 4,268 (35%) | 45% | 63% |

The concentration inside each carrier population is workable — **a hundred carriers cover
roughly two thirds of what carriers block, on either side**. That is bounded curation work,
and for the 2,853 reactions whose remainder already balances the direction is then a ΔE°′
*lookup* rather than a model.

`element_counts.parquet` already sits in the bake's seams at 6.8 MB with an exact
per-element recount, a residue-slot column and a five-route provenance ladder, and is read
by **zero** direction-lane code. Whatever the carrier work turns out to be, it starts there
rather than re-deriving counts from formulas.

### The residue

About **18,800 tier-0 reactions are reached by no named mechanism at all.** They are mostly
the three quarters of the no-SMILES population that the carrier vocabulary does not
explain, spread over some 9,000 distinct blockers whose visible head is enumerated lipid
species (`1,2-Diacyl-sn-glycerol(16:1(9Z)/20:5(…))` and its siblings, one accession per
acyl-chain combination), `Unknown`, and long-tail one-offs. Naming that number is the
point: it is the honest floor on what this gap reduces to without new chemistry.

## What was tested and rejected

`aam_blockers`' element-neutrality rule — adopt the structure of a same-name twin that
holds zero C/N/S/P — settles **303 reactions as a ceiling on a name key looser than the one
the real predicate uses**, against a prior replay through the actual twins machinery that
settled 14. The reason is in the recount: **1,411 of 1,434,162 compounds are element-neutral
at all.** The predicate has almost nothing to work with. It is in the table so nobody
re-derives it.

## What this does *not* fix

The three reactions the eydallin cohort is blocked on — glgA (`MNXR145046`) and glgP
(`MNXR145036`, `MNXR145038`) — **contain no water and are not beneficiaries of anything
here.** They are eQuilibrator-`unresolved` and dGbyG-`unbalanced`: the polymer-budget
defect the sibling journals name, which is a separate problem with a separate fix. They are
a good negative control for the re-bake and must not be counted as rescued.
