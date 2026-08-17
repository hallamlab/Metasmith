# How much of the direction gap is rescuable?

The r7 bake gives every MetaNetX reaction a directional conductance ratio, fused from two
thermodynamic estimators and MetaCyc's curated calls. **47,266 of 83,795 reactions carry no
vote at all** and land at ratio 1.0. That is a real no-op in the conductance model, so "no
evidence" and "genuinely reversible" are indistinguishable to every consumer downstream.

The sibling scopes had been treating that gap as an irreducible evidence problem. It is
not, mostly. This directory measures how much of it is machinery.

Reproduce every number below with `measure_rescue.py`; the header of that file has the
three commands. Nothing here runs a member — the two `direction forecast` tables and the
deployed annotation are enough, which is the point. `shard_cost.sh` is the one script that
does run one, and it runs it on a twentieth of the universe.

| file | what it answers |
|---|---|
| `measure_rescue.py` | the mechanism table — how much of the gap each repair reaches |
| `shard_cost.sh` | what the eQuilibrator member costs before and after the fix |
| `REBAKE_HANDOFF.md` | what the deferred re-bake session must not rediscover |
| `SHARD_COST.md` | the shard measurement, and the resource declaration it implies |

## The one-line answer

**Two defects in how the ensemble read its own inputs account for about a fifth of the
gap, and they are fixed.** The rest divides into a workable carrier-curation problem and a
residue nobody has a lever on.

## The two defects

**Water was filtered out of the compound table.** MetaNetX 4.5 files water only as the
pseudo-accession `WATER`, and `load_mnxm_props` admitted only accessions beginning `MNXM`.
Both members therefore abstained — before attempting any chemistry — on every one of the
**30,546 water-bearing reactions, 36% of the universe**. Measured on the deployed member
tables: not one water-bearing reaction has ever received a thermodynamic vote, zero in
tier 1 and zero in tier 2. `refdata.py`'s `_split_terms` documents the hazard 85 lines
above where the loader ignored it.

**`dir_method` named members that never spoke.** `combine.py` tested `is not None` against
columns produced by a pandas left merge, where an absent member arrives as `NaN` and
`NaN is not None` is `True`. 13,479 rows of the deployed bake name a member that was
silent — 12,405 dGbyG-only rows labelled `eq_gc_x_dgbyg`, 954 eQ-only ones the same, 120
tier-1 rows claiming a dGbyG that is not there. Ratios were never affected, which is
exactly why it went unnoticed, and exactly why no before/after member accounting could be
read until it was fixed.

Both landed in `T1: water was never a compound here, and NaN was never a vote`.

## The mechanism table

Counts are reactions; `in_graph` restricts to reactions carrying at least one atom pair,
which is the only population where a direction ratio changes a conductance. Confidence is
about whether *reaching the model* turns into *getting a number*, which is a different
question from reachability and the one that decides whether a repair is worth building.

| mechanism | reactions | in_graph | confidence |
|---|---:|---:|---|
| **water fix** — tier-0 reactions dGbyG can now score | **9,677** | **9,588** | high |
| **water fix** — tier-0 reactions eQuilibrator can now score | 5,339 | 5,241 | high |
| **water fix** — tier-0 reactions either member can now score | **9,963** | **9,802** | high |
| **water fix** — tier-3 rows gaining a thermo vote | 3,806 | 3,788 | high |
| carrier table, remainder already balanced | 2,853 | 2,489 | medium |
| wildcard capping, remainder already balanced | 3,679 | 3,632 | low–medium |
| carrier crosswalk ceiling (structures *and* a rebalance) | 6,171 | 4,080 | low |
| wildcard ceiling | 10,772 | 10,626 | low |
| unbalanced with nothing else in the way | 1,145 | 1,081 | very low |
| element-neutral twin (the `aam_blockers` rule) | 303 | 277 | **dead end** |

The eQuilibrator rows are measurements rather than upper bounds because `--resolution` was
supplied. Without it the eq arm over-counts badly and the forecast says so in its own
summary — the difference is 10,175 against 5,339 on the first row, which is the whole
argument for the `resolve` pass existing.

**Realistic near-term rescue is the water fix plus the two closed-remainder rows: roughly
13,000–16,500 reactions, of which about 12,900–15,900 carry graph edges.** The stretch case
reaches ~20,000 and depends on curation that does not exist yet.

## Is the forecast to be believed?

`forecast backtest` scores it against the r7 member tables, which carry the real `reason`
for all 83,795 reactions — a pure join, no run required. Built with `--mnxm-only` so the
comparison is like for like:

| member | exact mechanism | predicted silent but spoke | predicted to speak but silent |
|---|---:|---:|---:|
| dGbyG | 83,732 / 83,795 (**99.92%**) | **0** | 63 |
| eQuilibrator | 81,227 / 83,795 (**96.94%**) | **0** | 2,568 |

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
