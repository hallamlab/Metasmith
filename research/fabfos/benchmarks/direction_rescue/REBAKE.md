# Re-baking direction

**Purpose & Contents.** This is the protocol for re-baking the direction table, and the set
of things that cost a run each when found the hard way. It carries the sequence, the
staging rule, what every re-bake must re-derive, and the traps. The artifact itself — the
trio, its identity block, what `src_direction_sha256` is for — is documented at
`src/fabfos/build_references/REFERENCES.md` § R6 and is not repeated here. The direction
mechanism is documented where it runs, in `src/ecspr/bake/direction/canon.py`.

**r10 is the deployed direction table.** `data/fabfos/processed/metabolism_bake` pins md5
`dc5a7ca03e31d4b17063e5dec22a3cba.dir`, and `direction.parquet`'s `src_direction_sha256` is
`554b4d8f`. It is the first bake whose ratio is a PHYSIOLOGICAL quantity: every number is
`exp(dG'/RT)` with a reaction quotient formed from measured E. coli concentrations, where
every bake before it shipped `exp(dG'o/RT)` and called it a direction.

Accuracy against MetaCyc's own directional categories rises on all seven strata and falls on
none — 89.9% → 95.7% overall. No reaction that carried a vote stops carrying one: the tier
transition matrix r9 → r10 is diagonal at 36,151 / 2,171 / 39,401 / 6,072.

## Bake history, for anyone comparing two of them

Only what a reader comparing bakes needs. The rest is in the commits that made each one.

- **r8** fixed three defects in how the ensemble read its own inputs: water filtered out of
  the compound table, so both thermodynamic members abstained on 36% of the universe before
  attempting any chemistry; `dir_method` naming members that arrived as `NaN` through a left
  merge; and eQuilibrator's group cancellations promoted to tier 1.
- **r9** is the first bake whose members actually RECEIVE the substitution tables.
  `--substitutions` reached neither member lane until `f4642fc`, so every substitution row
  committed for r8 was inert in it. Read any r8-vs-r9 delta as the whole substitution lane
  arriving, not the acyl rows alone. Tier 0 fell 37,404 → 36,151 and σ₀ did not move,
  because nothing substituted lands on eQuilibrator's reactant-contribution arm.
- **r10** is the first bake whose ratio is a physiological quantity. It also moved the
  curated prior onto that same quantity, made the balance gate consult the member before
  refusing, and split the magnitude cap's two jobs into a numerical bound and a named
  `dG_suspect` signal. Tier counts do not move at all.

## The pricing ladder, and the assembly order

**Price the arms before assembling.** `reassemble.py` re-runs `curated`, `calibrate` and
`combine` off artifacts on disk in about fifteen seconds. `ladder.sh` drives six of those
runs, each adding exactly one flag to the one below it, so every delta belongs to exactly
one mechanism. `ladder_table.py` reads the six annotations into one table.

**Arm 1 must come out IDENTICAL, and three flags are needed for that.**
`--supplementary-crosswalk` is what r9 was baked with. `--clamp 100` is what r9 was baked
with, before canon moved to three decades without a re-bake. `--sigma0 23.489` is r9's
fitted value. Hold all three through arms 2-5, so each delta is its mechanism alone and arm
6's delta is the constants alone.

    arm  change          differs  clamped  wrong  accuracy  MNXR145036
      1  r9 reproduced         -    6,516    894    89.90%      0.2044
      2  + balance gate   17,231    6,516    898    89.86%      0.2044
      3  + robust width   17,231    6,372    531    94.11%      0.2044
      4  + quotient       83,795    5,796    398    95.65%      5.3161
      5  + prior on dG'   17,231    5,839    397    95.68%      5.3161
      6  + constants      83,795   19,872    397    95.68%      6.0012

The two prior arms move exactly the 17,231 rows carrying a curated category and the two
thermodynamic arms move all of them. Read arms 3 and 5 as unscored: `dG_raw` contains the
prior on every scored row, so that column scores the quotient cleanly and the prior not at
all. The prior's evidence is `research/fabfos/bake/poc_prior_holdout.py`.

**Then run the bake.** `curated` → `quotient table` → `quotient annotate` → `calibrate` →
fit sigma_0 → commit both canon copies → re-vendor `buildlib::ecspr` → `direction` →
`direction_bake`. Re-vendor after the last code edit and before the assembly, or the run
uses the old chemistry with nothing saying so.

**On a direction-only route the members do not re-run.** The quotient is a function of
stoichiometry and concentrations, so the deployed `direction_member_eq` and
`direction_member_dgbyg` stay unchanged inputs and the re-bake is a minutes-long local job
rather than a cluster run. That is what makes several changes in one bake tractable.

## Staging, verifying and promoting

**Build the new chunk beside the deployed one, verify against *that path*, then promote.**
Two reasons it is this order and not the convenient one:

- The chunk's files are read-only hardlinks into the shared DVC cache, shared with every
  sibling worktree. Editing one in place corrupts that bake for all of them and for every
  historical commit pinning it. Copy the tree, swap files in the copy, pin under a suffixed
  name, promote by renaming onto `metabolism_bake` and repinning.
- **A suffixed chunk is read by nothing.** Every consumer resolves
  `data/fabfos/processed/metabolism_bake`, so a green test suite before the promote proves
  only that the *old* bake still works. Exactly two verifiers can be aimed at a staged
  chunk: `check_references.py --results <path>` (it rglobs a results tree) and
  `research/fabfos/benchmarks/aam_v3_nostoc.py <chunk>`, the only benchmark that takes a
  chunk name. Everything else waits for the promote.

**Swap the seams with the table.** `seams/direction_annotation.parquet` is what
`src_direction_sha256` names, and nothing compares them, so a chunk can be left pointing at
an annotation it does not carry. It is also load-bearing downstream: `measure_rescue.py`
reads that seam, so a stale one silently re-measures the previous bake.

**Keep `logs/` and `aam_cache/` verbatim.** They are inputs the next *full* bake stages
from, and `aam_forecast/measure_recall.py` reads `logs/` as its ground truth (57,538 of
57,593). `promote_logs()` in the HPC driver would rebuild them from an empty run on a
direction-only route; it now refuses when the mapper caches are absent, and that refusal
should be confirmed in the retrieval output rather than assumed.

## What must be re-derived, every time

**σ₀, from the calibration run's own points table** — never from the annotation, which has
a different denominator. It is fitted on the measured arm, so any change to member coverage
moves it, and it sets the shrinkage on every row including the rows that gained nothing.
`combine` refuses a value outside `DIR_SIGMA_0_BAND` rather than warning. Update both
`canon.py` and `_deprecated_canon.py`. `check_direction_constants` compares them.

**A change to the curated PRIOR does not move σ₀.** σ₀ is the MARGINAL spread of the
anchors, so the prior enters neither the population nor the estimator. r10 expected it to
move and measured that it does not. The clamp is different: it is a property of the
posterior, so the prior does move it.

**Both fitted constants come out of `research/fabfos/bake/poc_constants.py`**, against a
run's own `_calibration_points.parquet` and `direction_annotation.parquet`. A fitted
quantity whose derivation lives only in a commit message cannot be re-run when the
distribution it describes moves.

**The deployed-bake test pins.** `S_NODES`, `S_EDGES` and the float32 guard in
`tests/ecspr/bake/test_deployed_bake.py` route through `ratio_by_code`, so a direction-only
re-bake moves them while leaving `BAKE`, `S_PAIR_ROWS`, `S_REACTIONS_USED` and
`S_METABOLITES` alone. Say in the commit which bake the new numbers describe — the identity
constant cannot say it for you.

**The evidence version.** The direction lanes pass an explicit `--version $DIRVER`, because
the default fingerprint hashes `bake/*.py` and not `bake/direction/*.py`. `DIRVER` moves
with any edit to `canon.py`, so recompute it (`ecspr.bake.evidence fingerprint --package
direction`) and never quote a previous run's value.

## Every tier-3 ratio moves, and that is correct

The curated prior is fitted per category on the same measured arm, so all `biocyc_only`
rows re-price without any of those reactions gaining a member of its own. Do not read a
large tier-3 delta as a bug. Two of r10's five changes move the prior, so it moves every one
of those rows and nothing else — which is what the ladder's `differs` column shows.

## Derived artifacts that do not record which bake they came from

This is the bug class this lane keeps producing, and it is silent by construction: the
consumer gets a plausible table from the previous bake and no error. The eydallin decode
cache is fixed — it stamps each entry with the bake identity *paired with*
`src_direction_sha256`, since the identity alone does not move on a direction-only re-bake.
Two instances remain live and are not this lane's to fix:

- `data/fabfos/nostoc/ecspr/networks/*/{atom_pairs,gpr,direction}.parquet` — pre-composed
  from a bake older than r7, so `nostoc_ecspr_verify.py --structural` fails by construction.
- `verify_bake_join.py` reads the retired `data/fabfos/benchmark/reference_tier4` pin and
  cannot run at all.

Before adding a cache here, ask what it would serve after the next repin.

## What r9 added to the mechanism

**A substitution is admitted per member, not per table.** `sigma_sub` and the congener
spread are `congeners_eq`/`gap_eq` and `congeners_dgbyg`/`gap_dgbyg`, `load()` refuses to
run without a `member=`, and a row whose anchor drifts past `DIR_DECADE` for one member is
refused **for that member only** — `member_drift` is non-fatal by design, so one member's
disagreement cannot cost the other its vote. `member_unscored` stays fatal: an arm nobody
ran the anchor for is an absence of evidence, not a small number.

**A `thioester` row predicts a zero offset, like a `polymer` one.** Both are the same
transformation written twice, so there is no potential to declare and zero is the
prediction rather than the absence of one — `ZERO_OFFSET_KINDS`. The anchor is then the
entire safety argument for the kind, which is why a row is refused unless the restaged
equation lands on the number the deployed bake already holds from different accessions.

**The acyl carriers are dGbyG's alone, and eQuilibrator's refusal is its own arithmetic.**
Modelling acyl-[ACP] as acyl-4′-phosphopantetheine — ACP's actual prosthetic arm, which
MetaNetX carries readably — makes the anchor a real comparison. dGbyG places all 19 within
6e-05; eQuilibrator places 17 of them at a constant (8.8776 for the C8–C14 series, 15.1953
acetyl, 14.0755 malonyl). The constant does not vary with the acyl group, so it is not a
property of the thioester bond, and the transacylation `X-S-Ppant + CoA = X-S-CoA + Ppant`
carries identical groups on both sides yet returns the same constants. `MNXR204097` carries
acetyl *and* malonyl, where the offsets partly cancel to 1.12 and the row passes — additive
per thioester, which is what a decomposition artifact looks like. The rows therefore go to
dGbyG through the ordinary one-sided path, with nothing special-cased and `DIR_DECADE`
untouched. Because tier 1 needs eQuilibrator's reactant-contribution arm, these reactions
can never reach it.

## Traps paid for the hard way

**The relay workspace is shared across agent homes.** It is `/tmp/msm_<login-node>_<user>`,
symlinked from each new agent home, so a watcher that fails to hand over wedges the *next*
run's `StageWorkflow` — the client sees `produced no output for 300s` while the remote
process sits at zero CPU. Clear that directory and kill any leftover `msm_relay` before
launching. (`pkill -f 'relay/msm_relay'` matches its own ssh command line and kills the
session; bracket the pattern.)

**Verify a float table with a tolerance, not with `!=`.** The eq lane runs sharded now and
ran single-pass in r8, and no member promises bit-identical accumulation across a different
batching. `dg`, `flag` and `reason` are identical on every uncovered row; `sigma` moves by
up to 2.8e-14 kJ/mol. `prove_subs.py` compares verdict columns exactly and measured ones to
1e-9, and prints the largest drift it tolerated so a real one cannot hide under the bound.

**The forecast must be rebuilt with `--substitutions` before `measure_rescue` can read the
bake.** Against a forecast built without them the two disagree about which members spoke,
which is the check doing its job. Rebuilding also needs a `resolve` pass covering the model
compounds — all 32 resolve against eQuilibrator's cache, so no substituted reaction is
silenced by an unlookup-able stand-in.

**A `bake/direction/*.py` edit moves `DIRVER` whether or not it moves chemistry.** The
fingerprint hashes the package, so the `forecast.py` union fix taken after r9's artifacts
were produced carried the tree `lib-direction-2865c03abc32` → `lib-direction-d938deb31ec7`
while every member table, annotation and ratio stayed exactly as baked. r9's staged
artifacts are stamped `2865c03abc32` and that is the version that describes them; r10's is
`lib-direction-c1c105165235`. Recompute it per bake and never "fix" a mismatch by
restamping anything.

## One decision the lane deliberately does not carry

**Branching glycogen stays out**, and it costs exactly three tier-0 reactions --
`MNXR136341`, `MNXR145038`, `MNXR145039`. 411 MetaNetX compounds carry the acceptor
formula `C18H32O16` and at least three are defensible branched alpha-glucans (panose
`MNXM1104683`, isomaltotriose `MNXM1104226`/`MNXM1106015`, 6-O-glucosylmaltose
`MNXM1107398`). No gate separates them, so authoring a row would be choosing one by hand
and calling it a lookup. Open for the principal, not refused on evidence.

## The negative control, and why the old one expired

**A control has to be chosen against the repair being made.** glgA (`MNXR145046`) and glgP
(`MNXR145036`) served r7 and r8 because they carry no water and therefore could not move
under a water fix. They carry linear glycogen (`MNXM738130`), which is precisely what the
polymer substitution row covers, so under r9 both leave tier 0 — `MNXR145036` to 0.204.
That is the mechanism working. Reading it as a regression would have meant reverting the
row that was built to reach it.

`MNXR145038` is the part of the old control that still holds: it carries *branching*
glycogen (`MNXM8348`), left uncovered on purpose above, and stays tier 0 at 1.000 through
r10 as well.

**r10 needed a third control, because it repairs a different thing again.** A concentration
term moves a reaction the substitution lane already reached, so `MNXR145036` is the
POSITIVE control there and not a negative one: it goes 0.204 → 6.00, and `poc_glgp.py`
re-derives that correction from the pinned table with no reference to the lane and checks it
against the annotation. `MNXR145038` stays the negative control, because a reaction with no
substitution has no restaged equation for a quotient to price.

So a control for a substitution bake must be a reaction the tables **do not cover**, and it
must name which member's uncovered set it comes from — the two differ, and `covers()` now
answers per member.

## Resources

The measured cost of both members is in `SHARD_COST.md`. The short form: the eQuilibrator
lane runs the whole universe unsharded in about 39 minutes on one cpu in 4 GB, and the
declaration that used to ask for 4 cpus / 32 GB / 12 hours was never a measurement.

**Substitutions cost roughly what the sharding saved.** On r9 the eQuilibrator lane took
37 min at 16-wide and dGbyG 20 min at 32-wide, against a plan expecting 4 and 12 — a newly
readable equation is a component-contribution call the previous bake never made, so
coverage and wall clock move together. dGbyG is no longer the critical path; eQuilibrator
is, again, for a different reason than before.
