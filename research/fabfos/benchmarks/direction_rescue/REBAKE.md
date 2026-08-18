# Re-baking direction

r8 is the deployed direction table. This is the protocol that produced it and the set of
things that cost a run each when found the hard way. The artifact itself — the trio, its
identity block, what `src_direction_sha256` is for — is documented at
`src/fabfos/build_references/REFERENCES.md` § R6, and is not repeated here.

## Where the lane stands

Three defects in how the ensemble read its own inputs are fixed and baked. Water was
filtered out of the compound table, so both thermodynamic members abstained on 36% of the
universe before attempting any chemistry. `dir_method` named members that had arrived as
`NaN` through a left merge, where `NaN is not None`. And eQuilibrator's group cancellations
— dG′ = 0 at the sigma floor, a statement about the equation rather than a measurement of
it — were being promoted to tier 1.

Tier 0 fell 47,266 → 37,404 with nothing losing a vote; tier 1 is 2,171 rows, none at the
floor. What is left of the gap is a carrier-curation problem, measured in `README.md`.

## The sequence

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
`canon.py` and `_deprecated_canon.py`; `check_direction_constants` compares them.

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
large tier-3 delta as a bug.

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

## Two decisions r9 deliberately does not carry

**Branching glycogen stays out**, and it costs exactly three tier-0 reactions --
`MNXR136341`, `MNXR145038`, `MNXR145039`. 411 MetaNetX compounds carry the acceptor
formula `C18H32O16` and at least three are defensible branched alpha-glucans (panose
`MNXM1104683`, isomaltotriose `MNXM1104226`/`MNXM1106015`, 6-O-glucosylmaltose
`MNXM1107398`). No gate separates them, so authoring a row would be choosing one by hand
and calling it a lookup. Open for the principal, not refused on evidence.

**Calibrate's stale balance gate is deferred to r10.** `calibrate.py:85` returns
`unbalanced` from raw `reac_prop` BEFORE consulting the member, discarding 479 reactions
the member balanced after restaging. Sigma_0 is 23.489 either way, because the committed
fit is the unsubstituted subset -- so the constant is settled and only the bins move
(`PHYSIOL-LEFT-TO-RIGHT` tau 98.27 -> 112.18). Fixing it inside r9 would make every ratio a
mix of chemistry and calibration change and cost the attribution the four-way pricing was
built to give.

## The negative control

glgA (`MNXR145046`) and glgP (`MNXR145036`, `MNXR145038`) contain no water and gain nothing
from any of this — eQuilibrator-`unresolved`, dGbyG-`unbalanced`. They are the polymer
budget, a separate defect with a separate fix. If they leave tier 0 after a direction
re-bake, something is wrong.

## Resources

The measured cost of both members is in `SHARD_COST.md`. The short form: the eQuilibrator
lane runs the whole universe unsharded in about 39 minutes on one cpu in 4 GB, and the
declaration that used to ask for 4 cpus / 32 GB / 12 hours was never a measurement.
