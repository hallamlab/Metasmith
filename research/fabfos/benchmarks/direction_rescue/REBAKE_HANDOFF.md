# Re-baking direction: what not to rediscover

The two ensemble defects are fixed and tested (`T1: water was never a compound here, and
NaN was never a vote`), and `ecspr.bake.direction.forecast` predicts what the members will
do without running them. **The re-bake itself is deferred to the session that reads this.**

What follows is the set of things that will cost a run each if they are found the hard way.
It is ordered by how expensive the mistake is, not by when it happens.

## 1. `DIR_SIGMA_0` must be re-derived, and the combiner will refuse a bad one

`canon.DIR_SIGMA_0 = 9.505` is the reversible-default prior width, and it is the one value
in `canon.py` that is fitted rather than committed — the robust marginal spread
(1.4826·MAD) of measured dG′ on eQuilibrator's reactant-contribution arm, frozen from a
calibration run over 464 measured reactions.

**That basis grows when water returns**, because water-bearing reactions were absent from
the measured arm entirely. Spot re-derivations over the r7 annotation's own measured arm
already land well above the pin — ~18.1 kJ/mol over the 585 non-transport balanced
measured-arm rows, ~17.0 over the 355 of those carrying a curated category. So this is not
a cosmetic refresh: σ₀ sets the shrinkage λ = σ₀²/(σ₀² + s²) on **every** row, and doubling
it moves every ratio in the table.

`combine.py` raises `SystemExit` if `--sigma0` falls outside `DIR_SIGMA_0_BAND = (5.0,
40.0)`. That is deliberate — a value outside the band is a finding, not a constant — but it
means a re-derivation that lands at 45 stops the lane rather than warning. Derive it from
the calibration run's own points table, not from the annotation.

> **The mirror will not be checked for you.** `check_references.check_direction_constants`
> compares `ecspr.bake.direction.canon` against `fabfos.canon` — and `src/fabfos/canon.py`
> no longer exists, having been renamed to `_deprecated_canon.py`, which still carries the
> whole `DIR_*` block at lines 715–737. The import raises, the check emits a `note()` and
> passes. So: update **both** files by hand, and fix the import while you are there, or the
> next divergence is silent in exactly the way that function exists to prevent.

## 2. Every tier-3 ratio moves, and that is correct

All 10,402 `biocyc_only` rows get their ratio from the curated prior, and the prior is
fitted per category on **the eQuilibrator measured arm**. Water-bearing reactions enter
that arm for the first time, so the per-category medians move, so every tier-3 ratio moves
— without any of those reactions gaining a member of its own. Do not read a large tier-3
delta as a bug.

The forecast expects ~3,800 of those 10,402 to leave tier 3 outright by gaining a real
thermo vote.

## 3. The evidence fingerprint will not move on a `direction/` edit

`evidence.buildlib_fingerprint()` hashes `Path(__file__).parent.glob("*.py")`, and
`__file__` is `src/ecspr/bake/evidence.py` — so it covers `bake/*.py` and **not**
`bake/direction/*.py`. Every change this scope made is invisible to it. Two runs of
materially different direction code would write their evidence under one version
directory, which is exactly what that directory exists to prevent.

**Pass an explicit `--version` to `ecspr.bake.evidence collect` for the direction lanes**,
or widen the glob. The former is a flag; the latter changes the fingerprint of every
existing artifact.

## 4. `promote_logs()` runs after `direction_bake` and has no mapper behind it

The HPC driver calls `promote_logs()` for branches `direction_bake` and `reference`. It
rebuilds `logs/` from the AAM cache and the run sandboxes via `aam.runlogs`. **A
direction-only re-bake has no mapper sandboxes**, so it would rewrite the bake's logs from
an empty run and destroy the record r7 wrote. Either stage the AAM evidence alongside, or
suppress the call for this route.

## 5. Three sibling caches are keyed on nothing and must be deleted

```
fabfos/directionality/research/fabfos/benchmarks/eydallin/cache/direction_ratios.parquet
fabfos/bench-eydallin/research/fabfos/benchmarks/eydallin/cache/direction_ratios.parquet
fabfos/nosco/research/fabfos/benchmarks/eydallin/cache/direction_ratios.parquet
```

(and `fabfos/bake/.../cache/metabolism_bake/direction_ratios.parquet`, which at least
names the bake it came from). None of the first three carry a bake identity, so a consumer
reading one after the re-bake gets r7's ratios silently. Delete them as part of the
re-bake, not after somebody notices a number that will not reproduce.

## 6. The deployed-bake test pins will move again

`tests/ecspr/bake/test_deployed_bake.py` was re-derived this session against the promoted
r7 trio (`0ffd4c8c6231696e`): sulfur is 7,833 nodes / 11,078 edges over 26,352 pair rows,
18,142 reactions, 6,613 metabolites. Those run through `ratio_by_code`, so they are
**direction-sensitive as well as topology-sensitive** — a re-bake that changed only the
direction table still moves them. Re-derive and say in the commit message which bake the
new numbers describe; the file's own docstring prescribes exactly that.

## 7. Stage beside, then promote

r7's sequence: build the new chunk beside the deployed one, verify it, then promote. Its
lesson, learned the expensive way, is that **a suffixed chunk is read by nothing** — the
consumers resolve `data/fabfos/processed/metabolism_bake`, so a `metabolism_bake_r8` is
invisible to every benchmark and every test until the promote happens. Verify against the
staged path explicitly; do not assume a green test suite saw the new artifact.

## 8. Resource declarations

The eQuilibrator lane declares `Resources(cpus=4, memory=Size.GB(32),
duration=Duration(hours=12))`. **That is not a measurement** — r7's step log shows it
finished in about 35 minutes. `SHARD_COST.md` has the measured version; the short form:

- **The fix costs no measurable time.** Three post-fix runs of one shard spanned 3:45–4:39,
  so the 20 seconds between the before and after arms is noise.
- **Almost the whole cost is fixed**: 23 s of startup and **all** of the 2.37 GB peak RSS,
  with only ~48 ms per reaction marginal. So memory does not amortise across a fan-out, it
  **multiplies** — 20 shards want ~47 GB, over the declared 32.
- For a sharded lane the declaration is per shard:
  `Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30))` with **10–12
  shards**. Whole universe serially is ~68 minutes, so the 12-hour figure is off by an order
  of magnitude in the safe direction and should simply be corrected.
- **Stagger the launch.** Each process re-hashes the 1.34 GB cache through pooch. It was
  free here against a warm page cache (zero filesystem inputs on the second run) and will
  not be on a cluster.
- `EquilibratorMember` memoises per-compound resolution, and crc32 sharding fragments that
  cache N ways. `forecast resolve` already demonstrates the alternative — resolve all 24,151
  distinct participants once and hand the members a table. Flagged as a restructure, not
  adopted, because the sharded lane is affordable as it stands.

## 9. dGbyG is unvalidated locally

`build-refs-dgbyg` does not exist on this workstation and would need the pinned clone for
the 100 weight heads, so **only the eQuilibrator member was run locally this session**.
dGbyG's behaviour is the predictable one — it answered 25,053 of 25,053 reactions that got
past its guards in r7, and the forecast reproduces its `no_smiles` and `wildcard` counts
exactly — but its post-fix run has not been observed, only predicted.

## 10. A known defect, deliberately left unfixed

`combine.thermo_vote` treats eQuilibrator's structural zeros as authoritative measurements.
**4,814 of tier 1's 5,554 rows have `eq_sigma ≤ 1e-4`** (4,684 of them dG′ exactly 0.0 at
sigma 1e-5), against **740 real measurements**. `calibrate.py` already rejects exactly these
via `SIGMA_FLOOR_KJ = 1e-4`, and documents why: an estimate with no uncertainty carries no
information whatever value it takes, and the cancellation that makes it meaningless is the
same cancellation that makes it look clean.

The combiner does not apply that floor, so those rows enter as measurements at
`S_MEAS_FLOOR = 0.1` and take tier 1. Shrinkage keeps the damage small — it changes the
ratio on roughly 20 reactions — so this is a **provenance-honesty** matter rather than a
coverage one, and it was left alone rather than folded into a change whose effect on the
re-bake would then be unattributable. Fix it in its own commit, before or after, never
during.

## The negative control

glgA (`MNXR145046`) and glgP (`MNXR145036`, `MNXR145038`) — the three reactions the
eydallin cohort is blocked on — **contain no water and gain nothing from any of this.**
Verified on the deployed tables: eQuilibrator `unresolved`, dGbyG `unbalanced` on C and O.
That is the polymer budget, which is a separate defect with a separate fix, and the
`.awm` journal's framing of it as the reason those three carry no direction evidence
remains correct. If they leave tier 0 after the re-bake, something is wrong.
