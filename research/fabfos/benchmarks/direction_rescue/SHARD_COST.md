# What the eQuilibrator member costs, before and after the water fix

The lane used to declare `Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=12))`
and **none of that was a measurement** — r7's step log shows it finished in about 35 minutes,
and it was about to stop short-circuiting on roughly 14,000 reactions it had been refusing
for free. This is the measurement that replaced it. The declaration now reads
`cpus=1, GB(4), hours=3`, and r8's run settled it: **38:58 for the whole universe,
unsharded**, against the sixteen-fold walltime and eight-fold memory the old one reserved.

Reproduce with `shard_cost.sh <work-dir> [i/n]`. Shard 0/20 is 4,218 of the 83,795
reactions; crc32 sharding makes it a uniform sample, so the histogram extrapolates.

## The two arms

Same shard, same cache, same everything except `refdata.load_mnxm_props` — the `before`
tree is this scope's `src/ecspr` with that one file taken from `f26fe10`, the last commit
before the fix.

| shard 0/20 · 4,218 reactions | before | after | delta |
|---|---:|---:|---:|
| `no_props` | 2,446 | 1,988 | **−458** |
| `unresolved` | 954 | 979 | +25 |
| `uninformative` | 133 | 227 | +94 |
| **`ok`** | **685** | **1,024** | **+339** |
| answered | 16.2% | 24.3% | **+8.1 pts** |
| wall clock | 4:05.8 | 3:45.4 | −20 s |
| peak RSS | 2.372 GB | 2.370 GB | −2 MB |

`no_props` collapses and `ok` rises by half again, which is the fix doing exactly what it
was supposed to. `unresolved` and `uninformative` both grow, and that is the fix too: a
reaction that used to stop at water now walks past it and reaches whichever later
participant eQuilibrator's frozen cache does not hold, or gets a number with a degenerate
sigma. Extrapolated over the universe that is about **+6,700 reactions eQuilibrator
answers**, against the forecast's +7,832 predicted `expected_ok` — the difference being
that `expected_ok` includes the `uninformative` outcome the forecast declines to predict.

## The fix does not measurably cost time

The `after` arm ran *faster*, which is not a result — it is noise. Three post-fix runs of
this same shard came in at **3:45, 4:20 and 4:39**, a 24% spread, so a 20-second gap between
arms says nothing. The honest statement is that the water fix's effect on this lane's wall
clock is **below the run-to-run variance on identical inputs**.

That is less surprising than it looks. The extra work is not free, but it is small next to
what the step pays regardless.

## Almost all of the cost is fixed, and that is what sets the shard count

Measured on a 39-reaction shard (`0/2000`), which isolates the startup:

| | fixed (39-reaction shard) | 4,218-reaction shard | marginal |
|---|---:|---:|---:|
| wall clock | 22.9 s | 3:45 – 4:39 | **~48 ms / reaction** |
| peak RSS | **2.372 GB** | 2.370 GB | **~0** |

**The 2.37 GB is entirely fixed** — the parsed `chem_prop` props dictionary plus
component-contribution's preprocessor matrices — and it does not grow with the reaction
count. Which means it does not amortise across a fan-out: it **multiplies** by the shard
count.

- **Serial, whole universe:** 23 s + 83,795 × 48 ms ≈ **68 minutes**, at 2.4 GB. Consistent
  with r7's ~35 minutes on faster hardware with 4 cpus.
- **20 shards:** ~3.6 minutes each, but **20 × 2.37 GB ≈ 47 GB concurrent** — over the
  declared 32 GB. Twelve shards is about the ceiling that declaration allows, and ten leaves
  headroom.
- Sharding also duplicates the 23-second startup, so 20 shards spend **7.7 minutes of pure
  duplicated setup** to save an hour of chemistry. Still worth it; just not free.

### The declaration this implies — and what was adopted

For a **sharded** eQuilibrator lane the resources would be per shard, not per universe:
`Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30))` with 10–12 shards.

**That restructure was not adopted, and the r8 run is why.** The lane kept r7's unsharded
shape with the declaration corrected to `cpus=1, GB(4), hours=3`, and finished the whole
universe in **38:58** — comfortably inside the extrapolation above, on one cpu, at a peak
the 4 GB covers. Sharding buys about half an hour and costs the duplicated startup, the
fragmented compound cache and a staggered launch; at 39 minutes there is nothing to buy.
One cpu because the member is single-threaded and `OMP_NUM_THREADS=1` is already set.

**Stagger the launch.** Each process re-hashes the 1.34 GB cache through pooch on
construction. That was free here — the second run reported *zero* filesystem inputs against
a warm page cache — and it will not be on a cluster, where N simultaneous shards means N
cold 1.34 GB reads and the first measurement is of disk rather than of chemistry.

## The restructure this measurement argues for

`EquilibratorMember` memoises compound resolution per process, so crc32 sharding fragments
that cache N ways and every shard re-resolves the compounds it shares with the others.
`forecast resolve` already demonstrates the alternative: resolve all 24,151 distinct
participants once (~25 minutes, 2.2 GB) and hand the members a table. That would make the
per-shard cost nearly pure arithmetic and remove the duplication entirely.

**Flagged, not adopted.** Doing it mid-prototype would change what the before/after arms
above are comparing, and the numbers say the sharded lane is affordable as it stands.

## The assembly step, and why it can be measured without a cluster

`direction_ensemble` declared `cpus=4, GB(32), hours=2` beside a comment reading
"Minutes." It now reads `cpus=1, GB(4), minutes=30`, and this is the measurement.

Every input the step takes is on disk from r8 — the two member seams in the bake, the
MetaCyc `reactions.dat`, MetaNetX — and none of the three commands imports heavy
chemistry, so the whole step runs locally in the `msm` env. It reproduces r8 exactly:
the calibration and its 17,184 points frame-identical to the deployed ones, and
`direction_annotation.parquet` frame-identical to the deployed seam, whose own sha256
is `src_direction_sha256`.

| command | wall clock | peak RSS |
|---|---:|---:|
| `direction.curated` | 9.4 s | 224 MB |
| `direction.calibrate` | 3.6 s | 252 MB |
| `direction.combine` | 2.3 s | 354 MB |

Single-threaded, because the lane sets `OMP_NUM_THREADS=1` itself.

**That the assembly reproduces r8 locally is worth more than the resource number.** It
means the calibration, σ₀ and combine arms of a re-bake can be re-run and compared off
the cluster, one variable at a time, against member tables the cluster produced.

## What was not measured

**dGbyG was not run locally.** `build-refs-dgbyg` does not exist on this workstation and
would need the env plus the pinned clone for the 100 weight heads. Its behaviour is the
predictable arm — it answered 25,053 of 25,053 reactions that got past its guards in r7, and
the forecast reproduces its `no_smiles` and `wildcard` counts exactly — but its post-fix run
has been predicted and not observed. Its own lane docstring justifies 20 shards with a
measurement (0.249 s alone, 0.286 s under sixteen-way contention, 2.15 GB per process that
does not grow with concurrency), which is the standard this file is trying to meet.

## The forecast, checked against the tool it predicts

The `after` shard is a post-fix run of the real member that no forecast had seen. Scored
against the post-fix forecast built from table reads alone:

| predicted \ actual | expected_ok | no_props | unresolved | uninformative |
|---|---:|---:|---:|---:|
| `expected_ok` | 1,024 | 0 | 0 | 227 |
| `no_props` | 0 | **1,988** | 0 | 0 |
| `unresolved` | 0 | 0 | **979** | 0 |

3,991 of 4,218 exact (94.62%), **zero** reactions predicted silent that spoke, and every one
of the 227 disagreements is `uninformative` — the one mechanism the lane declares it cannot
predict from tables. This is the claim that matters: the forecast predicts a *future* run,
not just the deployed one it was backtested against.
