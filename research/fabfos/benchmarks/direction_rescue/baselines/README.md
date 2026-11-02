# The r8 baselines r9 is measured against

Taken here, before any of r9's chemistry touched the tree, because that is the only point
at which an r8 reading is uncontaminated by the fixes. Every file beside this one carries a
`.from` stamp naming the bake it describes — `<vocab_sha256>:<src_direction_sha256>`, the
pairing `bake_identity.py` explains.

**r8 is `0ffd4c8c…:96cc532c…`.**

## The glycogen delivery split

`glycogen_delivery.py`, `e_coli_ag1` GEM background, element C, glucose (`MNXM1364061`) in
and glycogen (`MNXM738130`) out.

| route | r7 (published) | r8 (here) |
|---|---:|---:|
| `MNXR145036` glgP/malP, phosphorylase run backwards | 0.5678 | **0.5342** |
| `MNXR145046` glgA, synthase | 0.3276 | **0.4044** |
| `MNXR145021` glgB/glgX, branching | 0.1046 | **0.0614** |

`fabfos/bench-eydallin` published the r7 column as current. Their decode cache carries no
bake stamp and holds 53,939 reactions at ratio 1.0 against r8's 44,095;
`scan_direction_caches.py` reports it. Re-solving over byte-identical atom pairs and the
same 2,308-reaction background, with only the direction table swapped, reproduces their
published figures to four decimals — so **the whole 0.034 shift off the phosphorylase is
r8, and billing it to r9 would credit one fix with two generations of work.** Reproduce the
attribution with `--against <their cache>`.

The phosphorylase is still the largest feeder, so the finding they reported stands. What
moved is the size of the correction still outstanding.

## The glycogen share

`glycogen_share_baseline.py`, biomass grounding — glycogen plus the AG1 model's 48
carbon-bearing biomass precursors as real ports, everything else draining at `leak=1e-3`.
The quantity `fabfos/bench-eydallin` moved its cohort work onto, because a share can fall
where a two-point conductance provably cannot.

| | r7 | r8 (here) |
|---|---:|---:|
| share of injected carbon reaching glycogen | 0.119216 | **0.140811** |

Same 49 ports, same atom pairs, direction table swapped: **r8 alone raised it 18%**.

**This is one number and not their panel.** `two_ground_panel.py` sweeps folds, ground-B
choices and leak magnitudes to ask whether talA's *sign* survives — a
glycogen-deficient phenotype arising from competition rather than damage. That question
needs their whole instrument and is **not** covered here. What is covered is a before-value
on the quantity all of it rests on, taken while r8 was still deployed.

## The glycogen module under r8

| reaction | tier | ratio | what |
|---|---:|---:|---|
| `MNXR145036` | 0 | 1.000000 | glgP/malP, written G1P → glycogen |
| `MNXR145038` | 0 | 1.000000 | glgP/malP, second accession |
| `MNXR145046` | 0 | 1.000000 | glgA, synthase |
| `MNXR145021` | 2 | 0.088876 | glgB/glgX branching — **no chain-length change, the near-neighbour control** |
| `MNXR145639` | 2 | 0.204402 | maltodextrin ladder n=7 — **the polymer anchor, already scored** |

`MNXR145639` is why the polymer fix's outcome is pre-registered rather than awaited: it is
the same chemistry `MNXR145036` will get, it is already on disk, and it favours elongation.

## The test baseline

"No new skips" needs a skip count to compare against. Three environments, because no one
of them carries rdkit, networkx and the metasmith engine together:

| env | selection | result |
|---|---|---|
| `rdkit-scratch` | `tests/ecspr/bake` | 267 passed, 2 skipped |
| `ecspr` | `tests/ecspr` | 126 passed, 35 skipped |
| `msm` | `tests/fabfos` | 38 passed (5 min) |

`rdkit-scratch` cannot collect `tests/ecspr` at all — no networkx — and `msm` and `ecspr`
cannot run the bake tests, which need rdkit. Run the row that covers what you changed.

## The other two

`aam_v3_nostoc_metabolism_bake.txt` — the N2 probe, unchanged by any direction work and
kept as the invariant. 8,091 reactions used, effective conductance 2.9687155091503.

`rescue_scope.tsv` in the parent directory is the mechanism table, reproduced with
`measure_rescue.py --substitutions none --expect`. Its own header records the bake and the
forecast tables it was computed from.

## r9, now deployed

**r9's own readings are here too**, stamped `0ffd4c8c…:e8f72b8b…` — the pairing that
separates it from r8, since a direction-only re-bake inherits r7's vocab identity byte for
byte. The rescue reading was taken against r9's OWN rebuilt forecast (`--substitutions
src/ecspr/bake/direction`); read against r8's forecast the two disagree about which members
spoke, which is the check working rather than a defect.

The promote renamed the staged chunk onto `metabolism_bake`. The three verifiers were
re-run at that path and reproduce the staged readings exactly — `aam_v3_nostoc` returns the
same effective conductance to the last digit, `measure_rescue --expect` agrees on all ten
rows, and `check_references` passes. Only `S_EDGES` in `test_deployed_bake.py` moved,
11,084 → 11,076, which is the direction-sensitivity that file's docstring predicts;
`S_NODES`, `S_PAIR_ROWS`, `S_REACTIONS_USED` and `S_METABOLITES` all held.

The two glycogen readings were taken after the promote, so they describe the bake every
consumer now resolves:

| route | r7 (published) | r8 | **r9 (deployed)** |
|---|---:|---:|---:|
| `MNXR145036` glgP/malP, phosphorylase run backwards | 0.5678 | 0.5342 | **0.5476** |
| `MNXR145046` glgA, synthase | 0.3276 | 0.4044 | **0.3895** |
| `MNXR145021` glgB/glgX, branching | 0.1046 | 0.0614 | **0.0629** |

| | r7 | r8 | **r9** |
|---|---:|---:|---:|
| share of injected carbon reaching glycogen | 0.119216 | 0.140811 | **0.144764** |

Both move *further* in the direction `fabfos/bench-eydallin` reported as over-fed. That is
the mechanism working: `MNXR145036` left tier 0 for a tier-2 vote at ratio 0.204402, and a
reaction the ensemble had been abstaining on is now one it has an opinion about. The
opinion favours synthesis.
