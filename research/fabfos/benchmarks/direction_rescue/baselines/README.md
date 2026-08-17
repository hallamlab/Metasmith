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

## The other two

`aam_v3_nostoc_metabolism_bake.txt` — the N2 probe, unchanged by any direction work and
kept as the invariant. 8,091 reactions used, effective conductance 2.9687155091503.

`rescue_scope.tsv` in the parent directory is the mechanism table, reproduced with
`measure_rescue.py --substitutions none --expect`. Its own header records the bake and the
forecast tables it was computed from.
