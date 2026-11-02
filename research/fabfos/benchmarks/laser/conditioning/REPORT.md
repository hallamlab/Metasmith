# Conditioning the ECSPr readout

**The readout is not badly scaled. It is being read over the wrong set.** The response is a
partition with an effective size near ten, and the members carrying it already span one to two
decades. What spans fifteen is the tail that carries the last one percent, reported as if
every member of it were a measurement.

## The complaint, measured

On the 4,102-clone ASKA sweep — two-point glucose → glycogen, fold ×2 — 79.1% of genes return
exactly zero and the nonzero responses run from 10⁻¹⁵·⁵ to 10⁻⁰·³², median 10⁻⁶·⁴³. Nothing
can be regressed against that.

## Where the decades come from

`circuit_bench.py` isolates the candidates on circuits with closed forms.

| family | what it says | decades of spread it creates |
|---|---|---|
| chain | every backbone edge of a length-L chain gets exactly 1/L; the dead-end branches get exactly 0 | **0** — distance costs nothing, and the floor mode is topology |
| fanout | `pair_w` 0.5 against 1.0 halves the level and leaves the spread bit-identical | **0** |
| diode | the throttled edge's share falls one decade per decade of backward ratio; an edge in **series** behind it falls two | **1–2 per decade of ratio**, until it stops at `DIODE_BACKWARD_FLOOR` = 1e-9 |

`decade_budget.py` puts the same knob on the k12 and ag1 curated carbon graphs, three
terminals each. The input census agrees with the circuits: `pair_w` spans 0.301 decades, the
existence conductance `gp` 0.6 between its 5th and 95th percentiles, the per-edge backward
ratio **17.5**, the per-reaction ratio **28.7**.

## Two things the cap sweep settles

**Most of the direction table is inert.** Bounding |log10 ratio| at 6 decades, at 9, and not
at all give the same graph to four significant figures on every axis and both hosts. The
effective conductance saturates earlier still, by 3 — 1.9682 against 1.9630 unbounded on the
glycogen axis, 0.27%. Twenty-five of the table's 28.7 decades change no answer.

**Capping cannot reach the target.** With every ratio at 1.0 the elasticities still span 5.1
decades between the 5th and 95th percentile. That floor is topological and no transform of the
inputs goes below it.

| bound (decades) | C_eff | n_eff | elasticity 5–95 | rxn carrying 90% | their span |
|---|---|---|---|---|---|
| 0 (symmetric) | 8.761 | 6.84 | 5.10 | 13 | 1.60 |
| 1 | 3.354 | 12.71 | 5.88 | 19 | 1.49 |
| 2 | 2.094 | 12.16 | 6.14 | 16 | 1.05 |
| 3 | 1.968 | 10.89 | 6.51 | 13 | 1.03 |
| 6 | 1.963 | 10.82 | 7.15 | 13 | 1.03 |
| unbounded | 1.963 | 10.82 | 7.18 | 13 | 1.03 |

*(ag1, curated, carbon, glucose → glycogen, r9.)*

## What does reach it

Read the response over the set that carries it. Gene-side, over the whole ASKA library:

| reported set | members | span |
|---|---|---|
| the library | 4,102 | **7.7 decades** (5–95) |
| carrying 90% of the response | **8** | **1.00 decade** |
| carrying 99% | 25 | 2.47 decades |

Nothing about the network differs between those rows. `scoring.responders` is that set, and
the complement keeps its own count and mass — a member that leaves has not been shown to be
zero.

## The recommendation

1. **`ecspr.model.scoring.responders(values, coverage=0.90)`** at the reporting seam. This is
   the change that makes the readout regressable, and it is the only one that does.
2. **`ecspr.model.build.cap_direction_ratios(ratios, DIRECTION_DECADE_CAP=3.0)`** at the model
   seam, off by default so a load reports what the table says. It buys 0.7 decades off the
   tail and removes 25 decades that provably change nothing. Three decades is where the level
   stops moving and where the bake's own `DIR_DG_CLAMP` now sits; a test asserts the two
   agree, because a table baked under one bound and read under another fails silently.

## What the bound costs, measured r9 against r9

The committed eydallin numbers are **not** a baseline for this: they report a base
conductance of 5.689489 where the same code path on the pinned bake returns 1.962979. The
difference is r9's direction table, so every comparison below is r9 against r9.

| guard | unbounded | bounded at 3 | control |
|---|---|---|---|
| ASKA classifier AUC, 86 positives | 0.5375 | 0.5380 | size control 0.5409 |
| … glycogen module struck | 0.5107 | 0.5112 | size control 0.5204 |
| eydallin signed share ρ, n=23 | +0.1366 (p=0.53) | +0.1227 (p=0.58) | — |
| glgA route share, τ=1 → 100 | 0.390 → 0.967 | 0.389 → 0.967 | — |
| `tests/ecspr` | 123 passed, 34 skipped | | |

The classifier moves by 0.0005 and stays below its size control either way, which is the
committed finding unchanged. The route reallocation — the instrument's one demonstrated
directional effect — moves by 0.001.

The ASKA arm is scored first order, `(Σ eps_r)·log f` from the one solve that produced the
elasticities. Against exact re-solves on 40 clones: Pearson 0.995, Spearman 0.984.

## Three things found on the way

- **Every committed eydallin number is on a superseded bake.** Base glucose → glycogen
  conductance 5.689489 there, 1.962979 on r9, same code path. The signed share moved with it:
  +0.200 committed, +0.1366 here.
- **`sweep_aska.py` cannot run on this tree.** Four reactions the ASKA clone GPR carries —
  `MNXR145836`, `MNXR146084`, `MNXR152618`, `MNXR152661` (fabA, fabG, fabZ, bioH) — are absent
  from the rebuilt ag1 host GEM, and it refuses rather than let a clone create a reaction its
  background never had. The committed sweep scored all four, so the two tables have drifted
  apart since. The guard is right; the tables need rebuilding together.
- **The `ecspr` env's installed package resolves to `fabfos/nosco/src/ecspr`.** Pin
  `PYTHONPATH="$PWD/src"` or the tests import another worktree's engine.
