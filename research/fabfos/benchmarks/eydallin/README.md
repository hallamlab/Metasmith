# Eydallin: can ECSPr see glycogen?

Pilot for a benchmark on Eydallin et al. 2010 (*DNA Research* 17(2):61–71), the
genome-wide screen of genes whose enhanced expression alters glycogen accumulation in
*E. coli*. `run_pilot_glycogen.py` is the driver; `cache/*.json` are the executed solves.

**Result: glycogen cannot rise, for any condition, on this reference basis.** Not a
statement about ECSPr's sensitivity — glycogen is disconnected from central carbon in
the built graph. Read the diagnosis before designing any scoring.

## What the pilot measured

Universal-ground (`measure_leak`) probe, source D-glucose, element C, leak 1e-6, host
`e_coli_k12` (iML1515), tier4 atom pairs + bake direction — the same basis as
`../laser/pilot/run_pilot.py`, so the two are comparable. Base graph: 18,140 nodes /
29,215 edges from 1,997 reactions.

Three conditions, one gene each, perturbation modelled as a **conductance fold-change**
on that gene's reactions:

| probe | base draw | glgC ×2 | glgA ×2 | glgC ×0 |
|---|---|---|---|---|
| D-glucose 6-phosphate | 1.08e-3 | −3.6e-14 | 0 | +1.17e-6 |
| D-glucopyranose 1-phosphate | 1.08e-3 | −3.6e-14 | 0 | +1.17e-6 |
| ADP-alpha-D-glucose | 1.08e-3 | +3.4e-11 | 0 | **absent** |
| **Glycogen** | **−6.5e-13** | **0** | **0** | **0** |
| Branching glycogen | +6.5e-13 | 0 | 0 | 0 |

Glycogen's draw is 7e-10 of the solve total and *negative* — it is leak noise, not flux.

## The diagnosis

Glycogen **is** a node — 24 atom ranks, 42 incident edges — but its only partner
metabolite is Branching glycogen. It is a closed two-metabolite island.

The break is that **glycogen synthase carries no atom pairs**. `MNXR145046` (GLCS1,
glgA) and `MNXR145036` (GLCP, glgP) have *zero* carbon rows in the atom-pair table. The
route glucose → G6P → G1P → ADP-glucose is intact and carries 1.08e-3 at every step, and
then stops: nothing maps carbon into the polymer.

The underlying cause is not missing chemistry, it is **id fragmentation**. The glycogen
module exists twice in MetaNetX under two namespaces, and the two halves are disjoint:

- **BiGG side** — what iML1515 gives us: `MNXR145046`/`MNXR145050` over glycogen
  `MNXM738130` and G1P `MNXM1364212`. In the host, **not atom-mapped**.
- **KEGG side** — what the AAM tier mapped: `MNXR132767` (G1P `MNXM1364214` → glycogen
  `MNXM738131`, 24 carbon rows). Atom-mapped, **not in the host**.

Each half alone is a dead end, which is why `MNXM738131` never becomes a node at all.

`glgA ×2` is the cleanest demonstration: doubling the conductance of glycogen synthase
leaves the graph **bit-identical** — same node count, same edge count, delta 0 on every
probe including the total. Perturbing the gene that makes glycogen is a no-op on the
network. Meanwhile `glgC ×0` behaves exactly as it should — ADP-glucose disappears
entirely, the total falls 1e-6, and G6P/G1P *rise* as flow reroutes. The machinery is
responsive; the glycogen node specifically is unreachable.

This is the same class of finding as the sulfate/S gap in the repo README: a coverage
finding about the AAM tier, not a solver failure.

## Two consequences for the benchmark

**The target cannot be glycogen** until GLCS1/GLCP are atom-mapped or the two id
namespaces are reconciled. Scoring all 86 conditions today would return exactly zero for
every one and read as a uniform method failure that is really a reference-tier gap.

Cheapest real fix is the id reconciliation, not new chemistry: the mapped route into
glycogen already exists at `MNXR132767`, it just names species the host GEM does not
use. Retargeting to **ADP-alpha-D-glucose (`MNXM1105977`)** is the honest fallback — it
is live, on-path, and one step before the break — but it is a near-leaf reachable only
through GLGC, so most of the 86 hits would not move it either. Note this id, not the
`MNXM729838`/`MNXM10599` that `chem_prop` returns for "ADP-glucose"; those have zero
atom-pair coverage. The same trap holds for G1P: `MNXM1364214` is a different entry from
the `MNXM1364212` the host actually carries, and probing the wrong one reads as "G1P is
absent from the graph."

**The overexpression fold-change model yields no signal.** Doubling one reaction's
conductance inside a 29,215-edge network moved ADP-glucose by 3e-11 on a base of 1.08e-3
— a relative change of 3e-8, indistinguishable from numerical noise. Eydallin 2010 is an
ASKA *overexpression* screen, so this is the operation the whole cohort needs. Deletion
is measurable (`glgC ×0` moves the total by 1e-6 and removes a species); doubling is
not. Either the GOF arm needs a different readout — voltage drop across the perturbed
edge, which is what actually says whether a step is rate-limiting — or it is not
measurable under a conductance-delta statistic at all.

## Cohort state

`data/benchmarks/eydallin/` already holds the extraction (86 genes: 28 excess / 58
deficient) but scores nothing: all 86 `mnxr` are null, `Y/expectations.tsv` is empty,
and the study tier labels the cohort `arm=lof` / `n_del=1` — the opposite of the
overexpression perturbation the paper performed. `build_references/transforms/acquire/
bench_eydallin.py` also pins the wrong article (`PMC2900218` is an unrelated ADHD paper;
the real one is `PMC2853380`), so re-acquisition is broken and the extraction survives
only because it is stored as bytes.

## Running it

```bash
docker run --rm -v "$PWD":/ws -w /ws fabfos:local \
    python main/benchmarks/eydallin/run_pilot_glycogen.py --gene glgC --fold 2.0
```

`--fold 0` deletes the gene's reactions (LOF); `--fold >1` is the overexpression model.
Needs `data/processed/metabolism_bake`, `data/originals/metanetx` and
`data/benchmark/reference_tier4` checked out, and both submodules initialised.
