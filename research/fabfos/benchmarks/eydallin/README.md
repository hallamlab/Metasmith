# Eydallin: can ECSPr see glycogen?

Pilot for a benchmark on Eydallin et al. 2010 (*DNA Research* 17(2):61–71), the
genome-wide screen of genes whose enhanced expression alters glycogen accumulation in
*E. coli*. `run_pilot_glycogen.py` is the driver; it writes solves to the gitignored
`cache/`, so every number below has to be re-run to be checked.

**Result: glycogen is a live target — since tier4 was retired.** The pilot ran on tier4
atom pairs, where glycogen was disconnected from central carbon and no condition could
move it. On the bake it carries flux and a two-point solve terminates there. The tables
below are the tier4 measurement, kept because the diagnosis is why this cohort stalled,
not because they still describe the basis — and kept *here* because `cache/` is
gitignored, so the solves they came from are gone and only this file records them.

## What the pilot measured

Universal-ground (`measure_leak`) probe, source D-glucose, element C, leak 1e-6, host
`e_coli_k12` (iML1515). Basis was tier4 atom pairs + bake direction, matching
`../laser/pilot/run_pilot.py`; it is now bake for both tables, and that pilot has not
moved yet, so the two are no longer comparable. Base graph 18,140 nodes / 29,215 edges
on tier4, 19,675 / 29,562 on the bake.

Three conditions, one gene each, perturbation modelled as a **conductance fold-change**
on that gene's reactions (**tier4 numbers**):

| probe | base draw | glgC ×2 | glgA ×2 | glgC ×0 |
|---|---|---|---|---|
| D-glucose 6-phosphate | 1.08e-3 | −3.6e-14 | 0 | +1.17e-6 |
| D-glucopyranose 1-phosphate | 1.08e-3 | −3.6e-14 | 0 | +1.17e-6 |
| ADP-alpha-D-glucose | 1.08e-3 | +3.4e-11 | 0 | **absent** |
| **Glycogen** | **−6.5e-13** | **0** | **0** | **0** |
| Branching glycogen | +6.5e-13 | 0 | 0 | 0 |

Glycogen's draw is 7e-10 of the solve total and *negative* — it is leak noise, not flux.

## The diagnosis — and what was tier4's, not the chemistry's

On tier4, glycogen was a node — 24 atom ranks, 42 incident edges — whose only partner
metabolite was Branching glycogen: a closed two-metabolite island. The break was that
**glycogen synthase carried no atom pairs**. `MNXR145046` (GLCS1, glgA) and `MNXR145036`
(GLCP, glgP) had *zero* carbon rows. The route glucose → G6P → G1P → ADP-glucose was
intact and carried 1.08e-3 at every step, then stopped: nothing mapped carbon into the
polymer.

**On the bake those same reactions are mapped** — glgA 21 carbon rows, glgP 12 and 10 —
and every consequence reverses. Glycogen takes G1P as a partner, its incident edge count
goes 42 → 64, its draw goes −6.5e-13 to 1.04e-3, and `glycogen_endpoint.py` returns a
finite glucose → glycogen conductance of 5.82 where tier4 returned a hard `terminals
disconnected`. `reach_to_glycogen.py` puts malP, glgA, glgB and glgP at 0 hops and eight
of the ten carbohydrate hits within 2–6, all through one neck: G6P → G1P → glycogen.

The id fragmentation underneath is real and survives the basis change. The glycogen
module exists twice in MetaNetX under two namespaces:

- **BiGG side** — what iML1515 gives us: `MNXR145046`/`MNXR145050` over glycogen
  `MNXM738130` and G1P `MNXM1364212`. Atom-mapped on the bake, not on tier4.
- **KEGG side** — `MNXR132767` (G1P `MNXM1364214` → glycogen `MNXM738131`). Atom-mapped
  on both, **not in the host** — so `MNXM738131` is still not a node at all.

Reconciling them was the cheapest fix while the BiGG side was unmapped. It is no longer
on the critical path: the BiGG half now works alone, and the KEGG half stays unreachable
either way. Two id traps remain live — ADP-glucose is `MNXM1105977`, not the
`MNXM729838`/`MNXM10599` that `chem_prop` returns for that name, and G1P in the host is
`MNXM1364212`, not `MNXM1364214`. Probing the wrong one reads as "absent from the graph."

## One consequence for the benchmark

**The overexpression fold-change model yields no signal**, and the basis change does not
rescue it. Doubling one reaction's conductance inside a ~29,500-edge network moved
ADP-glucose by 3e-11 on a base of 1.08e-3 on tier4 — 3e-8 relative. On the bake `glgA ×2`
is no longer the bit-identical no-op it was, but it moves glycogen by 2e-9 on a base of
1.04e-3, which is 2e-6 relative: a live edge and still numerical noise. Eydallin 2010 is
an ASKA *overexpression* screen, so this is the operation the whole cohort needs. Deletion
is measurable (`glgC ×0` moves the total by 1e-6 and removes a species); doubling is
not. Either the GOF arm needs a different readout — voltage drop across the perturbed
edge, which is what actually says whether a step is rate-limiting — or it is not
measurable under a conductance-delta statistic at all.

## The phenotype is a figure, not a table

**The screen publishes no raw data table.** Tables 1 and 2 are COG-classified gene lists
split excess/deficient, Supplemental Table 1 is gene → function prose, and the
supplementary index holds one morphotype figure. The 86 measured glycogen contents appear
in exactly one place — Fig. 1's bar chart — and in the PDF that figure is a 952×374
grayscale JPEG, not vector, so there are no drawing operators to read heights off.

`digitize_fig1.py` measures the pixels and writes
`data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv`: 86 rows keyed on `condition_id`,
percentage of WT and the absolute value the caption's WT mean (45 nmol glucose mg
protein⁻¹) implies. **It is the only file in that study folder the study tier does not
produce**, so a real `study_tier` run writes a fresh `Y/` without it; re-run this script
after one.

A bar top is a stroke drawn ON the datum, so the datum is the stroke's centre — reading the
first inked row biases every value up by half a stroke, which is what the first version of
this did. The fit is two smoothed steps a stroke-width apart, with the width, the stroke
level and the blur fitted GLOBALLY across all 86 bars because they belong to the rasteriser
rather than to any bar. Per-bar they are degenerate.

**The precision that matters is not the measurement's.** The 86 fitted heights do not
scatter — they sit on a lattice of 0.6865 px at a concentration (|R| = 0.905) that no
unquantised set reaches, and the y-axis tick spacings sit on the same lattice, alternating
49 and 50 steps as a 49.41-step gap must. The figure in this PDF is a downscaled raster of
a larger one, and the lattice is its native pixel. So the fit is good to ±0.07 points and
**the figure is good to ±0.5** — the source image rounded these values before anyone
digitised them, and no amount of subpixel work narrows that bin. Values are reported
snapped to it, which removes the measurement noise and nothing else.

Bar heights are measured; the bar ORDER is a human reading of the rotated labels, and that
is the part that could be silently wrong. Two checks run on every invocation and refuse
rather than warn: the 86 labels must equal the extraction's 86 genes as a set, and the
bars below WT must be exactly its 58 `glycogen_deficient` genes. A misread name breaks the
first, a one-position slip breaks the second at the boundary. The paper's own text is a
third, unmechanised check and agrees throughout — `glgC` and `glgA` top the collection at
454% and 328%, `glgB` is deficient at 28% despite being anabolic, and `csrA`, `csrD`,
`malP`, `malT`, `mlc`, `glgP` and `aspP` all land where the discussion says they do.

What this buys the cohort is a graded readout where it had a binary one: the 23-strain
tie that sank the ASKA/FFA arm cannot happen here, because these 86 clones span 8–453% of
WT with no two on the same background.

## Cohort state

`data/fabfos/benchmarks/eydallin/` holds the extraction (86 genes: 28 excess / 58 deficient) and
its conditions now say what the paper did: `arm=gof`, `action=add`, one direction per gene
from the paper's own label, read against **`e_coli_ag1`** — the strain the ASKA library
lives in, borrowing DH1's model under a measured genotype edit
(`build_references/check_ag1_identity.py`). Before this it was `arm=lof` / `n_del=1`
against MG1655, which is the opposite perturbation in a strain nobody ran the screen in.
Conditions are **C only**: glycogen is a glucose polymer, so the measured direction is a
carbon claim, and the N/P/S copies asserted three directions nobody measured.

**The study folder's own `gpr_manual.parquet` still carries no reactions, and that is
correct.** It is the curator's reading, and the curator resolved none — so
`Y/expectations.tsv` stays empty and the cohort cannot be scored through it. The edges
live beside it instead, two independent readings of the same 86 names:

| | |
|---|---|
| `data/fabfos/runs/eydallin_clones/annotations/` | the clone ORFs as W3110 proteins, plus `clone_resolution.tsv` |
| `data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet` | what iECDH1ME8569_1439 asserts — `clone_gem_census.tsv` has the per-clone counts |
| `data/fabfos/runs/eydallin_clones/gpr/gpr_denovo.parquet` | what the four annotation lanes infer — `BUILD_denovo.json` has the lane set and the counts |

The lanes see several times the clones the model does, and both tables carry
`in_atom_universe` against the same bake so the two are comparable row for row. A scoring
run reads these; nothing rewrites the study folder to hold them, because the study tier
checks that folder holds exactly five names.

`build_references/transforms/acquire/bench_eydallin.py` still pins the wrong article
(`PMC2900218` is an unrelated ADHD paper; the real one is `PMC2853380`), so re-acquisition
is broken and the extraction survives only because it is stored as bytes.

## Running it

```bash
docker run --rm -v "$PWD":/ws -w /ws fabfos:local \
    python main/benchmarks/eydallin/run_pilot_glycogen.py --gene glgC --fold 2.0
```

`--fold 0` deletes the gene's reactions (LOF); `--fold >1` is the overexpression model.
`glycogen_endpoint.py` grounds at glycogen instead of leaking universally;
`reach_to_glycogen.py` is the hop table. All three need `data/fabfos/processed/metabolism_bake`
and `data/fabfos/originals/metanetx` checked out, and both submodules initialised.

`digitize_fig1.py` needs none of that — only the acquisition chunk and an env with pypdf
and Pillow: `mamba run -n figure-net python main/benchmarks/eydallin/digitize_fig1.py`.

`bake_pairs.py` decodes the bake into the schema `ecspr.build.load_pairs` reads. Handing
that loader the encoded table does not raise — the element filter compares ints to `"C"`
and returns zero rows — so the graph comes back empty rather than obviously wrong. Clear
`cache/*.parquet` after a bake repin.

## Whole-metabolome delta panel — ECSPr has real discriminating power here

`delta_panel.py --gene <g> --rxn <MNXR> --fold <f>` runs a base-vs-perturbed universal-leak
solve (source D-glucose) and reports `delta = draw_pert - draw_base` for every metabolite
that became a node, not just glycogen; `plot_delta_panel.py` histograms it with glycogen
marked. Two runs on the bake basis, host `e_coli_k12`, fold ×2:

| perturbed gene | reaction | hops from glycogen | glycogen's rank by \|delta\| |
|---|---|---|---|
| `glgC` (ADP-glucose pyrophosphorylase, the committed step) | `MNXR145050` | 1 | **40 / 1027** (top 4%) |
| `ddg` = `lpxP` (lipid A acyltransferase, unrelated pathway) | `MNXR97903` | unreachable in this neighbourhood | 888 / 1027 (bottom 14%) |

`ddg` is the historic gene-name synonym for `lpxP` (`NC_000913.3.gbk`
`/gene_synonym="ddg; ECK2374"`) — the host GEM's `feature_name` column only carries
`lpxP`, so a gene-name lookup alone misses it, but its reaction is already a host edge at
weight 1.0. This is the pair to reuse as positive/negative controls for any future
perturbation on this cohort.

`voltage_vs_minpath.py` / `plot_voltage_hist.py` probe the same base graph's per-metabolite
voltage. Under `attach_leak`'s uniform-per-metabolite leak, ~93% of metabolites sit within
a hair of one ceiling regardless of hop-distance from the source (Spearman ρ≈0.03 against
minpath, not significant) — the leak resistance dominates internal network resistance for
any well-connected node, so voltage here reads connectivity CLASS, not topological
distance. `leak_sweep.py` swept leak over 1e-8..1e4 (12 orders of magnitude) looking for a
window where drop-from-source tracks hop distance instead: there isn't one. The network
transitions almost directly from near-zero saturation (step-blind, ~93-94% pinned near
source potential) to near-max saturation (~94-100% collapsed to ground) with no
distance-tracking regime between them — a crossover, not a window, and confirmed on actual
current draw too, not just voltage. This is a structural property (massive parallelism
folds effective resistance over all paths; the uniform leak gives every node an equal local
escape route, so `dV`/power is dominated by which edges on a path happen to be weak, not by
hop count — the same hub-inversion already refuted for `dV`/power distance). No leak
retuning fixes it. The project's own prior work already names the sanctioned alternative
for a point-to-point question: the two-point resistance solve, not a universal ground.

## Cohort-wide delta panel — no signal, and a two-part diagnosis why

`cohort_delta_panel.py` extends the glgC/ddg pilot to every condition whose gene already
has a reaction in the curated host GEM's atom universe (`gpr_gem.parquet`, channel
`gem_gpr`) — 25 of the cohort's 86 — folding each gene's reaction(s) ×2 and correlating
glycogen's delta against `Y/measured_glycogen.tsv`. **Spearman ρ = +0.21 (p=0.31, n=25) —
not significant.** The two strongest measured hits point in opposite directions: `glgC`
(453% WT) shows the expected positive delta, but `glgA` (328% WT, one bond closer to the
product) shows the *largest-magnitude negative* delta in the cohort.

Two independent, verified causes, not noise:

1. **The GOF fold model can't distinguish anabolic from catabolic edges when the direction
   evidence is absent.** Conductance IS directional in this graph in general (`gp = E_r *
   pair_w` forward, `gm = ratio * gp` reverse, throttle-only by construction — see
   glgC's `MNXR145050` at ratio 27.9 and glgB's `MNXR145021` at 0.154, both genuinely
   asymmetric). But glgA's reaction (`MNXR145046`) and glgP's (`MNXR145036`/`MNXR145038`,
   shared with `malP` as an isozyme call in this GEM's GPR) each sit at an *explicit*
   ratio=1.0 — traced through `build_references/transforms/bake/direction_ensemble.py` to
   zero votes from every member: MetaCyc's xref table has no `metacyc.reaction:` row for
   `MNXR145046` at all, and the thermo members (eQuilibrator, dGbyG) abstain because
   MetaNetX represents glycogen as one fixed-formula molecule rather than a polymer
   increment, which fails their mass-balance check (`reac_prop.tsv`'s `is_balanced` column
   is empty for this reaction). Zero votes hits the fusion rule's own limit
   (`dir_combine.py`: empty `votes` → `mu_post=0` → `ratio=exp(0/RT)=1.0`), not a computed
   near-equilibrium estimate. So overexpressing glgA (anabolic) and glgP (catabolic) get
   the identical operation — "widen this pipe both ways" — with nothing in the graph
   saying which direction favours accumulation.
2. **Even a correctly-anabolic, directly-incident edge isn't guaranteed the right sign.**
   glgA's edge touches glycogen directly and is unambiguously synthetic biology, yet its
   modelled delta is still negative — the same non-monotonic conductance→draw behaviour
   the leak sweep above already diagnosed: strengthening one edge into a well-connected
   node can make that node more of a through-path than a trap under a universal leak,
   independent of whether the edge "should" be anabolic.

Net: this cohort's GOF arm is not fixable by leak tuning (closed above) or by a bigger
fold (noise-floor was already the wrong diagnosis for glgA/glgP specifically — the sign
itself is unreliable, not just small). A signed/directed perturbation model or the
point-to-point resistance readout are the two live directions; a bigger fold on the same
architecture is not.
