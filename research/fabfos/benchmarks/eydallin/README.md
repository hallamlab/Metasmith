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

**Verdict, as of the library sweep: ECSPr does not predict this phenotype.** Not as a
regression (closed by Rayleigh monotonicity — the probe is one-sided and the phenotype is
not) and not as a classifier (`REPORT.md`: chance AUC once the glycogen module is struck,
and beaten by reaction count). The network says why: the minimum reaction cut from
D-glucose to glycogen is three, all three are glg reactions, and five times the metabolism
does not add a fourth. The sections below are in the order the question was asked, which is
also the order in which each framing closed.

**One of those closures reopened.** "Regression is closed by Rayleigh" is true of the
two-point probe and not of the method: grounding at the biomass precursors instead of at
glycogen makes the readout a share, shares are signed, and the signed correlation against
Fig. 1 goes from undefined to +0.40. It is n = 23 and p ≈ 0.03–0.07, and it moves no AUC.

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
`Y/expectations.tsv` stays empty and the cohort cannot be scored through it. Every one of
its 86 nulls now has a gene-specific verdict rather than a blank cell, though:
`resolve_gene_manual.py` reads the model's own annotations by two independent joins
(current symbol and b-number), searches it directly for anything either join missed,
confirms the near-misses are transport-excluded by MetaNetX's own flag, and publishes
both `data/fabfos/benchmarks/eydallin/gpr_manual.parquet` (host `e_coli_dh1`) and a
`gpr_manual_report.tsv` beside it naming why. The edges live beside those instead, two
independent readings of the same 86 names:

| | |
|---|---|
| `data/fabfos/runs/eydallin_clones/annotations/` | the clone ORFs as W3110 proteins, plus `clone_resolution.tsv` |
| `data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet` | what iECDH1ME8569_1439 asserts, read directly against `e_coli_dh1` |
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

`bake_pairs.py` decodes the bake into the schema `ecspr.model.build.load_pairs` reads. Handing
that loader the encoded table does not raise — the element filter compares ints to `"C"`
and returns zero rows — so the graph comes back empty rather than obviously wrong. Each
`cache/` entry is stamped with the bake it was decoded from and rebuilds itself when that
moves, so a repin no longer needs anybody to remember to clear it — the stamp pairs the
bake identity with `direction.parquet`'s `src_direction_sha256`, because the identity alone
is unchanged by a direction-only re-bake and would have served the stale table happily.

## Whole-metabolome delta panel — a leak reading, and it moves with host and bake

`delta_panel.py --gene <g> --rxn <MNXR> --fold <f>` runs a base-vs-perturbed universal-leak
solve (source D-glucose) and reports `delta = draw_pert - draw_base` for every metabolite
that became a node; `plot_delta_panel.py` histograms it. `ag1_delta_panel.py` is the same
model on the strain the screen was actually run in, several genes per invocation.

**Read the probe before the number.** `measure_leak` grounds at OMEGA, so `draw[m]` is the
current through *m*'s own leak resistor. Glycogen is never a terminal here and this is not
a glucose → glycogen measurement — for that, see *The two-point probe* below. Nothing in
this section, or in the cohort panel built on the same probe, can answer a question about
flux to glycogen, whatever its direction ratios become.

Glycogen's rank by |delta| under `glgC ×2` moves with both the host and the bake, which is
the reason to distrust it as a specificity read:

| host / basis | glgC ×2 | ddg ×2 |
|---|---|---|
| `e_coli_k12`, pre-r7 bake | 40 / 1027 (top 4%) | 888 / 1027 |
| `e_coli_k12`, r7 bake | 165 / 1041 | — |
| **`e_coli_ag1`, r7 bake** | **975 / 1060 (bottom 8%)** | 288 / 1060 |

On the correct host, glgC — the cohort's strongest measured hit — is *less* specific than
the unrelated-pathway control, so the glgC/ddg pair this file once proposed as
positive/negative controls does not separate there. `pfkA`/`pfkB` are the better negative
control (below). A fold-1.0 run returns bit-exact zero, so these are real numbers rather
than solver jitter; they are just not measuring reachability to the target.

`ddg` is the historic gene-name synonym for `lpxP` (`NC_000913.3.gbk`
`/gene_synonym="ddg; ECK2374"`). iML1515's `feature_name` carries only `lpxP`, so a
gene-name lookup misses it there; the ag1 GEM (iECDH1ME8569_1439) carries `ddg` directly.
Its reaction `MNXR97903` is a host edge at weight 1.0 under either name.

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

**This section's diagnosis was wrong, and the correction is the useful part.** It read
glgA's negative delta as evidence that the fold model cannot tell anabolic from catabolic
when the direction evidence is missing, and it is true that glgA's `MNXR145046` and glgP's
`MNXR145036`/`MNXR145038` sit at an *explicit* ratio = 1.0 from zero ensemble votes —
MetaCyc has no xref row for `MNXR145046`, and the thermo members abstain because MetaNetX
models glycogen as a fixed-formula molecule rather than a polymer increment, so
`reac_prop.tsv` cannot balance it and `dir_combine.py` falls to its empty-votes limit. That
gap is real and worth fixing. It is not what produced the sign. Re-measuring the identical
genes at the identical ratios under a probe that actually grounds at glycogen gives glgA a
*positive* delta (below), so the direction evidence cannot be the cause of a sign the
direction evidence never touched. What produced it was the probe: `cohort_delta_panel.py`
inherits `measure_leak`, and a current through glycogen's own leak resistor is not flux to
glycogen. **Do not re-run this panel to test a direction fix.** No ratio can make an
OMEGA-grounded readout answer this question.

## The two-point probe — the right measurement, and what it can and cannot say

`twopoint_panel.py` (a few genes, both fold directions) and `twopoint_cohort.py` (the 25
cohort genes the curated AG1 GEM can see) ground at glycogen: source D-glucose
`MNXM1364061`, sink glycogen `MNXM738130`, element C, host `e_coli_ag1` on the r7 bake,
base conductance **5.689489**. Overexpression is a ×2 conductance fold on the gene's
reactions, as before.

**Direction is inexpressible under this probe, by a theorem rather than by a gap in the
data.** Effective conductance is non-decreasing in every edge conductance (Rayleigh), so a
fold > 1 can only raise the readout and a fold < 1 can only lower it — verified 8/8 both
ways in `twopoint_panel.py`. A signed correlation against a phenotype that goes both ways
is therefore not weak here; it is undefined. Every number below is a magnitude.

The theorem binds this PROBE, not the method. It bites because the sink IS the target, so KCL
delivers the whole injection there and the conductance is the only readout left. Ground
elsewhere and the readout becomes a share, which is signed — see *The signed probe* below.

What the probe does separate is on-path from off-path, by about five orders of magnitude:

| gene | log2 FC of I_eff under ×2 |
|---|---|
| malP | 0.484 |
| glgP | 0.314 |
| glgA | 0.127 |
| glgC | 0.063 |
| glgB | 0.052 |
| talA | 2.0e-3 |
| pfkA / pfkB (external controls) | 1.9e-6 / 4.5e-7 |

Against the graded phenotype this is still not a regression: |log2FC| measured vs log2FC
I_eff over the 25 is Spearman ρ = +0.31 (p = 0.14). That is the last regression framing this
cohort will get. The gap between glgA and pfkA is a *classification* claim, and the next
section tests it as one.

## The whole ASKA library as the null — the classifier test, and it fails

**See `REPORT.md` for the result; this section is the plumbing.** Short version: beyond the
glycogen module, which the probe finds tautologically, ECSPr does not nominate Eydallin's
genes. Full-library AUC 0.536 curated / 0.464 de-novo, both beaten by reaction count alone;
strike glgA/glgB/glgC/glgP/glgS/malP and the top 25 holds one positive against an
expectation of half of one.

Eydallin screened the entire library, so the ~4,000 clones absent from the paper are
measured negatives rather than unlabelled ones. That is the property this benchmark has and
the others do not, and it is what makes an AUC available at all.

`build_aska_gpr.py --publish` builds the population into `data/fabfos/runs/aska/gpr/` —
4,123 clones over 4,102 gene names from the GFP-minus roster, both channels on the shared
GPR schema — its core plus the attribution, feature, universe and cohort blocks — and one
census row per clone. Neither channel runs an annotator:
an ASKA clone is a chromosomal *E. coli* ORF, so AG1's own curated GPR and its own de-novo
GPR already answer "what reactions does this clone carry". The de-novo side joins through
`NC_017638.1.faa`'s headers, whose first token *is* the de-novo table's `orf`; that
table's `feature_name` is blank for all but 73 of its 4,363 ORFs and must not be joined on,
and its `in_atom_universe` is entirely null and is recomputed here.

Resolution decides the answer's honesty. All 86 positives label, six of them only through
the b-number (aspP/nudF, csrD/yhdA, mlc/dgsA, rutF/ycdH, yifJ/wzxE, ppK/ppk); the
census carries `b_number` and `eydallin_phenotype` so the labels ship with the population.
As a cross-check the curated channel reproduces the cohort panel exactly — the same 25
genes, the same reaction sets, the same deltas.

`sweep_aska.py --channel {gem,denovo}` is `twopoint_cohort.py` with the gene list widened
and nothing else changed. **Each channel folds against its own background** (2,022
atom-mapped reactions curated, 10,998 de-novo): `graph_from_pairs` builds a graph out of
exactly the reactions its weight dict names, so mixing them would let a clone *create* a
reaction its background never had, which is not what a second plasmid copy does. Absolute
conductances are therefore not comparable across channels; only ranks within one are.
Rows append as they finish, so an interrupted sweep resumes. Budget 0.2 s/clone curated and
0.9 s/clone de-novo at four workers — 2.6 and 54 minutes.

`analyse_aska_sweep.py` scores it. `--suffix` reads a variant sweep's table (`_lanes2`,
`_dir2x100`) and writes its own report beside it. Two things it does that a first draft
would not:
the AUC is the mid-rank Mann–Whitney form throughout, because most of the library ties at
exactly zero and a strictly-greater-than count scores every one of those ties as a loss;
and each AUC is printed beside the same AUC computed on reaction count alone, because that
confound is what sank the ASKA/FFA arm and a number that does not beat it carries no
information.

## The anabolic/catabolic ratio over the library — a sign, not a ranking

`sweep_aska_ratio.py` is the same sweep carrying a second two-point solve per clone,
glycogen → pyruvate (`MNXM23`), and scoring the ratio C(glucose→glycogen) /
C(glycogen→pyruvate). Both conductances rise under any fold, so the ratio cancels the part
of the rise that is network-wide; on the 45-condition DH1 panel that took sign agreement
from 10/45 to 30/45. Outputs and `analyse_aska_ratio_sweep.py`'s report land in
`data/fabfos/runs/aska/ecspr/`.

**It does not move the classifier.** Full-library AUC 0.534 curated / 0.489 de-novo against
0.546 / 0.500 for the raw numerator re-run on the same r10 bake, and a reaction-count
control of 0.540 / 0.485 — the same tie for last place, one probe or two. The README's
0.536 / 0.464 are pre-r10 and pre-de-novo-rebuild, so they are not the comparison to quote.

**What the ratio does buy is the sign, and only on the de-novo channel**, where glgA, glgB,
malP, glgP and glgC are the whole top five of 4,102 clones — no null clone reaches any of
their magnitudes — and all five signs match Fig. 1, malP and glgB included. On the curated
channel those two stay wrong-signed (+5.81%, +2.08%) while still sitting at the 99.9th
percentile of the null, so the miss is a direction error rather than a failure to see them.
Across all labelled positives the sign agrees 61% curated / 70% de-novo, neither better than
the deficient-heavy base rate (Fisher p = 1 and 0.6).

## How much was there to find — the network, not the ranking

**See `REPORT.md` § *How much was there to find in the first place*; this section is the
plumbing.** Short version: three routes from D-glucose to glycogen, all three glg, and about
six reactions holding the whole response.

The lever these four scripts share is `ecspr.model.build.reaction_elasticities`. Effective
conductance is homogeneous of degree one in the conductances, so each reaction's
`dlog C_eff / dlog g_r` is its share of the dissipated power and the shares SUM TO 1 — the
measurement partitions rather than merely ranking, and one solve gives the whole partition.
Read the spread, not the top: `1/sum(eps^2)` is the effective number of reactions the probe
can respond to at all.

| script | what it answers |
|---|---|
| `glycogen_cut.py` | the minimum reaction cut to glycogen, on both channels, plus which route carries the arriving carbon |
| `glg_arm_share.py` | how the partition splits across Eydallin's 86, the library's non-hits, and reactions no clone carries |
| `pathway_complexity.py` | the closed form's validation: an exhaustive per-reaction fold sweep plus every single knockout |
| `target_complexity.py` | glycogen's percentile against every other reachable target — the number is meaningless without it |
| `direction_sensitivity.py` | what changes if the direction ensemble stops abstaining on the polymer |

Three traps these hit and a reader would not expect:

- **The cut must be taken UNDIRECTED.** The solve is a resistor network and will push carbon
  through a reaction either way; a cut computed on the written direction understates what
  the probe can reach. It is also why the phosphorylase shows up as a synthesis route at
  all.
- **`sweep_aska.py --ratio-override` is a different measurement, and the filename says so.**
  It tags the sweep `_dirNxT`, because a sweep run under a supplied direction must never be
  confused with one run on the bake's own ensemble.
- **Absolute conductances move under an override** (5.689 → 2.686 at ratio 100), so only
  ranks within one setting are comparable — the same rule the two channels already have.

## The signed probe — grounding somewhere other than the target

**See `REPORT.md` § *Rayleigh binds the readout, not the method* and § *The signed probe on
the real network*; this section is the plumbing.** Short version: Rayleigh binds the
two-point conductance, not ECSPr. Read a SHARE of the injected carbon instead and the
elasticities sum to zero rather than to one, so negative levers must exist — 1,396 of them
here, against 23 positive.

| script | what it answers |
|---|---|
| `monotonicity_ladder.py` | eight toy circuits, from a fork up, that isolate exactly which readout and which topology admit a fall. Every claim is an assertion, so it is a test as much as a demonstration |
| `glycogen_share.py` | the same reading on AG1. `--diagnose` is the grounding × leak grid, `--spectrum` the signed per-reaction elasticity, `--cohort` the 25 genes against Fig. 1, `--scan` the whole grid with a permutation p |

`sweep_aska.py --probe share --ground biomass --leak 1e-3` runs the library under it, and
`analyse_aska_sweep.py --score absdelta --direction` scores it — `--score absdelta` because
under a signed probe "moved glycogen at all" and "moved it up" are different questions, and
`--direction` because "did it move the right WAY" only becomes askable here at all.

Four things that will bite:

- **An alternative path is not an alternative ground.** With one sink KCL delivers the whole
  injection to it, so the delivered current is pinned at 1 and only the conductance varies.
  Rung D exists to make that failure mode impossible to talk past.
- **The leak has to be big enough that a competitor can actually drain.** At 1e-6 — what
  `cohort_delta_panel.py` used — a shunt's elasticity is zero to five decimals. Naming real
  ports (`--ground biomass`) is the fix that does not depend on the leak at all: the answer
  is flat from 1e-6 to 1e-1 there.
- **A fold scales `gp` AND `gm` together**, so re-orienting a reaction changes a lever's
  magnitude and not its sign. Only a reaction whose net current already runs away from
  glycogen can be a negative lever, which is why rectifying the phosphorylase toward
  degradation still leaves it at ε = +0.005.
- **An edge between two members of a merged sink terminal is shorted away by contraction**,
  so the merged-terminal form cannot express an exit from the target. That is why the real
  probe uses `attach_leak`, whose drains are resistors to ground rather than shorts.
