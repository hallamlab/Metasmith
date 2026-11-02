# Would ECSPr find Fang's rfaY?

**No — and the two arms of this lane fail in the same way, which is the finding rather than
a coincidence.** Fang et al. screened the ASKA overexpression library for free-fatty-acid
overproduction and found `rfaY`, the LPS core heptose II kinase, as its strongest single
target. This lane asked twice whether an atom-resolved conductance can see that.

**Arm 1 — the glycerol → FFA panel** (§ *The glycerol → FFA panel arm*) ran first: one
two-point probe from glycerol to the merged fatty-acid readout, over the 87 conditions the
paper measured, scored against a size-matched null. The score carries no information about
which clone raised the titer — size-matched AUC 0.529, ρ = +0.152 against fold change — and
a great deal about how many reactions the clone contributes, ρ = **+0.69**. Its sharpest
result is not statistical: **23 conditions return the identical delta, 2.809 × 10⁻⁵, while
their measured titers span 303 to 5,736 mg/L.** In the model those strains are not similar,
they are *the same condition*.

That arm ended with a diagnosis: conductance to a product is the wrong observable, because
the paper's own conclusion is that its winners act through *membrane homeostasis* rather
than through carbon supply. **Arm 2 was built to take that diagnosis seriously** (§ *The
ratio arm*): score a two-point conductance **ratio** between competing fates, and ground the
numerator at the LPS core itself — the thing rfaY actually makes — instead of at the fatty
acid the GC measured.

It works, and then it does not. Ranked by C(acetyl-CoA → LPS inner core) / C(acetyl-CoA →
CO₂) across the whole 4,102-clone roster, ECSPr puts **rfaY at rank 15 of 4,102** — the
99.66th percentile of the non-hit clones, two positives in the top 50 against an expectation
of 0.1 (*p* = 0.005), atom-mapped AUC **0.728** against a reaction-count size control of
0.319. That is the first axis in this campaign to beat its size control while nominating a
paper's headline gene. Three controls, all declared before any number existed, take it apart:

1. **Move the sink one step upstream and rfaY collapses.** The declared size control is
   `inner_core_waaQ` — rfaY's own *substrate*, C₁₃₁ on both sides, differing from the sink by
   exactly RfaY's phosphate. On it rfaY falls from rank 15 to **701 of 4,102**
   (Δ = −6.7 × 10⁻⁵%), while `waaQ`, the enzyme that makes *that* metabolite, climbs from 14
   to **9**. The model ranks whichever gene catalyses the last edge into whatever terminal
   was declared.
2. **rfaY is indistinguishable from a gene the assay calls flat.** `rfaY` moves the ratio
   +1.781707% and `waaQ` +1.781775% — differing in the fifth decimal, ranking 15th and 14th,
   with waaQ *above*. Fang measured rfaY at 2461.3 mg/L and waaQ at 724.0 mg/L against the
   same 799.6 mg/L control. One rung down, `waaF` (+0.5635859%, up at 1172.2 mg/L) and
   `waaP` (+0.5635220%, **down** at 431.5 mg/L) are a matched pair the model cannot separate.
3. **Strike the LPS core module and the ranking falls below chance.** 34 module clones
   removed, five of them positive; the four remaining positives have **no atom-mapped
   reaction at all**, and the library AUC drops to 0.388 against a size control of 0.383.

**The axis that measured what the GC measured says nothing**, exactly as arm 1 found. On
hexadecanoate rfaY ranks **717 of 4,102**, within-hit AUC 0.442 against a size control of
0.449; phosphatidate agrees at rank 714. So the LPS axes did say something the FFA axis did
not — that rfaY is an LPS inner-core enzyme, which was the premise of choosing the sink, not
a finding.

**Both arms are the same failure seen from two sides.** Arm 1 could not separate strains
whose atom-mapped networks are literally identical. Arm 2 could not separate rfaY from
waaQ, or waaF from waaP, because each pair sits one edge apart on the route into the
declared sink. Changing the observable from a product to a ratio, and the sink from the
assay's readout to the mechanism's, did not fix it — it only made the confound nameable:
**declared-sink adjacency**, and measurable, by a control that differs from the primary sink
by one phosphate group and no carbon at all.

This is a negative result about the method, not about the study. The screen is sound, the
extraction reproduces the paper's own arithmetic, and the controls are the ones designed to
break the claim.

## What was measured

Fang et al. 2025 screened the **entire ASKA overexpression library** in *E. coli*
MG1655(DE3) ΔfadE carrying `pF` (P_T7:`tesA'`), sorted the top 0.1% of Nile-Red-stained
cells, sequenced the sorted pool, and rebuilt the 24 highest-read ORFs individually. A
second round repeated the whole procedure on `pRF` (`tesA'` + `rfaY`), and a 14-gene LPS
panel was added by hypothesis rather than by the screen.

### The population question, and what it licenses

**The paper supplies no per-gene enrichment score.** Only the sorted pool was sequenced;
Figs. 1b and 4b show a read-count scatter with the top 24 marked in red and no table stands
behind it; the supplementary carries strain, plasmid and NGS-primer tables and nothing else;
the data statement is *"Data will be made available on request."* The **hits-only** case
holds, so there is no measured null distribution to threshold against.

Two statistics are therefore reported, and they are not interchangeable:

| | population | what it assumes |
|---|---|---|
| **within-hit ranking** | the 58 genes Fang rebuilt and put through GC, 9 of which raised the titer | nothing — every gene in it was measured |
| roster-wide AUC | all 4,102 roster clones, binary label | that an unsequenced clone is a true negative |

Roster-wide numbers are quoted throughout because they are the only ones with a usable
sample size, and they are never quoted without this row. They are weaker than eydallin's
identical-looking figures, where each of ~4,000 non-hits was individually built, stained and
scored. `--assayed-only` is the switch between the two and it is not cosmetic here.

### The cohort

59 genes were rebuilt as single clones and assayed by GC, and **all 59 resolve to a W3110
protein**. The ASKA inserts were PCR-amplified from W3110, which Fang's own Methods confirm
from the other end by aligning the screen's reads to `AP009048.1`. The expression host is
MG1655(DE3), and **iML1515 is MG1655's own curated model rather than a proxy** — which is
where this arm differs from eydallin's, which had to borrow DH1's GEM for AG1.

`rfaY` resolves to `AB6907_RS05245`, *lipopolysaccharide core heptose(II) kinase RfaY*.

The two rounds used different controls — F0 = 799.6 mg/L for round 1 and the LPS panel, RF =
2240.3 mg/L for round 2 — so they are two tables, `gof.csv` (40 genes) and `gof_round2.csv`
(24 genes), each internally normalised against its own baseline. Five genes appear in both
because they were assayed on both backgrounds: `yafL` is flat at 853.2 mg/L alone and up 54%
at 3447.6 mg/L on rfaY, and collapsing that pair to one row deletes either the paper's
second-round headline or the control that makes it interesting.

Titers are digitised once, by `parse/digitise_ffa.py` and `parse/build_extraction.py`, off
the figure bars against the four titers the paper states in prose (F0 799.6, rfaY 2461.3,
RF 2240.3, rfaY-yafL 3447.6). `data/fabfos/benchmarks/fang/extraction.tsv` is the primary
artifact and both arms read it; nothing regenerates it.

### Reaction chemistry

| | `gof.csv` (40) | `gof_round2.csv` (24) |
|---|---|---|
| curated GEM (`iML1515`) | 27 | 12 |
| LLM review | **0** | **0** |
| de-novo gap-fill | **0** | **0** |
| no reaction, categorised | 13 | 12 |

**Both gap-fill channels are empty, and that is a finding rather than an omission.** The
eydallin arm promoted six genes by hand review and four by de-novo agreement. Here the 25
uncovered rows are regulators, transporters, envelope machinery and DUF proteins with no
characterised reaction, and no uncharacterised gene had ≥2 independent de-novo methods land
on one reaction. The one candidate that looked promotable, `opgD`, was checked and rejected:
MetaNetX carries the osmoregulated-periplasmic-glucan neighbourhood, but every reaction in it
belongs to OpgB/OpgE's phosphoglycerol and phosphoethanolamine transfers rather than to
OpgD's branching step, and the polymer's chain length is undefined on both sides.

**Only 9 of the 59 genes carry a reaction that changes a carbon skeleton** — `lpxH`, `lpxM`,
`tesA`, `waaC`, `waaF`, `waaG`, `waaQ`, `folE`, `lpxD`. Of Fang's nine beneficial targets
only **`waaF`** is among them. `rfaY` itself is `no_change`: `MNXR138512` transfers a
phosphate. That is precisely why the primary axis had to be defined by rfaY's *product*
rather than by its reaction, and it is read off the baked atom map's connected components,
never off the equation string, which would have called ATP → ADP a change.

Two gaps are recorded rather than papered over. `nepI` — one of the four round-1 strains
that beat F0 by >50% — **is not on the ASKA roster this tree carries** under any name or
b-number, so it has a `gof.csv` row and no clone in the sweep. And `MNXR100900`, the
LPS-transport reaction iML1515 assigns to all four of `lptA/B/D/E`, is **absent from
`reactions.parquet`**, so those four rows carry an `mnxr` and no equation.

## The glycerol → FFA panel arm

The arm that ran first, kept whole. It asks the question the assay asks: does the
atom-resolved conductance from glycerol to free fatty acid track the titer over the 87
conditions Fang measured? It was scored against a size-matched null drawn once, and it
is null. Its diagnosis is what the ratio arm below was built to act on.

### The four numbers

Numbers below are the two-point probe at the merged fatty-acid readout, scored
against the size-matched null. Regenerate with `sweeps/analyse_ffa_panel.py`; the tables under `out/`
beside this file are the source and this prose is not.

**1. The gate passes, and decisively.** All 33 control conditions — masks that
reach no reaction the basis can carry an edge for — return the baseline
*bit-for-bit*. `control_sd` and `control_max_abs` are exactly `0`. The solver's
numerical floor is therefore zero, and any non-zero null spread clears it, so
`null_over_control` is undefined by division rather than by weakness. Every z
below is measured against a real floor.

**2. Reach is good, and the 2×2 is not anti-correlated.** Of 87 measured
conditions, ECSPr can move 58; 29 are invisible to it. Among single-clone
conditions the fraction carrying an atom-mapped edge is 36.0% for the strains that
moved the phenotype and 36.8% for those that did not — the method's reach is
independent of the phenotype, which is the precondition for any score afterwards
to mean anything. Only 3 of the 13 increases the paper scores are invisible. The
study is not the degenerate case where everything interesting is off-model.

**3. The score does not track the titer.** Over the 58 conditions ECSPr can move:

| | ρ vs fold change | AUC, increases vs rest | ρ vs reaction count |
|---|---|---|---|
| global `z` | +0.009 | 0.424 | +0.693 |
| size-matched `z_stratum` | +0.152 | 0.529 | +0.190 |

Neither separates the strains the paper scores as an increase from the rest.
Size matching does what it is for — the reaction-count correlation drops from
+0.69 to +0.19 — and what it leaves behind is an AUC of 0.529, which is chance.
The ρ of +0.152 is not nothing, but with 10 positives in 58 it is comfortably
inside what noise produces.

**A note on which of these the null earns.** The global z is an *affine* transform
of the delta: every condition at a given readout is divided by the same null mean
and sd. So any rank statistic computed on global z is identical to the same
statistic on the raw delta, and drawing more of the null cannot change it — which
is exactly what was observed, ρ and AUC unmoved to three decimals from 664 draws
to 3,000. The null earns its cost on `z_stratum`, where each condition is
standardised against draws of its own size, and on the empirical p. It is not
doing work in the global ranking, and reporting it as though it were would
overstate what the pool bought.

**4. The score tracks reaction count instead.** ρ between global z and the number
of reactions the clone contributes is **+0.69**. That is what the method is
measuring. Under a two-point probe Rayleigh monotonicity guarantees any addition
raises the effective conductance, so this is not a bug — it is the probe behaving
exactly as specified. The question was whether it carries anything *else*, and
after size matching removes most of it, what remains does not separate the hits.

**5. The `ground` probe agrees, and does not provide an independent check.** It
was run because it answers a different question in principle: under universal
leakage the draw sums to one exactly, so a perturbation redistributes rather than
lifts, which should remove the size effect that dominates the two-point ranking.
It does not, here. Every statistic lands within 0.005 of its two-point twin
(ρ +0.006, AUC 0.422, size ρ +0.695), and the two probes' deltas rank the 91
conditions at **Spearman +0.9999**, with the ground delta a constant 0.1317× the
two-point delta across the interquartile range.

The reason is a choice in this study's conditions table rather than a property of
the probe: the fatty-acid species are named as `sink_hub`, which under `ground`
gives them a PORT to ground rather than a leak, so the readout is close to a
rescaled two-point conductance. A genuinely independent ground reading would name
biomass precursors as the ports and the fatty acids as readouts only — the package
keeps `sink_hub` and `readout_hub` in separate columns precisely so that is
expressible. That variant was not run and is the obvious next thing to try if
anyone wants to revisit this.

### The one table that shows why

Hold reaction count fixed at one, so the added conductance is identical by
construction and only *which* reaction differs. Among those 33 clones, ρ between
delta and fold change is **+0.060** and AUC is **0.533**.

Worse, **23 of them return the identical delta, 2.809 × 10⁻⁵**, while their
measured titers span **303 to 5,736 mg/L** — a forty-fold range:

| strain | measured | paper's call |
|---|---|---|
| `rfaY⁺-yafL⁺-fadR⁺` | 5736.1 mg/L | increase |
| `rfaY⁺` | 2461.3 mg/L | increase (+207.8%) |
| `RF` (rfaY⁺, round-2 parent) | 2240.3 mg/L | reference |
| `rfaY⁺-ytfK⁺` | 1900.6 mg/L | no change |
| `rfaY⁺-norR⁺` | 303.0 mg/L | decrease |

Every one of these strains overexpresses rfaY, and `waaY`/`b3625` contributes the
only reaction among them that the basis can carry. The partner ORFs — `yafL`,
`rimM`, `norR`, `ygdD`, `yggR`, `ybeF`, `hydN`, `yjdF`, `opgD`, `yafZ`, `wcaA`,
`tas`, `rsxG`, `ytfK`, `lacI` — have no atom-mapped reaction at all. So in the
model these are not merely similar conditions: **they are the same condition**,
and no scoring choice downstream can separate strains whose networks are
identical.

### What the panel arm is and is not evidence for

It is not evidence that ECSPr is broken. The controls are exact, the two arms
measure the same network, the null is size-matched and drawn once, and the probe
does what its specification says. The benchmark was built to be able to return a
positive answer and did not.

It is evidence that **conductance to a product is the wrong observable for this
phenotype**, and the paper says so independently: its own conclusion is that the
winners act through *membrane homeostasis* — LPS core phosphorylation, outer
membrane integrity, permeability — not through carbon supply to fatty acid. Six of
the paper's own top hits are transporters or membrane proteins. A method that
reads a metabolic network cannot see a mechanism that is not in it, and here the
mechanism is not in it. That the strongest single-gene effect in the study (`rfaY`,
3.1×) resolves to a single LPS heptose kinase reaction whose removal from the
carbon graph changes almost nothing is the same fact stated in the model's terms.

### Caveats that bound the panel arm

- **The scale of the effects is small in absolute terms.** `rfaY` moves the
  glycerol→FFA conductance by 2.8 × 10⁻⁵ against a baseline of 9.48, about 3 parts
  per million. It is far above the floor (which is exactly zero) but it is not a
  large perturbation of the network, and a study whose real effects are all this
  size gives the ranking little to work with.
- **TesA′ enters as duplicated host edges, not as a new reaction.** The plasmid's
  acyl-ACP thioesterase is represented by duplicating the four acyl-ACP ↔ free
  fatty acid reactions iML1515 already carries. It sits in the background of every
  condition including the baseline and every null draw, so it cancels in every
  delta — but the strain's actual flux route is more specific than that.
- **C16 has no thioesterase route in this basis.** The atom-pair table carries no
  palmitoyl-ACP thioesterase, so the paper's most abundant products reach the
  readout only through lysophospholipase. Per-species readouts are in the results
  table.
- **The tier and the basis disagree slightly.** The study tier's atom universe
  comes from bake v2 with transport excluded; ECSPr solves on the tier-4 decoded
  pairs. They disagree on a handful of reactions (`waaF` among them). Reach is
  reported both ways in `sweeps/analyse_ffa_panel.py` and neither reading changes the conclusion.
- **BH-q cannot reach 0.05 here** at any effect size: an empirical p over m draws
  floors at 1/(m+1), and the threshold across the tests is below that. `p_floor`
  is carried in the output rather than worked around.

### Provenance of the panel arm

The run is `research/fabfos/benchmarks/fang/out/n1000/`, scored against the **complete
3,000-draw null** — 1,000 per size stratum, from one seed (20260812), drawn once
and shared between the arms by sharing a file. `out/interim/` holds the
stratum-1-only scoring kept as the stability check described above.

Both probes are complete. the lane `README.md` lists the chain that
produced every input and the five things that would have gone wrong silently.

To regenerate either probe's numbers — the driver shards per condition and
resumes, so re-running it costs nothing now that both are done:

    python research/fabfos/benchmarks/fang/sweeps/analyse_ffa_panel.py --run research/fabfos/benchmarks/fang/out/n1000
    python research/fabfos/benchmarks/fang/sweeps/analyse_ffa_panel.py --run research/fabfos/benchmarks/fang/out/n1000 --probe ground

What is still genuinely open is the ground variant described in point 5: ports on
the biomass precursors, fatty acids as readouts only. That is the reading that
would actually exercise redistribution, and it is a change to
`build_ecspr_tables.py`'s `SINKS`/`readout_hub` split rather than to any solver.

## The ratio arm

Built to act on the panel arm's diagnosis: score a two-point conductance RATIO between
competing fates rather than one readout, and ground the numerator at the LPS core rather
than at the fatty acid, so the probe is asked about the mechanism the paper actually
claims. It follows the protocol the eydallin lane settled — a `gof.csv` keyed on the real
expression host, reaction chemistry through a four-channel cascade, a carbon-bond
restriction — and it is swept over the whole ASKA roster rather than the assayed subset.

### Axis reachability, per channel

Solved standalone on the unperturbed MG1655 background before any sweep ran
(`sinks/probe_axes.py`; the `axis_probe_*.tsv` files are the source).

| axis | sink | curated (iML1515, 1,418 rxn) | de-novo (4-lane, 8,570 rxn) |
|---|---|---|---|
| 1 LPS inner core | `MNXM75133` | **0.29951** (node, converged) | **not a node** |
| 2 Kdo₂-lipid A | `MNXM730505` | **0** (node, converged) | **26.144** |
| 3 phosphatidate C16:0 | `MNXM731668` | 3.1684 | 22.804 |
| 4 hexadecanoate | `MNXM108` | 2.7881 | 23.126 |
| — denominator, CO₂ | `MNXM13` | 6.2247 | 52.334 |
| ctl Kdo₂-lipid IVA | `MNXM1104740` | 2.8370 | 16.984 |
| ctl octadecanoate | `MNXM236` | 2.3307 | 27.804 |
| ctl inner core (WaaQ product) | `MNXM56168` | 0.31087 | not a node |
| ctl generic phosphatidate | `MNXM1107831` | not a node | 6.9724 |

**The two channels are complementary, not redundant, and each LPS axis lives on exactly
one.** The caveat carried into this arm held for axis 2 — Kdo₂-lipid A reads exactly 0 on
the curated channel with `sink_is_node=True` and `converged=True`, reproducing
`scales/REPORT.md`'s control — but **not** for axis 1, which reaches on curated at 0.2995 and
is absent from the de-novo carbon graph entirely. A curated-only run would have reported
axis 2 as a null; a de-novo-only run would have had no primary axis at all. Absolute
conductances are not comparable across channels; only ranks within one are.

**Axis 1 was resolved through the reaction, not the name.** MetaNetX spells this
neighbourhood as a large family of near-identical strings and no grep on "inner core" finds
RfaY's product. `MNXR138512` is the only ATP-dependent step between `MNXM56168` (the WaaQ
product, C₁₃₁P₃, carrying WaaP's phosphate on HepI) and `MNXM75133` (C₁₃₁P₄) — one added
phosphate, no carbon change, which is RfaY's characterised activity per the paper's own
citation (Heinrichs et al. 1998). iML1515 independently assigns that same `MNXR138512` to
`waaY`/`b3625`, and so does the extraction the panel arm built.

Two axes were re-pointed, both recorded in `data/fabfos/benchmarks/fang/axes.tsv`:

- **Axis 3.** The declared generic id `MNXM1107831` ("a phosphatidate") **is not a node on
  the curated channel** — curated models carry concrete acyl species, not the class. The axis
  is carried on `MNXM731668`, the C16:0/C16:0 diacyl member, which pairs with axis 4 on chain
  length. The generic id is kept as a control row so its absence is a measured fact.
- **Axis 4.** The paper states **no dominant chain length**: TesA′ hydrolyses C12–C18
  acyl-ACPs and the reported titer is the *sum* of C12–C18 saturated and monounsaturated
  FFAs. C16:0 is this arm's declared default, not one the paper licenses; C18:0 is carried as
  a control and agrees with it (rfaY at rank 717 on both).

### The fold multiplies

Checked on both channels before any sweep was believed
(`sweep_fang_ratio.py --fold-check rfaY`). Every ASKA clone is a chromosomal *E. coli* ORF
the host already carries, so a set-union "overexpression" is a silent no-op for the entire
library and returns a flat column indistinguishable from a real null.

| channel | rfaY's reaction | numerator, fold 1.0 → 2.0 | ratio move |
|---|---|---|---|
| curated | `MNXR138512` | 0.299508591 → 0.304845177 | **+1.7817%** |
| de-novo | `MNXR166612` | 26.144263182 → 26.144287190 | −0.0015% |

Both pass. The curated move is also *specific*: the same fold moves phosphatidate by
+0.000086% and hexadecanoate by +0.000054%.

### The numbers

Regenerate with `analyse_fang_ratio_sweep.py`;
`data/fabfos/runs/fang/ecspr/fang_ratio_classifier_report.{json,txt}` are the source and this
prose is not. 972 of the 4,102 clones are atom-mapped on the curated channel, 1,212 on the
de-novo one (`--min-lanes 2`); everything else scores an exact zero, so ties dominate the
ranking and every AUC is the mid-rank Mann–Whitney form.

#### Where rfaY lands

| channel | axis | rfaY's rank | percentile vs the 4,093 non-hits | Δratio |
|---|---|---|---|---|
| curated | **LPS inner core** | **15 / 4,102** | **0.9966** | +1.7817% |
| curated | inner core, WaaQ product *(size ctl)* | 701 / 4,102 | 0.8292 | −6.7 × 10⁻⁵% |
| curated | phosphatidate | 714 / 4,102 | 0.8258 | +8.6 × 10⁻⁵% |
| curated | hexadecanoate | 717 / 4,102 | 0.8251 | +5.4 × 10⁻⁵% |
| curated | octadecanoate *(ctl)* | 717 / 4,102 | 0.8251 | +3.9 × 10⁻⁵% |
| curated | Kdo₂-lipid IVA *(ctl)* | 734 / 4,102 | 0.8209 | +2.3 × 10⁻⁵% |
| de-novo | phosphatidate | 481 / 4,102 | 0.8830 | — |
| de-novo | hexadecanoate | 486 / 4,102 | 0.8817 | — |
| de-novo | Kdo₂-lipid A | 525 / 4,102 | 0.8725 | — |

**Rank 15 is a property of one sink, and the sink is the metabolite rfaY's own reaction
produces.** Every other axis on either channel puts it in the same undistinguished band
around rank 500–730.

#### Per axis, per channel

Ranking statistic is `|Δratio %|`. `size` is the same AUC computed on reaction count alone —
the confound that sank the ASKA/FFA arm — and ECSPr carries information only insofar as it
beats it.

| channel | axis | within-hit AUC (n=58) | atom-mapped (n=22) | roster AUC | size | roster, atom-mapped | size |
|---|---|---|---|---|---|---|---|
| curated | **LPS inner core** | 0.490 | 0.632 | 0.569 | 0.534 | **0.728** | 0.319 |
| curated | inner core, WaaQ *(size ctl)* | 0.458 | 0.386 | 0.551 | 0.534 | 0.489 | 0.319 |
| curated | phosphatidate | 0.433 | 0.193 | 0.529 | 0.534 | 0.224 | 0.319 |
| curated | hexadecanoate | 0.442 | 0.263 | 0.530 | 0.534 | 0.224 | 0.319 |
| de-novo | Kdo₂-lipid A | 0.596 | 0.459 | 0.642 | 0.618 | 0.553 | 0.422 |
| de-novo | phosphatidate | 0.533 | 0.318 | 0.582 | 0.618 | 0.422 | 0.422 |
| de-novo | hexadecanoate | 0.565 | 0.294 | 0.614 | 0.618 | 0.383 | 0.422 |

Only two cells beat their size control by anything: the curated LPS inner core (0.728 vs
0.319, *p* = 0.086) and, marginally, the de-novo Kdo₂-lipid A roster AUC (0.642 vs 0.618,
*p* = 0.032). Both are the LPS axes, and both are the tautology.

#### The tautology control, on both channels

Strike the 34 LPS-core-module clones — which removes five of the nine positives (`rfaY`,
`waaF`, `lpxK`, `lpxL`, `msbA`) and leaves four with **no atom-mapped reaction at all** — and
every axis on both channels lands at or below its size control:

| channel | axis | AUC struck | size |
|---|---|---|---|
| curated | LPS inner core | 0.388 | 0.383 |
| curated | phosphatidate | 0.388 | 0.383 |
| curated | hexadecanoate | 0.388 | 0.383 |
| de-novo | Kdo₂-lipid A | 0.454 | 0.462 |
| de-novo | phosphatidate | 0.361 | 0.462 |
| de-novo | hexadecanoate | 0.452 | 0.462 |

Nothing outside the module is nominated by any axis.

#### The same failure appears on the de-novo Kdo₂ axis

The de-novo channel's only live LPS axis reproduces the curated one's pathology on a
different module. Its top of library is lipid A biosynthesis, and it is ordered the wrong
way round: **`lpxM` ranks first (Δ +12.96%) and is one of the genes that went *down*** —
495.7 mg/L against F0's 799.6 — while `lpxL` (up, 1674.4 mg/L) is second at +3.16% and
`lpxK` (up, 1093.1 mg/L) is tenth at +0.79%. The axis finds the module and cannot order
within it, exactly as the curated LPS axis cannot separate rfaY from waaQ.

#### Reach is not confounded, which is what lets the above be read at all

Being a Fang beneficial target and being visible to the method are independent on both
channels: 3/9 positives atom-mapped (33.3%) against 969/4,093 of the rest (23.7%) curated,
OR = 1.61, *p* = 0.45; and 5/9 (55.6%) against 1,207/4,093 (29.5%) de-novo, OR = 2.99,
*p* = 0.14. So none of the AUCs above is the dependence rather than the measurement, and
every one of them is repeated on the atom-mapped subset where the dependence cannot
contribute.

#### The carbon-bond filter is too small to carry a statistic

Restricted to the 9 genes whose reaction changes a carbon skeleton, only one — `waaF` — is a
beneficial target. Every within-hit AUC on that slice is therefore computed from a single
positive: 0.875 on both curated LPS axes, 0.250 on every FFA and phosphatidate axis, *p* > 0.2
throughout. Reported because the filter was declared, not because it decides anything.

## What could not be resolved

- **`nepI`**, one of the four round-1 strains that beat F0 by more than 50%, is not on the
  ASKA roster (`aska_clone_minus.tsv`) under any name or b-number. It has a `gof.csv` row and
  no clone in the sweep, so eight of Fang's nine beneficial targets reach the ranking rather
  than nine.
- **`MNXR100900`**, iML1515's LPS-transport reaction and the primary for all four `lpt*`
  genes, is absent from `reactions.parquet`; those rows carry an `mnxr` and no equation.
- **`opgD`** has no MetaNetX reaction that belongs to OpgD rather than to OpgB/OpgE, and is
  categorised rather than mapped.
- **A dominant FFA chain length.** The paper states none; the axis default of C16:0 is this
  arm's, and the C18:0 control agrees with it to the rank.
- **The screen's own read counts.** They exist only as a scatter in Figs. 1b and 4b, with no
  table and no deposit, which is what forces the within-hit / roster-wide split above.

## What this does and does not overturn

It does not overturn the study: the screen is sound, the extraction reproduces the paper's
own arithmetic, and rfaY is a real and well-supported target.

It does not rescue the method. Across three studies now — SCALEs, Eydallin, and Fang — the
two-point conductance ranks the module that contains the declared sink and cannot order
genes inside it. What Fang adds is the cleanest instrument yet for saying so: a size control
that differs from the primary sink by **one phosphate group and no carbon at all**, on which
the headline gene falls from rank 15 to rank 701.

It also closes a question the panel arm left open. That arm ended by suspecting the
observable — conductance to a *product* — rather than the method, and named the ratio and a
mechanism-side sink as the thing worth trying. Both were tried here. The ratio is genuinely
two-sided, the sinks are the ones the paper's own mechanism names, every precondition passes,
and the ranking still cannot order two enzymes one edge apart. The observable was not the
problem.

The one thing neither arm has run is the ground variant the panel arm proposed in its
point 5: ports on the biomass precursors, fatty acids as readouts only. It remains the
obvious next thing to try, and it is a change to `gpr_build/build_ecspr_tables.py`'s
`SINKS`/`readout_hub` split rather than to any solver.
