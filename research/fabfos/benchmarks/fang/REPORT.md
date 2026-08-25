# Would ECSPr find Fang's rfaY?

**Yes, on the LPS axis — and its own size control shows the reason is sink adjacency, not
biology.** Swept blind across the whole 4,102-clone ASKA roster and ranked by the two-point
conductance ratio C(acetyl-CoA → LPS inner core) / C(acetyl-CoA → CO₂), ECSPr puts **rfaY at
rank 15 of 4,102** — the 99.66th percentile of the non-hit clones, two positives in the top
50 against an expectation of 0.1 (*p* = 0.005), atom-mapped AUC **0.728** against a
reaction-count size control of 0.319. That is the first axis in this campaign to beat its
size control while nominating a paper's headline gene, and it is the axis that asks the
paper's mechanistic question rather than the assay's.

Three controls, all declared before any number existed, take it apart.

**1. Move the sink one step upstream and rfaY collapses.** The declared size control is
`inner_core_waaQ` — rfaY's own *substrate*, C₁₃₁ on both sides, differing from the sink by
exactly RfaY's phosphate. On it, rfaY falls from rank 15 to **701 of 4,102** (Δ = −6.7 × 10⁻⁵%)
while `waaQ`, the enzyme that makes *that* metabolite, climbs from 14 to **9**. The model is
ranking whichever gene catalyses the last edge into whatever terminal was declared.

**2. rfaY is indistinguishable from a gene the assay calls flat.** On the LPS axis `rfaY`
moves the ratio +1.781707% and `waaQ` +1.781775% — differing in the fifth decimal, ranking
15th and 14th, with waaQ *above*. Fang measured rfaY at 2461.3 mg/L and waaQ at 724.0 mg/L
against the same 799.6 mg/L control. One rung down the same thing happens: `waaF`
(+0.5635859%, up at 1172.2 mg/L) and `waaP` (+0.5635220%, **down** at 431.5 mg/L) are a
matched pair the model cannot separate.

**3. Strike the LPS core module and the ranking falls below chance.** 34 module clones
removed, five of them positive (`rfaY`, `waaF`, `lpxK`, `lpxL`, `msbA`); the four remaining
positives have **no atom-mapped reaction at all**, and the library AUC drops to 0.388 against
a size control of 0.383.

**The axis that measured what the GC measured says nothing.** On hexadecanoate, rfaY ranks
**717 of 4,102**, within-hit AUC 0.442 against a size control of 0.449; phosphatidate agrees
to three decimals at rank 714. So the LPS axes did say something the FFA axis did not — that
rfaY is an LPS inner-core enzyme, which was the premise of choosing the sink, not a finding.

This is a negative result about the method, with a sharper edge than the campaign's earlier
ones: the failure is not noise but a specific, nameable, *measurable* confound —
**declared-sink adjacency** — and the instrument that measures it is the arm's own size
control.

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

Titers are **not re-digitised here**. `research/fabfos/benchmarks/aska/digitise_ffa.py`
already read them off the figure bars against the four titers the paper states in prose
(F0 799.6, rfaY 2461.3, RF 2240.3, rfaY-yafL 3447.6), and
`data/fabfos/benchmarks/aska_ffa/extraction.tsv` is the source. That lane asked a different
question of the same study — whether a **one-probe** glycerol → FFA conductance tracks the
titer — and answered no. What is new here is the ratio, the LPS sinks, and the `gof.csv`
reaction-chemistry schema.

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

## Axis reachability, per channel

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
`waaY`/`b3625`, and so does the earlier `aska_ffa` extraction.

Two axes were re-pointed, both recorded in `data/fabfos/benchmarks/fang/axes.tsv`:

- **Axis 3.** The declared generic id `MNXM1107831` ("a phosphatidate") **is not a node on
  the curated channel** — curated models carry concrete acyl species, not the class. The axis
  is carried on `MNXM731668`, the C16:0/C16:0 diacyl member, which pairs with axis 4 on chain
  length. The generic id is kept as a control row so its absence is a measured fact.
- **Axis 4.** The paper states **no dominant chain length**: TesA′ hydrolyses C12–C18
  acyl-ACPs and the reported titer is the *sum* of C12–C18 saturated and monounsaturated
  FFAs. C16:0 is this arm's declared default, not one the paper licenses; C18:0 is carried as
  a control and agrees with it (rfaY at rank 717 on both).

## The fold multiplies

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

## The numbers

Regenerate with `analyse_fang_ratio_sweep.py`;
`data/fabfos/runs/fang/ecspr/fang_ratio_classifier_report.{json,txt}` are the source and this
prose is not. 972 of the 4,102 clones are atom-mapped on the curated channel, 1,212 on the
de-novo one (`--min-lanes 2`); everything else scores an exact zero, so ties dominate the
ranking and every AUC is the mid-rank Mann–Whitney form.

### Where rfaY lands

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

### Per axis, per channel

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

### The tautology control, on both channels

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

### The same failure appears on the de-novo Kdo₂ axis

The de-novo channel's only live LPS axis reproduces the curated one's pathology on a
different module. Its top of library is lipid A biosynthesis, and it is ordered the wrong
way round: **`lpxM` ranks first (Δ +12.96%) and is one of the genes that went *down*** —
495.7 mg/L against F0's 799.6 — while `lpxL` (up, 1674.4 mg/L) is second at +3.16% and
`lpxK` (up, 1093.1 mg/L) is tenth at +0.79%. The axis finds the module and cannot order
within it, exactly as the curated LPS axis cannot separate rfaY from waaQ.

### Reach is not confounded, which is what lets the above be read at all

Being a Fang beneficial target and being visible to the method are independent on both
channels: 3/9 positives atom-mapped (33.3%) against 969/4,093 of the rest (23.7%) curated,
OR = 1.61, *p* = 0.45; and 5/9 (55.6%) against 1,207/4,093 (29.5%) de-novo, OR = 2.99,
*p* = 0.14. So none of the AUCs above is the dependence rather than the measurement, and
every one of them is repeated on the atom-mapped subset where the dependence cannot
contribute.

### The carbon-bond filter is too small to carry a statistic

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
