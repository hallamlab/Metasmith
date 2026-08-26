# Can ECSPr pick out the deletions that move a metabolite, when every gene was measured?

**Partly, and for the first time in this campaign the "partly" is a measurement rather than
a hope.** Across 99 metabolites chosen by reachability alone, the atom-resolved conductance
ratio beats its own reaction-count size control on 76 of them — 76.8%, binomial p = 4.3e-08,
median AUC difference +0.096, paired Wilcoxon p = 3.5e-10. Seventy-five of the 99 score above
0.5 at all (p = 1.4e-07). That is the campaign's first genome-scale evidence that the
conductance carries information a reaction count does not.

**Everything else is negative, and three of the negatives are sharp.** The probe cannot
distinguish a metabolite from its own one-reaction neighbour: on the declared panel the
precursor axis scores within ±0.03 of the target on every axis and higher on five of eight,
on both channels independently. It carries no directional information: sign agreement
exceeds its own majority-class base rate on 8 of 99 metabolites, which is exactly what
chance gives. And the effect it does have is small — median AUC 0.584 against a 0.492 size
control — while the genes it ranks are a minority of the screen that is not enriched for
movers: 22.5% of measured deletions are atom-mapped, and the positives are atom-mapped at
the same rate (median odds ratio 1.04 across 99 metabolites, p < 0.05 on 17).

This is a result about the method, not about the study. Fuhrer's screen is the best-powered
input this campaign has had, and it is the reason the numbers above are measurements at all.

---

## Limits, stated here rather than discovered later

1. **The declared panel was selected on a topology statistic.** Rule R4 keeps the eight ions
   with the highest published AUC, and that AUC is derived from network adjacency — close to
   the quantity under test. The panel is therefore biased toward metabolites where adjacency
   already works, and its median AUC (0.65 curated) sits above the genome-wide median
   (0.584). The genome-wide arm exists as the unbiased counterpart and is where the headline
   claim comes from.

2. **The readouts are masses, not metabolites.** Flow-injection MS does not separate isomers.
   Of 7,534 ions, 4,404 carry no KEGG annotation within 3 mDa and most annotated ones fit
   several compounds. Rule R1 admits only ions naming exactly one cross-referenced compound —
   993 of 7,534 — which keeps chemical ambiguity out but does not make the surviving
   annotation *correct*, only unique.

3. **A z-score is a relative abundance, not a concentration.** It is one ion's intensity
   against that ion's own distribution across the library. Nothing here converts it to a
   pool size, and no claim in this report depends on one.

4. **These numbers do not compare to the other three arms' except as ranks.** This is the
   campaign's only deletion arm and its only fold-0 arm. Severing an edge and doubling one
   are different sizes of perturbation, and the graph background differs besides (BW25113 /
   iML1515 rather than AG1, MG1655 or LW06). AUC, the size control and sign-against-base-rate
   cross the boundary; absolute deltas and the `movers` count do not.

5. **Only the declared panel ran on both channels.** The genome-wide sweep is curated-channel
   only. The de-novo background carries six times the reactions and its 105-axis sweep
   projects to roughly a day of wall clock against 18 minutes for the curated one. The bound
   is the channel, not the metabolite set, which is the cheaper thing to give up: the
   metabolite set stays complete and unbiased, and the declared panel shows the two channels
   agreeing on every qualitative conclusion.

6. **Table EV4 is not a like-for-like baseline** — see *The external baseline* below. It is
   printed everywhere it is relevant and never treated as though its label set were this
   arm's.

---

## What was measured

Fuhrer et al. profiled 3,806 Keio single-gene deletions against 7,534 metabolite ions by
flow-injection mass spectrometry in M9 glucose. The deposit (EBI BioStudies S-BSST5) carries
the modified z-score matrices; the article supplement carries the strain roster, the ion
annotations and a per-ion ROC AUC.

**Every join into those matrices is positional and none of them is asserted.** The matrices
are headerless numeric grids; row identity lives in two Excel 97 workbooks and column
identity in a third. `parse/decode_screen.py` proves the correspondence four ways and exits
rather than writing:

| guard | what it checks | outcome |
|---|---|---|
| G1 shape | 3,169 negative ions, 4,365 positive, 3,807 columns | reproduced, no threshold moved |
| G2 index→mass | the KEGG workbooks' `ion` index lands bit-exactly on their own `mz` | 0 mismatches in 13,509 rows |
| G3 independent order | Table EV1B's m/z is each deposit mass's nearest neighbour | holds for all but one adjacent pair, named below |
| G4 the paper's own summary | Table EV2's annotated-ion and compound counts, rebuilt | 1,484 / 2,093 and 1,646 / 2,075, exact |

G3's one exception is worth naming because it is the shape of an honest guard. Negative-mode
ions 2418 and 2419 sit 15.44 mDa apart while the two files' calibration differs by 9.37 mDa
there, so each one's nearest mass in the other file is its neighbour's. Nearest-neighbour is
arithmetically incapable of deciding that pair; the guard says so, counts it, and stays exact
on the other 7,532.

**The deposit's 3,807 columns are 3,806 deletions plus a wild-type control**, and nothing in
the deposit flags it. The wild type moves **zero** ions past the 0.1-percentile cutoff, which
is the floor every mutant's count is read against. Fuhrer's own four categories come back
from that cutoff: 1,716 silent, 1,109 rare, 401 moderate, 580 global.

Resolution is by Blattner identifier from Table EV1A — the Keio collection's own record of
which locus was deleted — cross-checked against Baba 2006, which disagrees on 8 strains
(reported, not reconciled). 3,537 of 3,806 place on a BW25113 protein by exact sequence, 77
more by symbol, 192 not at all. **646 of the screen's gene names have been retired since
2017**, which is why nothing in this arm joins on a name.

Coverage, at each step, over the 3,806 measured deletions:

| | curated (iML1515) | de-novo (4-lane) |
|---|---|---|
| named by the background | 1,348 | 2,311 |
| carrying an atom-mapped reaction | 858 | 1,840 (1,055 at `--min-lanes 2`) |

The census carries 163 further model genes the screen never measured, 120 of them
Keio-essential. They are flagged and dropped: an unmeasured gene is not a negative one.

---

## The declared panel: eight metabolites, both channels

Positives are deletions crossing the paper's own reproducibility threshold, |z| > 2.765 —
1% false-positive probability between biological replicates, established before any gene was
looked at. Every AUC is over the atom-mapped subset and printed beside two different size
controls and the published number.

**Curated channel (iML1515), 3,803 measured deletions, 856 atom-mapped:**

| axis | metabolite | n+ | ECSPr | n_rxn | precursor | struck | published | sign | base |
|---|---|---|---|---|---|---|---|---|---|
| c00209_neg870 | oxalate | 10 | 0.5953 | 0.3864 | 0.5535 | 0.5960 | 0.9878 | 0.600 | 0.600 |
| c01412_pos2 | butanal | 8 | 0.5390 | 0.3851 | 0.5420 | 0.5386 | 0.9245 | 0.625 | 0.625 |
| c01909_neg210 | dethiobiotin | 32 | 0.7462 | 0.4525 | 0.7363 | 0.7380 | 0.9164 | 0.656 | 1.000 |
| c03492_pos1554 | 4'-phosphopantothenate | 16 | 0.5813 | 0.4801 | 0.5839 | 0.5820 | 0.8992 | 0.750 | 0.812 |
| c04133_pos2741 | N-acetylglutamyl-5-P | 16 | 0.7107 | 0.5176 | 0.7192 | 0.7124 | 0.8826 | 0.438 | 0.625 |
| c04462_neg914 | succinylamino-oxoheptanedioate | 8 | 0.7465 | 0.3701 | 0.7762 | 0.7498 | 0.8612 | 0.750 | 0.750 |
| c00957_pos1518 | mercaptopyruvate | 12 | 0.5758 | 0.4660 | 0.5770 | 0.5771 | 0.8612 | 0.333 | 0.750 |
| c01100_pos999 | histidinol phosphate | 9 | 0.7437 | 0.5099 | 0.7436 | 0.7454 | 0.8573 | 0.556 | 0.556 |

**De-novo channel (4-lane, `--min-lanes 2`), 1,055 atom-mapped:**

| axis | n+ | ECSPr | n_rxn | precursor | struck | sign | base |
|---|---|---|---|---|---|---|---|
| c00209_neg870 | 8 | 0.6508 | 0.4078 | 0.6274 | 0.6517 | 0.625 | 0.625 |
| c01412_pos2 | 13 | 0.5981 | 0.4002 | 0.6017 | 0.5991 | 0.538 | 0.615 |
| c01909_neg210 | 32 | 0.6749 | 0.5140 | 0.6939 | 0.6650 | 0.906 | 1.000 |
| c03492_pos1554 | 16 | 0.5336 | 0.5328 | 0.5449 | 0.5341 | 0.875 | 0.812 |
| c04133_pos2741 | 23 | 0.5138 | 0.4294 | 0.5000 | 0.5147 | 0.478 | 0.565 |
| c04462_neg914 | 9 | 0.6309 | 0.2663 | 0.6307 | 0.6311 | 0.667 | 0.778 |
| c00957_pos1518 | 17 | 0.5249 | 0.4614 | 0.5088 | 0.5254 | 0.176 | 0.765 |
| c01100_pos999 | 8 | 0.6520 | 0.5688 | 0.6519 | 0.6532 | 0.500 | 0.500 |

Four things to read out of those two tables, in order of how much they matter.

### The reach 2×2, before any ranking statistic

Being visible to the model does not predict being a mover. On the curated channel, 10 of 36
oxalate positives are atom-mapped (27.8%) against 848 of 3,769 of the rest (22.5%), odds
ratio 1.32, Fisher p = 0.43. Dethiobiotin's is 22.2% against 22.6%, OR 0.98, p = 1. Across
the 99 genome-wide axes the median odds ratio is 1.04 and only 17 clear p < 0.05. **Every
ranking number below is computed inside a subset that is not enriched for what it is trying
to rank.**

### ECSPr beats the reaction-count control on all sixteen axis-channel pairs

0.51–0.75 against 0.27–0.57. The probe is not merely counting how many reactions the deleted
gene carries. This is the positive finding, and the genome-wide arm turns it into a
distribution.

### The precursor control says the probe ranks the module, not the metabolite

The `_precursor` axis is the sink's own one-reaction carbon neighbour, chosen off the
atom-pair table by shared mapped-carbon count rather than by hand, and scored against the
**target's** labels. If a target and its immediate precursor rank the population alike, the
two-point conductance has ranked the module containing both.

They rank it alike everywhere. Curated: the largest gap is 0.042 (oxalate), and on five of
eight the precursor is *higher*. De-novo: the largest gap is 0.023, precursor higher on three of eight. Two independent bases, sixteen comparisons, no separation.

This is *declared-sink adjacency* — the confound named in the eydallin, fang and woodruff
reports — and this is the first arm that measures it instead of arguing it. The previous
three inferred it from the identity of the top-ranked genes; here the counterfactual is
solved explicitly and it comes back flat.

### The tautology control is not what is carrying the signal

Striking every gene whose curated reactions touch the sink — computed off the atom-pair table,
removed from both classes — moves the curated AUCs by at most 0.003 and the genome-wide median
from 0.5839 to 0.5750. Only two axes had a positive in the struck set at all (dethiobiotin's
`bioD`, and one on the wide panel). Whatever the probe is doing, it is not scoring the sink's
own edges.

### Sign agreement is at or below its own base rate almost everywhere

Curated: equal to the base rate on four axes, below it on four, above on none. De-novo:
above on one of eight. This is the axis the campaign has never been able to test, because no
previous screen had a two-sided label — and the answer is that the ratio's sign carries no
information about whether the metabolite rose or fell. Dethiobiotin makes the reason to print
base rates plain: 55 positives, 55 of them up, base rate 1.000, so its 0.656 and 0.906
"agreements" are worse than answering "up" every time.

### One concrete illustration

On the histidinol-phosphate axis the seven most extreme curated predictions are `hisA`,
`hisB`, `hisC`, `hisF`, `hisG`, `hisH` and `hisI`, each at `delta_ratio_pct = -100` — the
deletion severs glucose from the sink entirely. **None of them is a positive.** Four of the
seven are *silent* in Fuhrer's own classification: zero differential ions out of 7,534, at
growth rates of 0.72–1.00. The probe's most confident calls on that axis are among the
screen's quietest strains.

---

## The genome-wide arm: 99 metabolites, chosen by reachability

The declared panel's selection rule consults the paper's AUC. This one does not. It applies
R1 (one unambiguous cross-referenced compound that is a curated node) and R2 (at least 20
deletions past |z| > 2.765), keeps each compound's best-*powered* ion, and stops. The funnel
is printed rather than assumed:

    7,534  ions measured
    4,404  carry no KEGG annotation within 3 mDa
      993  name exactly one cross-referenced compound
      239  of those compounds carry carbon in the curated background   (R1)
      227  have at least 20 deletions past |z| > 2.765                 (R2)
      105  distinct metabolites, each on its best-powered ion
       99  reach on the curated channel and were swept

The six that do not reach are named in the axis probe. Median positives per axis: 58 measured,
13 of them atom-mapped.

**The distribution:**

| | median | IQR | range |
|---|---|---|---|
| ECSPr AUC | 0.5839 | [0.5093, 0.6315] | [0.3837, 0.7498] |
| reaction-count control | 0.4922 | [0.4366, 0.5300] | |

- ECSPr beats its size control on **76/99** (76.8%), binomial p = **4.27e-08**
- paired Wilcoxon on the difference: p = **3.48e-10**, median difference **+0.0959**
- above 0.5 at all on **75/99**, p = 1.38e-07
- tautology control: median 0.5839 → 0.5750, median change +0.0009
- sign agreement above its own base rate on **8/99**, p = 1

The best-ranked metabolites are putrescine (0.750), the panel's own
succinylamino-oxoheptanedioate (0.747) and histidinol phosphate (0.744), dTMP (0.736) and IMP
(0.716). The worst are formate (0.384), cobinamide (0.385) and glyoxylate (0.410) — all three
below their own size control, and all three metabolites whose carbon is shared with most of
the network.

The declared panel's curated median of about 0.65 against this 0.584 is the size of the R4
selection bias, measured. It is real and it is modest.

---

## The external baseline, and why it is not one

Table EV4 publishes a per-ion-annotation ROC AUC computed by ranking deletion mutants on
|z| and calling an enzyme a true positive when it "either uses the predicted metabolite as
substrate or product". That is the **transpose** of this arm's question — Fuhrer ranks by the
phenotype and labels by adjacency; ECSPr ranks by topology and labels by the phenotype — and
both are monotone in the same gene–metabolite association.

Before printing it beside anything, `sweeps/reproduce_published_auc.py` rebuilds it from this
tree's adjacency, using the same `sink_module` the tautology control strikes. Over 1,283
scorable ion-annotations:

- Spearman ρ between published and recomputed: **0.1428** (p = 2.8e-07)
- median absolute difference **0.0949**; only **36.4%** agree within 0.05
- and it does **not** improve where the metabolite has exactly one adjacent gene (42.5% within
  0.05, ρ = 0.19), which is where the two definitions have the least room to differ

So the published column is a baseline on a label set this tree cannot reproduce. The
disagreement is expected in kind — Fuhrer annotated against KEGG `eco` and the Orth 2011
model, this tree runs iML1515 through MetaNetX 4.5 — but its size means the number cannot be
read as "what a good method scores here".

The **fair** transpose is the recomputation, on the same adjacency ECSPr's topology comes from.
Its median across the 99 genome-wide metabolites is **0.533**, against ECSPr's 0.584, and
ECSPr is higher on 57 of 88. Ranking deletions by how much they moved a metabolite barely
identifies that metabolite's own enzymes on this adjacency — which is a statement about how
loosely gene–metabolite proximity and metabolite response are coupled in this organism, and
it bounds what any adjacency-based predictor can achieve here.

Neither baseline correlates with ECSPr per metabolite (ρ = −0.02 fair, ρ = −0.20 published).
The two approaches do not agree about *which* metabolites are well predicted, even where both
score above chance.

---

## Where this leaves the campaign

Four papers, four screens, one protocol. Eydallin, Fang and Woodruff each returned a null and
each attributed it to declared-sink adjacency on the strength of which genes came out on top.
This arm had the power to test that attribution directly, and it confirms it: a metabolite and
its one-reaction neighbour are indistinguishable to the probe, on two independent bases, on
every axis tried.

What is new is the other half. With 3,806 measured deletions and 99 metabolites instead of a
handful of clones and one sink, the same protocol separates from its size control at
p = 4e-08. The effect is small — a median AUC of 0.58 — but it is not zero, it survives the
tautology control, and it is the first time in the campaign that the conductance has been
shown to carry anything a reaction count does not.

Both statements are about the same instrument. The two-point conductance ranks a
neighbourhood, and a neighbourhood is a weak but real predictor of which deletions perturb a
metabolite. It is not a predictor of *which* metabolite in that neighbourhood, and it is not a
predictor of direction at all.

## Open

- The genome-wide arm on the de-novo channel. Projected at roughly a day of wall clock; the
  declared panel says the two channels agree qualitatively, but that is eight axes, not 99.
- Whether the 76/99 separation survives a background matched on neighbourhood size rather
  than on reaction count. The reaction-count control is a competing ranker, not a
  neighbourhood-preserving null, and the precursor result says the neighbourhood is what the
  probe sees.
- Why `formate`, `glyoxylate` and `cobinamide` score *below* chance rather than at it. A
  below-chance AUC on a two-sided score is not noise, and three of them is a pattern.
