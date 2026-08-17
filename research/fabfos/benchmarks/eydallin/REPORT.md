# Would ECSPr find Eydallin's genes?

**No — beyond the glycogen module itself, which it finds tautologically.** Swept blind
across the whole ASKA library and ranked by the modelled glucose → glycogen response,
ECSPr puts five of Eydallin's 86 hits in its top ten, which looks decisive until you
notice that all five are glycogen synthesis or degradation enzymes and that the probe
grounds at glycogen. Strike those six genes and the ranking collapses to chance: AUC
0.509 on the curated channel, 0.436 on the de-novo one, one positive in the top 25
against an expectation of half of one. The full-library AUC — 0.536 curated, 0.464
de-novo — is *beaten by reaction count alone*, which is the same confound that sank the
ASKA/FFA arm. The one annotation artifact the de-novo channel had was found and removed,
and the AUC moved from below chance to chance.

This is a negative result about the method, not about the study. Reach is not confounded,
the resolution is complete, the controls are the ones designed to break the claim, and the
hundred random draws the question was originally posed as agree with the exhaustive sweep
to three decimals.

## What was measured

Eydallin et al. 2010 screened the **entire ASKA overexpression library** and reported 86
genes. That is what makes this benchmark unusual: the ~4,000 clones absent from the paper
were built, transformed, assayed and stained, and did not move glycogen. They are measured
negatives, not unlabelled ones, and they are the denominator every number below uses.

The population is the GFP-minus roster — **4,123 clones over 4,102 distinct gene names**.
Reaction tables for all of it are a join over tables already on disk rather than an
annotation run, because an ASKA clone is a chromosomal *E. coli* ORF and AG1 already
carries every gene the library overexpresses (`build_aska_gpr.py` →
`data/fabfos/runs/aska/gpr/`):

| channel | genes resolved | with a reaction | atom-mapped | of the 86 |
|---|---|---|---|---|
| curated (AG1 GEM `iECDH1ME8569_1439`) | 1,310 | 1,310 | **880** | **25** |
| de-novo (AG1 proteome, 4 lanes) | 3,784 | 3,775 | **3,679** | **73** |

**All 86 positives are labelled.** Six do not appear on the roster under the name the paper
prints — five because the roster still uses the 2005 symbol (aspP/nudF, csrD/yhdA,
mlc/dgsA, rutF/ycdH, yifJ/wzxE) and one on case alone (ppK/ppk) — and are recovered through
the b-number rather than dropped. A positive left unlabelled is a positive scored as a
negative, so this is not bookkeeping.

The measurement is the one `twopoint_cohort.py` already runs, widened from 25 genes to the
library and nothing else changed: source D-glucose `MNXM1364061`, sink glycogen
`MNXM738130`, element C, r7 bake, host `e_coli_ag1`, overexpression as a ×2 conductance
fold on the clone's reactions. Base conductance **5.689489** curated, **6.443092** de-novo.
A clone reaching no atom-mapped reaction is an exact zero and is never solved. Each channel
is folded against its own background — 2,022 atom-mapped reactions curated, 10,998 de-novo
— so a clone can only ever widen an edge its own background already had, which is what an
ASKA clone physically does. The two channels' absolute conductances are consequently not
comparable to each other; only ranks within a channel are.

Because a solve costs under a second, the sweep is **exhaustive** — every one of the 4,102
gene names, 2.6 minutes for the curated channel and 54 for the de-novo one. That strictly
contains the hundred random draws this question was originally posed as, and carries no
sampling error.

## The four numbers

Regenerate with `analyse_aska_sweep.py`; `aska_classifier_report.{json,txt}` beside the
sweep tables are the source and this prose is not.

**1. Reach is not confounded, which is the precondition for anything below.** Being an
Eydallin hit and being visible to the method are independent in both channels: 29.1% of
positives are atom-mapped in the curated channel against 21.3% of the rest (OR 1.52,
p = 0.085), and 84.9% against 89.8% in the de-novo one (OR 0.64, p = 0.15). Neither
survives a two-sided test. The study is not the degenerate case where the interesting genes
are the ones the method happens to see. Every AUC below is also repeated on the atom-mapped
subset alone, where the question cannot arise at all.

**2. The full-library AUC is at chance, and reaction count beats it.**

| channel | scope | ECSPr AUC | size control | 100-draw resample |
|---|---|---|---|---|
| curated | library (4,102; 86 pos) | 0.5362 (p = 0.053) | **0.5409** (p = 0.035) | 0.5368 ± 0.0190 |
| curated | atom-mapped (880; 25 pos) | 0.5731 (p = 0.11) | 0.5331 (p = 0.25) | — |
| de-novo | library (4,102; 86 pos) | 0.4636 (p = 0.88) | 0.4733 | 0.4639 ± 0.0253 |
| de-novo | atom-mapped (3,679; 73 pos) | 0.4851 (p = 0.67) | 0.4972 | — |

The claim that ECSPr carries information stands only if it beats the size control, and on
the whole library it does not: 0.5362 against 0.5409. On the curated atom-mapped subset it
does beat it (0.573 vs 0.533), which is the one place in this report where the measurement
is doing something the clone's size is not — but at p = 0.11 over 25 positives, and §3
shows what that 0.573 is made of.

The **resampled figures are the literal experiment asked for**: 2,000 draws of 100 random
clones as the negative set, at a fixed seed. They land on the exhaustive value to three
decimals with a 95% interval of about ±0.04, which is the width of the sampling error the
exhaustive sweep does not have.

**3. Strike the glycogen module and everything goes.** A glucose → glycogen probe ranking
glycogen synthase first is close to arithmetic, so the interesting question is about the
other eighty genes. Removing glgA, glgB, glgC, glgP, glgS and malP from the positives:

| channel | ECSPr AUC (library) | size control | top 10 | top 25 | top 100 |
|---|---|---|---|---|---|
| curated, all 86 | 0.5362 | 0.5409 | **5** (exp 0.2, p = 8e-7) | 6 (p = 9e-6) | 9 (p = 2e-4) |
| curated, module struck | **0.5092** (p = 0.35) | 0.5204 | **0** | 1 (p = 0.39) | 4 (p = 0.13) |
| de-novo, all 86 | 0.4636 | 0.4733 | **4** (p = 3e-5) | 4 (p = 1.6e-3) | 7 (p = 4.6e-3) |
| de-novo, module struck | **0.4353** (p = 0.98) | 0.4596 | 1 (p = 0.18) | 1 (p = 0.39) | 2 (p = 0.59) |

Every significant number in this report is the glycogen module. The precision-at-10 of 5/10
is real and its p-value of 8e-7 is real; it is also entirely malP, glgP, glgA, glgC and
glgB, and it survives removing nothing. On the curated atom-mapped subset the struck AUC
falls to 0.468 — below chance.

**4. The top of the ranking is carbohydrate metabolism, not Eydallin's screen.** The
curated channel's top 25 reads agp, glgX, lacZ, bglX, malZ, pgm, yqaB, malQ, galT, galK,
xylA — glucose- and glycogen-adjacent enzymes that Eydallin built, assayed and scored as
non-hits. The probe is measuring proximity to the glucose → glycogen path, which is a real
property of the network and simply is not the phenotype: overexpressing a gene that sits on
the path does not reliably change how much glycogen accumulates, and Eydallin's own data
says so.

## How much was there to find in the first place

The section above is a statement about a ranking. This one is a statement about the
network, and it is the reason the ranking came out that way: **there are three routes from
D-glucose to glycogen, all three are glg reactions, and the probe's whole response fits in
about six.** Every number here is the curated AG1 background on the r7 bake, the same one
the sweep ran on.

**The measurement partitions exactly, so shares are available rather than only ranks.**
Effective conductance is homogeneous of degree one in the conductances, so each reaction's
elasticity `dlog C_eff / dlog g_r` is its share of the dissipated power, and the shares sum
to 1 (`ecspr.model.build.reaction_elasticities`, one solve). `pathway_complexity.py`
checks the closed form against an exhaustive fold-1.01 sweep of all 1,553 reactions that
build an edge — 3,106 solves against one — and they agree: sum 1.0011 against 1.0000,
Spearman 0.990, and every one of the five reactions that carries the answer inside 0.25%.
The two places they part company are `fsaA` and `talA` at an elasticity of ~1e-3, where the
diode's smoothing makes the first-order form wrong by a factor; nothing that size is a lever.

**1. Three routes, named exhaustively.** Model the atom graph as a flow network with
metabolites at infinite capacity and each reaction at capacity one, and the minimum cut
between D-glucose and glycogen is **3** — so by Menger there are at most three
reaction-disjoint routes and the cut names all of them: `MNXR145046` glgA, `MNXR145036`
glgP/malP, `MNXR145021` glgB/glgX. Glycogen's only carbon partners in the whole host graph
are ADP-glucose, G1P and branched glycogen. There is no route to it that misses the glg
operon, and no single reaction is a cut on its own: the worst knockout leaves 58% of the
conductance, and only four of the 1,553 cost more than 10%.

**2. It is not a coverage gap.** The de-novo background carries 10,638 atom-mapped
reactions against the curated 2,022 — five times the metabolism — and the cut does not
grow, it **shrinks to 2** (glgA, glgB). Five times the reactions add zero new ways into
glycogen, which is what says the neck is the chemistry and not the curation.

**3. Six levers, and what the probe leans on.** `1/sum(eps^2)` — the effective number of
reactions the probe can respond to at all — is **6.13**. The top reaction holds 0.306, the
top five 0.746, the top ten 0.915; only 11 of 1,553 reactions clear an elasticity of 1e-2.
Against every other reachable target this is on the simple side but not freakish (28th
percentile of 984, median 11.96). The statistic that *is* extreme is distance: glycogen sits
**two metabolite steps** from D-glucose where the median target sits at five — the 1.5th
percentile. There is almost nothing between the source and the target for a response to
spread over.

**4. So the glg arm is not most of the signal, it is essentially all of it.** Splitting the
partition by who can reach it (`glg_arm_share.py`):

| | share of the probe |
|---|---|
| Eydallin's 86 hits | 0.685 |
| — of which the glycogen module | **0.684** |
| — of which the other 80 hits | **0.0015** |
| ASKA clones the screen scored as non-hits | 0.312 |
| reactions no clone in the library carries | 0.003 |

Twenty-five of the 86 are metabolic in the sense that matters here — they carry an
atom-mapped reaction in the curated GEM. Five of those 25 are glg genes and they hold
99.8% of what the screen can move. The other twenty — transaldolase, sulfite reductase,
glucosamine-6-phosphate deaminase, homoserine kinase and the rest — hold 0.15% between
them, and 61 of the 86 carry no atom-mapped reaction at all. The screen's own composition is
most of this: it is ~25 enzymes and ~60 regulators, transporters, prophage genes and
hypotheticals, and a stoichiometric model has no representation for csrA or rpoS whatever
its coverage.

The 0.312 the non-hits hold is not hidden signal either — it is the model's false positives,
led by `agp` (glucose-1-phosphatase) at **0.200**, the second-largest lever in the network
and a clone Eydallin built, assayed and scored as unchanged.

**5. Most of the modelled carbon arrives by running a catabolic enzyme backwards.** Of the
unit current delivered to glycogen, **0.568 arrives through glycogen phosphorylase**, 0.328
through glgA and 0.105 through debranching reversed. MetaNetX writes `MNXR145036` as G1P →
glycogen and the direction ensemble has zero votes on it, so it falls to an explicit ratio
of 1.0 and a symmetric edge is a free synthesis route. That is the polymer gap arriving at
the readout rather than at the bake.

`direction_sensitivity.py` supplies the direction the ensemble does not have, as a curve
rather than a setting. Pushing the phosphorylase toward degradation moves the delivered
carbon onto glgA — 0.328 → 0.765 at ratio 10, 0.965 at 100 — and reorders the top of the
ranking to glgA, glgC, malP, agp, glgP: the two glycogen-*excess* genes first, which is the
first time this benchmark has produced a sign-plausible ordering. **It changes no verdict.**
Re-sweeping the whole library at ratio 100 gives AUC 0.5397 against a size control of
0.5409, and 0.5130 with the module struck — the same two numbers, within noise of the
0.5362 / 0.5092 the bake's own ratios give. The direction gap decides *which* glg gene
leads; it does not create anything outside the module.

And the fix makes the structural point sharper, not softer: with the direction supplied the
effective number of levers falls from 6.13 to **3.18**, with glgA and glgC alone holding
0.77. Corrected, this target has one biosynthetic route, not three.

## What bounds this

**The de-novo channel has a specific pathology worth naming.** Twenty-seven unrelated ORFs
— secE, atpI, cyoA/B/C/D, nuoA, rpsJ, tolR among them — tie at a delta of 0.10577, and the
tie is one shared reaction: `MNXR145051`, glucose-1-phosphate adenylyltransferase, the
committed step of glycogen synthesis, assigned to all of them by the **ProtBERT lane** (29
clones carry that reaction library-wide, and 28 of the 31 rows asserting it are
`denovo_pbert`; the other three lanes contribute one each). A ribosomal protein and a
cytochrome oxidase subunit do not carry glgC's activity. That single promiscuous call fills
most of the de-novo channel's top thirty, and it is the concrete reason the de-novo AUC sits
*below* chance rather than merely at it. The four lanes are unioned here and unweighted,
deliberately — the lanes' `raw_score` is not normalised across them — so this is what an
unfiltered de-novo channel costs.

**That pathology was fixed, and fixing it changed nothing.** `sweep_aska.py --min-lanes 2`
credits a clone with a reaction only when at least two of the four lanes assert it — an
annotation-confidence rule chosen on its own merits and reported whichever way it came out.
It does exactly what it was supposed to: `MNXR145051` survives on glgC alone (three lanes)
and all 28 spurious attributions drop, the tie block disappears, and mean reactions per
clone falls from 9.66 to 2.10, which also makes the de-novo channel size-comparable to the
curated one. The AUC rises from 0.4636 to **0.4922** — from below chance to chance — and
with the glycogen module struck it is **0.4706 with zero positives in the top fifty**. The
contamination was real; there was no signal underneath it.

Applying the same rule to the *background* was tried first and is a finding in its own
right: it disconnects glycogen outright, because the glycogen-synthesis step has only
single-lane support in the de-novo annotation. Under a confidence rule strict enough to
trust, the de-novo channel cannot reach the target at all. The filter is therefore
clone-side only, and the background stays whole.

**The curated channel's positives are 25 genes.** The atom-mapped AUC of 0.573 rests on
them, and 25 is small. It is reported with its p-value rather than as a result.

**Direction remains inexpressible and this does not change that.** The two-point probe is
monotone in every edge conductance (Rayleigh), so a ×2 fold can only raise the readout;
glycogen-*deficient* and glycogen-*excess* hits get the same operation and the same sign.
The classification framing was adopted precisely because the regression framing is closed
by a theorem. The live test for direction is the asymmetric ratio-skew perturbation
`graph_from_pairs` can already express, and it is not this.

**What the sweep does not test** is whether a different terminal pair, a different fold, or
a signed perturbation would do better. It tests the probe as this benchmark has been running
it, over the population the screen was actually run over, and that probe does not find these
genes.

## What this leaves behind

`data/fabfos/runs/aska/gpr/` is now a maintained ASKA population — both channels on the
shared eighteen-column GPR schema, one census row per clone with its b-number and its
Eydallin label, and every clone present including the ones that resolve to nothing. The
existing ASKA/FFA arm builds its own null pool ad hoc from the roster and a GenBank parse;
it now has a table to read instead.
