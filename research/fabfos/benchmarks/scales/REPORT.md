# Would ECSPr find what SCALEs found?

**No, on both arms — and the mechanistic result that looked like a win does not survive its
own controls.** The headline finding was that ECSPr separates the production paper's
rank-one hit from its stated non-hit: g(glucose→trehalose) = 12.43 against
g(glucose→betaine) = 0. Three controls, all added after an adversarial review and all
declared in the axis file, take that apart:

- **D-maltose — trehalose's α-1,4 structural isomer, named in neither paper — scores
  29.62, 2.4× higher.** The property the trehalose axis is high for is being two glucoses.
- **The separation exists only on the curated channel.** On the de-novo channel, which sees
  89% of the screen instead of 23%, betaine scores 6.38 and choline 11.44. Same organism,
  same question, opposite answer.
- **Two metabolites *E. coli* certainly makes from glucose are also invisible to the curated
  channel** — Kdo₂-lipid A scores exactly 0 while passing every check the panel applies, and
  glucose 1-phosphate is not even a node — and the de-novo channel reaches both (20.37 and
  54.14). So `sink_is_node=True ∧ g=0` never licensed the phrase "genuine disconnection"
  that the earlier draft of this report leaned on, and the curated zeros are a property of a
  small basis rather than of the organism.

As a classifier the method is at chance on both channels: 24 axis × positive-set
combinations per channel, best atom-mapped AUC 0.541 (p = 0.081) curated and 0.626
(p = 0.065) de-novo, neither surviving the multiplicity, and the curated near-signal is
beaten by reaction count alone.

The sharpest single result is a mechanistic failure rather than a statistical one.
**serA** — fitness 13.78 at 15 g/L and 123.90 at 30 g/L, one of the strongest genes in the
screen, a confirmed rebuilt clone, three atom-mapped reactions, and the `glc_to_serine`
axis was declared *for it* — moves glucose→serine conductance by **1.65 × 10⁻⁶ on a base of
2.72**, rank 717 of 985. Doubling the committed step of serine biosynthesis is invisible to
the probe, on the axis chosen to see it. Whatever this method measures, it is not
sensitivity of a pathway to its own committed enzyme.

This is a negative result about the method, not about the study. The screens are sound, the
extractions reproduce the papers' own arithmetic, and the controls are the ones designed to
break the claim.

## What was measured

Two Gill-lab papers, both using SCALEs (multiScalar Analysis of Library Enrichments) to
assign a fitness value to every gene in the *E. coli* genome from a plasmid overexpression
library.

| | companion — the classifier arm | production — the mechanistic arm |
|---|---|---|
| citation | Metab Eng **15**:124–133 | Metab Eng **17**:1–11 |
| host | BW25113 ΔrecA | LW06 (engineered ethanologen) |
| medium | MOPS minimal + 2 g/L glucose | AMX minimal |
| readout | fitness at 15 and 30 g/L exogenous ethanol | production gene fitness, batch 8 |
| genes | **4,225, all named** | 4,103, of which **3 can be named** |

The production paper's gene symbols lived in a workbook that survives in its supplementary
PowerPoint only as a dead OLE link, so its distribution is recoverable but anonymous. The
companion paper ships a complete named workbook, and that is the arm with the statistics.

**Neither arm's fitness values may be joined to the other's.** The later paper recalculated
the earlier selections; the ranges differ sixfold and a trial join matched 337 of 4,103.

### How falsifiable each decode actually is

The companion extraction reproduces the paper's own arithmetic as a **refusal**: 158 genes
above fitness 1 at 15 g/L and 487 at 30 g/L. It passed on the first read with no tuning, and
an independent re-decode with a different library (openpyxl against the checked-in stdlib
decoder) reproduced every gene, both fitness columns and the b-number column **bit-exactly**,
including the 158/487.

The production decode is **weaker than the earlier draft of this report claimed, and the
difference matters.** It recovers rank from row position, asserting 0 monotonicity
violations across 4,103 rows, and checks that ranks 1052/1993/2243 carry particular values.
But the paper quotes **ranks**, not values — the values in `ANCHORS` are annotated
"quoted/implied by the prose". Where they are implied, the check verifies the decode against
itself. It confirms internal consistency and the absence of an off-by-one; it is **not** an
external falsification, and "both decodes are falsifiable" was wrong. One is.

**The production arm cannot honestly be binarised**, and the code says so rather than
inventing a threshold: the companion's fitness > 1 cut applied here labels 85% of the genome
enriched, and the production paper quotes no threshold at all.

## The hosts, computed rather than assumed

The production paper states LW06's parentage verbatim — *"BW25113 ΔldhA ΔackA ΔfrdABCD
ΔadhE attTn7::PLlacO-1 pdcZm adhBZm"* — so its edit list is a **superset** of BW25113's.
Both were computed by evaluating every GPR rule in iML1515 twice and keeping only reactions
that go dark: BW25113 loses 7 (`ARAI LACZ LYXI RBK_L1 RMI RMK RMPA`), LW06 three more
(`FRD2 FRD3 LDH_D`).

**Seven deleted genes, three dark reactions.** `ACKr` survives on purT/tdcD, both alcohol
dehydrogenases survive adhE's deletion on adhP, `ACALD` survives on mhpF — so **iML1515 says
LW06 can still make ethanol without its engineered pathway.** Asserted in
`check_lw06_identity.py` so it cannot drift.

BW25113's genome (GCF_050858555.1) was independently confirmed to be the strain's own
sequence: `lacZ`, `araB` and `rhaB` return no record in its proteome, as the Keio parent
genotype requires.

## The mechanistic arm

Effective conductance from D-glucose to each declared sink. Targets were declared before any
number existed; **controls were added after an adversarial review and are labelled as such**
— they make existing axes interpretable and add no new claim.

| axis | role | expected | curated | de-novo |
|---|---|---|---|---|
| trehalose | target | + | 12.4289 | 44.68 |
| T6P | target | + | 17.0938 | 72.44 |
| **betaine** | target | 0 | **0.0** | **6.38** |
| **choline** | target | 0 | **0.0** | **11.44** |
| serine | target | + | 2.7212 | 19.24 |
| ethanol | target | − (stated control) | 0.9804 | 7.65 |
| acetaldehyde | target | − (stated control) | 1.9233 | 16.16 |
| glycogen | target | + (tie to eydallin) | 5.6578 | 6.44 |
| **D-maltose** | **control** | + | **29.6205** | **90.60** |
| alpha-maltotriose | control | + | 16.8890 | 55.53 |
| glucose 1-phosphate | control | + | *not a node* | 54.14 |
| **Kdo₂-lipid A** | **control** | **+** | **0.0** | **20.37** |

Every solved row on both channels reports `converged=True` (33 of 36 curated, 36 of 36
de-novo); the three curated blanks are glucose 1-phosphate, which is not a node of the
curated carbon graph and is reported as uninterpretable rather than as a zero.

**The last two control rows are the whole argument.** Kdo₂-lipid A and glucose 1-phosphate
are both invisible to the curated channel — one returns a hard zero, the other is not even a
node — and the de-novo channel, on the same organism with 7.7× the reactions, reaches them
at 20.37 and 54.14. Two metabolites *E. coli* certainly makes from glucose. So the curated
channel's zeros are demonstrably a property of a small basis, and betaine and choline sit in
exactly that set: **0 curated, 6.38 and 11.44 de-novo.** The mechanistic "result" is
indistinguishable from the coverage gap that the controls expose.

### Why the trehalose/betaine separation is not the result it looked like

**The size control.** D-maltose is trehalose's α-1,4 isomer and appears in neither paper. It
scores 29.62 against trehalose's 12.43. Solving glucose→every one of the 1,019 host
metabolite nodes, every metabolite outscoring trehalose is a glucose oligomer or a glucose
phosphate, and Spearman(sink carbon count, g) = 0.44 across the panel. Trehalose does sit at
the 99th percentile of nonzero sinks — but so does any disaccharide of the source, and the
two stated negative controls (ethanol and acetaldehyde, 2 carbons each) land exactly where
any 2-carbon sink lands. The classifier arm prints a size control beside every AUC; this arm
had none until it was made to.

**The coverage control, and it is the one that settles the arm.** `MNXM730505`
(Kdo₂-lipid A) returns **g = 0 on the curated channel with `sink_is_node=True` and
`converged=True`** — while its precursor scores 0.075 and its successor 0.749 in the *same
biosynthetic chain*, and every reaction of that route is `in_atom_universe` in this host.
*E. coli* plainly makes Kdo₂-lipid A from glucose. The de-novo channel scores it **20.37**.
Glucose 1-phosphate tells the same story harder: not even a node curated, **54.14** de-novo.

So the node check rules out a missing id and does **not** rule out unmapped carbon across
the intervening chain — the dominant failure mode on large molecules. A zero on this panel
means "no atom-resolved carbon route in **this basis**", and the de-novo channel demonstrates
that the curated basis produces such zeros for metabolites the organism certainly makes.
Betaine and choline are in exactly that set. 68 of 1,019 curated host metabolites return
zero, so a zero still carries information — just not the information the earlier draft
claimed.

**The channel control.** On the de-novo background betaine scores 6.38 and choline 11.44.
The separation the headline rested on exists on one channel and not the other.

### What does survive

The betaine zero on the curated channel is not an artifact of the atom-universe filter, and
that was worth checking because the filter removes transport and choline uptake *is*
transport. Against the unfiltered background (all 2,270 host reactions) betaine and choline
are still exactly 0, with trehalose moving only 12.4289 → 12.6444. Compartments are not
modelled anywhere in this tree, so a transport step is a self-edge and carries nothing.
Choline is not an atom-map orphan either: 122 carbon reactions touch it bake-wide, and in
this host its only in-universe neighbours are GPDDA1 and CHOLD — a real local cut. Notably,
at the *metabolite* level choline **is** in glucose's connected component via
glycerophosphocholine; only the atom-resolved graph separates them, which is the method's
resolution doing genuine work.

### On "for the paper's own stated reason"

The earlier draft said ECSPr separates these two "for the reason the paper itself gives".
**That attribution is not evidenced in this repository.** The only verbatim quotation of the
production paper recorded anywhere here states the fact — that the betaine-biosynthesis genes
were not enriched — and not the reason. Every "the minimal medium supplies no choline" in
this tree is the benchmark's own gloss. It is a chemically sound gloss, and neither PDF is
text-extractable with the tooling present, so this says the repo records no such quote, not
that the paper omits it.

It matters because nothing here takes medium as an input: ECSPr returns the same 0 in a
choline-replete medium, where the bet operon *would* be selectable. The separation therefore
has no discriminative power over the very condition the explanation turns on.

### The engineered insertion

Exactly one of LW06's two Tn7 reactions is new to the **curated** background — MNXR103379,
pdcZm — confirming the `in_base=True` annotation asserted for adhBZm before any number
existed. It raises acetaldehyde 1.92329 → 2.34295 (+21.8%) and ethanol 0.98039 → 1.07889
(+10.0%), leaves every other axis at solver noise, and does not perturb the zeros. On the
**de-novo** background it adds nothing at all: both reactions are already there, because the
four annotation lanes nominate MNXR103379 from *E. coli* ORFs on their own.

## The classifier arm

All 4,225 measured genes, each gene's reactions doubled, eight axes, ranked by change in
conductance. Genes with no atom-mapped reaction are exact zeros — never solved, but each
gets its row, because a gene the screen measured and the method cannot see is a measured
negative, not an absence.

### Reach, reported before any ranking statistic

| channel | measured | resolved in the host model | atom-mapped |
|---|---|---|---|
| curated (BW25113-edited `iML1515`) | 4,225 | 1,503 | **985 (23.3%)** |
| de-novo (BW25113 proteome, 4 lanes) | 4,225 | 3,891 | **3,779 (89.4%)** |

(The curated column is resolution against the **BW25113-edited** table, not plain iML1515:
araA, lacZ, rhaA, rhaB and rhaD are iML1515 genes this host deletes, and the census records
them as unresolved. An independent re-derivation reaches 1,517 against this build's 1,503,
the 14-row gap being those 5 deletions plus 9 rows where only the sheet's *wrong* b-number
is in the model — a path this build deliberately refuses.)

The fraction atom-mapped is **flat across every phenotype class on both channels**:

| phenotype | curated | de-novo |
|---|---|---|
| neutral | 23.5% | 89.3% |
| tolerant at 15 g/L only | 20.3% | 88.6% |
| tolerant at both | 21.5% | 91.1% |
| tolerant at 30 g/L only | 22.8% | 90.7% |

Fisher gives OR 0.86–0.95 curated (p = 0.50, 0.73) and 1.18 de-novo (p = 0.35). The
tolerance genes are no more metabolic than the average gene in the genome.

**The de-novo channel removes the reach excuse.** It sees 89% of the screen and **all 12**
confirmed-clone genes (curated sees 5), and still ranks at chance. So the null is about the
ranking, not the coverage.

### The confirmed clones

Nine clones individually rebuilt and retested, 12 genes. Curated, five carry an atom-mapped
reaction — fadE (8 reactions), serA (3), lpcA (1), arnB (1), arnC (1) — and seven resolve to
nothing: tilS, yaeJ, yaeQ, yhfT, yhfU, yicE, zur. A tRNA-lysidine synthetase, a
peptidyl-tRNA hydrolase, an NCS2 transporter, a Zur repressor and two unknown membrane
proteins are not reaction-carrying genes, and no conductance probe can rank them.

But the de-novo channel *does* see all twelve, and its AUC on them is 0.55–0.63 with p
between 0.065 and 0.29. So "the method cannot see them" explains the curated arm and not the
benchmark.

### Ranking, every AUC beside its size control

| channel | axis | positive set | AUC library | size ctrl | AUC atom-mapped | size ctrl | p |
|---|---|---|---|---|---|---|---|
| curated | T6P | tolerant_30 | 0.5016 | 0.4967 | **0.5408** | 0.5151 | 0.081 |
| curated | trehalose | tolerant_30 | 0.4996 | 0.4967 | 0.5356 | 0.5151 | 0.111 |
| curated | trehalose | **confirmed** | 0.5226 | **0.5975** | **0.3031** | 0.5561 | 0.936 |
| de-novo | choline | **confirmed** | 0.6616 | 0.5331 | **0.6264** | 0.4778 | 0.065 |
| de-novo | betaine | **confirmed** | 0.6569 | 0.5331 | 0.6261 | 0.4778 | 0.066 |
| de-novo | trehalose | tolerant_30 | 0.5044 | 0.5101 | 0.4965 | 0.5033 | 0.596 |
| either | betaine, choline (curated) | any | — | — | — | — | *no ranking exists* |

**Nothing clears significance** at 24 combinations per channel. Two observations sharpen the
null:

**Where anything looks like signal on the curated channel, reaction count explains it
better.** On the confirmed set the size control reaches 0.5975 (p = 0.057) against ECSPr's
0.5226 — the closest thing to a significant result in the curated arm is the confound.

**On the curated atom-mapped subset the confirmed clones rank *below* chance** (0.30 on
trehalose, 0.33 on acetaldehyde) while reaction count puts them at 0.556. Among genes the
method can see, the genes that actually improved tolerance are multi-reaction enzymes that
are not on any path to the panel's sinks.

The one place ECSPr does beat its size control is the de-novo confirmed set on the
**betaine and choline** axes (0.626 against 0.478, p ≈ 0.065). Those are the axes on which
the paper reports a *non*-hit, so a signal there is not a biological result; at p = 0.065
across 24 combinations it is not a statistical one either.

**Two curated axes carry no ranking at all** — betaine and choline have base conductance
exactly zero there, so every gene's readout is identically zero. Reported rather than
dropped.

### The controls, including one that turns out to have no power

**Tautology control.** Each axis's own module — genes carrying a reaction incident to that
axis's sink, defined mechanically off the pair table so it cannot be widened per axis — is
struck and every figure recomputed. Striking it changes nothing, and the earlier draft drew
the wrong conclusion from that.

The reason it changes nothing is that **module ∩ positives = 0 on every axis but one**
(acetaldehyde/tolerant_30 = 1). Striking ≤17 rows of 985 containing ≤1 positive **cannot**
move an AUC. The control has no power; that is a different statement from there being no
tautology. And there *is* a tautology: the probe ranks the trehalose module **1st, 2nd and
3rd** (treA 6.000, treF 6.000, otsB 2.361), and 5 of the glycogen module in its top ten —
the same 5/10 that the eydallin benchmark reported as its own failure case. ECSPr finds the
module it grounds at, exactly as before; it is invisible to this control only because these
particular module genes are not genes the screen scored as positives.

**Size control.** Every AUC above is printed beside the same AUC on reaction count alone.

**Solver-jitter floor — the earlier draft's version of this was a tautology.** "Fold 1.0
returns bit-exact zero" cannot detect jitter: `{**base_w, **{r: base_w[r] * 1.0 for r in
rxns}}` *is* `base_w`, so that test detects nondeterminism and nothing else. The real floor
is visible in the shipped sweep as **negative deltas, which Rayleigh monotonicity forbids** —
raising an edge conductance cannot lower the readout:

| channel | axis | negative deltas | most negative | median positive delta | positives below \|max neg\| |
|---|---|---|---|---|---|
| curated | trehalose | 53 / 985 | −2.77e-10 | 4.47e-08 | **76** |
| curated | T6P | 30 / 985 | −7.99e-10 | 2.47e-07 | **65** |
| curated | serine | 34 / 985 | −8.88e-16 | 7.03e-05 | 22 |
| de-novo | trehalose | 36 / 3,779 | −5.14e-09 | 1.69e-06 | **227** |
| de-novo | T6P | 60 / 3,779 | −1.23e-08 | 4.88e-06 | **199** |

So on the trehalose and T6P axes roughly the bottom 6–8% of the ranking is noise ordering
that the AUC nonetheless consumes. Most other axes sit at one ULP and are harmless.

**Convergence.** `Solution.converged` was never read by either driver and now is. All 33
solved panel rows converged; the sweep's exact zeros are never solved and so report nothing.

**Sharding.** The curated sweep was rerun as 8 shards and reproduces the unsharded run
**bit for bit** after fixing a real reproducibility defect: `pandas.read_csv`'s default float
parser is not round-trip exact — a file correctly containing `1.9233290440672492` reads back
as `…88` under `float_precision` of `None`, `'high'` and `'legacy'` alike. Only
`'round_trip'` is exact. It cannot move a rank, but it made a reproducibility claim false by
accident. Merge refuses a missing shard *and* a present-but-short one.

**Resampled small-null cross-check.** 200 draws of 100 random genes against the confirmed
set: AUC 0.5214 ± 0.0218, agreeing with the exhaustive sweep.

## Limits, stated rather than discovered later

**No version of ECSPr in this tree takes medium as an input.** The conditions schema carries
a media column that nothing interprets. This is the ceiling on both arms and it is why the
betaine result cannot mean what it appeared to.

**A zero is a claim about the basis.** The Kdo₂-lipid A control is in the panel so that
every future reader meets this limit as a measurement rather than discovering it.

**The two channels' conductances are not comparable to each other** — backgrounds of 1,412
and 10,938 reactions. Only ranks within a channel are. And **nothing here is comparable to
the eydallin numbers**, which used an unfiltered weight dict; the glycogen axis is present to
make that scale difference visible, not to be compared across benchmarks.

**The trehalose genes sit at rank 1 in the production selection and near the bottom of the
companion one** — same genes, same lab, different host and medium. Nothing here can express
that difference.

## Provenance

| what | where |
|---|---|
| extractions | `data/fabfos/benchmarks/_extractions/scales_{tol,prod}/` |
| studies (tier-built) | `data/fabfos/benchmarks/scales_{tol,prod}/` |
| declared axes + controls | `data/fabfos/originals/benchmarks/scales/gof_scales.tsv` |
| host tables, GEM and de-novo | `data/fabfos/runs/e_coli_{bw25113,lw06}/gpr/` |
| per-gene tables + census | `data/fabfos/runs/scales/gpr/` |
| mechanistic arm | `data/fabfos/runs/scales/ecspr/sink_panel_{gem,denovo}_C.tsv`, `SINK_PANEL.md` |
| sweeps and scores | `data/fabfos/runs/scales/ecspr/scales_sweep_*`, `SCORE_*` |

Scripts are in `research/fabfos/benchmarks/scales/`; host identity checks are
`src/fabfos/build_references/check_lw06_identity.py` and `derive_lw06_denovo.py`.

The de-novo annotation ran on **sockeye** (fir carries none of the four lane databases), and
its workflow **exited green having run 7 of 9 steps** — a lineage key was dropped on one
lane's outputs, so the join feeding the last two steps matched nothing and Nextflow ended
early without error. The table was assembled from the four completed lanes instead. The
de-novo sweep ran on **fir** as 64 array tasks: 28 h of single-core work in 15 minutes of
wall clock.
