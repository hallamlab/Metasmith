# Would ECSPr find what Woodruff's SCALEs screens found?

**No, on all three arms.** Two were run first — a glucose-rooted sink panel over declared
metabolite axes (§ *The mechanistic arm*) and a whole-library classifier (§ *The classifier
arm*) — and both came back null. A third was added afterwards (§ *The ratio arm*), running
the protocol the eydallin benchmark settled: parse the screen into a `gof.csv` keyed on the
real expression host, attach reaction chemistry through a four-channel cascade, restrict to
genes whose reaction changes a carbon skeleton, and score a two-point conductance **ratio**
between competing metabolic fates rather than a single readout. It is null as well.

On the first arm, the mechanistic result that looked like a win does not survive its own
controls. ECSPr appeared to separate the production paper's rank-one hit from its stated
non-hit: g(glucose→trehalose) = 12.43 against g(glucose→betaine) = 0. Three controls, all
added after an adversarial review and all declared in the axis file, take that apart:

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

On the third arm the numbers land in the same place. Its best AUC is 0.5467 (p = 0.15)
against a reaction-count size control of 0.5347, on the population the method has a mechanism
for; on the confirmed clones it is 0.5823 against a size control of 0.5980, so reaction count
wins there too, exactly as it does above.

But the ratio arm's *mechanics* work, and that is worth separating from its statistics. Both
preconditions pass — every terminal is a reachable atom-mapped node, and a ×2 fold demonstrably
multiplies rather than unions — and the score is genuinely two-sided where a single-probe
readout cannot be: 566 of 981 solved genes raise the ethanol share, 367 lower it. The top of
the ranking is chemically exactly right (adhE +21.8%, ppc −14.3%, maeB −11.3%). It simply
does not correlate with what the screen measured.

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
companion paper ships a complete named workbook, and it is therefore the source of every
per-gene statistic in this report — on all three arms.

**Neither paper's fitness values may be joined to the other's.** The later paper recalculated
the earlier selections; the ranges differ sixfold and a trial join matched 337 of 4,103.

The third arm — § *The ratio arm* — straddles the two and says so on every page: it takes the
**companion paper's gene names and phenotype** because they are the only per-gene ones that
survive, and measures conductance on the **production paper's host** because LW06 is where
the production selection happened. It joins no fitness value to any other.

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

**The production screen's fitness cannot honestly be binarised**, and the code says so rather
than inventing a threshold: the companion's fitness > 1 cut applied here labels 85% of the
genome enriched, and the production paper quotes no threshold at all.

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

### Ranking on the eight axes, every AUC beside its size control

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

## The ratio arm

The eydallin benchmark settled a four-stage protocol and this arm runs it here unchanged in
shape: a `gof.csv` keyed on the real expression host, reaction chemistry attached through a
four-channel cascade, a restriction to genes whose reaction changes a carbon skeleton, and a
score that is the **ratio of two competing two-point conductances** rather than one readout.

    ratio = C(pyruvate MNXM23 → ethanol MNXM1108092) / C(pyruvate MNXM23 → oxaloacetate MNXM46)

**Why a ratio.** Effective conductance is monotone in every edge conductance (Rayleigh), so
a ×2 fold on any gene can only push a single readout *up*. A signed phenotype — this gene
helps production, that one hurts it — is inexpressible in one such number, and three
sessions of correlation against one said so before the ratio existed. Dividing two competing
fates of the *same* source metabolite is what makes the score two-sided: pyruvate is the
branch point LW06 was engineered around, ethanol is where it is supposed to send carbon, and
oxaloacetate is the anaplerotic fate that competes for it. A gene raising both legs equally
scores zero; only a gene that shifts the split registers. The two conductances are
**independent solves on one graph**, never two readings of one, and that doubles the
per-gene cost on purpose.

### Which population `gof.csv` describes, and why it is not the production paper's

The production screen ranked 4,103 genes and **4,100 of them are anonymous** — its workbook
survives in the supplementary PowerPoint only as a dead OLE link, so `parse/decode_mmc1_chart.py`
recovers the distribution and not the names. Exactly three genes are nameable, and only
because the prose names them: betA (rank 2243), betB (1993), betI (1052). A gene-keyed table
of three rows is not a screen.

So `gof.csv` keys on the **companion tolerance paper** instead — the same SCALEs library, the
same lab, screened in BW25113 Δ*recA*, with a complete named workbook: 4,225 genes, each with
a b-number and two fitness values. What survives the two screens' refusal to be joined is the
*library*: both papers screen the same genomic BW25113 fragment collection, so the gene
population is shared even though the phenotype is not.

**The consequence is stated rather than buried: the phenotype is the companion paper's and
the host is the production paper's.** The conductance is measured on LW06 because LW06 is
the organism the production selection ran in; the only per-gene phenotype either paper leaves
nameable is ethanol *tolerance* in a different strain and medium. Nothing here turns one into
the other, and no AUC below is a statement about production fitness. That mismatch is this
arm's largest limit, and it is a property of the surviving data rather than of the method.

| | |
|---|---|
| rows in `gof.csv` | **4,225** — every screened gene keeps a row |
| with a BW25113 locus tag and product | 3,891 |
| unresolved (blank identity, row kept) | 334 — BW25113 deletes araA/araB/araD, and 265 of its 4,400 proteins carry no gene symbol at all |
| phenotype column | `fitness_30gL_ethanol`, the companion paper's own freq_final/freq_initial |

### The four-channel cascade, and where it is weaker than eydallin's

`mnxr_mapping_method` says which channel resolved each gene; a blank `mnxr` and a blank
`no_mapping_reason` never both happen. The rules — `carbon_bond_change`, `legible_equation`,
`pick_primary`'s `in_atom_universe`-first tie-break, `denovo_gapfill` — are **imported from
`benchmarks/eydallin/parse/reaction_chemistry.py`, not copied**. Two copies of those rules is
how two benchmarks in one tree come to disagree about what `breaks` means.

| channel | genes | note |
|---|---|---|
| `GEM` | 1,503 | iML1515's GPR against the screened genes; 644 of them nominate more than one reaction |
| `LLM review` | 3 | eydallin's own hand-picked table, inherited verbatim and **not extended** |
| `denovo` | 104 | ≥2 independent projection methods agree on one reaction, pool ≤ 10 |
| blank | 2,615 | with a stated `no_mapping_reason` |

**Two places this arm is honestly weaker than the benchmark it copies.**

*The LLM-review channel was not extended.* Hand-reviewing a 4,225-gene screen is not
something that was done, and a partial hand-review would put curator attention exactly where
the reader cannot see it. Six eydallin entries are inherited; three of them are genes this
screen also measures and the curated GEM also misses. The rest of the miss list is left to
the mechanical channels.

*`no_mapping_reason` is mechanical here.* Eydallin's eleven-way functional split
(`regulatory`, `transport`, `dna_repair`, …) is a hand adjudication of 52 genes against each
gene's own literature. It does not scale to 2,700, and inventing it by keyword would be
exactly the fabrication the column exists to prevent. So this column reports **which channel
failed**, not why the biology has no reaction — `no_candidate_in_either_channel` (1,485),
`denovo_candidates_disagree` (1,032), `denovo_pick_is_nonspecific` (98). That is a weaker
statement than eydallin's and is labelled as one.

**The third category is eydallin's hand rejection, mechanised.** Eydallin refused `ucpA`,
`yabI`, `ylcG` and `yqjA` because a strong de-novo hit on a catalytic *fold* is not evidence
a gene does that fold's specific reaction. At this scale the non-specificity has a mechanical
signature: the same reaction gets nominated for many unrelated genes. Run unfiltered, the
cascade hands `MNXR153054` (`ADP + phosphate → ATP`) to **35** genes, a generic DNA
polymerase to 13 and a generic RNA polymerase to 5 — the identical artifact, at scale. A
gene-specific hit is nominated once, so a gap-fill reaction claimed by more than one gene is
dropped from every one of them.

`carbon_bond_change` is read off the baked atom map by connected components, never from the
equation string; the two give different answers and only the first is a measurement. Over
`gof.csv`: `no_change` 989, `creates` 169, `both` 144, `breaks` 126, unknown 2,797. **439
genes change a carbon skeleton**, and that is the restricted population below.

### Both preconditions pass, and they are the reason the null is readable

*Reachability.* All four metabolites resolve as nodes of the LW06 atom-mapped carbon graph —
pyruvate `MNXM23`, ethanol `MNXM1108092`, oxaloacetate `MNXM46`, D-glucose `MNXM1364061` —
and both legs are strictly positive. There is no `g = 0` anywhere in this arm and no
not-a-node. The distinction matters here because the sink panel above already showed the two
are different failures: Kdo₂-lipid A returns a hard zero with `sink_is_node=True`, glucose
1-phosphate is not a node at all, and only the second is uninterpretable. Neither happens
here. `sweep_woodruff_ratio.py` raises on a missing terminal rather than reporting it as zero.

*Fold semantics.* **Every gene in this library is a gene the host already carries, so an
implementation that added a clone's reactions to the background as a set would be the
identity map and the entire sweep would return flat — which reads as a null result rather
than as a bug.** `--fold-check` refuses to proceed unless multiplication moves the ratio and
union does not:

| `adhP` (ALCD2x, ALCD19 — the two alcohol dehydrogenase reactions LW06 keeps after Δ*adhE*) | ratio | Δ |
|---|---|---|
| fold 1.0 | 0.111255142 | +0.000000% |
| fold 2.0 | 0.134818939 | **+21.179962%** |
| fold 4.0 | 0.150798041 | +35.542536% |
| set-union control | 0.111255142 | **+0.000000%** |

The union control is exactly zero for every gene tested. The distinction is real and the
sweep is on the right side of it.

### The measurement

**This arm runs the curated channel only**, unlike the two above, which each report curated
and de-novo side by side. The de-novo background is 10,938 reactions and the ratio costs two
solves a gene; it was not run here, and no de-novo column below is missing — there is simply
no de-novo ratio number in this tree yet. What the classifier arm already established about
that channel — that it sees 89% of the screen instead of 23% and still ranks at chance — is
the reason a curated-only null is not merely a reach result.

Background is the `in_atom_universe` filter at uniform 1.0 — **1,409 reactions** on
`e_coli_lw06`'s edited iML1515, this benchmark's convention since `sweeps/sweep_scales.py`,
not the eydallin sweep's unfiltered dict. Nothing in this arm is comparable to an eydallin
number.

    C(pyruvate → ethanol)      0.698176795
    C(pyruvate → oxaloacetate) 6.275456419
    host ratio                 0.111255142

4,225 genes, 981 atom-mapped and solved, 3,244 exact zeros. That is the classifier arm's 985
curated atom-mapped genes less the four the next paragraph refuses; the population is
otherwise the same one. Every row reads `finite`; no gene severed either terminal at fold 2.

**Three reactions are refused rather than restored.** LW06 is BW25113 with ldhA, ackA,
frdABCD and adhE knocked out, while the clone GPR table was built against unedited iML1515 —
so `ldhA`, `frdA`, `frdB` and `frdD` nominate LDH_D, FRD2 and FRD3, reactions this host does
not have. Folding a weight the background does not carry is not dosage, it is topology, and
the fold semantics cannot express it. Restoring FRD2/FRD3 would be worse: its GPR rule is
`frdA and frdB and frdC and frdD` and all four are deleted, so no single-gene clone
reconstitutes the complex. Those four genes fall through to the exact-zero population with
the reason recorded in the manifest.

### Ranking on the ratio, every AUC beside its size control

|population | positive set | n | AUC library | size ctrl | AUC atom-mapped | size ctrl |
|---|---|---|---|---|---|---|
| all genes | tolerant_15 | 4,225 | 0.4854 (p 0.81) | 0.4902 | 0.5085 (p 0.43) | 0.5477 |
| all genes | tolerant_30 | 4,225 | 0.4998 (p 0.51) | 0.4973 | 0.5212 (p 0.23) | 0.5156 |
| all genes | **confirmed clones** | 4,225 | 0.5823 (p 0.087) | **0.5980** | 0.3600 (p 0.86) | 0.5565 |
| skeleton-changing | tolerant_15 | 439 | 0.4412 (p 0.80) | 0.4559 | 0.5097 (p 0.45) | 0.5341 |
| skeleton-changing | **tolerant_30** | 439 | **0.5467 (p 0.15)** | 0.5347 | 0.5226 (p 0.31) | 0.5093 |
| skeleton-changing | confirmed clones | 439 | *1 of 12 survives — no ranking* | | | |

**Nothing clears significance, and restricting to the skeleton-changing genes does not
rescue it.** The best number anywhere is 0.5467 at p = 0.15, and its size control is 0.5347 —
the improvement over reaction count alone is 0.012 of AUC. On the confirmed-clone set the
size control **beats** ECSPr again (0.5980 against 0.5823), which is the same confound that
sank the classifier arm above and the ASKA/FFA arm before it. Precision at the top is at or
below expectation everywhere except one cell (5 of the top 25, expected 2.6, p = 0.106,
module-struck).

The tautology control strikes 84 genes carrying a reaction incident to any of the three
terminals — defined mechanically off the atom-pair table, so it cannot be widened per arm.
Striking them moves nothing, and the reason is the one the classifier arm already found:
almost none of the module genes are genes the screen scored as positives. The tautology
itself is plainly there — the probe ranks **adhE first and adhP second**, the two alcohol
dehydrogenases sitting on the numerator's last step — exactly as ECSPr found the glycogen
module it grounded at in the eydallin benchmark.

### What the ratio does deliver

**It is two-sided, and that was the point.** Of 981 solved genes, 566 raise the ethanol share
and 367 lower it; among the 110 atom-mapped tolerant_30 positives, 67 up and 40 down. A
one-probe conductance cannot produce that split at all.

**The top of the ranking is chemically correct.** adhE +21.8% and adhP +21.2% (the alcohol
dehydrogenases), ppc −14.3% (the model's single PEP carboxylase, the largest lever on the
denominator), maeB −11.3% and mqo −5.2% (the malate route to oxaloacetate), ppsA −6.7% (PEP
synthase), pckA −2.6%, the citrate lyase subunits −3.4%. Every one of those is a gene that
genuinely moves the pyruvate branch point, and the sign is right in every case. The probe is
measuring what it claims to measure.

**The response is concentrated.** Median |Δratio| across solved genes is 0.0013%; only 116
genes move it past 0.1% and 38 past 1%. So the ranking's body is solver-scale noise ordering
that the AUC nonetheless consumes — the same defect the classifier arm's negative-delta table
documents.

**Two real positives sit in the extreme tail.** maeB (tolerant_30, Δ −11.26%) is beaten by 3
of 3,738 measured negatives, and glpQ (tolerant_30, +4.70%) by 6. A threshold at either would
carry an FPR under 0.2%. But with 487 positives in the set, two in the top six is not a
screen — it is what 487 draws from a 4,225-gene population produce.

### Where I7 bites

**iML1515 carries no pyruvate carboxylase.** `pyc` has zero GPR rows in this host. Every
pyruvate → oxaloacetate route is indirect — through PEP (`ppsA`, or `pykA`/`pykF` run
backwards, then `ppc`, the single PEP-carboxylase row) or through malate (`maeA`/`maeB`, then
`mdh` or `mqo`). The denominator therefore measures a diffuse anaplerotic capacity spread
over roughly eight reactions rather than the sensitivity of one committed enzyme, which is
what the eydallin benchmark's glycogen → pyruvate arm had. The ratio still solves and both
legs are finite; the null is correspondingly less sharp than eydallin's, and a reader should
not treat "the ratio did not rank these genes" as "doubling the committed step was
invisible", because there is no committed step here to double.

## Limits, stated rather than discovered later

**No version of ECSPr in this tree takes medium as an input.** The conditions schema carries
a media column that nothing interprets. This is the ceiling on every arm and it is why the
betaine result cannot mean what it appeared to.

**The ratio arm scores a production-host measurement against a tolerance-strain phenotype**,
because the production screen's per-gene identities do not survive on disk. That is not a
mismatch chosen for convenience — § *Which population `gof.csv` describes* has the whole
argument — but it is the single largest reason a null there is weaker evidence than the
classifier arm's.

**The ratio's denominator has no committed enzyme.** iML1515 has no pyruvate carboxylase, so
pyruvate → oxaloacetate runs through PEP or malate across roughly eight reactions. § *Where
I7 bites*.

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
| declared axes + controls | `data/fabfos/originals/benchmarks/woodruff/gof_scales.tsv` |
| host tables, GEM and de-novo | `data/fabfos/runs/e_coli_{bw25113,lw06}/gpr/` |
| per-gene tables + census | `data/fabfos/runs/woodruff_clones/gpr/` |
| mechanistic arm | `data/fabfos/runs/woodruff_clones/ecspr/sink_panel_{gem,denovo}_C.tsv`, `SINK_PANEL.md` |
| classifier sweeps and scores | `data/fabfos/runs/woodruff_clones/ecspr/scales_sweep_*`, `SCORE_*` |
| ratio arm: gene table + chemistry | `data/fabfos/runs/woodruff_clones/parse/gof/gof{,_reaction_edges}.csv` |
| ratio arm: sweep and score | `data/fabfos/runs/woodruff_clones/ecspr/woodruff_ratio_sweep_*`, `SCORE_woodruff_ratio_sweep_*` |

Scripts are in `research/fabfos/benchmarks/woodruff/`, grouped as `parse/`, `gpr_build/`,
`panels/` and `sweeps/`. The ratio arm is `parse/build_gof_table.py` →
`parse/build_gof_reactions.py` → `sweeps/sweep_woodruff_ratio.py` →
`sweeps/analyse_woodruff_ratio_sweep.py`; run `sweep_woodruff_ratio.py --fold-check` first,
which is cheap and refuses on either precondition. Host identity checks are
`src/fabfos/build_references/check_lw06_identity.py` and `derive_lw06_denovo.py`.

**The ratio arm's rules are imported from the eydallin benchmark, not copied**, and that
coupling is deliberate: `benchmarks/eydallin/parse/reaction_chemistry.py` owns
`carbon_bond_change`, `pick_primary`, `legible_equation` and `denovo_gapfill`, and
`benchmarks/eydallin/bake_pairs.py` owns the decoded atom-pair and direction tables —
the same import `sweeps/sweep_scales.py` already makes. Moving either file breaks this
benchmark.

The **study cohorts keep their `scales_{tol,prod}` names**. Those strings are `dataset`
values inside the tier-built extractions and `condition_id` prefixes throughout every GPR
parquet on disk; renaming them is a data migration, not a rename, and SCALEs is the library's
own name in both papers regardless of which author the benchmark is filed under.

The de-novo annotation ran on **sockeye** (fir carries none of the four lane databases), and
its workflow **exited green having run 7 of 9 steps** — a lineage key was dropped on one
lane's outputs, so the join feeding the last two steps matched nothing and Nextflow ended
early without error. The table was assembled from the four completed lanes instead. The
de-novo sweep ran on **fir** as 64 array tasks: 28 h of single-core work in 15 minutes of
wall clock.
