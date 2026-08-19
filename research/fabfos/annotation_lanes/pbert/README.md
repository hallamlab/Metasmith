# The pbert lane: where it was validated, and why our numbers disagree with that

The ProteinBERT transfer lane was benchmarked for precision and recall before it was
wired anywhere. That work is **not in this repository and not in the `fabfos` project** —
it lives in `scadc` on **capella**, which is why it does not turn up from here. This file
records where it is, what it measured, and the discrepancy between its result and what
this lane does in the Eydallin GPR build.

## Where it is

Two scopes on capella, `/home/tony/agentic_workspace/projects/scadc/`:

| scope | what it holds | journals |
|---|---|---|
| `metabolic-modelling` | the original study — `05_embed_transfer/` (scripts 10–90, `_shared.py`, `_eval.py`); `cache/embed_transfer_report.md`, `baseline_metrics.parquet`, `projector_metrics.parquet` | `68ca55d4` (2026-06-14), `4be91808` (promotion) |
| `fig-annotate` | the lane-choice benchmarks — `main/annotations/lane_benchmarks/` (`_bridges.py`, `01_`–`03_`); tables in `data/scadc/metabolic_modelling/lane_benchmarks/`, incl. `transfer_partA_c30.tsv` and `lane_benchmarks_summary.md` | `75566c63`, addendum `fa082b8c`, `23efdbd8`, `14cafb1c` |

Reach them with `scope(verb="fetch", peer="capella")`. The raw tables need a session on
capella; the numbers below are quoted from the journals.

## What it measured

Labels transfer by **distance-weighted kNN vote over the MNXR vocabulary directly** — not
through an EC→MNXR crosswalk — against a reference pool of fosmid + epi300 + metag
embeddings, with a vote floor of 0.2. The backbone is a 512-D in-repo ProteinBERT
reimplementation, CPU-runnable, and the last-512 reduction is verified bit-identical
across all three pool components (`36002732`).

**Dark regime** (`c30`: each gold ORF's entire ≥30%-identity DIAMOND cluster removed
before transfer; 57,448 multi-channel ORFs):

| lane | precision | recall | F1 | top-1 |
|---|---|---|---|---|
| ProteinBERT raw | **0.682** | 0.650 | 0.666 | 0.680 |
| ESM-C projected | 0.557 | **0.692** | 0.617 | — |
| DIAMOND homology | — | 0.37 | 0.532 | 0.407 |

ESM-C has the higher recall and casts a wider, noisier net; precision is what decided the
backbone choice, and the first journal reported F1 only — `fa082b8c` is the correction.
With no cluster removal everything sits at ~0.95 on sequence twins, which is a ceiling
rather than a result.

**Curated reference genomes**, where the ORFs are ordinary rather than dark:

| reference | transfer lane |
|---|---|
| EPI300 | P = R = 0.845 |
| DH10B | P = R = **0.839** (1081 / 1288 adjudicable) |

For scale on DH10B: KofamScan 98/96, UniRef50 100, UniRef90 100, CLEAN 95, DeepEC 88,
EZpred 86, ProteinBERT 84.

## The gating that came with it

In scadc the lane was promoted **dark-only** — 34,064 rows over 2,745 dark fosmid ORFs,
fosmid-only, floor 0.2. That restriction is the point of the lane: it was validated as a
complement *in the remote-homolog regime, where the other three lanes are blind*, and its
own author flagged it as exploratory-promoted with the ProteinBERT > ESM-C inversion still
unverified.

## The numbers above do NOT describe the lane in our GPR tables

Our tables carry channel **`pbert`**, which comes from
`src/metasmith_libraries/transforms/fabfos/gpr_4lane.py`. The scadc study measured
**`pbert_transfer`**, from `resources/lib/fabfos_embed_transfer.py`. Same backbone, two
different measurements, and that module's docstring says so outright — it refuses to share
the channel name precisely so the two numbers cannot land in one column:

| | `pbert_transfer` (validated) | `pbert` (ours) |
|---|---|---|
| reference pool | metag + epi300 + fosmid | Swiss-Prot `ref::reference_label_pool` (222k refs, ~13k MNXR) |
| which ORFs | **dark only** — zero evidence from the other three lanes | **every ORF** |
| P / R on record | 0.682 / 0.650 (c30); 0.84 on curated genomes | never measured |

So the P = 0.84 figure was never a claim about our channel. What we measured — 0.3%
concordance on the ASKA library, 4.0% at 3-digit EC, against kofam 89% and uniref50 88% —
is the first measurement this configuration has had.

The `glgC` case shows the mechanism: the retrieved neighbour is Q8DA54, *speA*, arginine
decarboxylase (EC 4.1.1.19), *Vibrio vulnificus*, yielding arginine decarboxylase and
generic ATP hydrolysis for a glucose-1-phosphate adenylyltransferase. `glgC` carries a
0.996 BSR uniref50 hit and a 527 kofam bitscore, so under scadc's dark-only gate the lane
would never have been consulted about it at all.

## The distance measure, and where precision is lost

Both implementations vote identically (`gpr_4lane.py::lane_embed`,
`fabfos_embed_transfer.py::apply_one`): **cosine similarity** on L2-normalised
embeddings, K = 30 neighbours, negative cosines clipped to zero, then

    w = vals / vals.sum()        # weights normalised WITHIN the top-K
    vote[label] = Σ w_i · 1[label ∈ labels(neighbour_i)]
    emit where vote >= PBERT_FLOOR (0.20)

**The normalisation divides the absolute similarity out.** A query whose nearest labelled
reference sits at cosine 0.99 and one whose nearest sits at 0.11 produce identically
scaled vote vectors. So `raw_score` is a *relative* vote share and structurally cannot
express "this ORF has no good neighbour" — which is how a wrong call scores 0.700, above
the lane's own median of 0.334.

The absolute quantity is `nn_similarity`, the top-1 cosine. `apply_one` computes **and
emits** it alongside `k_support`. `lane_embed` computes the same `argmax` but uses it only
to name the donor ORF and then **discards the value** — four columns out, no
`nn_similarity`, and the GPR schema downstream has no such field. The one number that
could gate this lane on quality is calculated and thrown away.

Note what that makes it, relative to its neighbours: uniref50 gates on BSR and kofam on
bitscore-vs-family-threshold, both absolute measures of match quality. **`pbert` is the
only lane with no absolute-quality gate at all.**

## Tuning for precision

In rough order of leverage:

1. **Keep and threshold `nn_similarity`.** The strongest knob and a ~2-line change plus a
   schema column. The study already brackets the curve at two points: precision ~0.95 when
   neighbours are sequence twins, 0.682 once every ≥30%-identity cluster relative is
   removed. That gap *is* precision as a function of neighbour distance — it just was
   never swept as such, and that is the experiment to run.
2. **Restore the dark-only gate.** Costs nothing where the lane earns its keep and removes
   the entire class of error found here.
3. **Require corroboration.** `sweep_aska.py --min-lanes 2` already exists downstream.
4. **Raise `PBERT_FLOOR`** (currently 0.20) — the knob that exists, and the weakest one:
   it thresholds a scale-free vote share, so it cannot reject a confidently-wrong call.
   scadc's coverage tiers show the cost: 98.9% of dark ORFs covered at 0.1, 50.9% at 0.7.
