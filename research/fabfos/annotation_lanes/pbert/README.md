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
| P / R on record | 0.682 / 0.650 (c30); 0.84 on curated genomes | 0.973 / 0.826 on DH10B, once the pool is repaired and the lane is gated |

So the P = 0.84 figure was never a claim about our channel.

## The pool index does not describe the pool stack

The 0.3% ASKA concordance, and the `glgC` case where the donor came back as *speA* from
*Vibrio vulnificus*, are one defect rather than a lane that transfers badly. `ref::reference_label_pool`'s
`orf_index.parquet` and `emb_pbert.npy` are not the same ordering: the embedder writes the
stack in shards of 1,024 and ASSEMBLE reads the `.npy` and `.csv` shards back through two
independent `sorted(glob(...))` calls that do not agree. Every row is then labelled with
some other protein's reactions, and nothing raises — the length check passes and the label
merge passes. It is the failure that transform's docstring warns about, arriving through
the one door it does not guard. **The ESM-C twin pool is unaffected**, so this is
ProteinBERT's shard layout rather than the builder's design.

Three checks pin it, none of which needs the embedder: 328 of 400 groups of pool
accessions carrying identical sequences have different embeddings; a DH10B ORF
byte-identical to a Swiss-Prot entry sits at cosine 0.26 from that entry's row and 1.000
from a row elsewhere; and those displacements are constant in blocks of 1,024.

`repair_pool_index.py` recovers the permutation with no re-embedding, by matching each
index block to the stack block with the same duplicate-item signature, and validates
442/443 on anchors the matching never saw. That is a research-side repair — it writes
beside the pinned artifact. **The transform still needs the real fix**: read the shards
once, in one order, rather than trusting two globs to agree.

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

## Tuning for precision — measured on DH10B

The sweeps in this directory settle the ranking that used to be a guess. Numbers are the
1,288-ORF DH10B curated cohort, rebuilt by the source study's own md5 method; see
`before_after_dh10b.tsv` and the `threshold_*` and `metrics_*` tables beside it.

1. **`nn_similarity` is the knob.** Gating on the top-1 cosine at 0.7716 — the 15th
   percentile, so it refuses the worst sixth of ORFs — takes the lane from P 0.701 / R
   0.667 to **P 0.973 / R 0.826**, coverage 0.951 → 0.849. It is one line plus a schema
   column, and it is the whole result.
2. **The distance metric is inert.** Cosine, correlation, per-dimension standardised
   cosine, PCA-whitened cosine and euclidean sit within 0.001 of each other once gated,
   and L1 ties them on a matched subsample. Only unnormalised dot is materially worse
   (F1 0.858 vs 0.893) — magnitude carries no signal here. Keep cosine.
3. **`PBERT_FLOOR` is the weakest knob, as suspected — it is worse than weak.** With the
   distance gate in place, best F1 sits at floor **0.00**: the floor only removes labels
   the gate has already decided to trust. At the shipped 0.20 the same gate gives P 0.811
   / R 0.662.
4. **The dark-only gate and corroboration** (`sweep_aska.py --min-lanes 2`) remain
   untested here; both narrow where the lane speaks rather than how well it speaks.

The leakage question the numbers depend on: the cohort's truth accessions are themselves
Swiss-Prot pool members, as they are for kofam and uniref50, which is what makes these
figures comparable to the seven-lane table. Hiding the ORF's own accession costs ~0.03 F1;
hiding every neighbour at cosine ≥ 0.99 — the cheap stand-in for the source study's
DIAMOND cluster removal — costs ~0.06, landing at P 0.906 / R 0.769.
