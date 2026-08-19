# Annotation lanes: the ablation to reproduce, and the lane set

Objective 1 of this scope is to reproduce the scadc lane ablation here. The study is not in
this repository and not in the `fabfos` project — it is on **capella**, project **`scadc`**,
scope **`fig-annotate`**. Copying the annotator outputs across is acceptable; the annotators
need not be re-run.

## The source

| journal | what it is |
|---|---|
| `e6d36f16` | **the ablation** — lane-justification UpSet, 8 tools × 21 combos, on EPI300 |
| `14cafb1c` | the DH10B replication; shows the result is robust to ORF-caller choice |
| `75566c63` + `fa082b8c` | reaction-space lane-choice benchmarks; `fa082b8c` corrects the first to precision |
| `23efdbd8` | compute requirements per lane (CLEAN 32 GB GPU, ESM-C ~12 GB, ProteinBERT CPU) |

Generators live at `main/annotations/lane_benchmarks/lane_justification/`
(`build_upset8.py` → `upset8.json` → `plot_upset8.py`, plus the `uniref_rhea_*` homology
scorers). **`e6d36f16` flags them as uncommitted**, so confirm they still exist before
planning around recovering them. Tables are in `data/scadc/metabolic_modelling/lane_benchmarks/`.

Truth is SwissProt-reviewed *E. coli* K-12 EC matched to EPI300 ORFs by exact sequence md5,
**N = 1280 adjudicable**.

## The lane set — seven, EZpred dropped

EZpred is **out** of this reproduction. The original ran eight tools; dropping EZpred leaves
the seven that matter:

| group | lane | recall /1280 | precision | metag coverage |
|---|---|---|---|---|
| EC | CLEAN @0.01 | 1170 | 0.95 | 31.4% |
| Homology | KofamScan | 1162 | 0.96 | 35.1% |
| Homology | UniRef50 | 1074 | 1.00 | 11.0% |
| Homology | UniRef90 | 1013 | 1.00 | 2.3% |
| EC | DeepEC | 898 | 0.88 | 6.0% |
| Transfer | ProteinBERT | 0 | — | 0 |
| Transfer | ESM-C | 0 | — | 0 |

Dropping EZpred costs nothing the study relied on: it was near-silent on dark matter at
production threshold (fires on 0 of 3,310 dark fosmid ORFs, where CLEAN reaches 3,278), and
its apparent coverage was sub-0.3 fan-out noise.

## Two findings the reproduction has to settle

**The transfer lanes' zero is a marginal-contribution zero, not an accuracy zero.**
`e6d36f16` draws ProteinBERT and ESM-C bar-less because "every transfer-containing combo
collapses onto its non-transfer twin" — on a curated chassis they add no ORF the other lanes
did not already get. `14cafb1c` separately reports the transfer lane at P = R = 0.839 on
DH10B and 0.845 on EPI300. Those reconcile as *accurate in isolation, redundant in
combination*, but no journal says so outright. Establish which quantity each number is
before either is quoted.

**CLEAN never abstains**, and that is the precedent for objective 2. It emits a full L4 EC
on ~99% of ORFs, so its 99.4% metag coverage was an artifact of not abstaining rather than
of reach. Its score *is* discriminative (correct 0.94 vs wrong 0.40), and thresholding at
0.01 cost almost no curated recall (0.921 → 0.914, precision 0.924 → 0.954) while collapsing
metag coverage to 31.4%. That is the same shape as the pbert problem in `pbert/README.md`:
a lane that cannot decline, given a threshold on a quantity that was discriminative all
along.
