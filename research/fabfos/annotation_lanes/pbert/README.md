# The pbert lane: what it is, what it was validated as, and why those differ

The ProteinBERT transfer lane was benchmarked for precision and recall before it was
wired anywhere. That work is **not in this repository and not in the `fabfos` project** —
it lives in `scadc` on **capella**, which is why it does not turn up from here. This file
records where that study is, why its figures are not about our channel, and what the lane
does now.

## Where the source study is

Two scopes on capella, `/home/tony/agentic_workspace/projects/scadc/`:

| scope | what it holds | journals |
|---|---|---|
| `metabolic-modelling` | the original study — `05_embed_transfer/`; `cache/embed_transfer_report.md`, `baseline_metrics.parquet`, `projector_metrics.parquet` | `68ca55d4` (2026-06-14), `4be91808` (promotion) |
| `fig-annotate` | the lane-choice benchmarks — `main/annotations/lane_benchmarks/`; tables in `data/scadc/metabolic_modelling/lane_benchmarks/` | `75566c63`, addendum `fa082b8c`, `23efdbd8`, `14cafb1c` |

Reach them with `scope(verb="fetch", peer="capella")`. The raw tables need a session on
capella; the numbers below are quoted from the journals.

## What it measured

Labels transfer by distance-weighted kNN vote over the MNXR vocabulary directly — not
through an EC→MNXR crosswalk — against a reference pool of fosmid + epi300 + metag
embeddings. The backbone is a 512-D in-repo ProteinBERT reimplementation, CPU-runnable,
and the last-512 reduction is verified bit-identical across all three pool components
(`36002732`).

**Dark regime** (`c30`: each gold ORF's entire ≥30%-identity DIAMOND cluster removed
before transfer; 57,448 multi-channel ORFs):

| lane | precision | recall | F1 | top-1 |
|---|---|---|---|---|
| ProteinBERT raw | **0.682** | 0.650 | 0.666 | 0.680 |
| ESM-C projected | 0.557 | **0.692** | 0.617 | — |
| DIAMOND homology | — | 0.37 | 0.532 | 0.407 |

Precision is what decided the backbone choice; the first journal reported F1 only, and
`fa082b8c` is the correction. With no cluster removal everything sits at ~0.95 on sequence
twins, which is a ceiling rather than a result.

**Curated reference genomes**: EPI300 P = R = 0.845; DH10B P = R = 0.839 (1081/1288
adjudicable). For scale on DH10B — KofamScan 98/96, UniRef50 100, UniRef90 100, CLEAN 95,
DeepEC 88, EZpred 86, ProteinBERT 84.

## Those figures are not about our channel

Our tables carry channel **`pbert`**, from
`src/metasmith_libraries/transforms/fabfos/gpr_4lane.py`. The scadc study measured
**`pbert_transfer`**, from `resources/lib/fabfos_embed_transfer.py`, whose docstring
refuses to share the channel name precisely so the two numbers cannot land in one column.

| | `pbert_transfer` (validated) | `pbert` (ours) |
|---|---|---|
| reference set | metag + epi300 + fosmid | Swiss-Prot `ref::label_transfer_landmarks` (222,019 refs, 13,112 MNXR) |
| which ORFs | **dark only** — zero evidence from the other three lanes | every ORF, minus those the lane refuses |
| where it was promoted | dark-only, fosmid-only, exploratory | the canonical 4-lane and 7-lane mappers |

The scadc lane was promoted as a complement *in the remote-homolog regime, where the other
three lanes are blind*. Ours is not restricted that way, so P = 0.84 was never a claim
about it.

## What the lane does now

`gpr_4lane.py::lane_embed`, with every threshold declared in
`lib::fabfos_evidence.py`:

    s  = cosines to the landmarks, clipped at 0, best first
    if s[0] < PBERT_NN_MIN:            refuse the ORF entirely
    admit i where s[i] >= max(PBERT_NN_MIN, PBERT_TAU * s[0])   # at most PBERT_K_MAX
    w  = s[admitted] / s[admitted].sum()
    emit labels whose weighted vote clears PBERT_FLOOR

**The two thresholds answer different questions and both are load-bearing.**
`PBERT_FLOOR` gates a vote normalised *within the admitted set*, so it measures neighbour
AGREEMENT and cannot express "nothing here resembles this protein" — thirty neighbours at
cosine 0.15 that agree score 1.0. `PBERT_NN_MIN` measures PROXIMITY, and is what lets the
lane decline. Both land before the GPR table exists, the way kofam has always dropped a
hit below its family's own cutoff, so a table carries the calls that cleared their lane's
threshold and there is nothing to opt out of.

Tuned on the 1,288-ORF DH10B cohort; see `threshold_cosine_dh10b.tsv` and
`before_after_dh10b.tsv`.

| DH10B, `twin` condition | ORF-level P | ORF-level F1 | label-level P | coverage | calls |
|---|---|---|---|---|---|
| as shipped (misaligned landmarks) | 0.0043 | 0.0041 | 0.0038 | 0.899 | 2,116 |
| landmarks repaired only | 0.6555 | 0.6374 | 0.5655 | 0.946 | 2,140 |
| **+ quota, as it ships** | **0.8985** | **0.8254** | **0.8050** | 0.849 | 2,392 |

Four things a reader should not have to rediscover:

- **The metric is inert.** Cosine, correlation, standardised cosine, PCA-whitened cosine
  and euclidean sit within 0.002 of each other; only unnormalised dot is materially worse.
  Keep cosine. (`metrics_dh10b.tsv`)
- **The vote floor has no job left.** Every increase from 0.00 costs F1 in all three
  leakage conditions. It was standing in for a proximity gate, badly; with a real one it
  removes only true positives.
- **`tau` is invisible on the ORF-level axis and does real work on the label-level one** —
  at nn_min 0.75 it lifts label precision 0.708 → 0.826 while moving ORF F1 by 0.008.
  Emitting more labels per ORF makes an ORF-level intersection with the truth set EASIER,
  so that axis structurally cannot see flooding. Tune the quota on the label-level axis.
- **Call volume went UP, not down** — 2,140 → 2,392 under `twin`. The gate removes 189
  ORFs outright, but hiding the near-identical twins leaves a more varied admitted set
  carrying more distinct labels. Precision roughly doubles either way; "fewer annotations"
  is not what this buys.

## The two ways to be wrong about these numbers

**Two scoring axes, and they are not interchangeable.** ORF-level in EC space is the
seven-lane table's axis; label-level macro set-overlap is the embed-transfer report's.
Label-level recall is structurally capped near 0.27 here, because truth MNXR comes from
EC→MNXR fanned through `reac_prop` while a landmark's `mnxr_list` is the bridge's direct
UniProt→MNXR — two different derivations. Only the precision half is comparable to the
study's 0.839. `_common.py` exists to make quoting one against the other hard.

**`twin` is a cosine stand-in, not homology removal.** It hides landmarks at cosine ≥ 0.99
— not paralogs, isozymes, or a related *E. coli* protein at 0.95, and Swiss-Prot contains
*E. coli*'s own proteome. Against lanes measured with DIAMOND cluster removal these are an
upper bound, not a like-for-like comparison.

## What the reference set's repair left behind

`ref::label_transfer_landmarks` is one parquet: accession, `mnxr_list` and the 512 floats
in the same row. It replaced a pair of files paired by sort order, which disagreed, so
every reference carried another protein's reactions and nothing raised — that is what the
0.0043 row above measures. **The invariant now is that an embedding table names its own
rows**; there is no ordering left to get wrong, on either the landmark or the query side.

`rebuild_landmarks.py` produced it from the retired pool with no re-embedding, gated on
three checks. `repair_pool_index.py` is its predecessor, kept only because it recovered
the same permutation independently and agreeing with it is part of why the rebuild is
trusted. `migrate_pbert_embeddings.py` converts a legacy query pair, and refuses rather
than guesses when the chunk order is genuinely ambiguous — two index conventions exist in
the deployed artifacts and neither file says which it is.

## Things measured here that constrain what comes next

- **`raw_score` is inert downstream for this lane.** `nomination_contributions` groups by
  `(orf, channel, intermediate_id)`, and pbert's `intermediate_id` is the single best
  donor, so there is one nomination group per `(orf, pbert)`, `w_n` is identically 1.0,
  and `contrib = 1/F_n/n_lanes`. The vote floor therefore changes how pbert's belief
  SPREADS across reactions, never how much it carries. No metric in these sweeps sees it.
- **The abstain moves the other lanes.** An ORF's belief splits across the lanes that
  fired, so an ORF pbert declines reweights its remaining lanes by `n_lanes`. Belief
  conservation still holds exactly; the redistribution is the designed semantics, not a
  defect. On the eydallin clones, 15 of 55 shared ORFs changed lane count.
- **`CLEAN_MIN_SCORE = 0.02` was tuned where every ORF has a curated EC.** The scadc
  metagenome sits at a median `clean_score` of 0.0015, so the same cut discards most of
  its calls. Coverage 0.950 is a curated-chassis number.
- **`sweep_*` numbers depend on a query stack whose pairing is unchecked.** `dh10b.faa` is
  4,126 records — five embedder chunks, so positional pairing is safe by arithmetic. Grow
  that cohort past 9,216 ORFs and `_common.load_query`'s positional zip silently measures
  a scrambled query set.

`check_vote_matches_lane.py` is what keeps this directory honest: it lifts `lane_embed`
out of the live transform and runs it beside `_knn.vote` across settings that exercise
each branch, so "the sweeps tune the rule the pipeline runs" is checked rather than
asserted. Run it after touching either.
