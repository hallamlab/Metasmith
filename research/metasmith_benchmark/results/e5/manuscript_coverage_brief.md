# E5 coverage brief for the d1 manuscript

## Purpose & Contents

This brief tells the manuscript's Results section what E5 covers: the analyses, the tools per stage, the datasets, the plan size, where each result lives and what is still open. It holds no new measurements. Every number comes from `findings/E5_RUN.md`, `findings/E5_HYBRID_PILOT.md`, the tables in `results/e5/` and the E5 journal, all on `feat/engine/bench-E5`. Paths are relative to `research/metasmith_benchmark/`. It was written on 2026-10-28. **P** marks a number to treat as provisional or a comparison to read with care. The reason follows it.

## What E5 is

E5 runs E2's, E3's and E4's targets together, as one combined metagenomics workflow, on every CAMI, Pratama 2026 and metaGEM run. That is 805 runs. The planner solves one plan per read type ("shape"). Each plan targets a MAG lane, a GEM lane and a viral lane from the same assembly. E5 ran from batch 0 (pilot, 2026-09-30) to close on 2026-10-17. Nothing is running now. All runs are archived on chinook `fir_bench_e5/`.

E1–E4 each cover one cell of the corpus × analysis grid: E1/E2 cover CAMI MAGs, E3 covers Pratama viromics and E4 covers metaGEM GEMs. E5 fills all nine cells. `results/e5/crossed_table.tsv` is that grid. Its `cell` column is `e1_e4` for the three cells E1–E4 also cover, and `new` for the other six.

## Analyses and tools, end to end

| Stage | Tool(s) | Notes |
|---|---|---|
| Read stats | seqkit | `sequences::read_qc_stats` |
| QC | bbduk on JGI's settings, all corpora | Pratama's own `bbduk_pratama` trim is not used. PacBio-only samples skip bbduk and run Filtlong |
| Assembly, short reads | MEGAHIT | |
| Assembly, hybrid (short + ONT) | MEGAHIT draft, then OPERA-MS 0.9.0 (`--no-ref-clustering --no-polishing`) | **Substitution** for E3's hybrid metaSPAdes. Chosen by the hybrid pilot and Tony on 2026-10-02 / 2026-10-08 |
| Assembly, PacBio CLR only | Filtlong, then Flye `--meta` (PACBIO_CLR mode) | CAMI marine and strain only. E1/E2 used those datasets' short reads |
| Read mapping for coverage | `assembly_stats` (minimap2) | Feeds all three binners |
| Binning | MetaBAT2, SemiBin2, COMEBin (CPU build, 24 CPUs) | Three binners in parallel. Seeds fixed |
| Bin refinement | DAS Tool (with its Prodigal call) | **A MAG is a DAS Tool bin** |
| MAG quality | CheckM2 | On DAS Tool bins only |
| GEM | per-MAG ORF call (`mag_orfs`), CarveMe 1.6.6 split into `carveme_annotate_bigg` (DIAMOND vs BiGG) and `carveme_carve_gapfill`, MEMOTE 0.17.0 | E4's "modern lane". Gap fill on SCIP, without gene scores |
| Viral calling | geNomad, VirSorter2, VIBRANT, DeepVirFinder | Four callers over contig chunks, merged per study |
| Viral curation | CheckV (batched), host trimming of proviruses | |
| vOTUs | MMseqs2 clustering (`votu_cluster`), representatives, island filter on ≥5 kb representatives | E3's lane, as E5's own copies of the transforms |
| Supplementary binning score | AMBER 2.0.7 on CAMI bins | Run off-plan by `drivers/e5_amber.py` |

**Not in E5:** no MAG taxonomy (no GTDB-Tk, no Metabuli, no CAT), no host prediction (iPHoP), no dRep/skani dereplication, no community modelling (SMETANA), no CoverM, DRAM-v, minced or MetaPop. The pilot driver lists these as gated or deferred (`drivers/e5_pilot.py:40-50`). The full run's targets do not include them (`drivers/e5.py` `build_targets`). geNomad's viral taxonomy table is the only taxonomic output, a by-product of the caller step.

Alternatives inside one plan: the three binners are parallel alternatives that DAS Tool reconciles, and the four viral callers are parallel alternatives that the merge step pools. Substitutions across corpora: the hybrid assembler (OPERA-MS for hybrid metaSPAdes) and the QC (standard bbduk for Pratama's trim). The planner picks the assembler from the read type. The target set is the same for every shape.

## Datasets and sample counts

| Shape | Corpus / studies | Runs | MAGs | Models | vOTU representatives |
|---|---|---|---|---|---|
| cami_pe | CAMI toy_mousegut, toy_hmp_airskinurogenital, toy_hmp_gastrooral | 98 | 1,806 | 1,806 | 7,266 / 3,414 / 3,761 |
| cami_long_pacbio | CAMI marine, strain | 110 | 83 | 83 | 44,017 / 177,726 |
| cami_hybrid_ont | CAMI toy_humangut, plant_associated | 41 | 755 | 755 | 29,011 / 7,203 |
| pratama_pe | Pratama 2026 short reads | 48 | 4,875 | 4,875 | 178,976 (one catalogue, short + hybrid) |
| pratama_hybrid_ont | Pratama MinION–Illumina pairings | 17 | 2,138 | 2,138 | in the Pratama catalogue |
| metagem_se | korem2015 | 48 | 153 | 153 | 343 |
| metagem_pe_split | li2019, bissett_base | 52 | 520 | 520 | 2,261 / 2,604 |
| metagem_pe_split | karlsson2013 | 145 | 3,782 | 3,782 | 50,163 |
| metagem_pe_split | sunagawa2015 (Tara, 6 chunks) | 246 | 9,975 | 9,971 | 225,110 |
| **Total** | 3 corpora, 13 studies | **805** | **24,087** | **24,083** | **731,855** |

Over all 24,087 MAGs, 8,123 are HQ (CheckM2 completeness >90, contamination <5), 12,543 are MQ and 3,421 are LQ. Four Tara bins have no model, because CarveMe's gap fill returns none. Hybrid samples are 58 of 805. The viral lane ran as 13 catalogues: one per CAMI dataset, one per metaGEM study and one for Pratama's short and hybrid runs together.

## Plan size

Distinct steps (transforms that ran) per shape, from `results/e5/cost.tsv`, not counting the driver:

- A short-read plan with its viral lane in the plan (cami_pe, metagem_pe_split) runs 30–31 steps: 14 for QC through MEMOTE (15 with the interleave step) and 16 for the viral lane.
- A plan whose viral lane ran as a separate catalogue plan (cami_hybrid_ont, pratama_pe, pratama_hybrid_ont) runs 14–15 steps. The catalogue plan runs the 16 viral steps.
- Across the whole run, 35 distinct transforms ran. The table also lists `carveme_from_orfs`, the unsplit CarveMe step left over from korem2015's GPU run.

The DAG figures are `page/dags/e5_<shape>.dag.svg`. **P:** they were drawn on 2026-10-02 to 2026-10-04 and predate the CarveMe split, so they show a single CarveMe node. Redraw them from `drivers/e5.py run --batch N --shape S --dag` before using one as a figure. No run is needed to redraw.

## Cost

Allocated core-hours from fir's `sacct`, every attempt included (`findings/E5_RUN.md` § Cost):

- Total 176,202 core-h, about 219 per run. The estimate before batch 0 was 0.5–1.0 M.
- By stage: COMEBin 60,845 (35%), viral 50,557, assembly 29,959, GEM 15,908 (about 0.66 per MAG), mapping 13,132, binners 3,175, QC 1,460, CheckM2 171, drivers 994.
- **P:** the totals include re-run work: a first Pratama viral pass that the catalogue recomputed, ~1,465 failed CarveMe attempts on cami_pe, Tara chunk 5's reset, and COMEBin run twice on li2019/bissett_base. `E5_RUN.md` lists each one. The table excludes the batch 0 pilots and E3's Pratama assembler ablation (43,019 core-h).

## Results and comparisons

Each comparison reuses a score E1–E4 already report. E5 adds no new matching (Tony, 2026-10-17: quality only, no new runs).

- **CAMI MAGs vs E1/E2 (nf-core/mag).** On the three short-read datasets E5 gives 1,806 MAGs and 1,117 HQ, against E1 1,813 / 1,143 and E2 1,836 / 1,148. That is within 2% on MAGs and 3% on HQ. On the hybrid datasets, OPERA-MS adds 167 HQ MAGs on toy_humangut and 39 on plant_associated over E1/E2's long-read-only runs. **P:** the PacBio rows (marine 37, strain 46 MAGs vs ~300–460 in E1/E2) compare different read sets, not methods. E5 used PacBio alone, E1/E2 used short reads.
- **CAMI cost vs nf-core/mag.** QC to CheckM2, per sample: cami_pe 76 core-h (E1 51.9, E2 145.0). cami_hybrid_ont 49 (E1 23.7, E2 56.8). **P:** E1/E2's figures cover nf-core/mag's full process set. Read this as an order of magnitude.
- **AMBER (supplementary).** Median DAS Tool `f1_score_bp` is close to E2's on the short datasets (0.199 vs 0.204, 0.266 vs 0.265, 0.373 vs 0.393). **P:** AMBER's gold standard is per-run, so cross-run scores do not strictly compare. 189 of 249 samples were scored. For 36 of them, the metrics come from before an AMBER plotting crash.
- **Pratama vOTUs vs E3.** 178,976 vOTUs. Recovery of Pratama's published vOTUs is 66.9%, against E3's matching rung R3 65.4% and the published-method rung R0 78.5%. Recovery of the hybrid lane's vOTUs is 37.2% (E3 R3 36.0%, R0 89.8%). The gap to R0 is the OPERA-MS substitution, which E3's ablation measured: −4.7 points overall and about a third of the hybrid assembly cost (107 vs 297 core-h).
- **metaGEM GEMs vs E4 and metaGEM.** 14,430 E5 MAGs against metaGEM's 14,105 published bins. Median reactions, metabolites and genes match E4's modern lane within 5% on karlsson2013, li2019 and sunagawa2015. Median MEMOTE total is 0.826–0.859, the same as E4 on four studies. On korem2015, E5 gives 0.828 against E4's 0.706. **P:** E4 recorded no cost, so E5's GEM cost has no comparator.
- **The six new cells.** CAMI viral, CAMI GEM, Pratama MAG, Pratama GEM, metaGEM MAG (vs metaGEM's bin counts) and metaGEM viral. Each holds quality distributions only, in `crossed_table.tsv`, `mags_closed.tsv` and `votu_quality.tsv.gz`.
- **vOTU quality over all 13 catalogues.** 731,855 representatives: 2,335 Complete, 5,079 High, 12,542 Medium, 331,322 Low and 380,577 Not-determined (CheckV).

## Open questions and caveats that change a sentence

1. **60 of 110 PacBio samples have no DAS Tool bin.** DAS Tool scores none above 0.5. Not investigated.
2. **PacBio catalogues are mostly undeterminable calls.** 97% (marine) and 88% (strain) of their vOTUs are CheckV Not-determined. This explains part of their high vOTU count per run.
3. **Pratama MAGs and GEMs are small.** Median genome 0.77 Mb (short) and 1.04 Mb (hybrid). Median 664–771 reactions and 118–158 genes, about half of every other dataset. Not investigated.
4. **MEMOTE consistency is bimodal.** Median about 0.5 on five CAMI datasets and about 0.9 elsewhere. E4 shows the same low peak on korem2015. Unexplained.
5. **OPERA-MS ran unpolished.** It leaves 400–650 indels per 100 kb in its gap fills (pilot). E3 asked for a polishing check on 2026-10-05. No record shows it ran. Its hypothesis is that polishing would raise the low aligned fraction of hybrid vOTUs.
6. **The hybrid pilot is small.** 19 samples. On real Nanopore (Pratama, n=2) it favoured Flye+POLCA on cost and contiguity. OPERA-MS won on CAMI.

## Where the results live

| What | Path |
|---|---|
| Narrative record: products, cost, comparisons, incidents, deletions | `findings/E5_RUN.md` |
| Hybrid assembler pilot and decision | `findings/E5_HYBRID_PILOT.md`, `results/e5_hybrid/` |
| Corpus × analysis grid with E1–E4 and published values | `results/e5/crossed_table.tsv` (built by `crossed_table.py`) |
| Every MAG: CheckM2, model size, MEMOTE | `results/e5/mags_closed.tsv` |
| Every vOTU representative with CheckV row and cluster size | `results/e5/votu_quality.tsv.gz` |
| Pratama vOTU recovery score | `results/e5/pratama_score.json` (`pratama_score.py`) |
| Core-hours per run and step | `results/e5/cost.tsv`, `results/e5/cost_extra.tsv` |
| One row per closed run: run keys, counts, archive | `results/e5/batches.tsv` |
| Assembly per sample (inputs to the catalogues) | `results/e5/assemblies.tsv` |
| AMBER supplementary table | `results/e5/amber_*` |
| Plans, shapes, batches, catalogues | `drivers/e5.py` |
| DAGs per shape | `page/dags/e5_*.dag.svg` (pre-split, see Plan size) |
