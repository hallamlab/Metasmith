# E5 full run: cost, products and comparison with E1–E4

## Purpose & Contents

This file records E5's full run over every CAMI, Pratama 2026 and metaGEM sample: what it produced, what it cost, how it compares with E1–E4, what went wrong on fir, and what it deleted. It holds findings only. `drivers/e5.py` is the source of truth for the plans, the batches and the catalogue groups. The tables it summarises are in `results/e5/`. Paths below are relative to `research/metasmith_benchmark/`.

- `results/e5/batches.tsv` holds one row per closed MAG run: its runs, MAG and model counts, core-hours, archive and evictions.
- `results/e5/cost.tsv` holds core-hours per run and step for the MAG runs. `results/e5/cost_extra.tsv` holds the same for the 5 catalogue plans and the upstream runs that `cost.tsv` omits.
- `results/e5/mags_closed.tsv` holds every MAG with its CheckM2 quality, model size and MEMOTE scores.
- `results/e5/amber_*` holds the supplementary AMBER table on E5's CAMI bins.
- `results/e5/cleanup.tsv` lists every deletion on fir with the copy that made it safe.

`findings/E5_HYBRID_PILOT.md` records the pilot that chose the hybrid lane. `findings/E3_ABLATION.md` on `feat/engine/bench-E3` records the Pratama assembler ladder that confirmed it.

## What ran

E5 solves one plan per read type. Each plan targets the MAG lane (QC, assembly, three binners, DAS Tool, CheckM2, one CarveMe model and MEMOTE score per MAG) and the viral lane (four callers, CheckV, MMseqs2 vOTUs). Hybrid samples assemble with MEGAHIT, then OPERA-MS. COMEBin runs at 12 CPUs on CPU.

The viral lane runs as 13 catalogues, one per CAMI dataset, one per metaGEM study, and one for Pratama's short and hybrid runs together. 8 of them ran inside their batch's plan. The other 5 ran as viral-only plans (`e5.py catalogue NAME`) over the cached assemblies listed in `results/e5/assemblies.tsv`.

| Shape | Studies | Runs | MAGs | Models | vOTU representatives |
|---|---|---|---|---|---|
| cami_pe | toy_mousegut, toy_hmp_airskinurogenital, toy_hmp_gastrooral | 98 | 1,806 | 1,806 | 7,266 / 3,414 / 3,761 |
| cami_long_pacbio | marine, strain | 110 | 83 | 83 | 44,017 / 177,726 |
| cami_hybrid_ont | toy_humangut, plant_associated | 41 | 755 | 755 | 29,011 / 7,203 |
| pratama_pe | Pratama short reads | 48 | 4,875 | 4,875 | 178,976 (one catalogue with the hybrid runs) |
| pratama_hybrid_ont | Pratama MinION pairings | 17 | 2,138 | 2,138 | in the Pratama catalogue |
| metagem_se | korem2015 | 48 | 153 | 153 | 343 |
| metagem_pe_split | li2019, bissett_base | 52 | 520 | 520 | 2,261 / 2,604 |
| metagem_pe_split | karlsson2013 | 145 | 3,782 | 3,782 | 50,163 |
| metagem_pe_split | sunagawa2015 (Tara, 6 chunks) | 246 | 9,975 | 9,971 | 225,110 |
| **Total** | | **805** | **24,087** | **24,083** | |

A MAG is a DAS Tool bin. Four Tara bins have no model, because CarveMe's gap fill returns no model for them.

CAUTION: 60 of the 110 PacBio samples have no DAS Tool bin, and the 50 others have 83. DAS Tool scores no bin above 0.5 on the 60. The PacBio catalogues hold far more vOTUs per run than the short-read ones. This run does not check why.

## Cost

Every figure is allocated core-hours from fir's `sacct`: allocated CPUs times elapsed time, summed over every attempt, failed and cancelled ones included. E1 and E2 report cost the same way.

| Shape | QC | Assembly | Mapping | Binners | COMEBin | CheckM2 | GEM | Viral | Driver | Total |
|---|---|---|---|---|---|---|---|---|---|---|
| cami_pe | 59 | 977 | 136 | 303 | 5,969 | 28 | 1,861 | 2,392 | 28 | 11,753 |
| cami_long_pacbio | 38 | 2,981 | 267 | 271 | 2,380 | 1 | 45 | 2,252 | 25 | 8,259 |
| cami_hybrid_ont | 20 | 760 | 71 | 93 | 1,062 | 17 | 682 | 2,854 | 17 | 5,575 |
| pratama_pe | 70 | 3,471 | 1,648 | 472 | 8,559 | 23 | 3,274 | 5,928 | 166 | 23,611 |
| pratama_hybrid_ont | 28 | 2,097 | 0 | 14 | 2,209 | 12 | 1,409 | 0 | 81 | 5,850 |
| Pratama catalogue | | | | | | | | 8,437 | | 8,437 |
| metagem_se | 7 | 55 | 12 | 61 | 381 | 2 | 169 | 247 | 0 | 935 |
| metagem_pe_split | 1,239 | 19,619 | 10,997 | 1,961 | 40,285 | 89 | 8,468 | 6,542 | 677 | 89,877 |
| Tara catalogue | | | | | | | | 21,905 | | 21,905 |
| **Total** | **1,460** | **29,959** | **13,132** | **3,175** | **60,845** | **171** | **15,908** | **50,557** | **994** | **176,202** |

- **Assembly** is MEGAHIT, the MEGAHIT draft, OPERA-MS and Flye. pratama_hybrid_ont's QC and assembly come from `results/e5/pratama_assembly_cost_lanes.tsv`, because its assemblies ran in the earlier run `zmwPgili`.
- **Mapping** is `assembly_stats`: the read mapping that feeds the binners' coverage.
- **Binners** is MetaBAT2, SemiBin2, DAS Tool and the Prodigal call DAS Tool reads.
- **GEM** is the per-MAG ORF call, CarveMe's two steps and MEMOTE.
- **Viral** in a shape's row is the viral lane that ran inside that shape's plans. The cami_hybrid_ont row holds the toy_humangut and plant_associated catalogues.

The full run cost 176,202 core-hours, about 219 per run. The estimate before batch 0 was 0.5–1.0 M. That estimate extrapolated E3's viral lane, about 125 core-hours per Pratama sample, and E2's COMEBin at 48 CPUs. E5's viral lane costs 30 core-hours per karlsson2013 run, 89 per Tara run and 130 per Pratama run. COMEBin at 12 CPUs is still 35% of the total.

Each total includes work that ran twice:
- pratama_pe's viral row (5,928) is a first viral pass in run `BxTeX5c0`. The Pratama catalogue recomputed it after the hybrid decision.
- cami_pe includes about 1,465 failed CarveMe attempts. A live `nxf_work` prune deleted their inputs, and run `Zze0nvLA` re-ran them.
- Tara chunk 5's run was reset to its raw reads after the 2026-10-12 quota hit. Its bbduk and interleave ran twice, and its MEGAHIT row includes 48 cancelled attempts.
- li2019 and bissett_base ran COMEBin twice. The first results were evicted for the CPU patch.
- The Tara catalogue includes 185 caller attempts cancelled when its first driver stopped for the queue-size relaunch.

The table excludes the batch 0 pilots and the Pratama assembler ablation (43,019 core-hours, on `feat/engine/bench-E3`).

### COMEBin with a 24 h first attempt

Tara chunks 1–4 ran COMEBin with a 6 h first attempt and a 12 h retry. 47 of 210 attempts died at 6 h. Chunks 5 and 6 ran a 24 h first attempt, on Tony's decision to "see how the scheduling plays out".

| Tara chunks | First attempt | Tasks done in one attempt | COMEBin core-hours per chunk |
|---|---|---|---|
| 1–4 | 6 h | 27 of 41 in each of chunks 1 and 2 | 4,944–6,307 |
| 5 | 24 h | 38 of 41 | 3,733 |
| 6 | 24 h | 41 of 41 | 3,900 |

The longer limit did not lengthen the queue wait. Chunk 5's COMEBin tasks waited p50 5, p90 13 and max 18 min, the same as chunks 1–4's 6 h tasks. It removed most retries, and it saved about 1,800 core-hours per chunk. Successful attempts ran p50 2.6 h, p90 6.5 h and max 7.4 h in chunk 5. Set COMEBin's first attempt to 24 h for samples of Tara's size.

COMEBin on CPU matches the GPU build. On korem2015, CPU run `cXNnetr5` gave 153 DAS Tool MAGs (70 HQ) against 152 (69 HQ) for GPU run `iMLq3ETu`, in 1 h 10 min against about 20 h of wall time. The Leiden-hang guard cancelled 44 hung COMEBin attempts over the whole run, and Nextflow retried each.

### A downstream array step waits for its whole upstream step

Each per-sample step runs as one Slurm array over the batch. DAS Tool starts only when all 41 COMEBin tasks of a chunk have finished. In Tara chunk 2, DAS Tool waited from about 08:00 to past 12:13 on 2026-10-11 for 3 COMEBin retries, with 38 samples ready and 4 tasks running on the account. The slowest sample's COMEBin sets each chunk's wall time. Releasing each sample to its next step as its own task finishes would remove that wait. E5 did not change it.

A Tara chunk took 2 d 11 h to 2 d 21 h on its first driver. The driver's limit is 3 days. Three chunks needed a relaunch after the quota hit. The relaunch hit every recorded shard in the cache.

## Comparison with E1–E4

E5 adds no new measurement. Each comparison below uses a score E1–E4 already report. Only the three short-read CAMI datasets compare row for row. E5's marine and strain are PacBio-only, but E1 and E2 scored their short reads. E5's toy_humangut and plant_associated are OPERA-MS hybrids, but E1 and E2 used their long reads alone.

### CAMI MAGs against E1 and E2

HQ is CheckM2 completeness above 90 and contamination below 5. MQ is completeness of 50 or more and contamination below 10, excluding HQ. E1 and E2 counts are from `findings/E1_E2_REPRODUCTION.md:296-333`.

| Dataset | Reads in E5 | DAS Tool MAGs E1 / E2 / E5 | HQ E1 / E2 / E5 | MQ E1 / E2 / E5 | LQ E1 / E2 / E5 |
|---|---|---|---|---|---|
| toy_mousegut | short | 1,042 / 1,046 / 1,049 | 671 / 675 / 660 | 336 / 337 / 356 | 35 / 34 / 33 |
| toy_hmp_airskinurogenital | short | 391 / 399 / 381 | 241 / 237 / 232 | 133 / 140 / 132 | 17 / 22 / 17 |
| toy_hmp_gastrooral | short | 380 / 391 / 376 | 231 / 236 / 225 | 123 / 127 / 132 | 26 / 28 / 19 |
| toy_humangut | hybrid (E1/E2 long) | 423 / 415 / 517 | 218 / 197 / 364 | 169 / 179 / 117 | 36 / 39 / 36 |
| plant_associated | hybrid (E1/E2 long) | 144 / 152 / 238 | 40 / 41 / 80 | 71 / 75 / 108 | 33 / 36 / 50 |
| marine | PacBio (E1/E2 short) | 319 / 314 / 37 | 156 / 160 / 0 | 145 / 140 / 12 | 18 / 14 / 25 |
| strain | PacBio (E1/E2 short) | 462 / 464 / 46 | 268 / 271 / 0 | 174 / 164 / 10 | 20 / 29 / 36 |

Over the three short datasets, E5 gives 1,806 MAGs and 1,117 HQ, against E1's 1,813 and 1,143 and E2's 1,836 and 1,148. That is within 2% of both on MAGs and within 3% on HQ. The hybrid lane adds MAGs over the long reads alone: 167 more HQ MAGs on toy_humangut and 39 more on plant_associated. The PacBio rows show what E5's PacBio-only shape gives up against short reads. They do not compare methods.

Cost of the MAG lane, QC to CheckM2, per sample: E5's cami_pe spent 76 core-hours, against E1's 51.9 and E2's 145.0 for the whole short arm (`E1_E2_REPRODUCTION.md:343-348`). E5's cami_hybrid_ont spent 49, against E1's 23.7 and E2's 56.8 for the long arm. E1 and E2's figures cover nf-core/mag's full process set, so read the comparison as an order of magnitude.

### AMBER, supplementary

AMBER scores binning conditional on each run's own assembly. Its gold standard is a vote of CAMISIM's read truth onto that run's contigs, so it cannot measure assembly error, and scores from two runs do not compare (`E1_E2_REPRODUCTION.md:394-398`). Tony proposed it as a supplementary table. `drivers/e5_amber.py` and `drivers/e5_amber.sbatch` score each sample with E2's vote script and AMBER 2.0.7.

Median DAS Tool `f1_score_bp` per dataset:

| Dataset | E2 | E5 | Samples scored in E5 |
|---|---|---|---|
| toy_mousegut | 0.204 | 0.199 | 49 |
| toy_hmp_airskinurogenital | 0.265 | 0.266 | 29 |
| toy_hmp_gastrooral | 0.393 | 0.373 | 20 |
| toy_humangut | 0.294 (long) | 0.220 (hybrid) | 20 |
| plant_associated | 0.114 (long) | 0.133 (hybrid) | 21 |
| marine | 0.187 (short) | 0.017 (PacBio) | 10 |
| strain | 0.031 (short) | 0.012 (PacBio) | 40 |

189 of 249 samples were scored. The 60 PacBio samples without a DAS Tool bin have nothing to score. AMBER 2.0.7 dies plotting a sample with one bin, so `e5_amber.sbatch` keeps the metrics it saved before the crash, for 36 samples.

### metaGEM GEMs against E4

E4 starts from metaGEM's published protein bins and builds models with the same GEM transforms as E5 (its modern lane). E5 assembles and bins the reads itself, so its bins are not E4's. The bin counts below compare E5's binning with metaGEM's. The model sizes compare the same GEM lane on two bin sets. E4's numbers are from `findings/E4_REPRODUCTION.md` and `results/e4/`.

| Study | metaGEM bins | E5 MAGs | E5 HQ / MQ / LQ | Median reactions / metabolites / genes, E4 modern | E5 | MEMOTE total, E4 modern / E5 |
|---|---|---|---|---|---|---|
| bissett_base | 274 | 370 | 33 / 274 / 63 | 1,373 / 958 / 525 | 1,497 / 1,031 / 638 | 0.861 / 0.859 |
| karlsson2013 | 4,134 | 3,782 | 2,072 / 1,501 / 209 | 1,215 / 809 / 539 | 1,212 / 806 / 538 | 0.830 / 0.826 |
| korem2015 | 154 | 153 | 70 / 58 / 25 | 1,356 / 935 / 557 | 1,299 / 898 / 460 | 0.706 / 0.828 |
| li2019 | 172 | 150 | 59 / 75 / 16 | 1,747 / 1,192 / 693 | 1,760 / 1,180 / 748 | 0.853 / 0.852 |
| sunagawa2015 | 9,371 | 9,975 | 1,886 / 6,143 / 1,946 | 1,355 / 963 / 451 | 1,345 / 952 / 478 | 0.859 / 0.859 |

Over the five studies E5 gives 14,430 MAGs against metaGEM's 14,105 bins: more on Tara and bissett_base, fewer on karlsson2013 and li2019. Median model sizes match E4's modern lane within 5% on karlsson2013, li2019 and sunagawa2015. E5's bissett_base models are larger, and its korem2015 models carry 17% fewer genes. E4 records no cost, so E5's GEM lane cost (15,908 core-hours, about 0.66 per MAG) has no comparator.

CAUTION: MEMOTE's consistency score is bimodal in E5. Its median is about 0.5 on toy_mousegut, toy_hmp_airskinurogenital, toy_humangut, plant_associated and strain, and about 0.9 elsewhere. E4 saw the same low peak on korem2015 (median 0.511), where E5 gives 0.906. Neither run explains the split.

### Pratama vOTUs against E3

The Pratama catalogue (`2ZnIRSUv`) runs the same assembler set as the ablation's R3: MEGAHIT on 48 short runs, and MEGAHIT then OPERA-MS on 17 pairings. `results/e5/pratama_score.py` scores it as the ablation scores its rungs.

| | vOTUs | Matched | Recovered | Recovery | Recovery by Pratama lane: metaSPAdes / MEGAHIT / hybrid | Paired median |
|---|---|---|---|---|---|---|
| E5 | 178,976 | 120,136 | 172,153 | 66.9% | 66.1 / 76.0 / 37.2% | 98.44% |
| E3 R3 | 172,295 | 123,599 | 168,336 | 65.4% | 60.7 / 78.3 / 36.0% | 98.88% |
| E3 R0, as published | 243,186 | 196,547 | 202,053 | 78.5% | 72.2 / 81.8 / 89.8% | 99.05% |

E5 agrees with R3 within 1.5 points. The difference is not a gain: E5 trims with the standard bbduk where E3 used `bbduk_pratama`, and runs its own copies of the viral transforms.

### The hybrid trade

E5 assembles hybrid samples with MEGAHIT, then OPERA-MS, in place of hybrid metaSPAdes. On Pratama the ablation measured the cost of that choice (R2 to R3):
- Overall recovery falls 4.7 points, from 70.1% to 65.4%.
- Recovery of Pratama's hybrid vOTUs falls from 74.9% to 36.0%. OPERA-MS barely scaffolds MEGAHIT's fragmented graph.
- Each hybrid assembly costs 107 core-hours against 297, about a third.

Only 58 of 805 samples are hybrid, so the saving is small at E5's scale. Tony chose OPERA-MS on 2026-10-08: "the decision is close, but slightly favouring opera-ms against my goals and constraints".

## Viral catalogues

| Catalogue | Runs | Plan | Wall time | vOTU representatives |
|---|---|---|---|---|
| toy_mousegut, airskin, gastrooral | 49, 29, 20 | in batch 1 (`TBeTrNqV`) | | 7,266 / 3,414 / 3,761 |
| marine, strain (PacBio) | 10, 100 | in batch 2 (`77Ja55m8`) | | 44,017 / 177,726 |
| li2019, bissett_base | 6, 46 | in batch 4 (`NlazkJl9`) | | 2,261 / 2,604 |
| korem2015 | 48 | in batch 4 (`cXNnetr5`) | | 343 |
| plant_associated | 21 | `dgWZ0owd` | 4 h 47 min | 7,203 |
| toy_humangut | 20 | `12QF5z5K` | 6 h 27 min | 29,011 |
| karlsson2013 | 145 | `vlYd6ssZ` | 22 h 47 min | 50,163 |
| Pratama | 65 | `2ZnIRSUv` | 31 h 34 min | 178,976 |
| sunagawa2015 | 246 | `qlwMaEID` | 32 h 46 min over two drivers | 225,110 |

MMseqs2 clustering and CheckV held at every pool size. CheckV runs in batches: 7 for karlsson2013, 26 for Pratama and 43 for Tara. `votu_cluster` took 34 min on Tara's 7,723,044 curated candidates.

### A catalogue's queue size serialised its arrays

Nextflow counts each element of a Slurm array against `executor.queueSize`, and `slurm.nf` submits arrays 100 wide. The catalogues ran at a queueSize of 112, so only one array was in flight at a time. A new array waited for the stragglers of the one before. Tara's four callers ran 3,464 tasks over 866 contig chunks, about 35 arrays in series.

- Tara's first driver ran 26 h 25 min and finished 2,579 of the 3,464 caller tasks.
- Relaunched at a queueSize of 800, the run replayed those 2,579 from the cache in 5 min and ran 8 arrays at once. It finished the callers in about 3 h and the whole catalogue in 6 h 14 min.
- karlsson2013's 22 h 47 min had the same cause, plus a 10 h 48 min queue wait on `viral_split`.

`CATALOGUE_QUEUE` in `drivers/e5.py` is now 800.

CAUTION: the driver's USR1 cancel stopped Nextflow but left a VirSorter2 array, submitted in the same second, running with no driver. Its results would have had no cache entry. Check `squeue` after a cancel, and cancel any task array left behind.

## fir incidents

Two byte incidents hit fir's 18.63 TiB project quota, both during Tara:
1. **2026-10-12, quota hit.** Chunk 4's read-mapping wave and chunk 5's 41 MEGAHIT tasks filled the quota. Chunks 4 and 5 failed, and neither indexed its cache. `assembly_stats` leaves a `temp.sam` of about 65 GB per sample, and chunk 4 held 3.3 TB of them. Recovery reset chunk 5 to its raw reads and relaunched chunks 3 and 4, which hit their recorded steps.
2. **2026-10-13, squeeze to 18.22 TiB.** Chunk 5's reset recomputed its trimmed reads while 37 MEGAHIT tasks grew, chunk 6's reads arrived, and chunks 3 and 4's read shards waited on their archive. Releasing chunks 5 and 6's raw reads brought fir to 16.91 TiB.

Rules that came out of them:
- Run at most two Tara chunks between trimming and DAS Tool at once. A chunk holds about 2.7 TB of read and BAM shards until it closes.
- Release a chunk's raw reads once its interleave, `seqkit_reads` and bbduk tasks have all exited 0. chinook holds the reads, and a restore preserves mtimes, so a relaunch still hits the cache.
- Clear `assembly_stats` temporaries every 15 min during a mapping wave, and throttle its array to 12 running tasks.
- Launch a chunk by a byte gate, not a phase gate.

CAUTION: a task that Slurm cancels still writes exit 0 through its wrapper. Judge a task by its outputs.

awm on altair restarted about every 30 min on 2026-10-13 and 2026-10-15. Each restart dropped the ssh master to fir, which blinded the pacer, the throttle and the temp cleanup. `fir_guard.sbatch` ran the throttle and the temp cleanup on fir itself after that.

## Deletions and archive

`results/e5/cleanup.tsv` lists all 95 deletions with the copy that made each safe and the job that ran it. They fall in five groups:

| Group | Copy that makes it safe |
|---|---|
| Finished experiments outside E5: the E2 and E3 agent homes, E1's close-out and controls, old checkouts, nf-core test scratch, the hybrid pilot | chinook `fir_bench_e1_e2/`, `fir_bench_e3/`, `fir_bench_e5/e5_hybrid/`, verified by a second Globus checksum pass that moved 0 bytes. Old checkouts are git commits |
| `nxf_work` of finished runs, and of live runs' finished CarveMe steps | none needed: every output is a promoted shard in `task_cache`. Each task dir keeps its `.command.cache` |
| Trimmed reads, interleaved reads and BAM shards in `task_cache` | chinook `fir_bench_e5/e5_home/task_cache`, verified by a zero-move repeat transfer |
| Per-MAG shards (ORFs, CheckM2, CarveMe, MEMOTE) and the run `results/` links to them | the same chinook archive |
| Raw metaGEM reads | chinook `fir_metagem/metagem/<study>`, the copy they were restored from |

E5 never evicted a MEGAHIT, Prodigal, binner, DAS Tool or `seqkit` shard. The viral catalogues read the assemblies, and a relaunch hits them.

To resume a closed run:
1. Restore its study's raw reads to `/scratch/phyberos/metagem/<study>` with `drivers/restore_metagem.sh`, preserving mtimes.
2. Restore the evicted shards from chinook `fir_bench_e5/e5_home/task_cache` to the same keys under `/scratch/phyberos/e5/metasmith/task_cache`.
3. Relaunch from the checkout named in the run's `batches.tsv` row. The probe reads the shard manifest on disk, so a restored shard hits.

fir held 8.05 TiB and 765K inodes at the end of the run, against an 18.63 TiB and 1 M inode quota. The top level of fir scratch still holds about 429 small scripts, logs and lists from E1–E4 and other sessions. They cost about 430 inodes, and some may belong to live sessions, so E5 left them in place.
