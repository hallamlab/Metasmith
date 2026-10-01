# E3: how the plan differs from Pratama 2026

## Purpose & Contents

E3 replicates the viral half of Pratama 2026 on its 65 short-read runs and 17 hybrid pairs. It assembles every run, pools the viral calls of every assembly lane with each contig still marked by sample, curates and clusters the pool to vOTUs, and scores the result against the paper's 257,252 published vOTUs of at least 5 kb. The goal is to recover those vOTUs, not to re-run the paper's commands literally.

This file compares each step of the paper with the plan `drivers/e3_pratama.py` solves to. It reports the recovery that plan reached on the full cohort, and lists the heuristic values it depends on, the steps out of scope and the open gaps. It holds findings only. The driver and `library/transforms/e3/` are the source of truth for the commands. The run history is in `R1_WAVES.md`, and the restore constraints are in `results/e3/RESUME.md`.

The paper's authority, highest first:
1. The authors' workflow files: `data/docs/pratama2026/Groundwater_virome/Workflows/MetaG_and_MAGs_bioinformatics.md` (MD below) and `Virus_bioinformatics.md` (VB below).
2. Supplementary Fig. 1 (`MOESM1`, page 2), for the curation counts.
3. The Methods section of `data/docs/pratama2026/PMC12960796.xml`.

Paths below are relative to `research/metasmith_benchmark/`, except `S/`, which is `src/metasmith_libraries/transforms/`. Line numbers are as of the commit that last changed this file.

## The plan

The solve has 34 steps. `page/dags/e3_pratama.dag.svg` draws it. The local plan key differs from fir's only because a transform id hashes its file path and modification time.

CAUTION: the plan binds 17 hybrid pairings only once each pairing has its own file on fir. Run `drivers/stage_hybrid_pairs.sh` on fir before a relaunch. The given library keys givens by path and resolves symlinks, so pairings that share a MinION file collapse to one given unless each is a separate hard link.

fir's task cache holds every step of wave e3_w9, so a relaunch of the same plan recomputes nothing. CAUTION: a cache hit needs the environment leaf ids the cache was written with, and those ids are local to the tree that compiled `resources/env/_metadata/index.yml`. Sync from a worktree carrying the env index of checkout `61c0eebc`. A fresh `-bm` mints new ids, and every task then misses the cache (`R1_WAVES.md` § HH).

## Decided differences

These are recorded decisions, not gaps:
- E3 stops at vOTUs. The MAG half and the annotation tail are out of scope (see below). E1 and E2 cover metagenomic assembly and binning.
- No Guppy step. SRA holds the basecalled FASTQ.
- Reads enter pre-interleaved from fir scratch.
- ERR3858126 is excluded. It is a second run of H32's 0.2 µm R1 metagenome, which the paper counts once as H32_0_2_1 (`drivers/e3_pratama.py:29-32`).
- The curation keep rules follow Supplementary Fig. 1, not the Methods. The Methods add a fourth rule, length ≥ 1 kb, which would keep nearly every contig. Supp Fig 1 omits it.
- The island filter matches the paper's categories only, not Antonio's broader list from `10_filtering_3.sh`. The paper names no annotation tool, so geNomad's `annotate` supplies the gene annotations. A transposon counts by its transposase.

## Read QC and assembly

Status: **match** is the same command and settings. **version** is the same command and settings under another tool version. **differs** is the same tool with a setting or behaviour that differs. **substitute** is a different tool or shape that produces the same type. **missing** has no step in the plan. Memory, threads, batching and file staging never count as a difference. The resource heuristics below list them.

| Paper step | Paper setting | E3 transform | Status | Difference |
|---|---|---|---|---|
| bbduk (MD 15-30) | `ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1` | `library/transforms/e3/bbduk_pratama.py:22-24` | differs | Same trimming flags. No stats file, and the image's `adapters.fa`. bbtools 39.49 against 39.01. |
| fastp report (MD 35) | `-R -j -h -w` | `e3/fastp_report_pratama.py:22` | match | Report only, as in the paper. fastp 1.0.1. |
| metaSPAdes (MD 44) | `--meta -k 21,33,55,77 -m 190` | `e3/spades_pratama.py:24` | version | SPAdes 3.15.5 against 3.15.2. |
| MEGAHIT (MD 52) | defaults | `S/assembly/megahit.py:28-31` | version | MEGAHIT 1.2.9 against 1.1.3. |
| Hybrid metaSPAdes (MD 146) | `--meta -m 380 --nanopore` | `e3/spades_hybrid_pratama.py:28` | version | SPAdes 3.15.5 against 3.15.2. |

## Viral identification and vOTUs

| Paper step | Paper setting | E3 transform | Status | Difference |
|---|---|---|---|---|
| DeepVirFinder (VB 19) | `-l 1000`, then score ≥ 0.9 and p ≤ 0.05 (Methods) | `e3/deepvirfinder_pratama.py:14-16,104` | match | Same length and cut. The transform drops contigs over 30% N before `dvf.py` sees them (`:94`). That is `dvf.py`'s own rule, applied early to dodge its crash when the accepted count is a multiple of 100 and the last record is rejected. |
| VIBRANT | `-f nucl -virome` | `e3/vibrant_pratama.py:105` | match | Adds `-no_plot`. VIBRANT 1.2.1. |
| geNomad (VB 34) | `end-to-end --cleanup --splits 48 --min-virus-marker-enrichment 1 --min-virus-hallmarks 1` | `e3/genomad_pratama.py:48-49` | version | Same flags. geNomad 1.11.0 against 1.5.1. Database unpinned. |
| VirSorter2 | `--include-groups dsDNAphage,ssDNA --keep-original-seq --min-score 0.5 --min-length 5000` | `e3/virsorter2_pratama.py:40-69` | version | Same flags. `\|\|full` and `\|\|lt2gene` calls span the whole contig, and partial calls use `full_bp_*`. VirSorter2 2.2.4 against 2.2.3. |
| Pooling | every caller on every assembly, one record per caller | `e3/merge_candidate_calls_pratama.py:28-38,69-72` | match | 3 lanes (metaSPAdes, MEGAHIT, hybrid metaSPAdes) × 4 callers. Each caller's call is its own record with its own boundaries, named `sample\|lane\|caller\|contig\|start_end`, as the published names carry sample, caller and assembler. |
| CheckV (VB 58) | `end_to_end` | `e3/checkv_batch_pratama.py` + `checkv_merge_pratama.py` | match | CheckV 1.0.3, database unpinned. |
| Curation (Supp Fig 1) | keep if > 0 viral genes, or 0 viral and 0 host genes, or ≥ 75% unknown genes; CheckV host trimming; spot checks | `e3/curate_trim_batch_pratama.py:29-34` + `curate_merge_pratama.py` | differs | Gene counts come from CheckV's quality summary. A kept provirus enters as its trimmed region from `proviruses.fna`, every other kept contig whole from `viruses.fna`. No spot checks. The paper goes from 4,717,962 to 4,708,626 here. |
| MMseqs2 (VB 50) | `easy-cluster --min-seq-id 0.95 -c 0.8` | `e3/mmseqs_votu_pratama.py:22` | match | Clusters the curated set. CAUTION: the Methods describe 80% coverage of the shorter sequence, but the workflow file's command uses cov-mode 0, which needs 80% of both. The command wins by the authority order. Under the Methods' wording a short call nested in a long one would cluster with it. |
| vOTUs ≥ 5 kb | | `e3/votu_representatives_pratama.py:14` | match | |
| Island filter (Methods) | drop vOTUs > 100 kb carrying transposon, LPS, endonuclease, integrase or plasmid-stability genes | `e3/genomad_island_annotate_pratama.py:18-36` + `island_filter_pratama.py:22-39` | match | The paper's categories, matched case-insensitively over geNomad's annotation columns. The paper removed 562 of 637 vOTUs over 100 kb (88%). E3 removed 502 of 639 (79%). |
| Recovery | | `S/viromics/pratama_votu_recovery.py:31-32`, `e3/final_votu_recovery_pratama.py:22-23` | | skani of the published vOTUs against the pooled set and against the final ≥ 5 kb set. |

The published names carry sample, assembler and caller, so recovery can be scored per lane and per caller. By assembler, the published set holds 113,105 metaSPAdes, 112,924 MEGAHIT and 31,223 hybrid vOTUs. By caller, it holds 105,001 VIBRANT, 89,689 geNomad, 52,601 VirSorter2 and 9,961 DeepVirFinder vOTUs.

## Recovery

Wave e3_w9 ran the plan on all 65 runs and 17 hybrid pairs (`R1_WAVES.md` § HH). `results/e3/votu_recovery_curve.py` scores each skani table against the 257,252 published vOTUs, per lane and per caller. A published vOTU counts as recovered when a sequence matches it at or above the ANI and aligned-fraction cutoff. **Any** accepts a match from any run. **Same** accepts only a match from the run that the published name carries.

Read one column per set:
- For the pool, read **same**. Every call is still its own record there, so a vOTU's own sample can supply it.
- For the final set, read **any**. MMseqs2 keeps one representative per cluster across all samples, so **same** falls as samples are added. It measures clustering, not recovery.

| Set | ANI / AF | Any | Same |
|---|---|---|---|
| Pool | 90 / 30 | 99.2% | 96.5% |
| Pool | 95 / 50 | 97.3% | 90.9% |
| Pool | 95 / 85 | 82.1% | 67.6% |
| Final ≥ 5 kb | 90 / 30 | 93.4% | 73.2% |
| Final ≥ 5 kb | 95 / 50 | 91.4% | 69.7% |
| Final ≥ 5 kb | 95 / 85 | 78.5% | 55.8% |

By lane and caller at 95 / 85. The final set is a subset of the pool, so compare final **any** with pool **any**, never with pool **same**:

| Published as | vOTUs | Pool, any | Pool, same | Final, any |
|---|---|---|---|---|
| hybrid metaSPAdes | 31,223 | 92.6% | 88.4% | 89.8% |
| MEGAHIT | 112,924 | 85.4% | 74.7% | 81.8% |
| metaSPAdes | 113,105 | 75.9% | 54.7% | 72.2% |
| geNomad | 89,689 | 83.2% | 67.0% | 81.0% |
| VIBRANT | 105,001 | 82.3% | 69.7% | 77.6% |
| VirSorter2 | 52,601 | 80.0% | 64.4% | 76.1% |
| DeepVirFinder | 9,961 | 81.1% | 66.3% | 77.8% |

The callers score within 6 points of each other. The lanes do not. Short-read metaSPAdes trails MEGAHIT in both years, and most in 2019. Pool same-sample recovery, per sample, with the vOTU-weighted figure in brackets:
- 2019 metaSPAdes: 32–49% (42%). 2019 MEGAHIT, from the same reads: 59–81% (75%).
- 2022 metaSPAdes: 57–91% (66%). 2022 MEGAHIT: 68–82% (75%).
- Hybrid metaSPAdes: 79–92% (88%).

2019 MEGAHIT runs on the same sample keys and scores like 2022, so the 2019 metaSPAdes shortfall is not a key mismatch in scoring. `results/e3/votu_recovery_by_sample.py` gives these figures per sample and lane. The shortfall is not a smaller assembly either. Per run, the median 2019 metaSPAdes assembly holds 134 Mbp in contigs of at least 5 kb, against 118 Mbp in 2022 and 142 Mbp for 2019 MEGAHIT (`results/e3/assembly_size.py`). Even pool **any** is low for 2019 metaSPAdes (68% against 83% in 2022), so many of those published vOTUs match no E3 sequence. The 2019 runs come from an earlier study of the same site (the paper's ref. 57) and were sequenced on a NextSeq 500, the 2022 runs on a NovaSeq 6000. The cause is open (see Open gaps).

Stage counts, against Supp Fig 1:

| Stage | Paper | E3 |
|---|---|---|
| Identified contigs | 4,717,962 | 7,552,829 calls |
| After the keep rules | 4,708,626 | 7,102,468 |
| vOTUs | 2,412,499 (≥ 1 kb) | 4,018,877 (no length floor) |
| vOTUs ≥ 5 kb | 257,814 | 243,688 |
| Island filter removes | 562 of 637 over 100 kb | 502 of 639 |
| Final vOTUs ≥ 5 kb | 257,252 | 243,186 |

The final set is 94.5% the size of the published one. The upstream rows do not compare directly. E3 counts one record per caller call, and Supp Fig 1 does not say whether its "identified contigs" count calls or contigs. The paper's vOTU count has a 1 kb floor and E3's has none. With that caveat, E3's pool is 1.6 times the paper's, and its keep rules drop 6.0% against the paper's 0.2%. The paper states 562 removed. Its 637 is 562 plus the 75 published vOTUs over 100 kb.

## Tool versions

Pratama's versions are the ones the Methods state. E3 keeps its current versions by decision.

| Tool | Pratama | E3 |
|---|---|---|
| BBMap (bbduk) | 39.01 | 39.49 |
| fastp | not stated | 1.0.1 |
| SPAdes, short-read and hybrid | 3.15.2 | 3.15.5 |
| MEGAHIT | 1.1.3 | 1.2.9 |
| Guppy | 6.0.1, sup model | not run |
| geNomad | 1.5.1 | 1.11.0 |
| VirSorter2 | 2.2.3 | 2.2.4 |
| VIBRANT | 1.2.1 | 1.2.1 |
| DeepVirFinder | 1.0 | image `multifractal/deepvirfinder:0.1`, code version not reported |
| CheckV | not stated | 1.0.3 |
| MMseqs2 | not stated | 17.b804f |
| SeqKit, skani | not used | 2.13.0, 0.2.2 |
| Databases | not stated | geNomad and CheckV unpinned, VIBRANT built for 1.2.1, VirSorter2 built for 2.2.4 (`drivers/_common.py:100-131`) |

## Out of scope

No target reaches these steps. Their E3 transforms stay in `library/transforms/e3/`.
- MAG half: MetaWRAP binning and refinement, BinSanity, abawaca, CheckM, the MAG quality filter, dRep, CoverM genome, GTDB-Tk and DRAM.
- Annotation tail: vConTACT3, the DRAM-v preparation, DRAM-v and AMG curation, iPHoP, minced and BLASTn spacers, MetaPop, CoverM contig and GTDB-Tk de novo.

## Scientific heuristics

These values change what the plan produces. Each is a choice the paper does not make, or makes differently.

| Value | Where | Paper |
|---|---|---|
| MinION–Illumina pairing by well and `02um_2022` | `drivers/e3_pratama.py:87-100` | 17 hybrids |
| Unknown fraction = (genes − viral − host) / genes, from CheckV | `e3/curate_trim_batch_pratama.py:34` | not stated |
| Island annotation by `genomad annotate` | `e3/genomad_island_annotate_pratama.py:36` | not stated |
| skani `--min-af 15` for vOTU recovery | `S/viromics/pratama_votu_recovery.py:32` | not applicable |

## Resource heuristics

These size tasks and do not change the results. Where one does, the scientific table lists it.
- Assembler memory flags follow the grant: metaSPAdes `-m` 0.95 × grant (`e3/spades_pratama.py:19`, paper 190), hybrid `-m` min(380, 0.95 × grant) (`e3/spades_hybrid_pratama.py:23`, paper 380), MEGAHIT `--memory` 0.85 × grant, bbduk `-Xmx` 0.85 × grant.
- Callers run on 240 Mbp contig batches (`S/logistics/splitContigsForAmr.py:10`, `e3/split_hybrid_contigs_pratama.py:15`), and CheckV on 500 Mbp slices of the pool. The paper runs each on the whole set.
- CAUTION: geNomad's score calibration estimates the composition of each input file, so a 240 Mbp batch can shift a borderline geNomad score slightly. The other callers score each contig on its own.
- Declared cpus, GB and hours live in each transform's resources. The largest are metaSPAdes 48/192/24 and hybrid 48/384/20.
- `SCALED` overrides the first attempt of 12 steps (`drivers/e3_pratama.py:47-60`). Each entry is sized to the sacct MaxRSS and wall of waves 1–8 and e3_w9.
- Each retry doubles memory and time (`drivers/_common.py:410-411`). Time clamps at 24 h (`:327`), except 36 h for metaSPAdes and hybrid (`:333-338`).
- Memory clamps at 192 GB for `SCALED` steps only (`:347`), except 768 GB for hybrid (`:354-356`). A step outside `SCALED` doubles its declared memory without a cap.
- Nextflow runs 4 tries, arrays of 25 and a queue of 500 (`drivers/e3_pratama.py:236`).
- CAUTION: DeepVirFinder at its memory cap hangs instead of exiting. At 16 GB, 12 tasks sat at the cap for 5 h 59 min on about 15 min of cpu, then ended OUT_OF_MEMORY (11) or FAILED (1). Each 32 GB retry finished within an hour.
- CAUTION: MMseqs2 sizes its k-mer table from the node's RAM, not the Slurm grant, so it does not split under a grant. It fails with OOM instead. Its 128 GB retry peaked at 116 GiB, so the grant fits only by margin. `--split-memory-limit` in `e3/mmseqs_votu_pratama.py` is the real fix.

## Open gaps

| Gap | What closing it takes |
|---|---|
| 2019 metaSPAdes recovery | Find why 2019 metaSPAdes recovers 42% same-sample in the pool against 66% in 2022 and 75% for 2019 MEGAHIT. Check whether the paper reused ref. 57's 2019 assemblies, and whether NextSeq poly-G tails survive bbduk. Then compare the published 2019 metaSPAdes vOTUs with our contigs directly: length, ANI and aligned fraction of their best hits. |
| Pool size and keep-rule drop | Find why E3 pools 7.55 M calls against the paper's 4.72 M, and why its keep rules drop 6.0% against 0.2%. Count the pool per caller and per length class first. |
| Curation spot checks | None. The paper's six example contigs illustrate figures and are not a sample. |
