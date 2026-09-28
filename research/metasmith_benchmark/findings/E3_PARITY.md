# E3: how the plan differs from Pratama 2026

## Purpose & Contents

E3 replicates the viral half of Pratama 2026 on its 65 short-read runs and 17 hybrid pairs. It assembles every run, pools the viral calls of every assembly lane with each contig still marked by sample, curates and clusters the pool to vOTUs, and scores the result against the paper's 257,252 published vOTUs of at least 5 kb. The goal is to recover those vOTUs, not to re-run the paper's commands literally.

This file compares each step of the paper with the plan `drivers/e3_pratama.py` solves to. It lists the heuristic values that plan depends on, the steps out of scope, and the open gaps. It holds findings only. The driver and `library/transforms/e3/` are the source of truth for the commands. The run history is in `R1_WAVES.md`, and the restore constraints are in `results/e3/RESUME.md`.

The paper's authority, highest first:
1. The authors' workflow files: `data/docs/pratama2026/Groundwater_virome/Workflows/MetaG_and_MAGs_bioinformatics.md` (MD below) and `Virus_bioinformatics.md` (VB below).
2. Supplementary Fig. 1 (`MOESM1`, page 2), for the curation counts.
3. The Methods section of `data/docs/pratama2026/PMC12960796.xml`.

Paths below are relative to `research/metasmith_benchmark/`, except `S/`, which is `src/metasmith_libraries/transforms/`. Line numbers are as of commit `a2814562`.

## The plan

The solve has 34 steps. `page/dags/e3_pratama.dag.svg` draws it. The local plan key differs from fir's only because a transform id hashes its file path and modification time.

CAUTION: the plan binds 17 hybrid pairings only once each pairing has its own file on fir. Run `drivers/stage_hybrid_pairs.sh` on fir before a relaunch. The given library keys givens by path and resolves symlinks, so pairings that share a MinION file collapse to one given unless each is a separate hard link.

A relaunch reuses wave 8's short-read assemblies and their geNomad and VIBRANT calls. It recomputes the hybrid assemblies, the VirSorter2 calls, the DeepVirFinder calls and every step from the merge onward.

## Decided differences

These are recorded decisions, not gaps:
- E3 stops at vOTUs. The MAG half and the annotation tail are out of scope (see below). E1 and E2 cover metagenomic assembly and binning.
- No Guppy step. SRA holds the basecalled FASTQ.
- Reads enter pre-interleaved from fir scratch.
- ERR3858126 is excluded. It is a second run of H32's 0.2 µm R1 metagenome, which the paper counts once as H32_0_2_1 (`drivers/e3_pratama.py:29-32`).
- The curation keep rules follow Supplementary Fig. 1, not the Methods. The Methods add a fourth rule, length ≥ 1 kb, which would keep nearly every contig. Supp Fig 1 omits it.
- The island filter uses Antonio's pattern list from `10_filtering_3.sh`, not the paper's shorter list. The paper names no annotation tool, so geNomad's `annotate` supplies the gene annotations.

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
| Pooling | every caller on every assembly | `e3/merge_candidate_calls_pratama.py:28-38` | differs | 3 lanes (metaSPAdes, MEGAHIT, hybrid metaSPAdes) × 4 callers. Overlapping or abutting calls on one contig merge to their union. Each record is named `sample\|lane\|contig\|start_end`. |
| CheckV (VB 58) | `end_to_end` | `e3/checkv_batch_pratama.py` + `checkv_merge_pratama.py` | match | CheckV 1.0.3, database unpinned. |
| Curation (Supp Fig 1) | keep if > 0 viral genes, or 0 viral and 0 host genes, or ≥ 75% unknown genes; CheckV host trimming; spot checks | `e3/curate_trim_batch_pratama.py:29-34` + `curate_merge_pratama.py` | differs | Gene counts come from CheckV's quality summary. A kept provirus enters as its trimmed region from `proviruses.fna`, every other kept contig whole from `viruses.fna`. No spot checks. The paper goes from 4,717,962 to 4,708,626 here. |
| MMseqs2 (VB 50) | `easy-cluster --min-seq-id 0.95 -c 0.8` | `e3/mmseqs_votu_pratama.py:22` | match | Clusters the curated set. |
| vOTUs ≥ 5 kb | | `e3/votu_representatives_pratama.py:14` | match | |
| Island filter (Methods) | drop vOTUs > 100 kb carrying transposon, LPS, endonuclease, integrase or plasmid-stability genes | `e3/genomad_island_annotate_pratama.py:18-36` + `island_filter_pratama.py:22-65` | differs | Antonio's list adds partition, `parA`, `parB`, toxin-antitoxin, `relE`, `hipA` and `stability`. It matches case-insensitively, so `parA` also hits words such as "separation". Expect more removals than the paper's 562. |
| Recovery | | `S/viromics/pratama_votu_recovery.py:31-32`, `e3/final_votu_recovery_pratama.py:22-23` | | skani of the published vOTUs against the pooled set and against the final ≥ 5 kb set. |

The published names carry sample, assembler and caller, so recovery can be scored per lane and per caller. By assembler, the published set holds 113,105 metaSPAdes, 112,924 MEGAHIT and 31,223 hybrid vOTUs. By caller, it holds 105,001 VIBRANT, 89,689 geNomad, 52,601 VirSorter2 and 9,961 DeepVirFinder vOTUs.

## Out of scope

No target reaches these steps. Their E3 transforms stay in `library/transforms/e3/`.
- MAG half: MetaWRAP binning and refinement, BinSanity, abawaca, CheckM, the MAG quality filter, dRep, CoverM genome, GTDB-Tk and DRAM.
- Annotation tail: vConTACT3, the DRAM-v preparation, DRAM-v and AMG curation, iPHoP, minced and BLASTn spacers, MetaPop, CoverM contig and GTDB-Tk de novo.

## Scientific heuristics

These values change what the plan produces. Each is a choice the paper does not make, or makes differently.

| Value | Where | Paper |
|---|---|---|
| MinION–Illumina pairing by well and `02um_2022` | `drivers/e3_pratama.py:87-100` | 17 hybrids |
| Interval union: overlapping or abutting calls merge | `e3/merge_candidate_calls_pratama.py:69-78` | not stated |
| Unknown fraction = (genes − viral − host) / genes, from CheckV | `e3/curate_trim_batch_pratama.py:34` | not stated |
| Island annotation by `genomad annotate` | `e3/genomad_island_annotate_pratama.py:36` | not stated |
| Database versions: geNomad and CheckV unpinned, VirSorter2 2.2.4 | `drivers/_common.py:100-131` | see the tables above |
| skani `--min-af 15` for vOTU recovery | `S/viromics/pratama_votu_recovery.py:32` | not applicable |

## Resource heuristics

These size tasks and do not change the results. Where one does, the scientific table lists it.
- Assembler memory flags follow the grant: metaSPAdes `-m` 0.95 × grant (`e3/spades_pratama.py:19`, paper 190), hybrid `-m` min(380, 0.95 × grant) (`e3/spades_hybrid_pratama.py:23`, paper 380), MEGAHIT `--memory` 0.85 × grant, bbduk `-Xmx` 0.85 × grant.
- Callers run on 240 Mbp contig batches (`S/logistics/splitContigsForAmr.py:10`, `e3/split_hybrid_contigs_pratama.py:15`), and CheckV on 500 Mbp slices of the pool. The paper runs each on the whole set.
- CAUTION: geNomad's score calibration estimates the composition of each input file, so a 240 Mbp batch can shift a borderline geNomad score slightly. The other callers score each contig on its own.
- Declared cpus, GB and hours live in each transform's resources. The largest are metaSPAdes 48/192/24 and hybrid 48/384/20.
- The driver scales MEGAHIT past its declaration to 32/128/12 (`drivers/e3_pratama.py:42-44`).
- Each retry doubles memory and time (`drivers/_common.py:398-399`). Time clamps at 24 h (`:315`), except 36 h for metaSPAdes and hybrid (`:321-326`). Memory clamps at 192 GB for scaled steps only (`:335`).
- Nextflow runs 4 tries, arrays of 25 and a queue of 500 (`drivers/e3_pratama.py:216`).

## Open gaps

| Gap | What closing it takes |
|---|---|
| No recovery number | Stage the hybrid pairs on fir, relaunch, and score both recovery tables per lane and per caller. |
| Curation spot checks | None. The paper's six example contigs illustrate figures and are not a sample. |
