# E3: how the plan differs from Pratama 2026

## Purpose & Contents

E3 replicates Pratama 2026's groundwater virome and MAG study on its 65 short-read runs and 17 hybrid pairs. Its goal is to recover the paper's vOTUs, MAGs and AMGs, not to re-run the paper's commands literally. This file compares each step of the paper with the plan `drivers/e3_pratama.py` solves to. It lists the heuristic values that plan depends on, and the open gaps with what it takes to close each one. It holds findings only. The driver and `library/transforms/e3/` are the source of truth for the commands. The run history is in `R1_WAVES.md`, and the restore constraints are in `results/e3/RESUME.md`.

The paper's authority, highest first:
1. The authors' workflow files: `data/docs/pratama2026/Groundwater_virome/Workflows/MetaG_and_MAGs_bioinformatics.md` (MD below) and `Virus_bioinformatics.md` (VB below).
2. The Methods section of `data/docs/pratama2026/PMC12960796.xml`.

Paths below are relative to `research/metasmith_benchmark/`, except `S/`, which is `src/metasmith_libraries/transforms/`. Line numbers are as of commit `c754aa09`.

## The plan

The default solve has 39 steps, and `--with-host-prediction --with-gtdbtk` adds GTDB-Tk and iPHoP's default pass for 41. The 39-step order matches wave 8's (`Qt0rbV1R`) step for step. The local plan key differs from fir's only because a transform id hashes its file path and modification time. `page/dags/e3_pratama.dag.svg` draws the default plan.

## Decided differences

These are recorded decisions (`PLAN.md`, 136–151), not gaps:
- No Guppy step. SRA holds the basecalled FASTQ.
- GTDB-Tk r232 instead of r202.
- No CCTyper. It is in none of the reference studies. Pratama's spacer caller is minced, which is a gap.
- No CoverM. `assembly_stats_pratama` gives per-base coverage instead.
- Reads enter pre-interleaved from fir scratch.
- ERR3858126 is excluded. It is a second run of H32's 0.2 µm R1 metagenome, which the paper counts once as H32_0_2_1 (`drivers/e3_pratama.py:29-32`).

## MAG half

Status: **match** is the same command and settings. **differs** is the same tool with a setting or version that differs. **substitute** is a different tool or shape that produces the same type. **missing** has no step in the plan.

| Paper step | Paper setting | E3 transform | Status | Difference |
|---|---|---|---|---|
| bbduk (MD 15-30) | `ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1` | `library/transforms/e3/bbduk_pratama.py:22-24` | differs | Same trimming flags. No stats file, image's `adapters.fa`, `-Xmx` is 0.85 × grant, bbtools 39.49 against 39.01. |
| fastp report (MD 35) | `-R -j -h -w` | `e3/fastp_report_pratama.py:22` | match | Report only, as in the paper. fastp 1.0.1. |
| metaSPAdes (MD 44) | `--meta -k 21,33,55,77 -m 190` | `e3/spades_pratama.py:19,24` | differs | `-m` is 0.95 × grant: 182 GB, then 364 GB on retry. SPAdes 3.15.5 against 3.15.2. |
| MEGAHIT (MD 52) | defaults | `S/assembly/megahit.py:28-31` | differs | `--memory` 0.85 × grant. MEGAHIT 1.2.9 against 1.1.3. Feeds the viral merge only, as in the paper. |
| Hybrid metaSPAdes (MD 146) | `--meta -m 380 --nanopore` | `e3/spades_hybrid_pratama.py:23,28` | differs | `-m` is min(380, 0.95 × grant) = 364. Its assemblies feed nothing downstream. The paper's feed the viral pool. |
| MetaWRAP binning (MD 61) | `--universal --maxbin2 --metabat2 --concoct` | `e3/metawrap_{metabat2,maxbin2,concoct}_pratama.py` | differs | One binner per task, each with its own bwa mapping. metawrap 1.3.0 against 1.3.2. |
| MaxBin2, 107 markers (MD 63) | second pass without `--universal` | none | missing | |
| BinSanity (MD 71-74) | | none | missing | New transform. |
| abawaca (MD 81-89) | | none | missing | New transform. |
| Refinement round 1 (MD 95) | abawaca ×2 + BinSanity | none | missing | Needs BinSanity and abawaca. |
| Refinement round 2 (MD 97) | `-c 50 -x 10`, metabat2 + concoct + round 1 | `e3/metawrap_refine_pratama.py:27-28,61-63` | substitute | One round over metabat2, maxbin2 and concoct. |
| CheckM 1.1.3 | inside refinement | inside the metawrap image | differs | Version inside the image not verified. |
| MAG quality filter (Methods) | QS ≥ 50, HQ/MQ, anvi'o CPR | none | missing | The paper goes from 1,778 to 1,275 MAGs here. |
| dRep (MD 104) | `-pa 0.90 -sa 0.99 -comp 50 -con 10` | none in E3 | missing | `bench/drep_sample.py` and `drep_study.py` carry the flags but take CheckM2 inputs, not MetaWRAP bins. |
| CoverM genome (MD 112) | 0.95 / 0.75 / 0.25, trimmed_mean | `e3/assembly_stats_pratama.py` | decided | minimap2 `-x sr` against the sample's own assembly. Mean depth with no identity or coverage filter, and no per-MAG rollup. |
| Read normalisation (Methods) | per QC'd reads per Tb | `S/assembly/seqkit_reads.py` | differs | seqkit counts the raw reads. |
| GTDB-Tk classify (MD 120) | defaults, r202 | `bench/gtdbtk_image.py:52` | decided | Only with `--with-gtdbtk`. r232, `--skip_ani_screen`. Runs on per-sample refined bins, because no dereplicated set exists. |
| DRAM (MD 128-129) | `--min_contig_size 1000`, then distill | `e3/dram_kofam_pratama.py:58`, `dram_pfam_pratama.py`, `dram_distill_pratama.py:26-34` | differs | KOfam and Pfam only. Two annotate runs, outer-joined. DRAM 1.5.0 against 1.3.6. |

## Viral half

| Paper step | Paper setting | E3 transform | Status | Difference |
|---|---|---|---|---|
| DeepVirFinder (VB 19) | `-l 1000`, then score ≥ 0.9 and p ≤ 0.05 (Methods) | `bench/deepvirfinder.py:47` | missing | Built but not a target, so its calls never reach the pool. The transform and `R1_WAVES.md` say the paper gives no cut. The Methods do give one. |
| VIBRANT | `-f nucl -virome` | `e3/vibrant_pratama.py:105` | match | Adds `-no_plot`. VIBRANT 1.2.1. |
| geNomad (VB 34) | `end-to-end --cleanup --splits 48 --min-virus-marker-enrichment 1 --min-virus-hallmarks 1` | `e3/genomad_pratama.py:48-49` | differs | Same flags. geNomad 1.11.0 against 1.5.1. Database unpinned. |
| VirSorter2 | `--include-groups dsDNAphage,ssDNA --keep-original-seq --min-score 0.5 --min-length 5000` | `e3/virsorter2_pratama.py:34-35,74-75` | differs | Same flags. The call writer takes `trim_bp_*` before `full_bp_*`, so the merge extracts the trimmed region despite `--keep-original-seq`. VirSorter2 2.2.4 against 2.2.3. |
| Pooling | all callers, all assemblies | `e3/merge_candidate_calls_pratama.py:29-35,58-66` | differs | metaSPAdes and MEGAHIT × geNomad, VirSorter2 and VIBRANT. Overlapping or abutting calls on one contig merge to their union. No hybrid assemblies, no DeepVirFinder. |
| Curation (Methods) | keep if > 0 viral genes, or 0 viral and 0 host genes, or ≥ 1 kb, or ≥ 75% unknown genes; CheckV host trimming; spot checks | none | missing | Host trimming happens before clustering, so it changes the vOTU set itself. A post-hoc join cannot close it. |
| Island filter (Methods) | drop vOTUs > 100 kb carrying transposon, LPS, endonuclease, integrase or plasmid-stability genes | none | missing | Removed 562 vOTUs in the paper. Its 82,807 ≥ 10 kb before this filter is the number E3's 70,785 compares with. |
| MMseqs2 (VB 50) | `easy-cluster --min-seq-id 0.95 -c 0.8` | `e3/mmseqs_votu_pratama.py:18` | match | Clusters the uncurated frozen set. |
| CheckV (VB 58) | `end_to_end` | `e3/checkv_batch_pratama.py:41` + `checkv_merge_pratama.py` | differs | Runs in slices. Keeps the four tables and discards `viruses.fna` and `proviruses.fna`. CheckV 1.0.3, database unpinned. |
| CoverM contig (VB 66) | 0.95 / 0.75 / 0.7, trimmed_mean, `--coupled` | none | decided | MEGAHIT-derived vOTUs get no coverage. |
| vConTACT3 (VB 72) | `--nucleotide --db-domain prokaryotes --db-version 220 --exports cytoscape`, on ≥ 10 kb | `e3/vcontact3_pratama.py:73-75` | differs | Runs on the ≥ 10 kb representatives, as in the paper. No `--db-domain`, database v230 (`drivers/_common.py:127`), cosmograph export. Assignments are produced but are not a target. |
| VirSorter2 prep for DRAM-v (VB 80) | `--prep-for-dramv` on CheckV `combined.fna` | `e3/dramv_checkv_pratama.py`, `dramv_vs2_prep_pratama.py` | match | |
| DRAM-v (VB 84-87) | `-v affi --min_contig_size 1000`, then distill | `e3/dramv_kofam_pratama.py:44`, `dramv_pfam_pratama.py`, `dramv_distill_pratama.py:47-49` | differs | Same flags. KOfam and Pfam only. VOGDB is null, so distill crashes on `vogdb_categories` and the V and P flags cannot fire. |
| AMG curation (Methods) | keep categories 1-3 and M; drop category 4, V/A/P/B flags, and named false-positive families | none | missing | |
| MetaPop (VB 98) | `--min_cov 70` | `bench/metapop_study.py` | missing | Built but not a target. bowtie2 at defaults. MetaPop 0.0.60 against 0.0.48. |
| GTDB-Tk de novo | outgroups Cyanobacteria / Altarchaeota | `S/metagenomics/taxonomy/gtdbtk_de_novo.py` | missing | Not a target. |
| iPHoP default (VB 115) | Sept_2021_pub_rw, on ≥ 10 kb vOTUs | `e3/iphop_predict_default_pratama.py:10-27` | differs | Only with `--with-host-prediction`. Reads the whole frozen set, not the ≥ 10 kb vOTUs. Aug_2023_pub_rw with iPHoP 1.3.3. `-m 90`. |
| iPHoP augmented (VB 112, 118) | `add_to_db` with the study's MAGs, then predict | `S/viromics/iphop_add_to_db.py`, `iphop_predict.py` | missing | Needs a dereplicated MAG set. |
| minced (VB 126) | `-minNR 3 -spacers` | none | missing | New transform. |
| BLASTn spacers | | `S/viromics/blast_spacers_to_contigs.py` | missing | Flags match. Unreachable without minced. |

## Scientific heuristics

These values change what the plan produces. Each is a choice the paper does not make, or makes differently.

| Value | Where | Paper |
|---|---|---|
| metaSPAdes `-m` = 0.95 × grant | `e3/spades_pratama.py:19` | 190 |
| Hybrid `-m` = min(380, 0.95 × grant) | `e3/spades_hybrid_pratama.py:23` | 380 |
| One refinement round, `-c 50 -x 10` | `e3/metawrap_refine_pratama.py:27-28` | two rounds, same cuts |
| CheckM `--quick` below 40 GB | `e3/metawrap_refine_pratama.py:29,55-58` | full tree. Not triggered at the 128 GB grant. |
| Interval union: overlapping or abutting calls merge | `e3/merge_candidate_calls_pratama.py:58-66` | not stated |
| VirSorter2 trimmed boundaries first | `e3/virsorter2_pratama.py:34-35` | original sequence |
| 240 Mbp per contig batch | `S/logistics/splitContigsForAmr.py:10` | none. The paper calls on whole assemblies. |
| ≥ 10 kb representatives | `e3/votu_representatives_10kb_pratama.py:23` | ≥ 10 kb |
| DRAM and DRAM-v on KOfam and Pfam only | `e3/dram_*`, `e3/dramv_*` | full DRAM databases |
| DRAM outer join, Pfam fills only columns KOfam lacks | `e3/dram_distill_pratama.py:26-34` | one annotate run |
| Database versions: geNomad and CheckV unpinned, iPHoP Aug_2023, DRAM 1.5.0, vConTACT3 v230, VirSorter2 2.2.4 | `drivers/_common.py:100-131` | see the tables above |
| minimap2 preset by read quality (`-x sr` for short reads) | `e3/assembly_stats_pratama.py:42-50` | bwa inside CoverM |
| GTDB-Tk `--skip_ani_screen` | `bench/gtdbtk_image.py:52` | defaults |
| MinION–Illumina pairing by well and `02um_2022` | `drivers/e3_pratama.py:105-114` | 17 hybrids |
| skani `--min-af 15` for vOTU recovery | `S/viromics/pratama_votu_recovery.py:32` | not applicable |

## Resource heuristics

These size tasks and do not change the results. Where one does, the scientific table lists it.
- Declared cpus, GB and hours live in each transform's resources. The largest are metaSPAdes 48/192/24, hybrid 48/384/20, CONCOCT 32/192/20 and GTDB-Tk 8/240/20.
- The driver scales three steps past their declarations: MEGAHIT 32/128/12, assembly_stats 32/64/12 and DRAM-v KOfam 48/64/12 (`drivers/e3_pratama.py:58-62`).
- Each retry doubles memory and time (`drivers/_common.py:398-399`). Time clamps at 24 h (`:315`), except 36 h for metaSPAdes, hybrid and refinement (`:321-326`). Memory clamps at 192 GB for scaled steps only (`:335`).
- Nextflow runs 4 tries, arrays of 25 and a queue of 500 (`drivers/e3_pratama.py:224`).
- vConTACT3 climbs 24, 48, 96 and 192 GB, in place.

## Open gaps

Each gap names what closing it takes.

| Gap | What closing it takes |
|---|---|
| DRAM-v steps 37-39 do not finish in 24 h, and distill would crash on null VOGDB | Chunk DRAM-v 16 ways and stage VOGDB (`results/e3/RESUME.md`). |
| 6 of 17 hybrid assemblies built on fir | Verify on fir that the 17 pairings collapse to the 6 distinct MinION files. Then mint one given per pairing. |
| Viral curation and host trimming | New transform between CheckV and MMseqs2, with CheckV on the frozen set first. Changes every downstream vOTU. |
| Island filter | New transform after annotation. |
| AMG curation | New transform after DRAM-v distill. |
| DeepVirFinder calls | Target it, record the ≥ 0.9 / p ≤ 0.05 cut in the transform, and add it to the merge. |
| Hybrid assemblies in the viral pool | Add a hybrid lane to the merge. |
| VirSorter2 boundaries | Prefer `full_bp_*` in the call writer. |
| vConTACT3 assignments | Add `viromics::vcontact3_assignments` as a target. |
| iPHoP input | Feed it the ≥ 10 kb representatives. |
| BinSanity, abawaca, refinement round 1, 107-marker MaxBin2 | Three new transforms and a second refinement step. |
| MAG quality filter and dRep | A filter transform and a dRep that takes MetaWRAP bins. |
| iPHoP augmented pass, GTDB-Tk de novo | Targets, after dRep. |
| minced, BLASTn spacers, MetaPop | minced is a new transform. The other two are targets. |
| No recovery number | Score `pratama::votu_recovery_table` against the published vOTUs, and add a MAG recovery. |
