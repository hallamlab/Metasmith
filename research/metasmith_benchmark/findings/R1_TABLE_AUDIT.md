# R1 tool-table audit

## Purpose & Contents

This file checks every cell of `page/tool_table.py` for E1–E5 against what R1's drivers and libraries run. The table is the source of truth. Where a driver disagrees, the driver changes, or the gap is named here as a gapfill.

Each cell has one status:
- **matched**: the plan runs the table's tool with the table's settings.
- **pinned**: a benchmark library transform fixes the table's settings. The standard transform it replaces is masked in that driver.
- **gated**: the transform exists, and a driver flag keeps it out until an outside dependency lands.
- **gapfill**: no transform yet. The item number points at the plan's T21 list, and the wave is the earliest that can carry it.

Settings for Pratama cells come from `research/pratama2026/reproduction_map.md`, which quotes the authors' workflow repository.

## E1: nf-core/mag 5.5.0

Tool choices come from `drivers/e1_nfcore/control.config` over the pipeline's own defaults.

| Tool | Status | Where |
|---|---|---|
| fastp, FastQC | matched | pipeline defaults (`clip_tool = 'fastp'`) |
| bowtie2 against phiX | matched: off | `keep_phix = true` |
| Porechop ABI, Chopper | matched | pipeline defaults (`longread_adaptertrimming_tool`, `longread_filtering_tool`) |
| MEGAHIT, Flye | matched | `skip_spades`, `skip_spadeshybrid`, `skip_metamdbg` |
| bowtie2 (short), minimap2 (long) | matched | `binning_map_mode = 'own'` |
| MetaBAT2, SemiBin2, COMEBin | matched | MaxBin2, CONCOCT and MetaBinner skipped |
| DAS Tool | matched | `refine_bins_dastool`, `postbinning_input = 'both'` |
| CheckM2 | matched | `run_checkm2` |
| Prodigal, standalone | matched | `skip_prodigal = false` by default |
| AMBER | matched: after the run | outside the pipeline |

## E2: metasmith, matching E1

Every transform lives in `library/transforms/e2` under `e2::` types.

| Tool | Status | Where |
|---|---|---|
| fastp | pinned | `e2/fastp.py`. It reads the split R1/R2 pair E1 reads, because `--detect_adapter_for_pe` fails silently on interleaved input |
| FastQC, raw and trimmed | pinned | `e2/fastqc_raw.py`, `e2/fastqc_trimmed.py` |
| Porechop ABI, Chopper | pinned | `e2/porechop_abi.py`, `e2/chopper.py` |
| MEGAHIT | pinned | `e2/megahit.py` |
| Flye | pinned | `e2/flye.py`, 64 GB |
| bowtie2, minimap2 | pinned | `e2/bowtie2_binning_bam.py`, `e2/minimap2_binning_bam.py` |
| MetaBAT2, SemiBin2, COMEBin, DAS Tool, CheckM2 | pinned | one transform each. MetaBAT2's depth step uses 2.15 and its binner 2.17, as E1's images do |
| AMBER | pinned | `e2/amber.py` |

## E3: metasmith, matching Pratama

The E3 driver loads `library/transforms/e3` and masks each standard transform it replaces (`REPLACED` in `drivers/e3_pratama.py`).

| Tool | Status | Where |
|---|---|---|
| bbduk | pinned | `e3/bbduk_pratama.py`: `ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1` |
| fastp report | pinned | `e3/fastp_report_pratama.py`, report only |
| seqkit read stats | matched | `assembly/seqkit_reads.py` |
| MEGAHIT, viral contigs only | matched | standard `megahit.py`, defaults as Pratama's `megahit -1 -2`. It feeds only the viral merge |
| metaSPAdes | pinned | `e3/spades_pratama.py`: `--meta -k 21,33,55,77` |
| metaSPAdes `--nanopore`, hybrid | gapfill 3, wave 2+ | 17 per-run hybrids, and H43 has no MinION run |
| assembly_stats | matched | standard, on the metaSPAdes assembly |
| MetaBAT2, MaxBin2, CONCOCT via MetaWRAP | pinned | `e3/metawrap_pratama.py`: `--universal`, on the metaSPAdes assembly only |
| BinSanity, abawaca | gapfill 4, wave 3+ | |
| MetaWRAP bin_refinement, 2 rounds | pinned with 1 round | the second round needs BinSanity and abawaca (gapfill 4) |
| CheckM inside MetaWRAP | pinned | `binning::metawrap_bin_stats` |
| dRep | gapfill 2, wave 2+ | replaces nothing in E3's plan: E3 never runs skani_dedup |
| GTDB-Tk r232 | gated | `--with-gtdbtk`, on ref::gtdb's representative genomes |
| Prodigal | matched | standard `prodigal.py`, on the metaSPAdes assembly |
| prodigal-gv, viral contigs | pinned | `e3/prodigal_gv_batch_pratama.py` over `e3::viral_contig_batch` slices, stacked by `e3/prodigal_gv_merge_pratama.py` under `e3::viral_orfs`. As `sequences::orfs` it answered the metaSPAdes Prodigal target |
| DeepVirFinder | gapfill 5, wave 3+ | |
| VIBRANT | pinned | `e3/vibrant_pratama.py`: `-virome` |
| geNomad | pinned | `e3/genomad_pratama.py`: `--splits 48 --min-virus-marker-enrichment 1 --min-virus-hallmarks 1` |
| VirSorter2 | pinned | `e3/virsorter2_pratama.py`: `dsDNAphage,ssDNA`, no `--prep-for-dramv` |
| viral merge over both assemblies | pinned | `e3/merge_candidate_calls_pratama.py` |
| CheckV | pinned | `e3/checkv_batch_pratama.py` per `e3::viral_contig_batch` slice, stacked by `e3/checkv_merge_pratama.py` into the four standard `viromics::checkv_*` types. Standard `checkv.py` is masked, so the merge is their only producer. Same command and settings, one slice at a time |
| MMseqs2 vOTU clustering | pinned | `e3/mmseqs_votu_pratama.py`: `--min-seq-id 0.95 -c 0.8`, cov-mode left at its default of 0 |
| vConTACT3 | matched | standard `vcontact3.py` |
| MetaPop | gapfill 5, wave 3+ | needs every BAM against one shared reference |
| DRAM-v, no manual curation | pinned | `e3/votu_representatives_10kb_pratama.py`, `e3/dramv_votus_pratama.py` |
| minced, BLASTn spacers | gapfill 5, wave 3+ | BLASTn exists, and minced is its only spacer source in E3 |
| GTDB-Tk de novo | gated | reached through iPHoP's augmented database, gapfill 2 |
| iPHoP, both databases | shipped pass gated, augmented pass gapfill 2 | `e3/iphop_predict_default_pratama.py` behind `--with-host-prediction`, once T8 stages the database |
| DRAM on MAGs | pinned | `e3/dram_mags_pratama.py`, per sample until dRep lands. dbCAN is absent from the staged database (BLOCKERS B3) |
| skANI recovery, vOTUs | matched | `pratama_votu_recovery.py` |
| skANI recovery, MAGs | gapfill 2, wave 2+ | needs dRep's dereplicated set |

Where the map and the authors' own `data/docs/pratama2026/Groundwater_virome/Workflows/` files differ, the Workflows files win:
- MetaWRAP's first binning pass uses `--universal`, which the map drops. `metawrap_pratama.py` passes it.
- The DRAM-v prep pass runs CheckV on the vOTUs first, then VirSorter2 with `--seqname-suffix-off --viral-gene-enrich-off --provirus-off --prep-for-dramv` on CheckV's combined output. `dramv_checkv_pratama.py` and `dramv_vs2_prep_pratama.py` do these as separate stages.
- Pratama's `metawrap binning --maxbin2` second pass at 107 markers (map row A6) has no transform.
- Refinement round 1 reads `-A abawaca_BINS_1 -B abawaca_BINS_2 -C binsanity_BINS_1` in the source itself, so two abawaca runs feed it. Gapfill 4 pins it that way.

## E4: metasmith, from metaGEM's MAGs

| Tool | Status | Where |
|---|---|---|
| Prodigal | matched | `prodigal_from_bin.py`, first step |
| CarveMe, 12 h | matched | `RESOURCE_OVERRIDES`. T9 makes it scale with the attempt |
| CPLEX 22.2 | matched | `modelling::cplex_installation` |
| MEMOTE | pinned | `modelling/memote_score.py`: the standard transform with `HOME="$PWD"`, so cobrapy can create its cache |
| GTDB-Tk r232 | gated | `--with-gtdbtk` |
| SMETANA | gapfill 5, wave 3+ | |
| skANI recovery | matched: after the run | |

## E5: the 9-sample pilot

| Tool | Status | Where |
|---|---|---|
| bbduk, seqkit | matched | standard |
| Chopper | gapfill, no pilot corpus | the pilot has no long-read corpus |
| MEGAHIT | matched | standard |
| Flye | matched: unused | no long-read corpus |
| minimap2 with assembly_stats | matched | standard |
| MetaBAT2, SemiBin2, COMEBin | matched | standard, with fixed seeds (`BINNER_PARAMS`) |
| DAS Tool | matched | standard |
| MAGScoT | gapfill 2, wave 2+ | |
| CheckM2 | gapfill 2, wave 2+ | no standard transform. E2's is fenced to `e2::` types. CheckM 1 left the E5 plan |
| dRep, skani_dedup per study | gapfill 2, wave 2+ | needs a study grouping type |
| GTDB-Tk r232, GTDB-Tk de novo, iPHoP | gated | `--with-gtdbtk` |
| Prodigal | matched | standard, on the assembly |
| prodigal-gv | gapfill 2, wave 2+ | as `sequences::orfs` it would also answer the assembly's Prodigal target |
| DeepVirFinder, MetaPop | gapfill 5, wave 3+ | |
| VIBRANT, geNomad, VirSorter2, CheckV, MMseqs2, vConTACT3 | matched | standard |
| DRAM-v, minced | matched: out | decided after E3 reports |
| KOfamScan, CLEAN, DIAMOND UniRef50, ProteinBERT | matched | standard |
| CarveMe with the open solver | matched | standard |
| MEMOTE | pinned | `modelling/memote_score.py`, as in E4 |
| SMETANA | gapfill 5, wave 3+ | |
| AMBER, skANI recovery | matched: after the run | |
