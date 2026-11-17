# software-suite

> before this, we need to decide on the software suite. in the future we can test with a subset of that suite.

## Status

Open. No decisions yet.

## Survey (2026-11-10)

- 12 templates ship in `src/metasmith_libraries/templates/`. The general metagenomics one is `metagenomics_from_paired_reads`: bbduk QC, megahit, prodigal, assembly stats and coverage, DIAMOND UniRef50, KOfamScan, metabuli, phyloFlash, metabat2 + semibin2 + comebin, then CheckM and GTDB-Tk per binner. 25 targets, about 29 steps. Rokaiya ran this shape.
- The heaviest reference databases, as the transform code states them:
  - Metabuli: about 1.4 TB at peak (634 GB packed). The step needs 180 GB of RAM.
  - GTDB r232: 61 GB packed, 98 GB unpacked. gtdbtk needs 128 GB of RAM.
  - KOfam, UniRef50, phyloFlash SILVA: no sizes recorded.
- comebin needs 48 cpus and 192 GB; semibin2 needs 64 GB, growing with the square of the contig count.
- Sockeye lab scratch `/scratch/st-shallam-1/` holds 9.39 of 10 TB and 2.6M of 10M files (9 Nov). About 600 GB are free, which is less than Metabuli's peak.
- No journal records a per-database file count. Rokaiya's fir run hit Errno 122 at CollectResults, and the cause was never measured.
- Only `metagenomics_from_paired_reads` goes from raw metagenomic short reads to MAGs (asked 2026-11-17). Five more templates sit downstream of an assembly: `annotation_palette_from_assembly`, `annotation_trio_from_assembly`, `binning_and_gpr_from_assembly` (needs long reads), `gpr_table_from_assembly` and `viromics_survey_from_assembly`. `isolate_assembly_from_long_reads` handles isolates, and `amplicon_asv_study_from_short_reads` handles 16S.
- The library also holds light read-based profilers that no template uses: sylph (4 cpu / 64 GB), metaphlan, kraken2, ganon2.
