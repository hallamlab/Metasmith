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
- Metabuli checked upstream on 2026-11-17. The pinned `gtdb232.tar.gz` reports a Content-Length of 633,960,254,218 bytes. `downloadMetabuliDB` keeps the archive as its own cached product, `ref::metabuli_sources`, and `buildMetabuliDB` unpacks it without deleting it. So the 1.4 TB is the steady state, not just a peak, and it exceeds a 1 TB Alliance scratch quota.

## Reference databases for metagenomics_from_paired_reads (2026-11-17)

Download sizes are upstream Content-Length readings. Unpacked sizes come from the transform comments. In every case the download step caches its archive as a product, and the build step does not delete it, so steady state is archive plus built database. CheckM's data ships inside its container.

| DB | download | built | steady state | files |
|---|---|---|---|---|
| Metabuli gtdb232 | 634 GB | ~744 GB | ~1.38 TB | unknown |
| GTDB-Tk r232 package | 61.2 GB (12 parts) | ~98 GB | ~159 GB | unknown; skani sketches replace the 424K-file genome tree |
| UniRef50 → DIAMOND | 9.2 GB fasta.gz | .dmnd, size not recorded | ~30 GB est. | few |
| KOfam 2026-08-02 | 1.56 GB | profiles/ | ~3 GB est. | ~27.5K (fir journal bab24dd0) |
| phyloFlash SILVA 138.2 | 0.20 GB | makedb indices | a few GB | tens |
| **Total** | ~706 GB | | **~1.57 TB** | |

Without metabuli the steady state is about 190 GB, which fits nibi's 1 TB. No database here plausibly reaches millions of files, so the beta tester's millions more likely come from the run itself: nxf_work, per-task bootstrap dirs (journal 44abb4ee: 14K orphan dirs in 5.5 h on fir) or container unpacks.
