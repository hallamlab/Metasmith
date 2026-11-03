# CPE3 enrichments: binning, taxonomy and functional annotation (plan)

Samples: the six CPE3 HiFi libraries from TCAG run DAV36689. These are cpe3_parent, cpe3_r2a, cpe3_r2a_cu1_5, cpe3_r2a_cu6, cpe3_r2a_cu7_8 and cpe3_r2a_cu9_11.

The plan handles each sample on its own. Metasmith solved both versions below for all six samples as one case. Nothing has run yet.

## Two ways in

- **From reads** (`beth_metag_dag_reads.png`, 31 steps). This version converts the HiFi BAM to fastq, assembles it with hifiasm-meta, and maps the reads back for coverage.
- **From your assemblies** (`beth_metag_dag_assembly.png`, 30 steps). This version starts from the contigs you already built, such as hybrid metaSPAdes or OPERA-MS, one assembly per sample. It still maps the HiFi reads to each assembly for coverage. Choose this if you want to keep your assemblies.

## What each sample gets

- **Read and assembly QC**: seqkit read stats, assembly stats, and per-contig and per-base coverage.
- **Binning**: MetaBAT2, SemiBin2 and COMEBin. Each binner gives its bins and a contig-to-bin table.
- **Bin quality and taxonomy**: CheckM2 and GTDB-Tk on each binner's bins.
- **Consolidated bins**: bins that pass quality from all three binners are pooled, then dereplicated with skani to give a cluster table.
- **Functional annotation (4 lanes)**: prodigal ORFs and GFF, then four lanes.
  - KOfamScan (KEGG KOs)
  - CLEAN (EC numbers)
  - DIAMOND against UniRef50
  - ProteinBERT label transfer
- **GPR table**: the four lanes are merged into one gene-to-reaction evidence table, `gpr_table`. Each lane's own output is kept as well.

## Notes

- Compute would run on fir, not Sockeye. The six BAMs are 40–50 GB each, and the binners plus the four lanes make this a multi-day job.
- The Illumina libraries from the same samples (DAV36690) are not used in either version. They could be added as a second coverage source for the binners if you want that.
- The 4-lane annotation can also run on a set of finished MAGs, such as your dRep representatives, instead of on whole assemblies.
- Before a run, the plan's database downloads (GTDB, KOfam, UniRef50) will be replaced with existing copies.
