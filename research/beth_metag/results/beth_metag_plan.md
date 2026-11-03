# Binning, taxonomy and functional annotation (plan)

This is a draft of the pipeline. The inputs are still open. The test solve used the six CPE3 HiFi libraries from DAV36689, but the same steps fit whatever samples you want to run. The plan handles each sample on its own. Nothing has run yet.

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

- Compute would run on fir, not Sockeye. With HiFi libraries of 40–50 GB each, the binners plus the four lanes make this a multi-day job.
- Short reads can be added as a second coverage source for the binners, or used as the main input instead.
- The 4-lane annotation can also run on a set of finished MAGs, such as your dRep representatives, instead of on whole assemblies.
- Before a run, the plan's database downloads (GTDB, KOfam, UniRef50) will be replaced with existing copies.
