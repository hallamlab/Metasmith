# DRAM port: comparison against upstream DRAM (planned, not run)

The library runs DRAM's distill over its own annotation table rather than running DRAM's annotate.
dbCAN, MEROPS and VOGDB are annotation-table lanes. KOfam, InterProScan Pfam and UniRef50 stand in
for DRAM's KOfam, mmseqs Pfam and UniRef90 searches. `lib::fabfos_gpr/dram_annotations.py` renders
one MAG's rows as DRAM 1.5 `annotations.tsv`, and `dram_distill_mag` runs `DRAM.py distill` on it.
This comparison checks that the substitution does not move what distill reports.

## Run

One CPE3 quality MAG on fir, both arms on the same Prodigal ORFs:

1. **Ours.** Target the five `annotation::dram_*` products. `probe_dram_solve.py` is the solve.
2. **Upstream.** Annotate the MAG's ORFs with DRAM 1.5.0 against a database holding KOfam, Pfam,
   dbCAN, MEROPS and VOGDB, without UniRef90. Then run `DRAM.py distill` on the result.
   `buildDramDB` does not select `peptidase`, so add it there or build the database by hand. Call
   `annotate_called_genes(..., use_vogdb=True, rename_genes=False)` from Python, as
   `dram_annotate_genes` does: the `annotate_genes` CLI has no flag that keeps the gene ids.

## Compare

- Per gene: agreement on `ko_id`, `cazy_best_hit` family, `peptidase_family` and `vogdb_id`. Report
  each disagreement by cause: a search-tool difference (DIAMOND against mmseqs for MEROPS) or an
  E-value difference (hmmsearch `-Z 5000` against DRAM's per-genome Z).
- `metabolism_summary.xlsx`: count differences per sheet and `gene_id`.
- `product.tsv`: module step coverage and function presence that differ.
- Expected and not a finding: `rank` is never A or B, because no reciprocal search runs. UniRef50
  ids replace UniRef90 ids. distill counts no Pfam id, because InterProScan reports Pfam accessions
  without their version.
