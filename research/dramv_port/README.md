# DRAM-v port: comparison against upstream DRAM-v (planned, not run)

The library runs DRAM-v's scoring and distill over its own annotation table rather than
running `DRAM-v.py annotate`. `viromics/dramv_distill_votu` takes the study's vOTU
representatives, which are the first column of `viromics::votu_cluster_table`. Their genes
are the frozen set's prodigal-gv ORFs. Their annotations come from the annotation table
built on those ORFs. RefSeq viral, DRAM-v's one search the library lacked, is the table's
`refseq_viral` lane (DIAMOND against `viral.1.protein.faa.gz`).

`lib::fabfos_gpr/dramv_annotations.py` renders the rows as DRAM-v `annotations.tsv`. It
also moves VirSorter2's affi-contigs genes into each representative's frame. VirSorter2
calls genes on the whole batch contig and keeps those coordinates for partial calls too,
so the shift is the frozen interval's start, read from `candidate_call_provenance`.
`dramv_flags.py` then calls DRAM-v 1.5.0's own `add_dramv_scores_and_flags` on the
result, and `DRAM-v.py distill` runs on its output. This comparison checks that the
substitution does not move which genes distill calls AMGs.

## Run

One CPE3 sample on fir, both arms on the same VirSorter2 run (`--prep-for-dramv`):

1. **Ours.** Target the four `annotation::dramv_*` products with `parents` on the frozen
   set. `probe_dramv_solve.py` is the solve. With one sample, the frozen set is that
   sample's calls and the vOTU representatives are its dereplicated contigs.
2. **Upstream.** Run `functionalAnnotation/dramv.py` (`DRAM-v.py annotate` and `distill`
   against a full `annotation::dram_db`) on the same contig batch. Then repeat it on the
   representatives only: cut the representatives out of VirSorter2's
   `final-viral-combined-for-dramv.fa` by source contig, so that both arms score the same
   contigs.

## Compare

- Per gene, joined by scaffold and overlap of 70% or more, as DRAM-v's `get_gene_order`
  joins them: `auxiliary_score`, `amg_flags`, `ko_id`, `vogdb_categories`, `viral_id`.
  Report each disagreement by cause. The causes to expect are the gene caller
  (prodigal-gv against prodigal), the frame (the frozen interval against VirSorter2's
  trimmed sequence) and the search tool (DIAMOND against mmseqs for RefSeq viral and
  MEROPS).
- `amg_summary.tsv`: AMGs found by one arm only, by `gene_id` and scaffold.
- `vMAG_stats.tsv`: gene count, strand switches, potential AMG count and transposase per
  contig.
- Expected and not a finding:
  - `VIRSorter category`, `Circular` and `Prophage` are empty here. distill reads them
    from VirSorter1 `-cat_N` suffixes, and the frozen ids carry none.
  - The ranks are C, D or E, as in the DRAM port.
  - Upstream DRAM-v calls genes on VirSorter2's trimmed partial sequences but reads the
    affi coordinates on the whole contig. Its auxiliary scores for proviruses therefore
    rest on a frame mismatch that the port does not reproduce.
