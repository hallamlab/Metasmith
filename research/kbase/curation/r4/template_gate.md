# The eleven shipped templates, before and after round 4

Round 4 added 21 transforms to the standard library, gave `bakta_noncoding` three more
products, and folded four rows out of the aspire lane. This file is the regression: what
the eleven shipped templates planned on `HEAD` and what they plan now.

Both sides were solved with the same driver, `plan_all.py`, which calls each template's own
`build_spec` and prints the sorted source path of every step in the solved plan -- a step
count alone would not catch a plan that swapped one producer for another and stayed the
same length. `HEAD` was solved from a `git archive HEAD` extract, so its engine and its
library are both the committed ones.

## Result

    11/11 templates built, 1 blocked        (unchanged; `dl_embeddings_from_orfs` is still
                                             blocked on the disabled `shardFasta.py`)

Ten of the eleven plans are **identical, step for step**. The eleventh is the amplicon lane
and it is shorter:

| template | HEAD | r4 |
|---|---|---|
| amplicon_asv_study_from_paired_reads | 18 | **15** |
| every other template | — | unchanged |

The three steps are nameable, and they are exactly T4's folds:

- `dereplicate.py` is gone, folded into `denoise.py`.
- `merge_reads.py` and `filter_reads.py` are gone; `merge_and_filter_reads.py` does both.
  Two steps became one.
- `prepare_blast_databases.py` is gone, folded into `mitomaster.py`, which now takes the two
  reference FASTAs directly.

Nothing else in that plan moved. `master_summary_optional_slot`, the fourth row T4 deleted,
never appeared in this template's plan, so its removal cost nothing here.

## Timing

No template got slower. Total wall clock 26s on `HEAD`, 27s here, and the one solve that
costs anything -- `metagenomics_from_paired_reads` at 21s -- moved to 22s, which is noise.
Both runs used the Python solver, since `src/metasmith/engine/` is empty in this worktree.

## The new ambiguity is real and it is latent

`probe_ambiguity.py` over the six standard groups counts 8 requirements with more than one
producer on `HEAD`; over the same six plus `kbase` it counts 12. Four are new:

| requirement | HEAD | r4 | the new producers |
|---|---|---|---|
| `sequences::read_qc_stats` | 1 | 3 | `qc_reads/fastqc.py`, `qc_reads/nanoplot.py` |
| `sequences::gbk` | 1 | 2 | `functionalAnnotation/bakta_noncoding.py` |
| `transcriptomics::stringtie_quant_gtf` | 1 | 2 | `quantify_expression/stringtie_feature_counts.py` |
| `modelling::metabolic_model` | 0 | 3 | all three are new, and two of them are deliberate |

Two more got wider without becoming newly ambiguous: `sequences::assembly` went 7 producers
to 9 (`seqkit_filter_contigs`, `polypolish`) and `sequences::clean_short_reads` 2 to 3
(`fastp`).

**None of it changed a plan.** Every template that demands one of these still picked the
producer it picked before -- `seqkit_reads` for the QC stats, `getNcbiAssembly` for the
`.gbk` the pangenome template reads, `stringtie_quant` for the GTF. That is the finding, and
it is a weaker guarantee than it looks: these targets are pinned by the templates' own
lineage, and an *unpinned* target downstream of any of them is now free to be answered from
either producer. The symptom would be a split plan, not an error. T9's analyses are where
that surfaces, because they are written from scratch with no such pinning -- analysis 3
(polish a long-read genome) and analysis 6 (transcriptome) sit directly on top of two of
these four.

`modelling::metabolic_model` is the one to watch. `cobra_gapfill` both consumes and produces
it, so a target naming that type can be answered by an unbounded chain of gapfills; and the
two builders (`gem_from_gpr`, `fetch_bigg_model`) exist precisely so a caller can pick, which
means any analysis downstream of a model must pin which one it wants.

## Evidence

- `_plans_head.txt`, `_plans_r4.txt` -- every step of every plan, both sides. `diff` them.
- `_head_templates.log`, `_r4_templates.log` -- the two `build_templates.py` runs.
- `_ambiguity_head.txt`, `_ambiguity_r4.txt` -- the full producer/demander listing.
