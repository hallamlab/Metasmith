# What the KBase port covers

Every number below is read from `catalog/census.json`, `catalog/ledger.jsonl`,
`narratives/dags.jsonl`, `curation/r1/shapes.jsonl` and
`templates/solve_results.jsonl`. Where this file disagrees with those, they win.

## The funnel, both lanes

### Apps to transforms

| stage | count |
|---|---|
| apps in the KBase catalog | 493 |
| marked active | 235 |
| runnable rather than a viewer | 431 |
| naming some product (typed output or a report) | 372 |
| converted to a metasmith transform | 371 |
| **active apps converted** | **216 of 235** |

### Narratives to planned workflows

| stage | count |
|---|---|
| public narratives | 3965 |
| with at least one app cell | 3664 |
| with more than one app cell | 3472 |
| with at least one edge between cells | 2407 |
| distinct workflow shapes after deduplication | 1633 |
| shapes trusted enough to solve (reference edges only, apps still in the catalog) | 623 |
| **shapes metasmith planned** | **431 of 623** |

Those 431 shapes account for 732 of the 975 narrative copies
behind the candidate set (75%). A shape is one workflow;
a copy is one scientist who ran it.

## Why a shape does not plan

| outcome | shapes | what it means |
|---|---|---|
| solved | 431 | metasmith planned the workflow. |
| no_plan_all_targets | 106 | no plan reaches the targets. Almost always a terminal app that declares no typed product, so the chain's end is untyped and nothing downstream can be asked for. |
| no_plan_some_targets | 86 | a plan exists for part of the target set. One unreachable target drops the rest with it. |

## Every extra target costs about a tenth of the solve rate

The strongest predictor of whether a shape plans is how many targets it names.
A target is a solver slot, not a wish: each one must be reached, and one
unreachable target drops the rest with it.

| targets named | shapes | solved | rate |
|---|---|---|---|
| 1 | 266 | 222 | 83% |
| 2 | 175 | 125 | 71% |
| 3 | 121 | 65 | 54% |
| 4 | 40 | 13 | 32% |
| 5 | 15 | 6 | 40% |
| 6+ | 6 | 0 | 0% |

The practical reading: derive one terminal product per workflow, not every
product every terminal app declares. Where a shape genuinely has several
endpoints, pin them to a shared ancestor rather than naming them side by side.

## How much of each workflow the plan reproduces

Of the solved shapes, 404 plan in fewer steps than the narrative ran,
21 in the same number and 6 in more.

A shorter plan is the expected case rather than a loss. The planner builds only
what a target needs, and a narrative runs plenty that nothing downstream asks
for -- a QC report beside an assembly, a viewer on a matrix. A longer plan is the
interesting case: it means the planner fanned out where the scientist did not,
running three binners because three bin types were asked for.

## The workflows that plan, by how many people ran them

| copies | narrative steps | planned steps | workflow |
|---|---|---|---|
| 45 | 2 | 2 | build_metabolic_model -> run_flux_balance_analysis |
| 30 | 2 | 1 | annotate_contigs -> reannotate_microbial_genome |
| 28 | 3 | 2 | build_metabolic_model -> run_flux_balance_analysis -> run_flux_balance_analysis |
| 24 | 6 | 2 | create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_stringtie -> run_DESeq2 ... |
| 14 | 2 | 1 | load_single_end_reads_from_URL -> run_kb_filtlong |
| 11 | 2 | 2 | annotate_contigs -> insert_set_of_genomes_into_species_tree |
| 10 | 2 | 1 | annotate_genome_assembly -> build_metabolic_model |
| 8 | 2 | 1 | merge_metabolic_models_into_community_model -> run_flux_balance_analysis |
| 8 | 2 | 2 | annotate_genome_assembly -> insert_set_of_genomes_into_species_tree |
| 8 | 3 | 1 | import_sra_as_reads_from_web -> runFastQC -> run_trimmomatic |
| 8 | 4 | 3 | load_single_end_reads_from_URL -> run_kb_filtlong -> annotate_contigs -> KButil_Build_GenomeSet |
| 7 | 3 | 3 | annotate_genome_assembly -> build_metabolic_model -> run_flux_balance_analysis |
| 7 | 3 | 1 | load_paired_end_reads_from_URL -> runFastQC -> run_trimmomatic |
| 6 | 2 | 1 | annotate_contigs -> run_CGViewAdvanced |
| 6 | 2 | 1 | annotate_genome_assembly -> run_kb_dram_annotate_genome |
| 6 | 3 | 1 | import_sra_as_reads_from_web -> runFastQC -> run_SPAdes |
| 5 | 2 | 2 | annotate_contigs -> KButil_Build_GenomeSet |
| 5 | 2 | 1 | annotate_genome_assembly -> build_metabolic_models |
| 5 | 3 | 1 | load_paired_end_reads_from_URL -> run_trimmomatic -> runFastQC |
| 5 | 8 | 1 | create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_stringtie -> compute_average_expression_matrix ... |

## Findings

**A mask is not a tuning knob, it is a correctness requirement.** 46 of the 370
transforms are pure sources, so they are the cheapest producer of anything and an
unmasked solve answers every target with "import it from staging". That plan is
complete and correct and useless. Every solve here runs against the shape's own apps.

**An optional KBase parameter must not become a metasmith requirement.** 138 of the
599 typed input parameters are optional. Modelling them as requirements made
`run_flux_balance_analysis` demand an expression matrix nobody has, and the
2-step workflow 45 narratives actually ran had no plan. Fixing it moved a 12-shape
sample from 7 solved to 10.

**A union type carries its token and nothing else.** `_` is a matched property, not
a comment. The first draft gave each union a human-readable description, which is a
property no member carries, so every union was unsatisfiable and every solve using
one returned no plan with no dropped targets. The lint now asserts each union is
satisfied by each of its declared members.

**KBase's own specs under-declare what apps produce.** The dominant failure class is
a terminal app with no typed output: `extract_bins_as_assemblies` extracts
assemblies and declares none, `run_checkM_lineage_wf` reports quality and declares
none. The workflow is real and the type graph cannot see its end. This is a limit of
the source, not of the planner, and it is where a hand-written override would buy the
most coverage.

**The witness rejects plans over this library.** The `feat/solver` msm_solver refuses
plans the Python solver emits, naming one clause: `indexed`, whose comment says a
violation means the encoder emitted malformed rows. The same binary solves the
shipped templates. Reproducer at `reports/witness_indexed_repro.json`; raised with
the engine/solver scope. Everything here was solved with the Python solver, which
is what this branch ships.

