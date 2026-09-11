# Eleven analyses, planned end to end

Round 3 measured parity against a table of KBase task verbs. This is the other question:
put in front of the planner an analysis somebody would actually run, and see whether the
chain connects. Eleven of them, written as questions rather than transform lists, each with
the inputs a lab would have and one or two terminal products.

The driver is `research/metasmith_libraries/probe_kbase_parity.py`; every record it writes
is in `research/metasmith_libraries/kbase_parity.jsonl` -- solved or not, the step count,
the transform files the planner picked, the dropped targets and a failure class.

**11 of 11 solve.** Two did not at first, and both failures were real. They are the most
useful thing in this file, so they come first.

## The two that failed, and why

### a3 -- finish a long-read isolate: `polypolish` could never run

`kbase/polish_assembly/polypolish.py` declared its short reads as a CHILD of the assembly:

    reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={asm})

No read set in this library descends from an assembly -- the arrow runs the other way -- so
the requirement had no candidates, `sequences::polished_assembly` was unreachable, and with
it every other target in the analysis went too (one unreachable target drops the whole
plan: 0 steps, `no_plan_at_all`, four dropped targets, none of them the actual problem).

Fixed to the idiom this library already uses for "these came from the same sample", which is
a shared `sequences::read_metadata` ancestor -- `assembly/assembly_stats.py` and
`assembly/megahit.py` both do exactly this. The reads and the assembly are siblings under
the metadata node, not parent and child.

The second half of the finding does not have a fix: **there is no isolate- or run-level
grouping type above `read_metadata`.** A hybrid assembly joins a long-read set to a
short-read library, and the only thing that can say they are the same isolate is one shared
`read_metadata` node covering both. That is what a3 now supplies, with `length_class` set to
`hybrid` -- honest as a value, but it means the metadata node is doing a job it was not
named for. A `sequences::sample` grouping node above it is the real answer, and it is a
library-wide decision rather than a round-4 one.

### a5b -- the co-occurrence network: lifting a transform does not lift its inputs

T5 lifted the six community-ecology transforms off `aspire::run` onto the generic
`amplicon::survey` grouping node. Four of the six then solved from a bare count table.
`spieceasi` and `graph_network` did not, and the reason is that a gate moved rather than
went away: their own inputs were still produced only inside the run.

    aspire::indicspecies_group1_summary   <- indicspecies        (gated on run)
    aspire::network_modules_*             <- network_modules     (gated on run)
    aspire::asv_mag_pairing               <- asv_mag_link*       (gated on run)

Five more rows were lifted the same way -- `indicspecies`, `indicspecies_absent`,
`network_modules`, `network_modules_absent`, `asv_mag_link_absent` -- and `indicspecies`'s
count input was re-typed from `aspire::analysis_counts` to `amplicon::asv_table` like the
first six. `asv_mag_link` itself stays gated on `run`, deliberately: it reads
`aspire::asv_filtered_seqs`, which nothing outside the ASPIRE lane produces. Linking ASVs to
MAGs remains a pipeline capability; declining to link them does not. All eleven edits are
row edits in `transforms/aspire/_generate.py` followed by a regenerate.

Two things a5b has to supply that a run would have supplied for it, both imports rather than
gaps: `amplicon::asv_taxonomy` (ASPIRE gets it from SINA against SILVA; a kraken-based
survey has no producer) and the five policy tokens that select the network arms.

## What solved

| # | question | steps | the round-4 transforms it used |
|---|---|---|---|
| a1 | Draft a metabolic model for an isolate I just sequenced | 23 | bakta_cds, gem_from_gpr, cobra_gapfill, cobra_fba, fastp, polypolish |
| a2 | The same flux question on a curated published model | 2 | fetch_bigg_model, cobra_fba |
| a3 | Finish and annotate a long-read isolate genome | 7 | nanoplot, fastp, polypolish, bakta_noncoding's new `.gbk` |
| a4 | Which genomes can I recover from this metagenome | 13 | seqkit_filter_contigs, fastqc, fastp |
| a5 | Who is in these samples and how do they separate | 7 | kraken_abundance, diversity_analysis, umap_clustering, measurement_association, paired_group_contrast |
| a5b | ...and a co-occurrence network from the same table | 8 | spieceasi, indicspecies, network_modules, asv_mag_link_absent, graph_network |
| a6 | What changed in the transcriptome | 8 | bowtie2_align, stringtie_feature_counts, expression_clusters |
| a6b | Which functions are over-represented in the DE genes | 5 | go_overrepresentation_analysis |
| a7 | What is core and accessory across a strain collection | 10 | ppanggolin_summary, gtdbtk_tree |
| a8 | Is this gene family present, and how do the copies align | 3 | blast_homolog_hits, mafft_msa |
| a9 | Which variants separate this isolate from its reference | 7 | samtools_stats, bcftools_variants |

Every one of round 4's 21 new transforms and all six lifted ecology transforms appears in at
least one solved plan. a2 is the shortest useful proof in the set: two steps, `bigg_model_id`
to `fba_solution`, and it is why both producers of `modelling::metabolic_model` exist -- the
drafted model in a1 is only interpretable against a curated one on the same medium.

## Non-minimal plans: valid, and more steps than the work needs

Two of the solved plans contain steps nobody asked for, and both are worth writing down
because neither is a bug in a transform.

**a4 runs `seqkit_filter_contigs` five times.** Targeting the assembly and the filtered
assembly alone gives exactly one filter step; adding `taxonomy::checkm_stats` over the bins
multiplies it. The type hierarchy is not the cause -- `filtered_assembly IsA assembly` is
true and `IsA putative_genome` is false, so no filter output can masquerade as a bin. Every
instance produces the same file from the same assembly. This is the search behaviour this
scope has already documented: the refiner cannot merge steps, so a duplicate is the author's
to pin out, and here the pin is not obvious.

**a1 grew a polish step.** Fixing `polypolish` made it reachable, and reachable means it is
a candidate producer of `sequences::assembly` wherever an assembly is wanted. a1's targets
are all pinned to descend from the megahit assembly, and a polished megahit assembly still
does -- so the constraint permits it. Making a transform reachable makes it a candidate
everywhere its output type appears, which is the cost of the same property subsumption that
makes the library composable at all.

Neither plan is wrong. Both are longer than the analysis needs, and there is no negative
constraint in the language to say "not this one".

## Regression state after these changes

- `build_templates.py`: 11/11, 1 blocked, 27s. Every plan step-for-step identical to the
  first r4 run (`_plans_r4.txt`), so neither the `polypolish` fix nor the second aspire lift
  moved a shipped template.
- `check_transform_compatibility.py`: 40 passed, 0 failed.
- `pytest tests/metasmith_libraries/test_type_hierarchy.py`: 27 passed.
- `_generate.py --lint`: 46 stubs, 132 types, 16 extends edges all subsume.
