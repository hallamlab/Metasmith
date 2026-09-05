# Curation round 4 -- the aspire topology, row by row

## Purpose & Contents

`transforms/aspire/` is a one-for-one port of a 45-process Nextflow pipeline, so it still
describes how that pipeline staged its files rather than what the analysis does. This file
is the verdict on each of the 50 rows in `transforms/aspire/_generate.py`'s `TABLE`: fold,
collapse, keep, or open. It is the record a later reader needs to tell a deliberate fold
from an omission.

A round never revises an earlier round's findings. Round 3's first refactor -- lifting the
community-ecology transforms out of the `aspire::run` gate -- is executed here, and the
mechanism it needed is at the bottom of this file.

**The rule.** A row is folded when its product has exactly one consumer AND the work is
staging, reshaping or concatenating rather than a result anyone would name as a target. A
row that is a method with a result is kept, however small. A curation pass that folds
something a researcher would ask for has made the lane worse, not tighter.

Every change is a row edit in `_generate.py` followed by a regenerate. The generated files
are never hand-edited.

## What changed

| | before | after |
|---|---|---|
| rows in `TABLE` | 50 | 46 |
| types in `data_types/aspire.yml` | 138 | 132 |
| `amplicon_asv_study_from_paired_reads` | 18 steps | see `../../../../src/metasmith_libraries/templates/` |

Four rows folded, one type collapsed, six types removed.

### Folded

**F1. `merge_reads` -> `filter_reads`, now `merge_and_filter_reads`.**
Two vsearch calls on one sample, back to back. `aspire::merged_reads` had exactly one
consumer and is not a target anyone names -- the merge RATE is what a reader wants, and
that is reported by `general_stats`, not by this file. Type removed: `aspire::merged_reads`.

**F2. `dereplicate` -> `denoise`.**
`vsearch --derep_fulllength` is a precondition of `--cluster_unoise`, not a result: a
dereplicated FASTA is a compression of its input. `denoise`'s own product, the UNOISE
centroids, is the first thing in this stretch a person asks for, and it is kept. Type
removed: `aspire::derep_fasta`.

**F3. `prepare_blast_databases` -> `mitomaster`.**
Two `makeblastdb` calls whose products each had one consumer, which was `mitomaster`. The
standard library already treats this as inside-the-transform work:
`amplicon/blast_map_asvs.py` runs `makeblastdb` and then `blastn` in one protocol. The two
reference FASTAs move up to `mitomaster` as requirements. Types removed:
`aspire::mito_blast_db`, `aspire::contaminant_blast_db`.

**F4. `master_summary_optional_slot` deleted, and `master_summary` loses its `opt` input.**
The port's own note says it: `asv_pipeline.nf:2792` fills the master summary's
optional-contributor slot with an empty placeholder unconditionally, and there is no arm
that fills it with anything else. A permanent stub feeding a permanently empty slot is
staging with no policy behind it and no content in it. Type removed:
`aspire::optional_outputs`.

### Collapsed

**C1. `aspire::concat_fasta` into `aspire::concat_counts_fasta`.**
`concat_fastas` emitted the same reads twice, differing only in whether the headers carry
the sample label. `vsearch --derep_fulllength` does not read headers, so one file serves
both consumers. `concat_fastas` keeps its row -- it is the study fan-in, which is a
structural event and not staging -- and now emits one file instead of two.

### Kept, and why

The other 45 rows stand. The ones where the call was not obvious:

- **`mitomaster` -> `mito_decontam`** -- three products, all with one consumer, and kept as
  two rows anyway: `mitomaster` gathers the evidence (MitoMaster calls, two BLAST hit
  tables) and `mito_decontam` makes the call. Both halves are things a reader asks to see,
  and folding them would hide the evidence behind the verdict.
- **`sina_trim` -> `taxonomy`** -- `sina_trimmed_seqs` has one consumer, but SINA alignment
  is a method with four outputs, three of which are terminal and read directly.
- **`grouping_diagnostics` -> `group_label_augmentation`** -- the soft labels have one
  consumer and are a result: the whole point of the diagnostic is to produce them.
- **The eight policy off-arms** (`sankey_absent`, `augmentation_passthrough`,
  `batch_correction_passthrough`, `indicspecies_absent`, `network_modules_absent`,
  `spieceasi_external`, `asv_mag_link_absent`, `graph_network_absent`). Deleting an arm
  deletes the ability to turn its stage off. The port expressed a Nextflow channel
  rebinding as two producers of one type each requiring a different token, and that
  mechanism is the only reason an optional stage is optional here. F4 is not an exception
  to this: it removed a slot that had no arms, not one arm of a pair.
- **`plot_upset`, `bubbleplotter`, `clustermaps`** -- plot-only rows, and the port folded
  plot-only processes into the transform that computed their tables. These three have no
  such transform: they read a consumer-facing channel and render it, so the rendering is
  the whole row.
- **The sixteen policy token types.** They cannot collapse to one valued type. The
  mechanism is that a token has NO producer, so the arm requiring it has no candidates; a
  single type carrying a value would give both arms candidates and instantiate both.
- **`aspire::analysis_counts` is NOT collapsed into `amplicon::asv_table`.** They are the
  same shape and not the same content: `asv_table` is raw counts before filtering, and
  `analysis_counts` is what survives table filtering, non-target removal and optionally
  batch correction. Collapsing them would let the planner feed raw counts to a diversity
  analysis, and would let `filter_table` consume its own descendant. See the lift below for
  what was done instead.

### Open

**`general_stats` requires `concat_counts_fasta` as an ordering barrier, not as data.**
The port says so outright: the `.nf` reaches into the publish directories for the per-stage
read counts and takes the concatenated FASTA only to sequence itself after them. It is the
one fake edge in the table. Correcting it means the four read counts become products of the
four stages that produce them, and this row becomes a fan-in over those -- which is right,
and is more than a curation pass can verify without the counting script. Left as it is,
named here so the next reader does not mistake it for a real dependency.

**`aspire::qc_reads_fwd/rev` and `aspire::fastp_report_json/html` duplicate types the
standard library now ships.** `kbase/clean_reads/fastp.py` produces
`sequences::clean_short_reads`, `sequences::fastp_json_report` and
`sequences::fastp_html_report` -- the same four files under generic names. Re-typing
`fastp_qc` onto them would collapse four types, and would also make the shipped transform a
second producer inside the aspire run, which is target ambiguity in a shipped template. The
collapse is right and it waits on the paired-end shape being settled in one place.

**`outlier_checker` silently requires batch correction to be on**, because
`aspire::asv_clr_selected` is produced only by the correction arm. Faithful to the `.nf`,
and invisible from the row.

## The ecology lift (round 3's first refactor)

`diversity_analysis`, `umap_clustering`, `measurement_association`,
`paired_group_contrast`, `spieceasi` and `graph_network` perform every one of KBase's
community-ecology operations -- `transform_matrix`, `ordinate`, `correlate_matrix`,
`test_statistics` and `build_network`, five verbs and 59 narrative copies. Each was gated on
`aspire::run` and read `aspire::analysis_counts`, so nothing outside that one pipeline could
reach any of them.

**What the lift needed that round 3 did not name.** Dropping `run` and re-typing the count
input to `amplicon::asv_table` is not enough on its own: every one of the six is a
run-level fan-in, and a metasmith fan-in needs a grouping node its inputs descend from --
which is what `run` was. Removing it and putting nothing back would leave the remaining
requirements with no lineage to hang off, so a metadata table from one study would satisfy
another study's diversity analysis.

So the gate is not removed, it is **generalised**. `amplicon::survey` is the generic
grouping node -- "to group per-sample abundance profiles into one count table", the same
job `transcriptomics::experiment` and `pangenome::pangenome` do in their own lanes -- and
`aspire::run` is given that property, so an ASPIRE run IS a survey and satisfies the
requirement unchanged. The six now require `amplicon::survey` and `amplicon::asv_table`,
group on the survey, and keep every lineage edge they had.

The aspire pipeline is unaffected: `create_count_matrix` already produces
`amplicon::asv_table`, and `aspire::analysis_counts` is declared with a property superset of
it, so the analysis-ready matrix still satisfies the lifted requirement while the raw one
does too. Outside aspire, `kbase/profile_abundance/kraken_abundance.py` produces an
`amplicon::asv_table` from kraken2 reports under an `amplicon::survey`, which is the chain
that reaches the whole ecology lane from a metagenome.

**What each of the six still needs supplied from outside.** These are imports, which is
round 3's own `deferred` category -- a template declares them as inputs:

| transform | still requires |
|---|---|
| `umap_clustering` | `aspire::analysis_asv_meta` |
| `diversity_analysis` | `aspire::analysis_metadata` |
| `measurement_association` | `aspire::analysis_metadata`, `aspire::analysis_asv_meta` |
| `paired_group_contrast` | `aspire::analysis_asv_meta` |
| `spieceasi` | `aspire::spieceasi_on`, `aspire::indicspecies_group1_summary` |
| `graph_network` | ten inputs -- the whole network lane, plus taxonomy and the module tables |

`graph_network` is reachable in principle and expensive in practice: it is a renderer over a
co-occurrence network, and running it outside aspire means supplying the network. That is a
property of what it does, not of the gate that was removed.

## Drift found on the way in

Regenerating rewrote all 46 stubs, and 35 of them changed only by GAINING the
`# generated by ... -- do not hand-edit` banner and the provenance docstring naming the
`.nf` process and line. The checked-in tree predates those, so the generator and the
committed files had already drifted before this round -- benignly, in the direction of
carrying more provenance. Recorded so the size of this round's diff is not read as the
size of its topology change: eleven files carry a real edit, and they are exactly the rows
named above.
