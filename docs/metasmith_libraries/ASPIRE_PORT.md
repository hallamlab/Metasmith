# Porting ASPIRE

`research/aspire/upstream/ASPIRE` is a standalone Nextflow amplicon/ASV pipeline: one 6,258-line
`asv_pipeline.nf` holding 45 processes, driven by a 780-line YAML with ~35 on/off
toggles. `transforms/aspire/` is that pipeline expressed as a typed graph so the
planner selects stages by what you ask for rather than by what you toggled.

**This is a topology pass.** Every transform is a stub: the requirements,
products, lineage and grouping are the port; the body touches its outputs and
returns. Nothing here runs anything yet, and no transform declares an `env::`
requirement — ASPIRE is 31 conda environments and zero containers, and env
resolution is orthogonal to whether the graph closes. Sourcing biocontainer URIs
is the gating item for ever running this off a workstation, and QIIME2, ConQuR
and SpiecEasi are the three that will resist.

## What goes in this file

The live record of one migration: what the ported graph does differently from the upstream
pipeline, and what is still open. It is a punch list, so an item leaves it when the item is
done rather than getting a note saying so.

## The table is the source

`transforms/aspire/_generate.py` holds one row per ported process and writes both
`data_types/aspire.yml` and all 37 stubs. Collapsing two nodes is a table edit
and a regenerate, which is the whole reason it exists — this pass is for looking
at the DAG and deciding what to merge. Once the bodies become real protocols,
regenerating would clobber them; stop running it then.

Each row carries the `.nf` process and line it came from, so the port stays
auditable against the source.

## What the port does to the pipeline

**`.done` sentinels are not ported.** Roughly a quarter of ASPIRE's edges are
ordering barriers, with the real data crossing by absolute path into a shared
staging directory. A sentinel is replaced by the artifact it stood for: a named
file where the consumer opened one, a directory where it scanned one. Nothing in
`aspire::` is a sentinel type.

**Optional stages with downstream consumers are gated on a policy token.**
Nextflow expressed those by *rebinding* the variable eleven consumers read;
Metasmith has no rebinding. So both arms produce the same consumer-facing type,
and each requires a different sibling token — `aspire::sankey_on` versus
`aspire::sankey_off`. The driver registers exactly one, the losing arm has zero
candidates for its token slot, and it is never instantiated. Six switches work
this way.

That mechanism is why `data_types/aspire.yml` writes the token types with
**list-form** `properties:` while everything else uses the mapping form.
`extends:` on a mapping merges key by key with the child winning, so a child that
restates any key the parent set — the `_:` description included — silently stops
satisfying the parent. A list is a set union, so subsumption always holds.
`python transforms/aspire/_generate.py --lint` asserts it for every edge.

**Processes are folded along method boundaries, not Nextflow ones.** Module 1
is ten rows for eighteen processes: `denoise` holds relabelling, concatenation,
dereplication, UNOISE, the chimera check and read mapping, and `curate` holds
`MITO_DECONTAM` and `FILTER_COUNTS`. A row is what an alternative method would
replace whole. Plot-only processes fold into the row that computed their tables.
`research/aspire/contracts.md` records every fold against its upstream lines.

**Augmentation, batch correction and the outlier checker are not ported.** Both
stages default off and sit outside the reads-to-ASV pipeline. The outlier checker
reads only batch correction's CLR table.

**Thresholds are a typed input.** Every module-1 row requires `aspire::params`, a
YAML file the driver registers under `run`. `research/aspire/presets/` offers the
tool defaults and ASPIRE's study values under the same keys. **CAUTION:** a 0
there means off, where upstream's Groovy `?:` would substitute the code default.

## The sample chain

`aspire::run` is a value the driver registers once. Each sample is a
`sequences::sample_name` under it, carrying a `sequences::read_metadata` and its
reads side by side. Per-sample rows `group_by` the name, and `denoise` groups by
`run`, which makes the study fan-in expressible at all.

Every transform requires `run` even where its protocol never opens it. That puts
`run` in each product's lineage, so a downstream `parents={run}` constraint has
something to resolve against.

**The reads are siblings of `read_metadata`, not its children.** A given four
ancestors deep (`run`, name, metadata, `read_pair`) loses its link to the
`read_pair` in the solver, and `interleave_zipped_short_reads` gets no candidates.
Any three-level chain solves. Nest the reads again only once the engine handles
that depth.

## Where the parities join

Paired and single-end reads join at `sequences::short_reads`, the type the rest of
the library already shares. A paired sample is given as zipped halves under a
`sequences::read_pair` and reaches it through `logistics/interleave_zipped_short_reads`.
A single-end sample is given as `short_reads_se`, which already is one. `fastp_qc`
and `merge_and_filter_reads` read the parity off `read_metadata` at run time.

**A study has one parity.** In one sample view the solver reaches `short_reads`
by the cheaper route only, so a mixed study plans without the interleave step and
its paired samples never reach the ASV table. The plan still reports success.
Split into per-sample views, the study becomes two solver cases, and the `run`
fan-in solves in neither. The pilot's given-count check is what catches the first.

**The long-read seam is the ASV table.** `denoise` emits `amplicon::asv_table`
and `amplicon::asv_seqs`, the way assembly emits `sequences::assembly`. A
long-read denoiser that produces those two types slots in under everything
downstream.

## Looking at it

    python research/aspire/aspire_asv_pipeline.py --switches
    python research/aspire/aspire_asv_pipeline.py default --parity single --dag
    python research/aspire/pilot.py run --parity paired --dag

The first two solve only: every input is `DEFERRED`. The pilot plans over the
mock dataset's fir paths and draws the full, step-only and legend views under
`research/aspire/reports/dag/`. `--on`/`--off` move the switches, and `--preset`
picks the thresholds. `tests/metasmith_libraries/test_aspire_workflow.py` holds the same
checks as assertions: both parities, and one case per switch arm.

## Known-rough, for the next pass

- Every transform is a stub. The topology, types and settings are the port, and
  no row has a protocol or an environment yet.
- The six token pairs and their six off-arm producers are machinery ASPIRE does
  not visibly have. Several stages are optional only because the `.nf` needed a
  flag, and once the planner selects by target some tokens can go.
- `aspire::analysis_counts` does not carry `amplicon::asv_table`'s properties, so
  the rows lifted onto `amplicon::survey` bind `denoise`'s raw table. The fix is
  a type-graph decision, recorded in `research/aspire/contracts.md`.
- `curate` models no negative controls and no lab contaminant set beyond the
  contaminant FASTA it already takes. A negative-control evidence input is the
  next addition there.
- `GROUP_POWER_ANALYSIS` is one Nextflow task hiding a bash driver, three Python
  drivers, six analysis scripts and an R script. It is the one candidate for
  *expansion* rather than collapse.
- `transforms/amplicon/blast_map_asvs.py` stays as the optional ASV-to-assembly
  placement, a cousin of `ASV_MAG_LINK`. The placeholder lane's taxonomy transform
  is gone, and its NB-plus-consensus merge rule lives in the `taxonomy` row's note.
  `data_types/amplicon.yml` is the vocabulary this pipeline shares with the rest of
  the library.
