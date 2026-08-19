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
`data_types/aspire.yml` and all 50 stubs. Collapsing two nodes is a table edit
and a regenerate, which is the whole reason it exists — this pass is for looking
at the DAG and deciding what to merge. Once the bodies become real protocols,
regenerating would clobber them; stop running it then.

Each row carries the `.nf` process and line it came from, so the port stays
auditable against the source.

## Three things the port does to the pipeline

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
candidates for its token slot, and it is never instantiated. Eight switches work
this way.

That mechanism is why `data_types/aspire.yml` writes the token types with
**list-form** `properties:` while everything else uses the mapping form.
`extends:` on a mapping merges key by key with the child winning, so a child that
restates any key the parent set — the `_:` description included — silently stops
satisfying the parent. A list is a set union, so subsumption always holds.
`python transforms/aspire/_generate.py --lint` asserts it for every edge.

**Plot-only processes are folded into the transform that computed their tables.**
Four of the 45: `INDICSPECIES_PLOTS` and `INDICSPECIES_ALIGNED_PLOTS` into
`INDICSPECIES`, `ASV_META_FROM_CORRECTED` into `ASV_BATCH_CORRECTION`, and
`RELABEL_FILTERED` into `CONCAT_FASTAS`.

## The run keystone

`aspire::run` is a value the driver registers once, `aspire::sample_id` hangs off
it, and read pairs hang off those. Per-sample stages `group_by=sample_id`; the
collector at `CONCAT_FASTAS` groups by `run`, which is what makes the study
fan-in expressible at all. Everything past that point is a singleton.

Every transform requires `run` even where its protocol never opens it — that is
what puts `run` in each product's lineage, so a downstream `parents={run}`
constraint has something to resolve against.

## Looking at it

    python research/aspire/aspire_asv_pipeline.py --switches
    python research/aspire/aspire_asv_pipeline.py all --dag

Solve only: no agent, no staging, no execution, and every input is `DEFERRED`.
`--on`/`--off` move the switches and the rendered DAG changes with them.
`pytest tests/metasmith_libraries/test_aspire_workflow.py` is the same thing as assertions, including
one case per switch checking that the chosen arm is in the plan and the other is
not.

## Known-rough, for the next pass

- The eight token pairs and their nine off-arm producers are machinery ASPIRE
  does not visibly have, and are where the port is most reducible: several stages
  are optional only because the `.nf` needed a flag, and once the planner selects
  by target some tokens can go.
- `GROUP_POWER_ANALYSIS` is one Nextflow task hiding a bash driver, three Python
  drivers, six analysis scripts and an R script. It is the one candidate for
  *expansion* rather than collapse.
- `master_summary_optional_slot` exists because `asv_pipeline.nf:2792` fills that
  slot with an empty placeholder unconditionally. It is a dead slot, now visible.
- **`transforms/amplicon` is NOT retired here**, which reverses the decision this
  port arrived with. It proposed moving `qiime2_taxonomy.py` and `blast_map_asvs.py`
  into `transforms/aspire/_disabled/` and deleting their tests, on the reasoning
  that `qiime2_taxonomy` is very nearly `TAXONOMY` and `blast_map_asvs` a cousin of
  `ASV_MAG_LINK`. Both readings are right, and both replacements are stubs: nothing
  under `transforms/aspire/` declares an environment or a container, and every
  protocol `touch`es its outputs and reports success. Trading two transforms that
  run for fifty that cannot would have been a downgrade dressed as a migration.

  They cost nothing to keep. The two families are never registered in the same
  solve — `test_aspire_workflow.py` loads `transforms/{aspire,logistics}` and
  `test_amplicon_workflow.py` loads `transforms/amplicon` — so the duplicate
  producers of `amplicon::asv_taxonomy` cannot make either plan ambiguous. Retire
  them when the stub that supersedes each one has a protocol, one at a time, and
  say so here.

  `data_types/amplicon.yml` is shared either way: `asv_table`, `asv_seqs`,
  `asv_taxonomy` and `silva_db` are the vocabulary this pipeline has in common
  with the rest of the library.
