# Porting ASPIRE

`research/aspire/upstream/ASPIRE` is a standalone Nextflow amplicon/ASV pipeline: one 6,258-line
`asv_pipeline.nf` holding 45 processes, driven by a 780-line YAML with ~35 on/off
toggles. `transforms/aspire/` is that pipeline expressed as a typed graph so the
planner selects stages by what you ask for rather than by what you toggled.

Twelve of the 31 rows run: the reads-to-counts lane (`fastp_qc` through `curate` and
`read_accounting`), plus `sankey`, `plot_metadata` and `indicspecies`. They ran green on
sockeye over 18 public 16S samples (`research/aspire/campaigns/cyano_r1/`). The other 19
rows are stubs that touch their outputs and return.

## What goes in this file

The live record of one migration: what the ported graph does differently from the upstream
pipeline, and what is still open. It is a punch list, so an item leaves it when the item is
done rather than getting a note saying so.

## The table is the source

`transforms/aspire/_generate.py` holds one row per ported process and writes
`data_types/aspire.yml` and every stub. It rewrites only files that begin with its banner. A
file without the banner is a real body and is never touched. Each row carries the `.nf`
process and line it came from, so the port stays auditable against the source.

**CAUTION** A row edit must be mirrored by hand in a real body. `_generate.py --lint` fails
until the body's requirements, products and grouping match its row.

## What the port does to the pipeline

**`.done` sentinels are not ported.** Roughly a quarter of ASPIRE's edges are
ordering barriers, with the real data crossing by absolute path into a shared
staging directory. A sentinel is replaced by the artifact it stood for: a named
file where the consumer opened one, a directory where it scanned one. Nothing in
`aspire::` is a sentinel type.

**Optional stages with downstream consumers are gated on a policy token.**
Nextflow expressed those by *rebinding* the variable eleven consumers read;
Metasmith has no rebinding. So both arms produce the same consumer-facing type,
and each requires a different sibling token — `aspire::graph_network_on` versus
`aspire::graph_network_off`. The driver registers exactly one, the losing arm has zero
candidates for its token slot, and it is never instantiated. Four switches work
this way. A stage whose off arm only fed its consumers an empty placeholder has no
switch: `sankey` and `indicspecies` run whenever a target needs them.

That mechanism is why `data_types/aspire.yml` writes the token types with
**list-form** `properties:` while everything else uses the mapping form.
`extends:` on a mapping merges key by key with the child winning, so a child that
restates any key the parent set — the `_:` description included — silently stops
satisfying the parent. A list is a set union, so subsumption always holds.
`--lint` asserts it for every edge.

**Processes are folded along method boundaries, not Nextflow ones.** Module 1
is ten rows for eighteen processes: `denoise` holds relabelling, concatenation,
dereplication, UNOISE, the chimera check and read mapping, and `curate` holds
`MITO_DECONTAM` and `FILTER_COUNTS`. A row is what an alternative method would
replace whole. Plot-only processes fold into the row that computed their tables.
`research/aspire/contracts.md` records every fold against its upstream lines.

**Not ported:** augmentation, batch correction and the outlier checker, which default off
and sit outside the reads-to-ASV pipeline. Also not ported: the four analyses that exist
upstream only for its lung study (`VOC_CORRELATION`, `GROUP_POWER_ANALYSIS`,
`TAXONOMY_GROUP_ASSOCIATION` and `PAIRED_GROUP_CONTRAST`).

**Thresholds are a typed input.** Every module-1 row requires `aspire::params`, a
YAML file the driver registers under the study. `research/aspire/presets/` offers the
tool defaults and ASPIRE's shipped values under the same keys. **CAUTION:** a 0
there means off, where upstream's Groovy `?:` would substitute the code default.

## How a real body runs

Each real body requires an `env::` type and runs every script it needs as a `lib::aspire`
resource. The `lib::aspire` entry in `data_types/lib.yml` says which scripts are upstream's
unmodified, which lost only lung-study code, and which are new. The six that decide the
count table are unmodified. `curate.py` is new, and it calls `filter_nontarget.py`'s own
functions in upstream's order.

The tools run from public images: fastp, vsearch, SINA, BLAST and QIIME2. The vendored
scripts need R with indicspecies and a python plotting stack that no public image carries.
They run in `quay.io/hallamlab/aspire`, which `docker/aspire/` builds.

**CAUTION** Apptainer on a cluster runs with `--no-home`. SINA's ARB library, numba and
kaleido each need a writable home, so `sina_trim`, `taxonomy` and `sankey` set `HOME` to the
work directory. SINA reports a missing home as a corrupt ARB database.

## Where the counts can differ from upstream

Every step up to the filtered ASV table uses upstream's commands, flags and shipped values:
fastp, pair merging, the expected-error and length filter, dereplication, UNOISE, uchime3,
the count table, table filtering, SINA and the classifier. Curation differs in three places,
and each can change the curated table.

1. **MitoMaster is gone.** Upstream posts every ASV to mitomap.org, and compute nodes have
   no internet. `mitomaster` writes an empty MitoMaster table, so only the BLAST screen
   marks mitochondria. That equals upstream with `run_mitomaster: false`.
2. **Curation drops no sample for its group.** Upstream deletes samples whose `Type_Group`
   has fewer than 3 members before the abundance filter. The port keeps every sample, so
   its table can hold more samples and more ASVs. See *Groups*.
3. **`plot_metadata` skips the mito tables when no ASV is mitochondrial.** Upstream passes
   `--make-mito` unconditionally, and the script raises on an empty table. Host-associated
   samples never reach that case, and a culture does.

## The sample chain

`aspire::study_metadata` is the root: the study's sample sheet, registered once. Each
sample is a `sequences::read_metadata` under it, whose JSON carries `sample`, `parity` and
`length_class`. The reads are children of their metadata. A paired sample's zipped halves sit
under a `sequences::read_pair`. Per-sample rows `group_by` the read metadata. `denoise`
groups by the study, which makes the study fan-in expressible.

The `sample` key is what keeps two samples' metadata distinct. `denoise` and
`read_accounting` read it through `context.SourceOf`, never by position in the group.

**CAUTION** Keep the chain three levels deep. A given four ancestors deep loses its link to
its `read_pair` in the solver, and `interleave_zipped_short_reads` gets no candidates. A new
level between the study and the read metadata brings that back.

## Groups

A group is any categorical label on the sample sheet. The sheet's first column is the
sample id, and every other column is a label. Numeric measurements go in an optional
`aspire::sample_measurements` table, which only `measurement_association` requires.
An analysis that compares groups runs once per label. `indicspecies` writes one results
table and one summary per label, and skips a label with fewer than two levels.

`analysis.min_level_size` replaces upstream's sample drop. `plot_metadata` blanks a label
value that fewer samples hold, so each analysis skips that level and keeps the sample.

## Where the parities join

Paired and single-end reads join at `sequences::short_reads`, the type the rest of
the library already shares. A paired sample reaches it through
`logistics/interleave_zipped_short_reads`. A single-end sample is given as
`short_reads_se`, which already is one. `fastp_qc` and `merge_and_filter_reads` read the
parity off `read_metadata` at run time.

**A study has one parity.** In one sample view the solver reaches `short_reads`
by the cheaper route only, so a mixed study plans without the interleave step and
its paired samples never reach the ASV table. The plan still reports success.
Split into per-sample views, the study becomes two solver cases, and the study
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

- No run has compared the port's ASV table with upstream's on the same reads. That diff
  is the test of the claims under *Where the counts can differ from upstream*.
- The mito and contaminant references are the mock dataset's FASTAs. Upstream's shipped
  config names NCBI mitochondria and a contaminant BLAST database. `mitomaster` also takes
  only FASTA, where upstream can read a prebuilt BLAST database.
- Nothing removes chloroplast ASVs, upstream included. A plant or algal study keeps them
  unless the contaminant FASTA covers them.
- The ASPIRE preset excludes *Homo sapiens* and Mammalia, a human-host setting. Its 20/20
  tail trims carry no stated source.
- SINA reads SILVA 138.2 and the classifier reads SILVA 138. The `amplicon::silva_db`
  bundle also carries an NB classifier that no row uses.
- `quay.io/hallamlab/aspire` is not on quay. A cluster runs it only after
  `run_cyano.py side-load-images` copies it into the image store.
- The four token pairs and their four off-arm producers are machinery ASPIRE does
  not visibly have. Several stages are optional only because the `.nf` needed a
  flag, and once the planner selects by target some tokens can go.
- `aspire::analysis_counts` does not carry `amplicon::asv_table`'s properties, so
  the rows lifted onto `amplicon::survey` bind `denoise`'s raw table. `indicspecies`
  therefore tests every raw ASV, not the curated ones. The fix is a type-graph decision,
  recorded in `research/aspire/contracts.md`.
- `curate` models no negative controls. A negative-control evidence input is the next
  addition there.
- `transforms/amplicon/blast_map_asvs.py` stays as the optional ASV-to-assembly
  placement, a cousin of `ASV_MAG_LINK`. `data_types/amplicon.yml` is the vocabulary this
  pipeline shares with the rest of the library.
