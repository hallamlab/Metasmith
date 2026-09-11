# metasmith/viromics/make-template

Turns Antonio Castellano-Hinojosa's 40-step manual viromics pipeline into a
metasmith template. `research/viromics/` holds the work:
`viromics_survey_from_paired_reads.py` is the author driver in the standard
`_authoring` shape — run it with no arguments to print the coverage report, solve
and render the DAG, or with `--author` to ship it. It sits under `research/`
rather than in the package because `A.author` asserts a complete solve, and the
transforms it plans are not implemented yet.

`research/viromics/reports/` holds the written-up version and the pictures behind
it. `mkdag.py` draws the intended topology by hand; `mkplandag.py` renders the
solved plan beside the shipped metagenomics template's; `mkchunkdags.py` cuts
that same solve into four readable views — spine, function, viral, taxonomy.
Re-run all three after changing the driver, or the report's numbers drift from
the plan's.

## The one decision this template makes

Antonio's pipeline is a **chain of filters**: each curation step writes a smaller
FASTA and every later tool reads whatever the previous filter left, so changing
any threshold means re-running everything below it. This template does not port
that chain.

geNomad, VirSorter2 and VIBRANT each reduce their own output to a normalised
interval table; those are merged into **one dereplicated candidate set, and that
set is frozen**. After it, nothing writes a FASTA. Lengths, clusters, CheckV trim
suggestions, taxonomy, function and host prediction are all tables keyed on the
frozen set's contig ids, and Antonio's four filter scripts become a join over
them — so a different threshold or a different vOTU slice costs a groupby rather
than a rerun. Ten of the forty rows are marked `dropped` in `pipeline_steps.yml`
for exactly this reason, and each names the join that answers it.

The template is an **offshoot of `metagenomics_from_paired_reads`**, not a second
pipeline: steps 1–4 and 26–40 are that template, targeted rather than re-derived.

## Where each tool runs

Cross-sample tools run on the frozen set, because pooling is what they are for —
MMseqs2 defines a vOTU by clustering across samples, and CheckV, vConTACT3, iPHoP
and the spacer BLAST all want one catalogue. Per-contig annotators (geNomad,
VirSorter2, VIBRANT, DRAM-v) run once per sample and reach a vOTU through
`viromics::candidate_call_provenance`.

That split is also load-bearing for the planner, and the reason is invisible from
any one file: **lineage constraints are ancestral, not immediate.** The frozen set
descends from every contig batch that fed the merge, so any requirement the frozen
set also satisfies makes a caller's output eligible to feed the merge that
produced it. Measured on this target set — with geNomad reachable on the frozen
set the solve took 43 s; adding VIBRANT there as well, it stopped finding any
complete plan at all. Hence: the four per-contig annotators require
`sequences::contig_batch`, and every consumer of the frozen set names
`viromics::dereplicated_candidate_virus` explicitly. Do not reintroduce a shared
contig contract that both satisfy.

## What it solves to

14 targets, **53 steps, zero dropped, about a second**. The plan covers every
module: reads → QC → bbduk → megahit → contig batches → coverage and bam; three
callers → the merge → the frozen set; CheckV, both MMseqs2 clusterings, the
length table, vConTACT3, prodigal-gv → KOfam; three binners → CheckM2 →
aggregator → skANI → GTDB-Tk → CCTyper → iPHoP; DRAM-v, DRAM and Metabuli.

**The gene table is one target, not four.** `annotation::gpr_table` pulls the
whole chosen-4 panel in behind it — KOfamScan, CLEAN, DIAMOND UniRef50 and
ProteinBERT, each chunked, merged and folded by `fabfos/gpr_4lane.py` — for one
slot instead of four. It goes beyond Antonio's step 25, which asks for KO
assignment alone. It also costs the driver the `fabfos` transform library, the
`resources/lib` resource library, and two study-wide references,
`ref::mnxr_lookup` and `ref::label_transfer_landmarks`.

**A study-wide input has to be named shared.** `sample_type` masks the input
library down to each sample and its relatives, so a deferred item with no
parents belongs to no sample and the solver never sees it — the solve then drops
*every* target, not just the unreachable one. `build_spec` reads the two GPR
reference paths back off the manifest and passes them in `shared_input_paths`.

**The target list is short on purpose.** A target is not a request for a file to
exist — it is a slot the planner must satisfy consistently with every other slot,
and this pipeline's outputs mostly arrive as dependencies of one another. Naming
all forty of Antonio's outputs made the solve take minutes and then return no
plan at all; naming the fourteen nothing else reaches solves the same graph in a
second and produces those forty outputs anyway. `probe_targets.py` is how that
list was found and how to check it before adding to it.

**One assembler, said outright.** `megahit_assembly` and `spades_assembly` both
satisfy `sequences::assembly`, and only the viral lane's targets pin one — so
the planner used to answer the MAG lane's generic slot with metaSPAdes and
assemble every sample twice. No lineage constraint fixes that, because matching
is ancestral: "descends from this assembly" cannot be told from "descends from
that one", and pinning it with another target builds both lanes instead of one.
`_assembly_without_spades()` in the driver hands the planner an assembly library
with metaSPAdes masked out. A saved template records a library by location, so
that mask does not survive `--author` — pin the assembler in the targets before
shipping it.

**Two gaps the plan makes visible.** GTDB-Tk runs on SemiBin2's bins only, while
`iphop_add_to_db` collects the aggregator's whole quality pool, so bins from
MetaBAT2 or COMEBin join to no taxonomy. And nothing classifies reads: the
shipped metagenomics template names phyloFlash for that and this driver does
not.

## State

Every transform under `src/metasmith_libraries/transforms/viromics/` runs its tool.
Each declares an `env::` requirement, and each tool needing a reference declares that
too. A solved plan now stands for work that would happen.

Nine of the twelve ran against real input with their products read: CheckV, CCTyper,
the spacer BLAST, both MMseqs2 clusterings, prodigal-gv, the length table, the merge,
VIBRANT and vConTACT3.

Three did not run, for reasons outside the code:

- `iphop_add_to_db` and `iphop_predict` need iPHoP's host database. `iPHoP_db_Aug23_rw`
  arrives as seventeen 10 GiB chunks, joins into one tarball, then unpacks in place.
  Peak disk is three times the download. Stage it on cluster scratch.
- `metagenomics/taxonomy/gtdbtk_de_novo.py` never executed. It exists because
  `iphop add_to_db` reads decorated trees, which only `de_novo_wf` writes.

The adapters on `genomad.py` and `virsorter2.py` remain the least verified part. No
image for either was available locally, so their column names resolve by name with a
loud assert rather than by confirmation.

**CAUTION**: a tool's own source does not list the files it writes. vConTACT3 carries
the strings `nodes.csv`, `edges.csv` and `ani_summary.tsv` in a docstring and in
comments, and writes none of them. Collect an output by pattern after a real run.

`implementation_handoff.md` gives the command, the container and the expected output
paths per transform, and calls each path a claim to check. `tool_probe_notes.md` is the
journal behind those claims. `pipeline_steps.yml` maps all forty rows to a transform and
a status.

## Antonio's material

`research/metasmith_libraries/viromics/reference/` carries it, tracked here rather
than on capella. `Viromics_workflow_table.xlsx` is the 40-row contract with tool,
version, parameters, inputs and outputs per step. The `01_`..`15_` shell scripts
cover rows 1–15 only and are the source for the exact thresholds — which are not
hand-tuned: per slack 2026-06-16 they were adopted wholesale from
https://www.nature.com/articles/s41467-026-68914-2#Sec10.

## Running it

**A fresh worktree has no `_metadata/`, and every solve raises before planning.**

    PYTHONPATH="$PWD/src" mamba run -n msm bash dev/libraries.sh -bm

`dev/libraries.sh -b` adds a solve of every shipped template — the gate on
any change to a shared transform. `research/metasmith_libraries/template_fingerprint.py`
prints step count *plus the transform behind every step*, which is what catches a
re-route that leaves the count unchanged. The reference is
`research/viromics/results/template_baseline.txt`.

**CAUTION**: `src/metasmith/engine/` is an untracked build product. A fresh worktree
holds no `msm_solver` and falls back to the python solver, which answers with a
different plan. Copy `src/metasmith/engine/*` into a worktree before comparing
fingerprints across commits.

Plan search runs ~15x faster with the Rust solver staged
(`src/workflow_solver/dev.sh --stage`, or `dev/metasmith.sh -bel` to build it).
At this target set that is the difference between minutes and hours.

See [[viromics/dev]] for the transform porting.
