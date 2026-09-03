# metasmith/viromics/make-template

Turns Antonio Castellano-Hinojosa's 40-step manual viromics pipeline into a
metasmith template. `research/viromics/` holds the work:
`viromics_survey_from_paired_reads.py` is the author driver in the standard
`_authoring` shape — run it with no arguments to print the coverage report, solve
and render the DAG, or with `--author` to ship it. It sits under `research/`
rather than in the package because `A.author` asserts a complete solve, and the
transforms it plans are not implemented yet.

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

13 targets, **45 steps, zero dropped, about a second**. The plan covers every
module: reads → QC → bbduk → megahit → contig batches → coverage and bam; three
callers → the merge → the frozen set; CheckV, both MMseqs2 clusterings, the
length table, vConTACT3, prodigal-gv → KOfam; three binners → CheckM2 →
aggregator → skANI → GTDB-Tk → CCTyper → iPHoP; DRAM-v, DRAM and Metabuli.

**The target list is short on purpose.** A target is not a request for a file to
exist — it is a slot the planner must satisfy consistently with every other slot,
and this pipeline's outputs mostly arrive as dependencies of one another. Naming
all forty of Antonio's outputs made the solve take minutes and then return no
plan at all; naming the nine nothing else reaches solves the same graph in a
second and produces those forty outputs anyway. `probe_targets.py` is how that
list was found and how to check it before adding to it.

**One known defect.** The plan carries an orphaned `spades` step — nothing
consumes its output, and on real data that is a second assembly of every sample
for nothing. It appears only once a per-sample target outside the viral lane is
named; the viral lane alone solves in 36 steps with none. `implementation_handoff.md`
has the diagnosis. Fix it before running this on anything real.

## State

Every transform under `src/metasmith_libraries/transforms/viromics/` is a **mock**:
the model — requirements, products, `parents`, `group_by`, `output_signature` — is
written for real, and the protocol touches its outputs and runs no tool. A solved
plan therefore proves the types line up and the planner reaches every target. It
proves nothing about whether CheckV likes the input.

`implementation_handoff.md` is what the next session needs: per mock, the command,
the outputs to copy out, the container, and the filename trap already known about
it. `tool_probe_notes.md` is the journal of running each tool to establish those
file shapes. `pipeline_steps.yml` maps all forty rows to a transform and a status.

The adapters on `genomad.py` and `virsorter2.py` are real code, not mocks, and are
the least verified part — no image for either was available locally, so their
column names are resolved by name with a loud assert rather than confirmed.

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

`dev/libraries.sh -b` adds a solve of all eleven shipped templates — the gate on
any change to a shared transform. `research/metasmith_libraries/template_fingerprint.py`
prints step count *plus the transform behind every step*, which is what catches a
re-route that leaves the count unchanged; the pre-change reference is
`research/viromics/results/template_baseline.txt`.

Plan search runs ~15x faster with the Rust solver staged
(`src/workflow_solver/dev.sh --stage`, or `dev/metasmith.sh -bel` to build it).
At this target set that is the difference between minutes and hours.

See [[viromics/dev]] for the transform porting.
