# FabFos tests

Unit tests for the three FabFos pipeline drivers (`src/fabfos/pipelines/{assembly,
annotation,ecspr}.py`). Each test calls the driver's own `generate_workflow`
directly -- the same function its CLI entry point uses -- so the plan under test
is exactly what a real invocation produces, not a hand-rolled duplicate. These
are **planning-only**: nothing is staged, containerised, or executed, so they
run in seconds and need no test data beyond empty stand-in files.

`conftest.py` puts `src/` on `sys.path` (appended, not prepended -- `fabfos`
has no installed copy in the test envs, but `src/metasmith` and
`src/metasmith_libraries` are sibling packages under the same `src/` that must
NOT shadow the installed `metasmith` package; see the module docstring).

## `test_assembly_driver.py`

The recovery half: raw pooled-clone reads through QC, cleaning, host
filtering, both assemblers, and `resolve_inserts` to `fabfos::putative_inserts`
+ `insert_metadata`, plus `assembly_stats` pinned to the recovered inserts.
Renders `tests/fabfos/artifacts/assembly_dag.svg`.

## `test_annotation_driver.py`

The annotation half: one ORF FASTA through the canonical four lanes
(`kofamscan`, `clean`, `diamond_uniref50`, `proteinbert`) into `gpr_4lane` ->
`annotation::gpr_table`. `annotation.py` only ever demands the canonical
target, so `gpr_7lane` / `gpr_table_7lane` is out of scope here -- it stays a
real library capability, just not one this driver builds. Renders
`tests/fabfos/artifacts/annotation_dag.svg`.

## `test_ecspr_driver.py`

The measurement: a GPR table + its condition set + the frozen MetaNetX
reference basis (atom pairs, direction ratios, metabolite names) into the
single `ecspr_measure` transform. Two units are supplied to exercise the
multi-unit staging path, but the per-unit job fan-out (`group_by=exp`) is a
runtime property invisible to planning -- see `metasmith-runtime-fanout` --
so the plan itself still shows one `ecspr_measure` step. Renders
`tests/fabfos/artifacts/ecspr_dag.svg`.

## What now runs, and what does not

`junction_split` and `cluster_contigs` are **implemented and executed**, not just
planned. Both drive `algorithm::fabfos_recovery.py`, a resource library shipped
inside the fabfos package at `src/fabfos/algorithm` — which is the point of it
living there rather than in `resources/lib`: the recovery methods can be run
directly against a directory of assemblies with no planner and no staging, which
is how a cut gets inspected before it is trusted. A run over 69 assemblies from
fir (35 pool-barcode units) produced the same 665 pieces pool-for-pool as the
direct run, and the same 162 internal junctions.

`ecspr_measure` still declares its contract and raises. So does everything the
measurement needs that does not exist yet — the component columns on the GPR
table, the coded reference schema, and the conditions tables. The reads-to-ECSPr
test therefore still checks that the chain *plans*, not that it runs.

## `test_reads_to_ecspr_workflow.py`

Joins the halves above into **one** plan and runs them into the measurement —
raw pooled-clone reads all the way to `ecspr::results` — so the seams are what is
under test. **14 steps.** Renders `tests/fabfos/artifacts/reads_to_ecspr_dag.svg`.

### The measurement stage

`ecspr_measure` is deliberately **one** transform:

    gpr_table + conditions + atom_pairs + direction_ratios + metabolite_names
        -> ecspr::results

The deployed method splits this across four transforms (`evidence_weights`,
`addition_weights`, `effects`, `significance`) plus a graph-building lane, but
every one of those boundaries is an internal staging decision rather than a fork
in the method — each intermediate has a single producer, a single consumer, and
nobody asks for it on its own. Collapsing them makes the contract state the
claim: this evidence, under these conditions, on this reference basis, measures
this.

Three of the five inputs are the frozen MetaNetX reference basis, and they are
staged **unpinned**: they are static functions of the MNXR/MNXM id space, so
parenting them to a run would imply the basis changes between runs, which is
what the pinned MNXREF release exists to prevent. `metabolite_names` is not
decoration — `atom_pairs` carries metabolite ids and canonical atom *ranks*, so
a graph node is a `(metabolite, rank)` key with no chemical meaning until it is
decoded, and the conditions' source/sink hubs cannot be located on the graph
without it.

`conditions` **is** per-experiment and pinned to the experiment, because the
condition set is the measurement's own claim about what it tests. The engine has
no default set and must not be given one.

The pin on `gpr_table` is the lineage constraint this stage was waiting on. It
forces the table to be the one built from *this* run's recovered inserts, and in
doing so pulls the whole recovery chain in behind it — demanding
`ecspr::results` alone is enough to plan all 14 steps. The intermediate targets
are kept anyway so a break localises to the step that broke instead of
surfacing as one unsatisfiable plan.

**The null is a separate pipeline** and is not an input here. Significance is
scored against draws over a *metagenomic* ORF pool, size-matched to each unit's
ORF count; that basis is built once, frozen, and shared. The deployed contract
agrees structurally — `ecsprGround/null.py` is `group_by=null_draw_spec` and has
no `recovery_experiment` requirement at all — so it takes no part in this
lineage and arrives as a staged reference when the port lands.

Not yet implemented: the transform declares its contract and raises. The logic
to port is the deployed atom lane (`ecspr/{evidence,addition}_weights.py`,
`ecsprGround/{effects,significance}.py`), whose engine primitives are already
staged here as `lib::ecspr_{build,graph,directed}.py`.

Both checks now pass. The lineage check used to be a strict `xfail`: the plan
resolved, but wrongly. The run tools satisfied their `sequences::orfs` demand
from *extra* `prodigal` runs on *extra* assemblies, bypassing the recovery chain
entirely, and only `gpr_4lane`'s own `orfs` edge followed the pin off
`resolve_inserts` — so the mapper folded annotations of one ORF set onto a
different one and every gene-id join in it would have come back empty.

The fix is library-side, as predicted, and is a one-line-per-lane change to each
GPR mapper: every lane requirement is pinned to the mapper's own `orfs`
requirement,

    kofam = model.AddRequirement(lib.GetType("annotation::kofamscan_results"), parents={orfs})

so a single shared ORF ancestor satisfies the mapper and all four (or seven)
lanes at once. That is also what collapses the duplicate prodigals — 20 steps
with three `prodigal` runs became 14 with one. The distinction that matters:
pinning a lane's output from a **target** makes the planner duplicate
`kofamscan`; pinning it inside the **transform** makes the planner share it. The
recovery transforms had been doing the latter all along.

**Only the canonical mapper is demanded**, and that is load-bearing:
`annotation::gpr_table` has one producer, so one unambiguous target reaches it
with nothing pinned twice. Demanding both mappers instead — two targets sharing a
pinned ancestor — drove the planner to 33 GB resident and still climbing after 10
minutes, against ~12 s here. **Two read pools** are kept so the cross-pool
aggregation is exercised across the seam.

## ORF sharding

`prodigal` now emits its ORFs as **N balanced shards** rather than one FASTA. The
contract is unchanged — still one `sequences::orfs` product — because the fan-out
is a runtime property: the protocol registers one manifest branch per shard, and
metasmith's output channel turns N files matching one product's glob into N
downstream items, so each annotator runs once per shard with its wall time
bounded by `orf_shard_size`.

Shards are balanced, not greedily filled: `n = ceil(total / cap)` fixes the count
and the remainder is spread one record per shard, so 11 ORFs at a cap of 10 give
**6 + 5**, never 10 + 1. A straggler shard costs a whole extra scheduling round
for a job that does almost no work. The GFF stays a single file — it is the
coordinate record for the assembly as a whole and nothing reads it per shard.

None of this shows up in the DAG, which is the point: the planner still sees one
`prodigal` → one `kofamscan` edge, and the shard count is a runtime decision.

> Note: editing a `data_types/*.yml` or transform in `src/metasmith_libraries`
> requires regenerating the per-library `_metadata/` snapshots before these tests
> see the change — `./dev/fabfos.sh -b` only bundles. See
> `src/metasmith_libraries`'s own build step, `dev/libraries.sh -b`
> (`python -m metasmith build all --types … --uniques … --transforms …`).

## `audit_final_steps.py`

Unrelated to the three drivers above -- a data-driven audit of the algorithms
in `chimera_split.py`/`coverage_trim.py` against real scadc ground truth, not
a planning compile-check. See its module docstring. Skips cleanly if the
scadc profile pickle isn't reachable. Not picked up by the default `pytest
tests/fabfos/` collection (its filename doesn't match `test_*.py`); run it
explicitly:

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/fabfos/audit_final_steps.py -v -s

## `assembly_stats_on_fir.py` — not a test

The executing half of the coverage lane the fosmids compile-check describes:
`data/fabfos/scadc_fosmids/sequences/inserts/inserts.fna` mapped against each of the 35 SCADC pools'
host-filtered reads on fir, one slurm job per pool. Like the executing drivers
in `research/fabfos/examples/`, plan-only is the default — `--run` executes, `--offline` plans
with no host contact at all, `--preflight` checks the remote prerequisites,
`--summarize` rebuilds the tables from an already-retrieved results directory.

Three things about it are worth knowing before changing it.

**The fan-out is one given with 35 parents.** The insert FASTA is added once,
parented to every pool's `read_metadata`. `Orchestrator.groovy`'s `group()`
filters candidate combinations by set *intersection* rather than equality
precisely for this aggregate-then-distribute shape, so the one file reaches all
35 jobs. A collapse to a single job plans, stages and runs without complaint, so
the driver asserts `len(step.group_by_instances) == 35` per step before it
deploys — the plan has two steps either way, and step count says nothing.

**The target cannot be pinned, so the plan is asserted instead.** The compile
check pins `assembly_stats` to the inserts *target*; here the inserts are a
*given*, and `TargetBuilder.Add` takes only targets as parents.
`TransformInstanceLibrary.AsView` looks like the substitute and is not — the view
supports planning but not staging, since `WorkflowTask.Pack` calls `GetKey()` on
every transform library and the view class defines neither `GetKey` nor
`PrepTransfer`. The whole domain is offered and the resolved transform set is
asserted to be exactly `{seqkit_reads, assembly_stats}`.

**Attribution comes from the manifests, not the work directory.** Staged names
are content hashes, but `_manifests/given.csv` maps each given's `instance_index`
to its path — and the driver names every `read_metadata` file for its pool — while
each product manifest entry carries the `lineage` that says which index it
consumed. Joining the two names every output, needs no run directory, and so
survives fir's scratch being cleaned. `research/fabfos/examples/verify_assembly_graphs.py` had to
scrape `nxf_work/*/*/.command.sh` only because one product type there was
published with no ancestry at all.

Outputs land under `data/scratch/resolve/insert_stats_<ts>/`: `pool_summary.tsv`
(per-pool `fraction_reads_mapped` and the read counts behind it),
`insert_coverage_matrix.tsv` (171 reference records × 35 pools of fold coverage —
the 170 inserts plus the pCC1fos backbone, whose row must be dropped before the
matrix is read as composition),
`pool_map.tsv` and `insert_set.json`. Everything except the BAMs is retrieved —
the whole per-bp coverage tree measured 60 MB against 20 GB of BAM, and minimap2
is not run with `--sam-hit-only`, so the BAMs carry every unmapped read too.
`promote_coverage_chunk.py` turns a retrieved directory into a pinnable chunk by
renaming each product to the pool attribution already resolved.

## `original_assembly_stats_on_fir.py` — not a test

The same transform against the other reference: each pool's reads mapped onto its
own megahit and spades assembly, 70 jobs, so the insert-set coverage has a
denominator. It imports the sibling's ssh helper, preflight, task-table check and
orphan resolver rather than copying them; what it redefines is the fan-out, the
plan assertion and the summary.

**The fan-out inverts, and both halves of it are asserted.** One `read_metadata`
per *(pool, assembler)* with its own assembly parented to it, and the reads added
once per pool with both metadata items as parents — the aggregate-then-distribute
shape moved onto the reads. That is what lets `seqkit_reads`, which groups on the
reads, stay at 35 tasks while `assembly_stats`, which groups on the metadata, runs
70; the driver asserts both counts. The read-QC product reaches both of a pool's
stats jobs through the same intersection filter.

**Declare the assemblies as `sequences::assembly`, never as the assembler's own
subtype.** `megahit_assembly` and `spades_assembly` both satisfy the requirement
by property superset, but the planner binds `asm` to *one* concrete given type:
declared that way, the resolved plan carried 35 spades givens, no megahit at all,
and still reported 70 tasks. Balance instead comes from refusing any pool that
does not have both assemblies.

## Executing drivers are NOT here

Everything in this directory is planning-only, and that is what lets it pass on a
machine holding none of the reference bytes. Anything that stages, containerises
or executes lives in `research/fabfos/examples/`:

| | |
|---|---|
| `research/fabfos/examples/annotation_references_build.py` | builds R3/R4/R5/R7 from the pinned `data/originals/` folders; APPTAINER |
| `research/fabfos/examples/scadc_gpr.py` | inserts → prodigal → the lanes → a GPR table; APPTAINER |
| `research/fabfos/examples/_driver.py` | what both need: the dev overlay, waiting on a run's log, reading manifests, publishing |

The three drivers that used to sit here — `build_references_dag.py` and the two
`build_references_stage*` halves — were deleted because none of them could plan any
more: they target types whose producers were replaced, and their shared helper
refused to import unless the engine carried the MAMBA executor, which the
annotation half does not use. A gate that cannot run is not a gate.

The plan-only gates for the two reference halves are
`research/fabfos/examples/annotation_references_dag.py` (10 steps, **no given at all**) and
`research/fabfos/examples/metabolism_references_dag.py` (exactly one given, the licensed MetaCyc
drop-in). Both still resolve from nothing.

## Running

Needs an environment with the `metasmith` package **and** graphviz's `dot` on
`PATH`. The `msm` conda env has both:

```bash
PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/fabfos/ -v
```

Each driver test also runs standalone to just print the plan and (re)write its
SVG, e.g.:

```bash
PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python tests/fabfos/test_assembly_driver.py
```

> Note: editing a `data_types/*.yml` or transform in `src/metasmith_libraries`
> requires regenerating the per-library `_metadata/` snapshots before these
> tests see the change -- `./dev/fabfos.sh -b` only bundles. See
> `src/metasmith_libraries`'s own build step, `dev/libraries.sh -b`
> (`python -m metasmith build all --types … --uniques … --transforms …`).
