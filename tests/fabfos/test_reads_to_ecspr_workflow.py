"""Compile-check for the end-to-end FabFos pipeline: raw reads -> ECSPr result.

This joins the half-pipelines that already have their own tests
(`test_fosmids_workflow.py`, `test_gpr_workflow.py`) into a single plan and runs
them into the measurement, so what is under test here is the **seams**:

    reads (per pool) -> host filter -> assembly (megahit + spades)
      -> junction split (blast pCC1 backbone, cut at the junctions)
      -> cross-assembler dedup -> putative inserts -> ORFs
      -> {kofamscan, clean, diamond_uniref50, proteinbert}
      -> gpr_4lane -> annotation::gpr_table
      -> ecspr_measure -> ecspr::results

with `sequences::assembly_stats` demanded alongside as a standalone coverage
target that nothing consumes -- pinned to the inserts, so it is the recovered
insert set that gets read-mapped, once per pool.

The measurement is ONE transform taking the GPR table, the experiment's
conditions, and the three frozen MetaNetX reference parquets (atom pairs,
direction ratios, metabolite names). The null it scores against is a separate
pipeline built over a metagenomic ORF pool, so it takes no part in this
lineage and is not staged here.

Two read pools are supplied so the cross-pool aggregation
(`group_by=experiment`) is exercised: clustering / coverage must see
both pools in one job.

Planning is type-driven -- nothing is staged, containerised, or executed, so
empty read files and type-only stand-ins for every staged `ref::*` are enough.
The MNXR bridges and reference label pool have no producer here on purpose;
their build transforms are deferred out of scope.

Renders the resolved DAG to `tests/artifacts/reads_to_ecspr_dag.svg`.

Two deliberate departures from the half-pipeline tests -- see `tests/README.md`:

* **Only the canonical mapper is demanded.** `annotation::gpr_table` has exactly
  one producer now (`gpr_4lane`), so a single unambiguous target reaches it and
  nothing has to be pinned twice. That is also what removed the planner blow-up
  this test used to hit when two mapper targets shared one pinned ancestor
  (33 GB resident and still climbing after 10 min). `gpr_7lane` keeps its own
  subtype and stays the annotation-half test's job.
* **The lineage assertion is real, and now passes.** It used to be a strict
  xfail: the annotation lanes attached to ORFs from a different assembly than the
  one the recovery half produces. Each GPR mapper now pins its lane requirements
  to its own `orfs` requirement, which both fixes the wiring and collapses the
  duplicate prodigals it used to imply.

Requirements: an env with `metasmith` + graphviz `dot` (e.g. `msm`):

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/test_reads_to_ecspr_workflow.py -v

Runs standalone too: `python tests/test_reads_to_ecspr_workflow.py`.
"""
from __future__ import annotations

from pathlib import Path


from metasmith.python_api import (
    Agent,
    Runtime,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
MLIB = REPO_ROOT / "src" / "metasmith_libraries"
# FabFos's own algorithm library lives in the package, not the transform
# library, so it loads from its own root (see fabfos.library.resolve_algorithm_library).
ALGO = REPO_ROOT / "src" / "fabfos" / "algorithm"
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

N_POOLS = 2

# Recovery half: reads -> putative inserts -> ORFs.
EXPECTED_RECOVERY = {
    "seqkit_reads",        # read QC (feeds bbduk)
    "bbduk",               # read cleaning
    "background_filter",   # host removal (coerced in by resolve_inserts' lineage)
    "megahit",             # assembler 1
    "spades",              # assembler 2
    "assembly_stats",      # coverage of the inserts, per pool (nothing consumes it)
    "resolve_inserts",     # backbone blast -> cut -> cross-assembler dedup -> inserts
    "prodigal",            # ORF prediction on the final inserts
}

# Annotation half: ORFs -> the canonical GPR table. `gpr_4lane` produces
# `annotation::gpr_table` itself, so the chosen-4 run tools are the ones on the
# canonical path; `gpr_7lane` stays available but is not demanded here.
EXPECTED_ANNOTATION = {
    "kofamscan",         # KO (HMM bitscore)
    "clean",             # CLEAN contrastive EC
    "diamond_uniref50",  # UniRef50 homology + analytic BSR
    "proteinbert",       # ProteinBERT embeddings
    "gpr_4lane",         # canonical mapper -> annotation::gpr_table
}

# Measurement: the GPR table + the experiment's conditions + the frozen MetaNetX
# reference basis -> the ECSPr result. One transform by design; see its docstring.
EXPECTED_MEASUREMENT = {
    "ecspr_measure",     # -> ecspr::results
}

EXPECTED_TRANSFORMS = EXPECTED_RECOVERY | EXPECTED_ANNOTATION | EXPECTED_MEASUREMENT

# The frozen reference basis ECSPr solves against. Network-agnostic (static in
# the MNXR/MNXM id space), so staged unpinned -- pinning them to a run would
# imply the basis changes between runs.
ECSPR_REFS = [
    "ecspr::atom_pairs",        # AAM
    "ecspr::direction_ratios",  # directions
    "ecspr::metabolite_names",  # names -- required to decode atom_pairs
]

# Staged `ref::*` inputs the annotation half consumes. Type-only stand-ins:
# planning only needs the type, not the bytes.
STAGED_REFS = [
    "ref::kofamscan_profiles",
    "ref::kofamscan_ko_list",
    "ref::uniref50_diamond_db",
    "ref::esm_c_600m_weights",
    "ref::ezpred_model",
    # mnxr_lookup replaced the ko/ec/uniprot bridge trio -- one table, one
    # stand-in; both GPR mappers slice it by id_source.
    "ref::mnxr_lookup",
    "ref::reference_label_pool",
]

ORFS = "sequences::orfs"
INSERTS = "fabfos::putative_inserts"


def _plan_reads_to_ecspr(work: Path):
    """Resolve reads -> the canonical GPR table in a single plan. Planning only."""
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("sequences.yml", "fabfos.yml", "annotation.yml", "ecspr.yml",
               "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    # one fabfos::experiment groups the whole run; each pool's read_metadata
    # (and thus its reads/assembly) descends from it, so the cross-pool
    # clustering/coverage jobs (group_by=experiment) see all pools at once.
    exp = inputs.AddValue(
        "experiment.txt", "fabfos_demo", "fabfos::experiment"
    )
    for i in range(N_POOLS):
        meta = inputs.AddValue(
            f"read_metadata_{i}.json",
            {"parity": "paired", "length_class": "short"},
            "sequences::read_metadata",
            parents={exp},
        )
        reads = work / f"pool_{i}.fq.gz"
        reads.touch()
        inputs.AddItem(reads, "sequences::short_reads_pe", parents={meta})

    # per-run references, both CHILDREN OF THE EXPERIMENT: the host genome
    # background_filter depletes against, and the pCC1 backbone resolve_inserts
    # blasts against the contigs.
    host = work / "host.fna"
    host.touch()
    inputs.AddItem(host, "sequences::background_genome", parents={exp})
    backbone = work / "pcc1.fna"
    backbone.touch()
    inputs.AddItem(backbone, "fabfos::vector_backbone", parents={exp})

    # type-only stand-ins for every staged reference the annotation half consumes
    for i, dtype in enumerate(STAGED_REFS):
        stub = work / f"ref_{i}.dat"
        stub.touch()
        inputs.AddItem(stub, dtype)

    # the frozen ECSPr reference basis -- shared, so NOT parented to the run
    for i, dtype in enumerate(ECSPR_REFS):
        stub = work / f"ecspr_ref_{i}.parquet"
        stub.touch()
        inputs.AddItem(stub, dtype)

    # the condition set IS per-experiment: what this measurement claims to test
    conditions = work / "conditions.parquet"
    conditions.touch()
    inputs.AddItem(conditions, "ecspr::conditions", parents={exp})
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        # `algorithm::` resolves against the package directory, not a library
        # resource dir -- that is what keeps the module runnable standalone.
        DataInstanceLibrary.Load(ALGO),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / d)
        # the GPR mappers AND the measurement live in the fabfos domain, the run
        # tools in functionalAnnotation, prodigal in metagenomics, and the ORF
        # chunker three of the four annotation lanes now feed from in logistics
        for d in ("assembly", "fabfos", "metagenomics", "functionalAnnotation",
                  "logistics")
    ]

    # the lineage chain is the seam: inserts -> orfs -> gpr table -> measurement.
    # Only `ecspr::results` needs demanding for the chain behind it to be pulled
    # in -- the intermediate targets are kept so a break is localised to the step
    # that broke rather than surfacing as one unsatisfiable plan.
    targets = TargetBuilder()
    ins = targets.Add("fabfos::putative_inserts")
    targets.Add("fabfos::insert_metadata")
    orfs = targets.Add(ORFS, parents={ins})
    gpr = targets.Add("annotation::gpr_table", parents={orfs})
    targets.Add("ecspr::results", parents={gpr})
    # coverage of the inserts, per read set: standalone (consumed by nothing) but
    # pinned to `ins`, so the assembly measured is the recovered insert set and
    # `group_by=meta` gives one job per pool.
    targets.Add("sequences::assembly_stats", parents={ins})

    agent = Agent(home=Source.FromLocal(work / "agent_home"), runtime=Runtime.APPTAINER)
    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos::experiment")),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    return task


def _step_transform_names(task) -> set[str]:
    return {Path(step.transform._path).stem for step in task.plan.steps}


def _producer_map(task) -> dict:
    """instance -> the step that produced it (DataInstance hashes on instance_id)."""
    return {
        inst: step
        for step in task.plan.steps
        for group in step.produces
        for inst in group
    }


def _orf_sources(task) -> dict[str, str]:
    """For every step consuming `sequences::orfs`, what that ORF FASTA was called on.

    Returns {consumer transform stem: the dtype the producing prodigal consumed},
    e.g. {"kofamscan": "sequences::spades_assembly"}. The value is what makes a
    mis-wire visible: it should be `fabfos::putative_inserts` for every consumer.
    """
    produced_by = _producer_map(task)
    sources: dict[str, str] = {}
    for step in task.plan.steps:
        for inst in step.uses:
            if inst.dtype_name != ORFS:
                continue
            caller = produced_by.get(inst)
            if caller is None:
                sources[Path(step.transform._path).stem] = "<supplied as input>"
                continue
            upstream = [
                u.dtype_name
                for u in caller.uses
                if not u.dtype_name.startswith(("env::", "lib::", "ref::"))
            ]
            sources[Path(step.transform._path).stem] = (
                upstream[0] if upstream else "<unknown>"
            )
    return sources


def _render_dag(task, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    task.plan.RenderDAG(out, blacklist_namespaces={"lib", "env"})
    return out


def test_reads_to_ecspr_pipeline_compiles(tmp_path):
    """Reads -> ecspr::results resolves as one plan containing every stage.

    Membership only. This asserts the transforms *compose into a single plan*; it
    deliberately says nothing about whether the annotation lane is attached to the
    right ORFs -- that is `test_annotation_runs_on_the_recovered_inserts` below.
    """
    task = _plan_reads_to_ecspr(tmp_path)

    assert task.ok, f"reads->gpr workflow failed to plan: {task.plan}"

    names = _step_transform_names(task)
    missing = EXPECTED_TRANSFORMS - names
    assert not missing, (
        f"end-to-end plan is missing expected stage(s): {sorted(missing)}; "
        f"plan used: {sorted(names)}"
    )

    svg = _render_dag(task, ARTIFACTS / "reads_to_ecspr_dag.svg")
    assert svg.exists() and svg.stat().st_size > 0, f"DAG SVG not written: {svg}"


def test_annotation_runs_on_the_recovered_inserts(tmp_path):
    """Every consumer of ORFs must be reading ORFs called on the recovered inserts.

    This used to be a strict xfail. The plan resolved, but wrongly: each run tool
    satisfied its unpinned `sequences::orfs` demand from its own extra `prodigal`
    on an extra assembly, while only `gpr_4lane`'s `orfs` edge followed the pin
    off `resolve_inserts` -- so the mapper folded annotations of one ORF set onto
    a different one and every gene-id join would have come back empty.

    The fix was library-side, as predicted, but simpler than expected: each GPR
    mapper now pins ITS OWN lane requirements to its `orfs` requirement
    (`AddRequirement(..., parents={orfs})`). One shared ORF ancestor then
    satisfies the mapper and all four lanes at once, which both wires the lanes
    to the right ORFs and collapses the duplicate prodigals -- the plan went from
    20 steps with 3 prodigals to 14 with 1. Pinning from the *target* was what
    made the planner duplicate `kofamscan`; pinning inside the transform does not.
    """
    task = _plan_reads_to_ecspr(tmp_path)
    assert task.ok, f"reads->gpr workflow failed to plan: {task.plan}"

    sources = _orf_sources(task)
    assert sources, "no step in the plan consumes sequences::orfs"

    wrong = {k: v for k, v in sources.items() if v != INSERTS}
    assert not wrong, (
        "these steps annotate ORFs that were not called on the recovered inserts: "
        f"{wrong} (all ORF sources: {sources})"
    )


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        task = _plan_reads_to_ecspr(Path(td))
        if not task.ok:
            raise SystemExit(f"FAILED to plan: {task.plan}")
        names = _step_transform_names(task)
        print(f"Plan OK -- {len(task.plan.steps)} steps")
        for step in sorted(task.plan.steps, key=lambda s: s.order):
            prods = [i.dtype_name for g in step.produces for i in g]
            print(f"  step {step.order}: {Path(step.transform._path).stem} -> {prods}")
        missing = EXPECTED_TRANSFORMS - names
        print(f"missing stages: {sorted(missing) if missing else 'none'}")
        print("ORF sources per consumer (all should be fabfos::putative_inserts):")
        for consumer, src in sorted(_orf_sources(task).items()):
            flag = "  " if src == INSERTS else "<-- WRONG"
            print(f"  {consumer:<20} <- ORFs called on {src} {flag}")
        svg = _render_dag(task, ARTIFACTS / "reads_to_ecspr_dag.svg")
        print(f"DAG written to: {svg}")
