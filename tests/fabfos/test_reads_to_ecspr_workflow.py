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
ALGO = REPO_ROOT / "src" / "fabfos" / "algorithm"
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

N_POOLS = 2

EXPECTED_RECOVERY = {
    "seqkit_reads",
    "bbduk",
    "background_filter",
    "megahit",
    "spades",
    "assembly_stats",
    "resolve_inserts",
    "prodigal",
}

EXPECTED_ANNOTATION = {
    "kofamscan",
    "clean",
    "diamond_uniref50",
    "proteinbert",
    "gpr_4lane",
}

EXPECTED_MEASUREMENT = {
    "ecspr_measure",
}

EXPECTED_TRANSFORMS = EXPECTED_RECOVERY | EXPECTED_ANNOTATION | EXPECTED_MEASUREMENT

ECSPR_REFS = [
    "ecspr::atom_pairs",
    "ecspr::direction_ratios",
    "ecspr::metabolite_names",
]

STAGED_REFS = [
    "ref::kofamscan_profiles",
    "ref::kofamscan_ko_list",
    "ref::uniref50_diamond_db",
    "ref::esm_c_600m_weights",
    "ref::ezpred_model",
    "ref::mnxr_lookup",
    "ref::label_transfer_landmarks",
]

ORFS = "sequences::orfs"
INSERTS = "fabfos::putative_inserts"


def _plan_reads_to_ecspr(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("sequences.yml", "fabfos.yml", "annotation.yml", "ecspr.yml",
               "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

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

    host = work / "host.fna"
    host.touch()
    inputs.AddItem(host, "sequences::background_genome", parents={exp})
    backbone = work / "pcc1.fna"
    backbone.touch()
    inputs.AddItem(backbone, "fabfos::vector_backbone", parents={exp})

    for i, dtype in enumerate(STAGED_REFS):
        stub = work / f"ref_{i}.dat"
        stub.touch()
        inputs.AddItem(stub, dtype)

    for i, dtype in enumerate(ECSPR_REFS):
        stub = work / f"ecspr_ref_{i}.parquet"
        stub.touch()
        inputs.AddItem(stub, dtype)

    conditions = work / "conditions.parquet"
    conditions.touch()
    inputs.AddItem(conditions, "ecspr::conditions", parents={exp})
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(ALGO),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / d)
        for d in ("assembly", "fabfos", "metagenomics", "functionalAnnotation",
                  "logistics")
    ]

    targets = TargetBuilder()
    ins = targets.Add("fabfos::putative_inserts")
    targets.Add("fabfos::insert_metadata")
    orfs = targets.Add(ORFS, parents={ins})
    gpr = targets.Add("annotation::gpr_table", parents={orfs})
    targets.Add("ecspr::results", parents={gpr})
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
    return {
        inst: step
        for step in task.plan.steps
        for group in step.produces
        for inst in group
    }


def _orf_sources(task) -> dict[str, str]:
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
