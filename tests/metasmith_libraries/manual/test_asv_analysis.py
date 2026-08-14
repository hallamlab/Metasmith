"""ASV Amplicon Analysis - maps ASVs to contigs and classifies taxonomy via SILVA."""

import time
from pathlib import Path
from metasmith.python_api import Agent, Runtime
from metasmith.python_api import DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary
from metasmith.python_api import Source
from metasmith.python_api import TargetBuilder, Resources, Size, Duration

WORKSPACE = Path(".").resolve()
MLIB = WORKSPACE.parent  # tests/ is inside MetasmithLibraries/
DATA_DIR = Path("/home/tony/workspace/asv_task")

print(f"WORKSPACE: {WORKSPACE}")
print(f"MLIB: {MLIB}")

# =============================================================================
# Step 1: Deploy an Agent
# =============================================================================
print("\n=== Step 1: Deploy Agent ===")

agent_home = Source.FromLocal(WORKSPACE / "msm_home")
smith = Agent(
    home=agent_home,
    runtime=Runtime.DOCKER,
)

if not (WORKSPACE / "msm_home" / "msm").exists():
    smith.Deploy()
    print("Agent deployed.")
else:
    print("Agent already deployed, reusing.")

# =============================================================================
# Step 2: Register Inputs
# =============================================================================
print("\n=== Step 2: Register Inputs ===")

inputs_path = WORKSPACE / "asv_inputs.xgdb"

try:
    inputs = DataInstanceLibrary.Load(inputs_path)
    print("Loaded cached inputs.")
except:
    inputs = DataInstanceLibrary(inputs_path)
    inputs.Purge()
    inputs.AddTypeLibrary(MLIB / "data_types/amplicon.yml")
    inputs.AddTypeLibrary(MLIB / "data_types/sequences.yml")

    # Register ASV sequences (shared across all samples)
    asv_seqs = inputs.AddItem(
        (DATA_DIR / "ASV_seqs.fasta").resolve(),
        "amplicon::asv_seqs",
    )

    # Register contigs per sample
    for contig_file in sorted(DATA_DIR.glob("contigs/*.fna")):
        inputs.AddItem(
            contig_file.resolve(),
            "sequences::assembly",
            parents={asv_seqs},
        )

    # Register SILVA source marker
    inputs.AddValue("silva_source", "SILVA_138.2_SSURef_NR99", "amplicon::silva_source")

    inputs.Save()
    print("Inputs registered and saved.")

print("Input contents:")
for path, type_name, endpoint in inputs.Iterate():
    print(f"  [{type_name}] {path}")

# =============================================================================
# Step 3: Generate Workflow
# =============================================================================
print("\n=== Step 3: Generate Workflow ===")

resources = [
    DataInstanceLibrary.Load(MLIB / f"resources/{n}")
    for n in ["env"]
] + [
    view
    for view in inputs.AsSamples("amplicon::silva_source")
]

transforms = [
    TransformInstanceLibrary.Load(MLIB / f"transforms/{n}")
    for n in ["logistics", "amplicon"]
]

targets = TargetBuilder()
targets.Add("amplicon::asv_contig_map")
targets.Add("amplicon::asv_taxonomy")

task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("sequences::assembly"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)

print(f"This workflow is called [{task.GetKey()}]")
print(f"Generated plan has [{len(task.plan.steps)}] steps")

workflow_diagram_path = f"{task.GetKey()}.dag.svg"
task.plan.RenderDAG(workflow_diagram_path)
print(f"Workflow diagram saved to: {workflow_diagram_path}")

# =============================================================================
# Step 4: Execute Workflow
# =============================================================================
print("\n=== Step 4: Execute Workflow ===")

smith.StageWorkflow(task, on_exist="update")
print("Workflow staged.")

smith.RunWorkflow(
    task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=dict(
        executor=dict(
            cpus=15,
            queueSize=3,
        ),
        process=dict(
            tries=1,
        ),
    ),
    resource_overrides={
        "*": Resources(
            memory=Size.GB(2),
        ),
    },
)
print("Workflow triggered. Waiting for Nextflow to complete...")

# Poll for completion
results_path = smith.GetResultSource(task).GetPath()
while not (results_path / "_metadata").exists():
    time.sleep(10)
    print("  ... still running")

print("Workflow execution complete.")

smith.CheckWorkflow(task)

# =============================================================================
# Step 5: Receive Outputs
# =============================================================================
print("\n=== Step 5: Receive Outputs ===")

results = DataInstanceLibrary.Load(results_path)

print(f"Results at: {results_path}")
print(f"Report: {results_path / '_metadata/logs.latest/nxf_report.html'}")
print(f"Timeline: {results_path / '_metadata/logs.latest/nxf_timeline.html'}")

print("\nOutput files:")
for path, type_name, endpoint in results.Iterate():
    if path.is_absolute():
        continue  # inputs have absolute paths
    full_path = results_path / path
    print(f"  [{type_name}] {full_path}")
    if full_path.exists() and full_path.stat().st_size < 10000:
        print(f"  --- contents (first 20 lines) ---")
        lines = full_path.read_text().splitlines()
        for line in lines[:20]:
            print(f"    {line}")
        if len(lines) > 20:
            print(f"    ... ({len(lines) - 20} more lines)")
        print(f"  ----------------")
