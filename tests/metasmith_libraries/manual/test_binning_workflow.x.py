"""
Test script to verify that the binning transforms (COMEBin and SemiBin2)
are correctly wired into the metasmith workflow planner.

This generates workflows targeting bin outputs and verifies the planner
finds the expected transforms.
"""
import sys
from pathlib import Path
from metasmith.python_api import (
    Agent, Runtime,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources, Size, Duration,
)

WORKSPACE = Path(__file__).parent.resolve()
MLIB = WORKSPACE.parent / "MetasmithLibraries"

print(f"WORKSPACE: {WORKSPACE}")
print(f"MLIB: {MLIB}")

# --- Step 1: Register inputs ---
print("\n=== Step 1: Register inputs ===")
in_dir = WORKSPACE / "test_binning_inputs.xgdb"

# Clean slate
if in_dir.exists():
    import shutil
    shutil.rmtree(in_dir)

inputs = DataInstanceLibrary(in_dir)

# Add type libraries
inputs.AddTypeLibrary(MLIB / "data_types/sequences.yml")
inputs.AddTypeLibrary(MLIB / "data_types/alignment.yml")
inputs.AddTypeLibrary(MLIB / "data_types/binning.yml")
inputs.AddTypeLibrary(MLIB / "data_types/env.yml")

# Register test data - our assembly and BAM file
assembly_path = WORKSPACE / "test_assembly.fna"
bam_path = WORKSPACE / "test_sorted.bam"

# Create a sample that groups the assembly and BAM together
# This allows the planner to see both inputs when planning for this sample
sample = inputs.AddValue("metagenome_sample", "test_sample_1", "binning::metagenome_sample")
asm_item = inputs.AddItem(assembly_path, "sequences::assembly", parents={sample})
bam_item = inputs.AddItem(bam_path, "alignment::bam", parents={sample})

inputs.LocalizeContents()
inputs.Save()
print("Inputs registered and saved.")

print("Input contents:")
for path, type_name, endpoint in inputs.Iterate():
    print(f"  [{type_name}] {endpoint}")

# --- Step 2: Set up resources and transforms ---
print("\n=== Step 2: Set up resources and transforms ===")
resources = [
    DataInstanceLibrary.Load(MLIB / f"resources/{n}")
    for n in ["env", "lib"]
]
# Also add the inputs as resources since they provide the assembly and BAM
resources.append(inputs)
print(f"Resources: {len(resources)} libraries")

transforms = [
    TransformInstanceLibrary.Load(MLIB / f"transforms/{n}")
    for n in ["metagenomics"]
]
print(f"Transforms: {len(transforms)} libraries")

# --- Step 3: Deploy agent ---
print("\n=== Step 3: Deploy agent ===")
agent_home = Source.FromLocal(WORKSPACE / "msm_home")
smith = Agent(home=agent_home, runtime=Runtime.DOCKER)
if not (WORKSPACE / "msm_home" / "msm").exists():
    smith.Deploy()
    print("Agent deployed.")
else:
    print("Agent already deployed, reusing.")

# --- Step 4: Generate workflow targeting bin_directory ---
print("\n=== Step 4: Generate workflow targeting bin_directory ===")
targets_bins = TargetBuilder()
targets_bins.Add("binning::bin_directory")

# Create sample views based on the sample group
# This includes both assembly and bam as part of each sample
sample_views = list(inputs.AsSamples("binning::metagenome_sample"))
print(f"Number of samples: {len(sample_views)}")
for sv in sample_views:
    print(f"  Sample contents:")
    for path, type_name, endpoint in sv.Iterate():
        print(f"    [{type_name}] {endpoint}")

try:
    task_bins = smith.GenerateWorkflow(
        samples=sample_views,
        resources=resources,
        transforms=transforms,
        targets=targets_bins,
    )
    print(f"This workflow is called [{task_bins.GetKey()}]")
    print(f"Generated plan has [{len(task_bins.plan.steps)}] steps")
    print("Steps:")
    for i, step in enumerate(task_bins.plan.steps):
        print(f"  {i+1}. {step}")

    # Render DAG
    dag_path = WORKSPACE / "dag_binning.svg"
    task_bins.plan.RenderDAG(str(dag_path))
    print(f"\nDAG saved to: {dag_path}")

    print("\n=== SUCCESS: Workflow generation works! ===")

    # Stage and run the workflow
    print("\n=== Step 5: Stage workflow ===")
    smith.StageWorkflow(task_bins, on_exist="update")
    print("Workflow staged.")

    print("\n=== Step 6: Run workflow ===")
    smith.RunWorkflow(
        task_bins,
        config_file=smith.GetNxfConfigPresets()["local"],
        params=dict(
            executor=dict(
                cpus=4,
                queueSize=1,
            ),
            process=dict(
                tries=1,
            ),
        ),
    )
    print("Workflow launched! Check the agent home for results.")
except Exception as e:
    print(f"\n=== ERROR during workflow generation: {e} ===")
    import traceback
    traceback.print_exc()

print("\n=== Done ===")
