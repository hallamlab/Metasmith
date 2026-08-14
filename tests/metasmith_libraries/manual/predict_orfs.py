"""Predict ORFs from Ana_PS.fna for annotation testing."""
from pathlib import Path
from metasmith.python_api import *

MLIB = Path(__file__).parent.parent.parent
TEST_DATA = MLIB / "tests/test_data"

# Setup
agent = Agent(home=MLIB / "tests/test_msm_home")
containers = DataInstanceLibrary.Load(MLIB / "resources/env")
transforms = [TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation")]

# Create input with assembly
inputs = DataInstanceLibrary.Create(
    agent.home / "runs" / "orf_prediction" / "inputs",
    types=[MLIB / "data_types/sequences.yml"],
)
inputs.AddItem(TEST_DATA / "Ana_PS.fna", "sequences::assembly")
inputs.LocalizeContents()
inputs.Save()

# Generate workflow
targets = TargetBuilder()
targets.Add("sequences::orfs")

task = agent.GenerateWorkflow(
    samples=list(inputs.AsSamples("sequences::assembly")),
    resources=[containers, inputs],
    transforms=transforms,
    targets=targets,
)

if task.ok:
    agent.StageWorkflow(task, on_exist="clear")
    agent.RunWorkflow(task, config_file=agent.GetNxfConfigPresets()["local"])
    print(f"Workflow running. Check: {agent.home}/runs/{task.run_id}/")
else:
    print(f"Workflow failed: {task}")
