#!/usr/bin/env python3
"""
Run the downloadUniRef50DB transform via the metasmith agent,
then copy the resulting .dmnd file to tests/test_data/uniref50/.
"""
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from metasmith.python_api import (
    Agent,
    Runtime,
    DataInstanceLibrary,
    Resources,
    Size,
    Duration,
    Source,
    TargetBuilder,
    TransformInstanceLibrary,
)

TESTS_DIR = Path(__file__).parent.parent
MLIB = TESTS_DIR.parent
TEST_DATA_DIR = TESTS_DIR / "test_data"
TEST_MSM_HOME = TESTS_DIR / "test_msm_home"
OUTPUT_DIR = TEST_DATA_DIR / "uniref50"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set up agent
agent_home = Source.FromLocal(TEST_MSM_HOME)
smith = Agent(home=agent_home, runtime=Runtime.DOCKER)
if not (TEST_MSM_HOME / "msm").exists():
    smith.Deploy()

# Load logistics transforms
logistics_transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

base_resources = [DataInstanceLibrary.Load(MLIB / "resources/env")]

# Generate workflow
import tempfile
tmp_dir = Path(tempfile.mkdtemp())
targets = TargetBuilder()
targets.Add("ref::uniref50_diamond_db")

samples = [{}]
print(f"Samples: {samples}")

task = smith.GenerateWorkflow(
    samples=samples,
    resources=base_resources,
    transforms=logistics_transforms,
    targets=targets,
)
assert task.ok, f"Workflow generation failed: {task}"
print(f"Workflow generated: {len(task.plan.steps)} steps")

smith.StageWorkflow(task, on_exist="clear")
smith.RunWorkflow(
    task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=dict(
        executor=dict(cpus=8, queueSize=1, memory="80 GB"),
        process=dict(tries=1),
    ),
    resource_overrides={
        "*": Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=24)),
    },
)

print("Workflow started, waiting for completion...")
results_path = smith.GetResultSource(task).GetPath()
start = time.time()
while not (results_path / "_metadata").exists():
    elapsed = time.time() - start
    print(f"  waiting... {elapsed:.0f}s", flush=True)
    time.sleep(30)

smith.CheckWorkflow(task)
results = DataInstanceLibrary.Load(results_path)
print(f"Workflow complete. Results at: {results_path}")

# Copy dmnd to test_data/uniref50/
for path, type_name, endpoint in results.Iterate():
    if "uniref50_diamond_db" in type_name:
        if not path.is_absolute():
            full_path = results_path / path
        else:
            full_path = path
        if full_path.exists():
            dest = OUTPUT_DIR / "uniref50.dmnd"
            shutil.copy2(full_path, dest)
            print(f"Copied {full_path} -> {dest}")

print("Done!")
