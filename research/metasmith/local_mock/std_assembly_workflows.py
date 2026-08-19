from pathlib import Path
import os
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()

base_file = Path(__file__)
base_dir = base_file.parent/"cache"


path_to_agent_home = (base_dir/"local_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)

inputs = DataInstanceLibrary("std_assembly_data.xgdb")
inputs.Add(
    items = [
        (base_dir / "empty", "emptyshort", "std::short_reads_accession"),
        (base_dir / "empty", "emptylong", "std::long_reads_accession"),
    ]
)

task = smith.GenerateWorkflow(
    given=[containers, inputs],
    transforms=[transforms],
    targets=[
        dtypes["bakta_annotations"].WithLineage([dtypes["hybrid_assembly"]]),
    ],
    max_refine=256,
)
task.plans[0].RenderDAG(base_dir / f"dag_{base_file.stem}", font="IBM Plex Mono", hide_images=True)

for s in task.plans[0].steps:
    print(s.order, s.transform.name)
    for u in s.uses:
        print(f"  {u.dtype.key} {u.dtype_name}")
    print()
