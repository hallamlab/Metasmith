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
# smith.Deploy()

inputs = DataInstanceLibrary("std_assembly_data.xgdb")
inputs.Add(
    items = [
        (base_dir / "empty", "emptyshort", "std::short_reads_accession"),
        (base_dir / "empty", "emptylong", "std::long_reads_accession"),
        # (base_dir / "empty", "emptyasm", "std::assembly"),
    ]
)

task = smith.GenerateWorkflow(
    given=[containers, inputs],
    transforms=[transforms],
    targets=[
        # dtypes["short_reads_assembly"],
        # dtypes["long_reads_assembly"],
        # dtypes["hybrid_assembly"],
        dtypes["bakta_annotations"].WithLineage([dtypes["hybrid_assembly"]]),
        # dtypes["functional_annotations"].WithLineage([dtypes["long_reads_assembly"]]),
        # dtypes["functional_annotations"].WithLineage([dtypes["hybrid_assembly"]]),
        # dtypes["per_contig_coverage"].WithLineage([dtypes["short_reads_assembly"]]),
    ],
    max_refine=256,
)
task.plans[0].RenderDAG(base_dir / f"dag_{base_file.stem}", font="IBM Plex Mono", hide_images=True)

for s in task.plans[0].steps:
    print(s.order, s.transform.name)
    for u in s.uses:
        print(f"  {u.dtype.key} {u.dtype_name}")
    print()

# output_tests = [
#     "long_reads_assembly",
#     "short_reads_assembly",
#     "hybrid_assembly",
# ]

# print("\n\nRunning workflows...")
# print("====================")
# for dtype in output_tests:
#     target = dtypes[dtype]
#     print(f"\nRunning workflow generation for {dtype}:")

#     task = smith.GenerateWorkflow(
#         given=[containers, inputs],
#         transforms=[transforms],
#         targets=[target]
#     )

#     # Gather transforms and outputs
#     print_outputs = [
#         (step.transform.name, out.dtype_name)
#         for step in task.plan.steps
#         for out in step.produces
#     ]

#     max_transform_width = max(len(transform) for transform, _ in print_outputs)

#     # Print aligned output
#     for transform, output in print_outputs:
#         print(f"    {transform.ljust(max_transform_width)}    ----->    {output}")
