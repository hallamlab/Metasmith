from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()

base_file = Path(__file__)
base_dir = base_file.parent


path_to_agent_home = Path("./std_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


inputs = DataInstanceLibrary("std_assembly_data.xgdb")
inputs.Add(
    items = [
        (base_dir / "sample_data/empty", "emptyshort", "std::short_reads_accession"),
        (base_dir / "sample_data/empty", "emptylong", "std::long_reads_accession"),
    ]
)

output_tests = [
    "long_reads_assembly",
    "short_reads_assembly",
    "hybrid_assembly",
]

print("\n\nRunning workflows...")
print("====================")
for dtype in output_tests:
    target = dtypes[dtype]
    print(f"\nRunning workflow generation for {dtype}:")

    task = smith.GenerateWorkflow(
        given=[containers, inputs],
        transforms=[transforms],
        targets=[target]
    )

    # Gather transforms and outputs
    print_outputs = [
        (step.transform.name, out.dtype_name)
        for step in task.plan.steps
        for out in step.produces
    ]

    max_transform_width = max(len(transform) for transform, _ in print_outputs)

    # Print aligned output
    for transform, output in print_outputs:
        print(f"    {transform.ljust(max_transform_width)}    ----->    {output}")
