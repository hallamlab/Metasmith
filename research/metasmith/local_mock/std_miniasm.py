from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()
base_file = Path().resolve()


path_to_agent_home = Path("./std_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


inputs = DataInstanceLibrary("std_data.xgdb")
inputs.Add(
    items = [
        (base_file / "sample_data/long_reads_subsample.fastq", "subsample.fastq", "std::long_reads"),
    ]
)


task = smith.GenerateWorkflow(
    given      = [containers, inputs],
    transforms = [transforms],
    targets    = [dtypes["long_reads_assembly"].WithLineage([dtypes["miniasm_estimate"]])]
)
smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)
