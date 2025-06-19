from pathlib import Path
from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary

dtypes, containers, transforms = Std()
base_dir = Path.cwd()


path_to_agent_home = Path("./std_home").resolve()
smith = Agent(
    home = Source.FromLocal(path_to_agent_home),
)
smith.Deploy()


inputs = DataInstanceLibrary("std_data.xgdb")
inputs.Add(
    items = [
        (base_dir / "sample_data/empty", "fastq_short", "std::long_reads_filtered"),
    ]
)

task = smith.GenerateWorkflow(
    given = [containers, inputs],
    transforms = [transforms],
    targets = [dtypes["long_reads_assembly"]]
)
smith.StageWorkflow(task, "clear")
smith.RunWorkflow(task)

smith.CheckWorkflow(task)
